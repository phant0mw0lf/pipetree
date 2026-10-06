from pathlib import Path

import pytest

from pipetree.config.errors import ConfigError
from pipetree.config.loader import load_config

MINIMAL_VALID = """
defaults:
  system_columns: [_inserted_at, _updated_at]

systems:
  crm_dataverse:
    type: synapse_link
    landing_path:
      secret: crm-landing-path

bronze:
  tables:
    customer:
      source:
        system: crm_dataverse
        object: account
      business_key: [accountid]
      strategy: scd2
"""


def write_config(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "pipeline.yaml"
    path.write_text(text)
    return path


def test_loads_a_minimal_valid_config(tmp_path: Path):
    raw = load_config(write_config(tmp_path, MINIMAL_VALID))

    assert raw["systems"]["crm_dataverse"]["type"] == "synapse_link"
    assert raw["bronze"]["tables"]["customer"]["strategy"] == "scd2"


def test_rejects_invalid_yaml_syntax(tmp_path: Path):
    path = write_config(tmp_path, "bronze:\n  tables: [this is not: valid")

    with pytest.raises(ConfigError):
        load_config(path)


def test_rejects_table_with_both_source_and_logic(tmp_path: Path):
    config = (
        MINIMAL_VALID
        + """
      logic: notebooks/bronze/customer.sql
"""
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_config(path)

    assert exc_info.value.path == "bronze.tables.customer"
    assert "source" in exc_info.value.reason
    assert "logic" in exc_info.value.reason


def test_rejects_table_with_neither_source_nor_logic(tmp_path: Path):
    config = """
systems: {}
bronze:
  tables:
    customer:
      business_key: [accountid]
      strategy: scd1
"""
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_config(path)

    assert exc_info.value.path == "bronze.tables.customer"


def test_rejects_unresolved_system_reference(tmp_path: Path):
    config = """
systems: {}
bronze:
  tables:
    customer:
      source:
        system: does_not_exist
        object: account
      business_key: [accountid]
      strategy: scd1
"""
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_config(path)

    assert exc_info.value.path == "bronze.tables.customer.source.system"
    assert "does_not_exist" in exc_info.value.reason


def test_rejects_scd2_combined_with_hard_delete(tmp_path: Path):
    config = (
        MINIMAL_VALID
        + """
      merge:
        delete_mode: hard
"""
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_config(path)

    assert exc_info.value.path == "bronze.tables.customer.merge.delete_mode"
    assert "scd2" in exc_info.value.reason


def test_allows_scd2_with_soft_delete(tmp_path: Path):
    config = (
        MINIMAL_VALID
        + """
      merge:
        delete_mode: soft
"""
    )
    path = write_config(tmp_path, config)

    raw = load_config(path)

    assert raw["bronze"]["tables"]["customer"]["merge"]["delete_mode"] == "soft"


def test_rejects_invalid_delete_mode_value(tmp_path: Path):
    config = (
        MINIMAL_VALID
        + """
      merge:
        delete_mode: purge
"""
    )
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_config(path)

    assert exc_info.value.path == "bronze.tables.customer.merge.delete_mode"
    assert "soft|hard|ignore" in exc_info.value.reason


def test_rejects_invalid_strategy_value(tmp_path: Path):
    config = """
systems:
  crm_dataverse:
    type: synapse_link
bronze:
  tables:
    customer:
      source:
        system: crm_dataverse
        object: account
      business_key: [accountid]
      strategy: full_refresh
"""
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_config(path)

    assert exc_info.value.path == "bronze.tables.customer.strategy"
    assert "scd2|scd1|replace|append" in exc_info.value.reason


def test_rejects_unknown_member_without_business_key(tmp_path: Path):
    config = """
systems:
  crm_dataverse:
    type: synapse_link
gold:
  tables:
    dim_customer:
      logic: notebooks/gold/dim_customer.sql
      strategy: scd2
      unknown_member: true
"""
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_config(path)

    assert exc_info.value.path == "gold.tables.dim_customer.unknown_member"


def test_rejects_invalid_depends_on_value(tmp_path: Path):
    config = """
systems:
  crm_dataverse:
    type: synapse_link
silver:
  tables:
    customer_enriched:
      logic: notebooks/silver/customer_enriched.sql
      business_key: [accountid]
      strategy: replace
      depends_on: yesterday
"""
    path = write_config(tmp_path, config)

    with pytest.raises(ConfigError) as exc_info:
        load_config(path)

    assert exc_info.value.path == "silver.tables.customer_enriched.depends_on"


# ------------------------------------------------------------ surrogate_key


def _sk_config(**table_overrides) -> dict:
    table = {
        "logic": "dim.sql",
        "business_key": ["product_id"],
        "strategy": "scd2",
        "surrogate_key": "product_sid",
        **table_overrides,
    }
    return {"systems": {}, "gold": {"tables": {"dim_product": table}}}


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
def test_accepts_surrogate_key_on_scd1_and_scd2(strategy):
    from pipetree.config.loader import validate_raw
    from pipetree.model import PipelineConfig

    raw = _sk_config(strategy=strategy)
    validate_raw(raw)

    table = PipelineConfig.from_validated_raw(raw).tables["gold.dim_product"]
    assert table.surrogate_key == "product_sid"


def test_surrogate_key_defaults_to_none():
    from pipetree.model import PipelineConfig

    raw = _sk_config()
    del raw["gold"]["tables"]["dim_product"]["surrogate_key"]

    assert PipelineConfig.from_validated_raw(raw).tables["gold.dim_product"].surrogate_key is None


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"strategy": "replace"}, "only valid for scd1/scd2"),
        ({"strategy": "append"}, "only valid for scd1/scd2"),
        ({"business_key": []}, "requires 'business_key'"),
        ({"surrogate_key": "product_id"}, "collides with business key"),
        ({"surrogate_key": "_execution_id"}, "reserved"),
        ({"surrogate_key": "_valid_from"}, "reserved"),
        ({"surrogate_key": "__pipetree_is_delete__"}, "reserved"),
        ({"surrogate_key": ""}, "non-empty column name"),
        ({"surrogate_key": ["a"]}, "non-empty column name"),
    ],
)
def test_rejects_invalid_surrogate_key(overrides, reason):
    from pipetree.config.loader import validate_raw

    with pytest.raises(ConfigError) as exc_info:
        validate_raw(_sk_config(**overrides))

    assert "gold.tables.dim_product.surrogate_key" in str(exc_info.value)
    assert reason in str(exc_info.value)
