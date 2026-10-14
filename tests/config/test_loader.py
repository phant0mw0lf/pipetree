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


def test_accepts_snapshot_with_its_options_on_scd2(tmp_path: Path):
    config = (
        MINIMAL_VALID
        + """
      merge:
        snapshot: true
        snapshot_max_missing_ratio: 0.2
        snapshot_allow_empty: true
"""
    )

    raw = load_config(write_config(tmp_path, config))

    merge = raw["bronze"]["tables"]["customer"]["merge"]
    assert merge["snapshot"] is True and merge["snapshot_max_missing_ratio"] == 0.2


def test_rejects_snapshot_on_replace(tmp_path: Path):
    config = MINIMAL_VALID.replace("strategy: scd2", "strategy: replace") + (
        """
      merge:
        snapshot: true
"""
    )

    with pytest.raises(ConfigError) as exc_info:
        load_config(write_config(tmp_path, config))

    assert exc_info.value.path == "bronze.tables.customer.merge.snapshot"
    assert "scd1 and scd2" in exc_info.value.reason


@pytest.mark.parametrize("key", ["snapshot_max_missing_ratio", "snapshot_allow_empty"])
def test_snapshot_options_need_snapshot_true(tmp_path: Path, key: str):
    value = "0.3" if key == "snapshot_max_missing_ratio" else "true"
    config = MINIMAL_VALID + f"\n      merge:\n        {key}: {value}\n"

    with pytest.raises(ConfigError) as exc_info:
        load_config(write_config(tmp_path, config))

    assert exc_info.value.path == f"bronze.tables.customer.merge.{key}"
    assert "snapshot: true" in exc_info.value.reason


@pytest.mark.parametrize("ratio", ["1.5", "-0.1", "high", "true"])
def test_rejects_a_snapshot_ratio_outside_0_to_1(tmp_path: Path, ratio: str):
    config = (
        MINIMAL_VALID
        + f"\n      merge:\n        snapshot: true\n        snapshot_max_missing_ratio: {ratio}\n"
    )

    with pytest.raises(ConfigError) as exc_info:
        load_config(write_config(tmp_path, config))

    assert exc_info.value.path == "bronze.tables.customer.merge.snapshot_max_missing_ratio"


# ------------------------------------------------------------ unknown keys


def _table_config(**table_overrides) -> dict:
    table = {
        "source": {"system": "crm", "object": "account"},
        "business_key": ["accountid"],
        "strategy": "scd2",
        **table_overrides,
    }
    return {"systems": {"crm": {"type": "synapse_link"}}, "bronze": {"tables": {"customer": table}}}


def _reject(raw: dict) -> ConfigError:
    from pipetree.config.loader import validate_raw

    with pytest.raises(ConfigError) as exc_info:
        validate_raw(raw)
    return exc_info.value


def test_rejects_a_typo_in_a_table_key():
    error = _reject(_table_config(merg={"sequence_by": ["versionnumber"]}))

    assert error.path == "bronze.tables.customer.merg"
    assert error.reason == "unknown key 'merg'; did you mean 'merge'?"


def test_a_typo_in_source_is_named_before_the_missing_source():
    raw = _table_config()
    table = raw["bronze"]["tables"]["customer"]
    table["sorce"] = table.pop("source")

    error = _reject(raw)

    assert error.path == "bronze.tables.customer.sorce"
    assert "did you mean 'source'?" in error.reason


def test_rejects_a_typo_in_a_merge_key():
    error = _reject(_table_config(merge={"sequnce_by": ["versionnumber"]}))

    assert error.path == "bronze.tables.customer.merge.sequnce_by"
    assert error.reason == "unknown key 'sequnce_by'; did you mean 'sequence_by'?"


def test_rejects_a_typo_in_a_snapshot_option():
    error = _reject(_table_config(merge={"snapshot": True, "snapshot_alow_empty": True}))

    assert error.path == "bronze.tables.customer.merge.snapshot_alow_empty"
    assert "did you mean 'snapshot_allow_empty'?" in error.reason


def test_rejects_a_typo_in_a_source_key():
    error = _reject(_table_config(source={"system": "crm", "object": "account", "objekt": "x"}))

    assert error.path == "bronze.tables.customer.source.objekt"
    assert "did you mean 'object'?" in error.reason


def test_an_unknown_key_without_a_close_match_lists_the_allowed_keys():
    error = _reject(_table_config(source={"system": "crm", "object": "account", "zzz": 1}))

    assert error.path == "bronze.tables.customer.source.zzz"
    assert error.reason == "unknown key 'zzz'; allowed keys: object, system"


@pytest.mark.parametrize("key", ["name", "layer", "fqn"])
def test_fields_the_loader_sets_are_not_table_keys(key: str):
    error = _reject(_table_config(**{key: "x"}))

    assert error.path == f"bronze.tables.customer.{key}"


def test_systems_stay_free_form():
    from pipetree.config.loader import validate_raw

    raw = _table_config()
    raw["systems"]["crm"].update(landing_path={"secret": "p"}, any_reader_option=1)

    validate_raw(raw)


def test_a_config_using_every_table_key_still_loads():
    from pipetree.config.loader import validate_raw
    from pipetree.model import PipelineConfig

    raw = _table_config(
        table_schema="raw",
        merge={
            "sequence_by": ["versionnumber"],
            "delete_when": "isdelete = true",
            "delete_mode": "soft",
            "ignore_columns": ["modifiedon"],
            "snapshot": True,
            "snapshot_max_missing_ratio": 0.2,
            "snapshot_allow_empty": False,
        },
        depends_on=[],
        unknown_member=True,
        schema_policy="fail",
        surrogate_key="customer_sid",
        tags=["crm"],
    )
    validate_raw(raw)

    table = PipelineConfig.from_validated_raw(raw).tables["raw.customer"]
    assert table.merge.snapshot_max_missing_ratio == 0.2
    assert table.tags == ["crm"]


def test_the_merge_model_forbids_unknown_keys():
    from pydantic import ValidationError

    from pipetree.model import Merge

    with pytest.raises(ValidationError):
        Merge.model_validate({"sequnce_by": ["x"]})


# ------------------------------------------------------------ business_key


@pytest.mark.parametrize("strategy", ["scd1", "scd2"])
@pytest.mark.parametrize("business_key", [None, []])
def test_scd1_and_scd2_require_a_business_key(strategy: str, business_key: list[str] | None):
    raw = _table_config(strategy=strategy)
    table = raw["bronze"]["tables"]["customer"]
    if business_key is None:
        del table["business_key"]
    else:
        table["business_key"] = business_key

    error = _reject(raw)

    assert error.path == "bronze.tables.customer.business_key"
    assert error.reason == "is required for scd1 and scd2"


@pytest.mark.parametrize("strategy", ["replace", "append"])
def test_replace_and_append_need_no_business_key(strategy: str):
    from pipetree.config.loader import validate_raw

    raw = _table_config(strategy=strategy)
    del raw["bronze"]["tables"]["customer"]["business_key"]

    validate_raw(raw)
