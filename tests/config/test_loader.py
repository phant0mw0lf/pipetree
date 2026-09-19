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
