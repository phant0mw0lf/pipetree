from pipetree.config.loader import validate_raw
from pipetree.model import Defaults, Merge, PipelineConfig


def test_defaults_resolves_system_columns_when_absent():
    defaults = Defaults.model_validate({})

    assert defaults.system_columns == [
        "_inserted_at",
        "_updated_at",
        "_is_deleted",
        "_execution_id",
        "_source_system",
    ]


def test_defaults_schema_policy_is_evolve_by_default():
    assert Defaults.model_validate({}).schema_policy == "evolve"


def test_merge_defaults_when_absent():
    merge = Merge.model_validate({})

    assert merge.delete_mode == "soft"
    assert merge.sequence_by == []
    assert merge.ignore_columns == []
    assert merge.delete_when is None


RAW = {
    "defaults": {"system_columns": ["_inserted_at", "_source_system"]},
    "systems": {
        "crm_dataverse": {
            "type": "synapse_link",
            "landing_path": {"secret": "crm-landing-path"},
        }
    },
    "bronze": {
        "tables": {
            "customer": {
                "source": {"system": "crm_dataverse", "object": "account"},
                "business_key": ["accountid"],
                "strategy": "scd2",
                "merge": {
                    "sequence_by": ["versionnumber"],
                    "delete_when": "isdelete = true",
                },
            },
        }
    },
    "silver": {
        "tables": {
            "customer_enriched": {
                "logic": "notebooks/silver/customer_enriched.sql",
                "business_key": ["accountid"],
                "strategy": "replace",
                "depends_on": "auto",
                "table_schema": "silver_custom",
            },
        }
    },
}


def test_builds_pipeline_config_from_validated_raw():
    validate_raw(RAW)  # sanity: fixture is valid per the loader's own rules

    config = PipelineConfig.from_validated_raw(RAW)

    assert set(config.tables) == {"bronze.customer", "silver_custom.customer_enriched"}
    assert config.defaults.system_columns == ["_inserted_at", "_source_system"]
    assert config.systems["crm_dataverse"].type == "synapse_link"


def test_source_table_gets_fqn_from_layer_name_by_default():
    config = PipelineConfig.from_validated_raw(RAW)
    customer = config.tables["bronze.customer"]

    assert customer.name == "customer"
    assert customer.layer == "bronze"
    assert customer.table_schema == "bronze"
    assert customer.fqn == "bronze.customer"
    assert customer.source is not None
    assert customer.source.system == "crm_dataverse"
    assert customer.source.object == "account"
    assert customer.logic is None
    assert customer.merge.sequence_by == ["versionnumber"]
    assert customer.merge.delete_when == "isdelete = true"


def test_table_schema_override_changes_fqn_but_not_layer():
    config = PipelineConfig.from_validated_raw(RAW)
    enriched = config.tables["silver_custom.customer_enriched"]

    assert enriched.layer == "silver"
    assert enriched.table_schema == "silver_custom"
    assert enriched.fqn == "silver_custom.customer_enriched"
    assert enriched.source is None
    assert enriched.logic == "notebooks/silver/customer_enriched.sql"
    assert enriched.depends_on == "auto"


def test_schema_policy_falls_back_to_defaults_when_not_set_per_table():
    config = PipelineConfig.from_validated_raw(RAW)

    assert config.tables["bronze.customer"].schema_policy == "evolve"


def test_schema_policy_can_be_overridden_per_table():
    raw = {
        **RAW,
        "defaults": {**RAW["defaults"], "schema_policy": "evolve"},
        "bronze": {
            "tables": {
                **RAW["bronze"]["tables"],
                "customer": {**RAW["bronze"]["tables"]["customer"], "schema_policy": "fail"},
            }
        },
    }

    config = PipelineConfig.from_validated_raw(raw)

    assert config.tables["bronze.customer"].schema_policy == "fail"
