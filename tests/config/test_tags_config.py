"""`tags:` on a table block: syntax, duplicates, type, and the model field."""

import pytest

from pipetree.config.errors import ConfigError
from pipetree.config.loader import validate_raw
from pipetree.model import PipelineConfig


def raw_with(tags) -> dict:
    table = {
        "source": {"system": "crm", "object": "account"},
        "business_key": ["id"],
        "strategy": "scd1",
    }
    if tags is not ...:
        table["tags"] = tags
    return {"systems": {"crm": {"type": "csv"}}, "bronze": {"tables": {"customer": table}}}


def test_a_table_without_tags_has_an_empty_list():
    raw = raw_with(...)
    validate_raw(raw)

    table = PipelineConfig.from_validated_raw(raw).tables["bronze.customer"]

    assert table.tags == []


def test_tags_are_read_into_the_model_in_order():
    raw = raw_with(["daily", "crm"])
    validate_raw(raw)

    table = PipelineConfig.from_validated_raw(raw).tables["bronze.customer"]

    assert table.tags == ["daily", "crm"]


@pytest.mark.parametrize("tag", ["a", "daily", "crm-eu", "nightly_2", "9lives", "a" * 40])
def test_valid_tag_syntax_is_accepted(tag: str):
    validate_raw(raw_with([tag]))


@pytest.mark.parametrize(
    "tag", ["Daily", "-daily", "_daily", "has space", "dot.ted", "a" * 41, "", "ü", "a,b"]
)
def test_invalid_tag_syntax_is_a_config_error_with_the_key_path(tag: str):
    with pytest.raises(ConfigError) as exc_info:
        validate_raw(raw_with([tag]))

    message = str(exc_info.value)
    assert "bronze.tables.customer.tags[0]" in message
    assert "lowercase" in message


def test_a_duplicate_tag_is_a_config_error():
    with pytest.raises(ConfigError) as exc_info:
        validate_raw(raw_with(["daily", "crm", "daily"]))

    message = str(exc_info.value)
    assert "bronze.tables.customer.tags[2]" in message
    assert "duplicate" in message
    assert "daily" in message


@pytest.mark.parametrize("tags", ["daily", {"daily": True}, 5, None])
def test_tags_must_be_a_list(tags):
    with pytest.raises(ConfigError) as exc_info:
        validate_raw(raw_with(tags))

    assert "bronze.tables.customer.tags" in str(exc_info.value)
    assert "list" in str(exc_info.value)


@pytest.mark.parametrize("tags", [[1], [True], [["a"]], [None]])
def test_a_tag_must_be_a_string(tags):
    with pytest.raises(ConfigError) as exc_info:
        validate_raw(raw_with(tags))

    assert "bronze.tables.customer.tags[0]" in str(exc_info.value)
    assert "string" in str(exc_info.value)


def test_the_tags_field_is_described_for_the_generated_reference():
    from pipetree.model import Table

    assert Table.model_fields["tags"].description
