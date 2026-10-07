from pipetree.executor.events import ProgressStatus, TableEvent
from pipetree.executor.notes import notes_for
from pipetree.executor.status import TableResult, TableStatus


def test_nothing_to_note_gives_an_empty_list():
    assert notes_for({}) == []
    assert notes_for({"rows_written": 5}) == []


def test_zero_counts_and_empty_changes_produce_nothing():
    assert notes_for({"duplicates_dropped": 0, "null_keys_dropped": 0, "schema_changes": []}) == []
    assert notes_for({"duplicates_dropped": None, "schema_changes": None}) == []


def test_each_kind_has_its_own_wording():
    assert notes_for({"duplicates_dropped": 3}) == ["3 duplicate row(s) dropped"]
    assert notes_for({"null_keys_dropped": 2}) == ["2 NULL-key row(s) dropped"]
    assert notes_for({"schema_changes": ["added: email"]}) == ["schema: added: email"]


def test_order_is_duplicates_then_null_keys_then_each_schema_change():
    details = {
        "schema_changes": ["added: a", "widened: b"],
        "null_keys_dropped": 1,
        "duplicates_dropped": 4,
    }

    assert notes_for(details) == [
        "4 duplicate row(s) dropped",
        "1 NULL-key row(s) dropped",
        "schema: added: a",
        "schema: widened: b",
    ]


def _result(details):
    return TableResult("bronze.a", TableStatus.SUCCEEDED, 1, 1, 2, 5, details=details)


def test_table_result_notes_are_computed_from_details():
    assert _result({"duplicates_dropped": 2}).notes == ["2 duplicate row(s) dropped"]
    assert _result({}).notes == []


def test_table_event_from_result_carries_the_notes():
    event = TableEvent.from_result(_result({"duplicates_dropped": 2, "null_keys_dropped": 1}))

    assert event.status == ProgressStatus.SUCCEEDED
    assert event.notes == ("2 duplicate row(s) dropped", "1 NULL-key row(s) dropped")


def test_table_event_notes_default_to_empty():
    assert TableEvent("bronze.a", ProgressStatus.RUNNING, 1).notes == ()
