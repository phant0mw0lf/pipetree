"""A table that runs while a direct parent does not gets a note, for every
kind of partial run (select and tags alike)."""

import logging

from pipetree.executor.retry import RetryPolicy
from pipetree.executor.runner import run_pipeline
from pipetree.executor.status import TableStatus
from pipetree.runlog.notifier import format_digest

from ..helpers import AlwaysFails, FakeAdapter, make_graph


def graph():
    return make_graph(
        {"bronze.a": "scd1", "bronze.x": "scd1", "silver.b": "replace"},
        edges={"silver.b": {"bronze.a", "bronze.x"}},
    )


def test_a_table_reading_parents_outside_the_run_gets_one_note_per_parent():
    digest = run_pipeline(graph(), FakeAdapter(), execution_id=1, selected={"silver.b"})

    assert digest.results["silver.b"].notes == [
        "input not refreshed: bronze.a",
        "input not refreshed: bronze.x",
    ]
    assert digest.results["silver.b"].status == TableStatus.SUCCEEDED
    assert digest.exit_code == 0


def test_only_the_parents_that_are_left_out_are_noted():
    digest = run_pipeline(graph(), FakeAdapter(), execution_id=1, selected={"bronze.a", "silver.b"})

    assert digest.results["silver.b"].notes == ["input not refreshed: bronze.x"]
    assert digest.results["bronze.a"].notes == []


def test_a_full_run_has_no_such_notes():
    digest = run_pipeline(graph(), FakeAdapter(), execution_id=1)

    assert all(result.notes == [] for result in digest.results.values())


def test_tables_outside_the_run_carry_no_note():
    digest = run_pipeline(graph(), FakeAdapter(), execution_id=1, selected={"silver.b"})

    assert digest.results["bronze.a"].status == TableStatus.SKIPPED
    assert digest.results["bronze.a"].notes == []


def test_a_warning_per_table_at_the_start_lists_the_parents(caplog):
    with caplog.at_level(logging.WARNING, logger="pipetree.executor"):
        run_pipeline(graph(), FakeAdapter(), execution_id=1, selected={"silver.b"})

    messages = [r.getMessage() for r in caplog.records if "input not refreshed" in r.getMessage()]
    assert messages == ["silver.b: input not refreshed: bronze.a, bronze.x"]


def test_no_warning_for_a_full_run(caplog):
    with caplog.at_level(logging.WARNING, logger="pipetree.executor"):
        run_pipeline(graph(), FakeAdapter(), execution_id=1)

    assert not [r for r in caplog.records if "input not refreshed" in r.getMessage()]


def test_the_console_digest_line_shows_the_note():
    digest = run_pipeline(graph(), FakeAdapter(), execution_id=1, selected={"silver.b"})

    line = next(ln for ln in format_digest(digest).splitlines() if "silver.b" in ln)
    assert "notes: input not refreshed: bronze.a; input not refreshed: bronze.x" in line


def test_the_note_survives_a_failure_of_the_table_itself():
    fast = RetryPolicy(base_delay=0.001, max_delay=0.005)
    adapter = FakeAdapter(behaviors={"silver.b": AlwaysFails(exc_factory=lambda: ValueError("x"))})

    digest = run_pipeline(
        graph(), adapter, execution_id=1, selected={"silver.b"}, retry_policy=fast
    )

    assert digest.results["silver.b"].status == TableStatus.FAILED
    assert "input not refreshed: bronze.a" in digest.results["silver.b"].notes


def test_the_event_for_the_finished_table_carries_the_note():
    seen = []

    class Watcher:
        def on_run_start(self, graph, run_set, execution_id): ...
        def on_table_event(self, event):
            seen.append(event)

        def on_run_end(self, digest): ...

    run_pipeline(graph(), FakeAdapter(), execution_id=1, selected={"silver.b"}, observer=Watcher())

    final = [e for e in seen if e.fqn == "silver.b" and e.status.is_final]
    assert final and final[0].notes[0] == "input not refreshed: bronze.a"
