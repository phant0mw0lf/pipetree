from pipetree.executor.retry import RetryPolicy
from pipetree.executor.runner import run_pipeline
from pipetree.executor.status import TableStatus

from ..helpers import AlwaysFails, FakeAdapter, make_graph

FAST_RETRY = RetryPolicy(base_delay=0.001, max_delay=0.005)


def test_no_selection_runs_every_table():
    graph = make_graph({"bronze.a": "scd1", "bronze.b": "scd1"})
    adapter = FakeAdapter()

    digest = run_pipeline(graph, adapter, execution_id=1)

    assert digest.results["bronze.a"].status == TableStatus.SUCCEEDED
    assert digest.results["bronze.b"].status == TableStatus.SUCCEEDED


def test_a_table_outside_the_selection_is_skipped_and_never_run():
    graph = make_graph({"bronze.a": "scd1", "bronze.b": "scd1"})
    adapter = FakeAdapter()

    digest = run_pipeline(graph, adapter, execution_id=1, selected={"bronze.a"})

    assert digest.results["bronze.a"].status == TableStatus.SUCCEEDED
    assert digest.results["bronze.b"].status == TableStatus.SKIPPED
    assert adapter.calls == ["bronze.a"]


def test_a_selected_table_does_not_wait_for_an_unselected_parent():
    # bronze.a is skipped; silver.b is selected on its own and must not
    # block waiting for a parent that will never run in this invocation.
    graph = make_graph(
        {"bronze.a": "scd1", "silver.b": "replace"}, edges={"silver.b": {"bronze.a"}}
    )
    adapter = FakeAdapter()

    digest = run_pipeline(graph, adapter, execution_id=1, selected={"silver.b"})

    assert digest.results["silver.b"].status == TableStatus.SUCCEEDED
    assert digest.results["bronze.a"].status == TableStatus.SKIPPED
    assert adapter.calls == ["silver.b"]


def test_with_dependents_selection_propagates_failure_within_the_subtree():
    graph = make_graph(
        {"bronze.a": "scd1", "silver.b": "replace", "gold.c": "replace", "bronze.other": "scd1"},
        edges={"silver.b": {"bronze.a"}, "gold.c": {"silver.b"}},
    )
    adapter = FakeAdapter(
        behaviors={"bronze.a": AlwaysFails(exc_factory=lambda: ValueError("boom"))}
    )

    digest = run_pipeline(
        graph,
        adapter,
        execution_id=1,
        retry_policy=FAST_RETRY,
        selected={"bronze.a", "silver.b", "gold.c"},
    )

    assert digest.results["bronze.a"].status == TableStatus.FAILED
    assert digest.results["silver.b"].status == TableStatus.UPSTREAM_FAILED
    assert digest.results["gold.c"].status == TableStatus.UPSTREAM_FAILED
    assert digest.results["bronze.other"].status == TableStatus.SKIPPED


def test_skipped_tables_do_not_affect_the_overall_success_of_the_run():
    graph = make_graph({"bronze.a": "scd1", "bronze.b": "scd1"})
    adapter = FakeAdapter()

    digest = run_pipeline(graph, adapter, execution_id=1, selected={"bronze.a"})

    assert digest.succeeded
