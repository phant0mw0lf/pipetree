from __future__ import annotations

import threading

from pipetree.executor.retry import RetryPolicy, Throttled
from pipetree.executor.runner import run_pipeline
from pipetree.executor.status import TableStatus

from ..helpers import AlwaysFails, Capabilities, FakeAdapter, Flaky, make_graph

FAST_RETRY = RetryPolicy(base_delay=0.001, max_delay=0.005)


def test_table_with_no_dependencies_succeeds():
    graph = make_graph({"bronze.orders": "scd1"})
    adapter = FakeAdapter()

    digest = run_pipeline(graph, adapter, execution_id=1)

    assert digest.results["bronze.orders"].status == TableStatus.SUCCEEDED
    assert digest.results["bronze.orders"].attempts == 1
    assert adapter.calls == ["bronze.orders"]


def test_table_waits_for_all_parents_to_succeed_before_starting():
    started_order: list[str] = []
    lock = threading.Lock()

    def record(name: str):
        def behavior():
            with lock:
                started_order.append(name)
            return {}

        return behavior

    graph = make_graph(
        {"bronze.a": "scd1", "silver.b": "replace", "gold.c": "replace"},
        edges={"silver.b": {"bronze.a"}, "gold.c": {"silver.b"}},
    )
    adapter = FakeAdapter(
        behaviors={
            "bronze.a": record("bronze.a"),
            "silver.b": record("silver.b"),
            "gold.c": record("gold.c"),
        }
    )

    digest = run_pipeline(graph, adapter, execution_id=1, max_workers=4)

    assert digest.succeeded
    assert started_order == ["bronze.a", "silver.b", "gold.c"]


def test_independent_tables_run_concurrently_not_sequentially():
    barrier = threading.Barrier(2, timeout=5)

    def wait_for_both():
        barrier.wait()
        return {}

    graph = make_graph({"bronze.a": "scd1", "bronze.b": "scd1"})
    adapter = FakeAdapter(behaviors={"bronze.a": wait_for_both, "bronze.b": wait_for_both})

    digest = run_pipeline(graph, adapter, execution_id=1, max_workers=2)

    assert digest.succeeded


def test_transient_error_is_retried_and_recovers():
    graph = make_graph({"bronze.orders": "scd1"})
    adapter = FakeAdapter(
        behaviors={"bronze.orders": Flaky(n_failures=2, exc_factory=lambda: Throttled("429"))}
    )

    digest = run_pipeline(graph, adapter, execution_id=1, retry_policy=FAST_RETRY)

    result = digest.results["bronze.orders"]
    assert result.status == TableStatus.RETRIED_SUCCEEDED
    assert result.attempts == 3


def test_non_transient_error_fails_without_retrying():
    graph = make_graph({"bronze.orders": "scd1"})
    adapter = FakeAdapter(
        behaviors={"bronze.orders": AlwaysFails(exc_factory=lambda: ValueError("bad schema"))}
    )

    digest = run_pipeline(graph, adapter, execution_id=1, retry_policy=FAST_RETRY)

    result = digest.results["bronze.orders"]
    assert result.status == TableStatus.FAILED
    assert result.attempts == 1
    assert result.error_type == "ValueError"
    assert result.error_message is not None
    assert "bad schema" in result.error_message


def test_transient_error_exhausting_attempt_budget_fails():
    graph = make_graph({"bronze.orders": "scd1"})
    policy = RetryPolicy(max_attempts=3, base_delay=0.001, max_delay=0.005)
    adapter = FakeAdapter(
        behaviors={"bronze.orders": Flaky(n_failures=10, exc_factory=lambda: Throttled("429"))}
    )

    digest = run_pipeline(graph, adapter, execution_id=1, retry_policy=policy)

    result = digest.results["bronze.orders"]
    assert result.status == TableStatus.FAILED
    assert result.attempts == 3


def test_failing_table_marks_descendants_upstream_failed_independent_branch_still_succeeds():
    graph = make_graph(
        {
            "bronze.a": "scd1",
            "silver.b": "replace",
            "gold.c": "replace",
            "bronze.d": "scd1",
        },
        edges={"silver.b": {"bronze.a"}, "gold.c": {"silver.b"}},
    )
    adapter = FakeAdapter(
        behaviors={"bronze.a": AlwaysFails(exc_factory=lambda: ValueError("boom"))}
    )

    digest = run_pipeline(graph, adapter, execution_id=1, retry_policy=FAST_RETRY)

    assert digest.results["bronze.a"].status == TableStatus.FAILED
    assert digest.results["silver.b"].status == TableStatus.UPSTREAM_FAILED
    assert digest.results["gold.c"].status == TableStatus.UPSTREAM_FAILED
    assert digest.results["bronze.d"].status == TableStatus.SUCCEEDED
    assert not digest.succeeded

    # descendants of the failed table must never actually run
    assert "silver.b" not in adapter.calls
    assert "gold.c" not in adapter.calls


def test_append_table_does_not_retry_without_delete_capability():
    graph = make_graph({"silver.appended": "append"})
    adapter = FakeAdapter(
        behaviors={"silver.appended": Flaky(n_failures=1, exc_factory=lambda: Throttled("429"))},
        capabilities=Capabilities(supports_delete_by_execution_id=False),
    )

    digest = run_pipeline(graph, adapter, execution_id=1, retry_policy=FAST_RETRY)

    result = digest.results["silver.appended"]
    assert result.status == TableStatus.FAILED
    assert result.attempts == 1
    assert adapter.deleted_execution_ids == []


def test_append_table_retries_via_delete_by_execution_id_when_supported():
    graph = make_graph({"silver.appended": "append"})
    adapter = FakeAdapter(
        behaviors={"silver.appended": Flaky(n_failures=1, exc_factory=lambda: Throttled("429"))},
        capabilities=Capabilities(supports_delete_by_execution_id=True),
    )

    digest = run_pipeline(graph, adapter, execution_id=42, retry_policy=FAST_RETRY)

    result = digest.results["silver.appended"]
    assert result.status == TableStatus.RETRIED_SUCCEEDED
    assert result.attempts == 2
    assert adapter.deleted_execution_ids == [("silver.appended", 42)]


def test_execution_id_is_generated_when_not_given():
    graph = make_graph({"bronze.orders": "scd1"})
    adapter = FakeAdapter()

    digest = run_pipeline(graph, adapter)

    assert digest.execution_id > 0
    assert str(digest.execution_id) != "1"  # sanity: it's a real generated timestamp, not a stub
