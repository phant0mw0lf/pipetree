import logging

from pipetree.executor.retry import RetryPolicy, Throttled
from pipetree.executor.runner import run_pipeline

from ..helpers import AlwaysFails, FakeAdapter, Flaky, make_graph

FAST_RETRY = RetryPolicy(base_delay=0.001, max_delay=0.005)


def test_logs_a_message_when_a_table_succeeds(caplog):
    graph = make_graph({"bronze.orders": "scd1"})
    with caplog.at_level(logging.INFO, logger="pipetree.executor"):
        run_pipeline(graph, FakeAdapter(), execution_id=1)

    assert any("bronze.orders" in r.message and "succeeded" in r.message for r in caplog.records)


def test_logs_a_warning_on_each_retry_attempt(caplog):
    graph = make_graph({"bronze.orders": "scd1"})
    adapter = FakeAdapter(
        behaviors={"bronze.orders": Flaky(n_failures=1, exc_factory=lambda: Throttled("429"))}
    )
    with caplog.at_level(logging.WARNING, logger="pipetree.executor"):
        run_pipeline(graph, adapter, execution_id=1, retry_policy=FAST_RETRY)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("bronze.orders" in r.message for r in warnings)


def test_logs_an_error_when_a_table_fails(caplog):
    graph = make_graph({"silver.bad": "replace"})
    adapter = FakeAdapter(
        behaviors={"silver.bad": AlwaysFails(exc_factory=lambda: ValueError("bad schema"))}
    )
    with caplog.at_level(logging.ERROR, logger="pipetree.executor"):
        run_pipeline(graph, adapter, execution_id=1, retry_policy=FAST_RETRY)

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("silver.bad" in r.message and "bad schema" in r.message for r in errors)


def test_logs_a_warning_for_each_upstream_failed_table(caplog):
    graph = make_graph(
        {"bronze.a": "scd1", "silver.b": "replace"}, edges={"silver.b": {"bronze.a"}}
    )
    adapter = FakeAdapter(behaviors={"bronze.a": AlwaysFails(exc_factory=lambda: ValueError("x"))})
    with caplog.at_level(logging.WARNING, logger="pipetree.executor"):
        run_pipeline(graph, adapter, execution_id=1, retry_policy=FAST_RETRY)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("silver.b" in r.message and "upstream_failed" in r.message for r in warnings)
