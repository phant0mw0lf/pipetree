from __future__ import annotations

import contextlib
import logging
import threading

from pipetree.executor.events import ProgressStatus, TableEvent
from pipetree.executor.retry import RetryPolicy, Throttled
from pipetree.executor.runner import run_pipeline
from pipetree.executor.status import RunDigest, TableStatus

from ..helpers import AlwaysFails, FakeAdapter, Flaky, make_graph

FAST_RETRY = RetryPolicy(base_delay=0.001, max_delay=0.005)


class RecordingObserver:
    """Records every callback, plus the thread it arrived on."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.threads: set[int] = set()
        self.started: tuple | None = None
        self.digest: RunDigest | None = None
        self._in_call = False
        self.overlapping = False

    def _enter(self) -> None:
        if self._in_call:
            self.overlapping = True
        self._in_call = True
        self.threads.add(threading.get_ident())

    def on_run_start(self, graph, run_set, execution_id) -> None:
        self._enter()
        self.started = (graph, frozenset(run_set), execution_id)
        self.calls.append(("start", execution_id))
        self._in_call = False

    def on_table_event(self, event: TableEvent) -> None:
        self._enter()
        self.calls.append(("event", event))
        self._in_call = False

    def on_run_end(self, digest: RunDigest) -> None:
        self._enter()
        self.digest = digest
        self.calls.append(("end", digest))
        self._in_call = False

    def events(self, fqn: str | None = None) -> list[TableEvent]:
        out = [c[1] for c in self.calls if c[0] == "event"]
        return [e for e in out if fqn is None or e.fqn == fqn]  # type: ignore[union-attr]

    def statuses(self, fqn: str) -> list[ProgressStatus]:
        return [e.status for e in self.events(fqn)]


def chain_graph():
    return make_graph(
        {"bronze.a": "scd1", "silver.b": "replace", "gold.c": "replace"},
        edges={"silver.b": {"bronze.a"}, "gold.c": {"silver.b"}},
    )


def test_run_start_comes_first_and_run_end_last():
    observer = RecordingObserver()

    digest = run_pipeline(chain_graph(), FakeAdapter(), execution_id=7, observer=observer)

    assert observer.calls[0] == ("start", 7)
    assert observer.calls[-1] == ("end", digest)
    assert observer.started is not None
    assert observer.started[1] == {"bronze.a", "silver.b", "gold.c"}


def test_each_table_goes_running_then_succeeded_in_dependency_order():
    observer = RecordingObserver()

    run_pipeline(chain_graph(), FakeAdapter(), execution_id=1, observer=observer)

    for fqn in ("bronze.a", "silver.b", "gold.c"):
        assert observer.statuses(fqn) == [ProgressStatus.RUNNING, ProgressStatus.SUCCEEDED]

    finished = [e.fqn for e in observer.events() if e.status == ProgressStatus.SUCCEEDED]
    assert finished == ["bronze.a", "silver.b", "gold.c"]
    # a child never starts before its parent's success has been reported
    order = [(e.fqn, e.status) for e in observer.events()]
    assert order.index(("bronze.a", ProgressStatus.SUCCEEDED)) < order.index(
        ("silver.b", ProgressStatus.RUNNING)
    )


def test_final_event_carries_timing_and_attempt():
    observer = RecordingObserver()

    digest = run_pipeline(chain_graph(), FakeAdapter(), execution_id=1, observer=observer)

    final = observer.events("bronze.a")[-1]
    result = digest.results["bronze.a"]
    assert final.attempt == 1
    assert final.started_at == result.started_at
    assert final.ended_at == result.ended_at
    assert final.duration_ms == result.duration_ms
    running = observer.events("bronze.a")[0]
    assert running.started_at is not None
    assert running.ended_at is None


def test_retry_emits_retrying_then_running_again_then_retried_succeeded():
    graph = make_graph({"bronze.orders": "scd1"})
    adapter = FakeAdapter(
        behaviors={"bronze.orders": Flaky(n_failures=2, exc_factory=lambda: Throttled("429"))}
    )
    observer = RecordingObserver()

    run_pipeline(graph, adapter, execution_id=1, retry_policy=FAST_RETRY, observer=observer)

    events = observer.events("bronze.orders")
    assert [e.status for e in events] == [
        ProgressStatus.RUNNING,
        ProgressStatus.RETRYING,
        ProgressStatus.RUNNING,
        ProgressStatus.RETRYING,
        ProgressStatus.RUNNING,
        ProgressStatus.RETRIED_SUCCEEDED,
    ]
    assert [e.attempt for e in events] == [1, 1, 2, 2, 3, 3]
    assert events[1].error_type == "Throttled"
    assert events[1].error_message == "429"


def test_failure_emits_failed_and_descendants_upstream_failed():
    graph = chain_graph()
    adapter = FakeAdapter(behaviors={"bronze.a": AlwaysFails(lambda: ValueError("bad <x>"))})
    observer = RecordingObserver()

    run_pipeline(graph, adapter, execution_id=1, observer=observer)

    assert observer.statuses("bronze.a") == [ProgressStatus.RUNNING, ProgressStatus.FAILED]
    failed = observer.events("bronze.a")[-1]
    assert failed.error_type == "ValueError"
    assert failed.error_message == "bad <x>"
    assert observer.statuses("silver.b") == [ProgressStatus.UPSTREAM_FAILED]
    assert observer.statuses("gold.c") == [ProgressStatus.UPSTREAM_FAILED]
    order = [(e.fqn, e.status) for e in observer.events()]
    assert order.index(("bronze.a", ProgressStatus.FAILED)) < order.index(
        ("silver.b", ProgressStatus.UPSTREAM_FAILED)
    )


def test_long_error_messages_are_summarised_on_the_event_but_not_in_the_digest():
    graph = make_graph({"bronze.a": "scd1"})
    long_message = "x" * 5000
    adapter = FakeAdapter(behaviors={"bronze.a": AlwaysFails(lambda: ValueError(long_message))})
    observer = RecordingObserver()

    digest = run_pipeline(graph, adapter, execution_id=1, observer=observer)

    event_message = observer.events("bronze.a")[-1].error_message
    assert event_message is not None
    assert len(event_message) <= 1000
    assert digest.results["bronze.a"].error_message == long_message


def test_tables_outside_the_selection_emit_skipped_right_after_start():
    observer = RecordingObserver()

    run_pipeline(
        chain_graph(), FakeAdapter(), execution_id=1, selected={"bronze.a"}, observer=observer
    )

    assert observer.calls[0][0] == "start"
    assert observer.started is not None
    assert observer.started[1] == {"bronze.a"}
    assert observer.statuses("silver.b") == [ProgressStatus.SKIPPED]
    assert observer.statuses("gold.c") == [ProgressStatus.SKIPPED]
    first_events = {e.fqn for e in observer.events()[:2]}
    assert first_events == {"silver.b", "gold.c"}


def test_observer_calls_are_serialised_on_the_calling_thread_under_a_threaded_run():
    # Wide fan-out: many tables really run concurrently on worker threads.
    strategies = {f"bronze.t{i:02d}": "scd1" for i in range(20)}
    strategies["gold.all"] = "replace"
    edges = {"gold.all": {f"bronze.t{i:02d}" for i in range(20)}}
    graph = make_graph(strategies, edges=edges)  # type: ignore[arg-type]
    barrier = threading.Barrier(4, timeout=5)

    def rendezvous():
        with contextlib.suppress(threading.BrokenBarrierError):
            barrier.wait()
        return {}

    adapter = FakeAdapter(behaviors={fqn: rendezvous for fqn in strategies if fqn != "gold.all"})
    observer = RecordingObserver()

    digest = run_pipeline(graph, adapter, execution_id=1, max_workers=4, observer=observer)

    assert digest.succeeded
    assert observer.threads == {threading.get_ident()}
    assert not observer.overlapping
    assert observer.statuses("gold.all") == [ProgressStatus.RUNNING, ProgressStatus.SUCCEEDED]
    finals = [e for e in observer.events() if e.status == ProgressStatus.SUCCEEDED]
    assert len(finals) == 21
    assert finals[-1].fqn == "gold.all"


def test_several_observers_each_get_every_callback():
    first, second = RecordingObserver(), RecordingObserver()

    run_pipeline(chain_graph(), FakeAdapter(), execution_id=1, observer=[first, second])

    assert [c[0] for c in first.calls] == [c[0] for c in second.calls]
    assert len(first.events()) == 6


class ExplodingObserver(RecordingObserver):
    def __init__(self, fail_on: str) -> None:
        super().__init__()
        self.fail_on = fail_on

    def on_run_start(self, graph, run_set, execution_id) -> None:
        super().on_run_start(graph, run_set, execution_id)
        if self.fail_on == "start":
            raise RuntimeError("observer broke")

    def on_table_event(self, event: TableEvent) -> None:
        super().on_table_event(event)
        if self.fail_on == "event":
            raise RuntimeError("observer broke")

    def on_run_end(self, digest: RunDigest) -> None:
        super().on_run_end(digest)
        if self.fail_on == "end":
            raise RuntimeError("observer broke")


def test_a_raising_observer_never_fails_the_run_and_is_disabled_after_one_warning(caplog):
    broken = ExplodingObserver(fail_on="event")
    healthy = RecordingObserver()

    with caplog.at_level(logging.WARNING, logger="pipetree"):
        digest = run_pipeline(
            chain_graph(), FakeAdapter(), execution_id=1, observer=[broken, healthy]
        )

    assert digest.succeeded
    assert all(r.status == TableStatus.SUCCEEDED for r in digest.results.values())
    # disabled after the first failure: only one event reached it, and no run_end
    assert len(broken.events()) == 1
    assert broken.digest is None
    warnings = [r for r in caplog.records if "observer" in r.getMessage()]
    assert len(warnings) == 1
    # the healthy observer still saw everything
    assert len(healthy.events()) == 6
    assert healthy.digest is digest


def test_an_observer_raising_on_start_or_end_does_not_fail_the_run():
    for where in ("start", "end"):
        broken = ExplodingObserver(fail_on=where)
        digest = run_pipeline(chain_graph(), FakeAdapter(), execution_id=1, observer=broken)
        assert digest.succeeded


class IdleObserver(RecordingObserver):
    def __init__(self) -> None:
        super().__init__()
        self.idle_calls = 0

    def on_idle(self) -> None:
        self._enter()
        self.idle_calls += 1
        self._in_call = False


def test_optional_on_idle_hook_is_called_while_a_slow_table_runs():
    release = threading.Event()

    def slow():
        release.wait(timeout=2)
        return {}

    graph = make_graph({"bronze.slow": "scd1"})
    observer = IdleObserver()
    timer = threading.Timer(0.6, release.set)
    timer.start()
    try:
        run_pipeline(
            graph,
            FakeAdapter(behaviors={"bronze.slow": slow}),
            execution_id=1,
            observer=observer,
            idle_interval_s=0.05,
        )
    finally:
        timer.cancel()

    assert observer.idle_calls >= 2
    assert observer.threads == {threading.get_ident()}


def test_no_observer_still_runs_exactly_as_before():
    digest = run_pipeline(chain_graph(), FakeAdapter(), execution_id=1)

    assert digest.succeeded
