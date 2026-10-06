from __future__ import annotations

import io
import sys
import types
from pathlib import Path

import pytest

from pipetree.executor.events import ProgressStatus, TableEvent
from pipetree.executor.retry import RetryPolicy, Throttled
from pipetree.executor.runner import run_pipeline
from pipetree.notebook import (
    ClearOutputBackend,
    HtmlFileObserver,
    IPythonDisplayBackend,
    LiveGraphView,
    TextBackend,
    detect_backend,
)

from .helpers import AlwaysFails, FakeAdapter, Flaky, make_graph


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeBackend:
    def __init__(self) -> None:
        self.updates: list[tuple[str, str, bool]] = []
        self.closed = False

    def update(self, html: str, summary: str, *, final: bool) -> None:
        self.updates.append((html, summary, final))

    def close(self) -> None:
        self.closed = True


def chain_graph():
    return make_graph(
        {"bronze.a": "scd1", "silver.b": "replace", "gold.c": "replace"},
        edges={"silver.b": {"bronze.a"}, "gold.c": {"silver.b"}},
    )


def started_view(clock: FakeClock | None = None) -> tuple[LiveGraphView, FakeBackend, FakeClock]:
    clock = clock or FakeClock()
    backend = FakeBackend()
    view = LiveGraphView(backend=backend, clock=clock, title="t")
    graph = chain_graph()
    view.on_run_start(graph, frozenset(graph.tables), 42)
    return view, backend, clock


def test_run_start_renders_the_whole_graph_pending_immediately():
    view, backend, _ = started_view()

    assert len(backend.updates) == 1
    html, summary, final = backend.updates[0]
    assert not final
    assert html.count('data-status="pending"') == 3
    assert "42" in html
    assert "0/3 done" in summary


def test_updates_are_throttled_to_one_per_interval():
    view, backend, clock = started_view()

    clock.advance(0.2)
    view.on_table_event(TableEvent("bronze.a", ProgressStatus.RUNNING, 1))
    clock.advance(0.2)
    view.on_table_event(TableEvent("bronze.a", ProgressStatus.SUCCEEDED, 1, duration_ms=5))
    assert len(backend.updates) == 1  # still inside the 1s window

    clock.advance(0.7)  # 1.1s since the first render
    view.on_table_event(TableEvent("silver.b", ProgressStatus.RUNNING, 1))
    assert len(backend.updates) == 2
    html = backend.updates[-1][0]
    assert 'data-fqn="bronze.a" data-status="succeeded"' in html
    assert 'data-fqn="silver.b" data-status="running"' in html


def test_idle_flushes_a_held_back_update_but_never_renders_without_changes():
    view, backend, clock = started_view()

    clock.advance(0.3)
    view.on_table_event(TableEvent("bronze.a", ProgressStatus.RUNNING, 1))
    view.on_idle()
    assert len(backend.updates) == 1

    clock.advance(1.0)
    view.on_idle()
    assert len(backend.updates) == 2

    clock.advance(5.0)
    view.on_idle()
    assert len(backend.updates) == 2  # nothing changed since


def test_final_render_always_happens_with_the_digest_summary():
    graph = chain_graph()
    clock = FakeClock()
    backend = FakeBackend()
    view = LiveGraphView(backend=backend, clock=clock)

    digest = run_pipeline(graph, FakeAdapter(), execution_id=7, observer=view)

    html, summary, final = backend.updates[-1]
    assert final
    assert "Run digest" in html
    assert html.count('data-status="succeeded"') == 3
    assert "SUCCEEDED" in summary
    assert view.html() == html
    assert digest.succeeded


def test_final_render_is_not_throttled():
    view, backend, clock = started_view()
    view.on_table_event(TableEvent("bronze.a", ProgressStatus.RUNNING, 1))

    from pipetree.executor.status import RunDigest, TableResult, TableStatus

    digest = RunDigest(
        execution_id=42,
        results={
            fqn: TableResult(fqn, TableStatus.SUCCEEDED, 1, 1, 1, 1)
            for fqn in ("bronze.a", "silver.b", "gold.c")
        },
    )
    view.on_run_end(digest)  # same instant as the start render

    assert len(backend.updates) == 2
    assert backend.updates[-1][2] is True


def test_live_view_over_a_real_threaded_run_with_a_retry_and_a_failure():
    graph = make_graph(
        {"bronze.a": "scd1", "bronze.b": "scd1", "silver.c": "replace", "silver.d": "replace"},
        edges={"silver.c": {"bronze.a"}, "silver.d": {"bronze.b"}},
    )
    adapter = FakeAdapter(
        behaviors={
            "bronze.a": Flaky(n_failures=1, exc_factory=lambda: Throttled("429")),
            "bronze.b": AlwaysFails(lambda: ValueError("<bad>")),
        }
    )
    backend = FakeBackend()
    view = LiveGraphView(backend=backend, min_interval_s=0)

    run_pipeline(
        graph,
        adapter,
        execution_id=1,
        max_workers=4,
        retry_policy=RetryPolicy(base_delay=0.001, max_delay=0.002),
        observer=view,
    )

    final_html = backend.updates[-1][0]
    assert 'data-fqn="bronze.a" data-status="retried_succeeded"' in final_html
    assert 'data-fqn="bronze.b" data-status="failed"' in final_html
    assert 'data-fqn="silver.d" data-status="upstream_failed"' in final_html
    assert "&lt;bad&gt;" in final_html
    seen = {s for html, _, _ in backend.updates for s in ("retrying",) if f'"{s}"' in html}
    assert seen == {"retrying"}  # unthrottled: the retry was visible while it happened


def test_close_closes_the_backend_and_html_is_empty_before_a_run():
    backend = FakeBackend()
    view = LiveGraphView(backend=backend)

    assert view.html() == ""
    view.close()
    assert backend.closed


# --- file view ---------------------------------------------------------------


def test_html_file_observer_refreshes_while_running_and_not_after(tmp_path: Path):
    target = tmp_path / "out" / "run.html"
    clock = FakeClock()
    observer = HtmlFileObserver(target, clock=clock)

    graph = chain_graph()
    observer.on_run_start(graph, frozenset(graph.tables), 3)
    live = target.read_text(encoding="utf-8")
    assert '<meta http-equiv="refresh" content="2">' in live
    assert live.count('data-status="pending"') == 3

    digest = run_pipeline(graph, FakeAdapter(), execution_id=3)
    observer.on_run_end(digest)
    final = target.read_text(encoding="utf-8")
    assert "http-equiv" not in final
    assert "Run digest" in final
    assert sorted(p.name for p in target.parent.iterdir()) == ["run.html"]  # no temp left


def test_html_file_observer_throttles_writes(tmp_path: Path, monkeypatch):
    target = tmp_path / "run.html"
    clock = FakeClock()
    observer = HtmlFileObserver(target, clock=clock)
    writes: list[str] = []
    real_replace = __import__("os").replace

    def counting_replace(src, dst):
        writes.append(str(dst))
        real_replace(src, dst)

    monkeypatch.setattr("pipetree.notebook.os.replace", counting_replace)

    graph = chain_graph()
    observer.on_run_start(graph, frozenset(graph.tables), 3)
    observer.on_table_event(TableEvent("bronze.a", ProgressStatus.RUNNING, 1))
    clock.advance(1.5)
    observer.on_table_event(TableEvent("bronze.a", ProgressStatus.SUCCEEDED, 1))

    assert len(writes) == 2


# --- display backends --------------------------------------------------------


class FakeHandle:
    def __init__(self, fail: bool = False) -> None:
        self.updates: list[object] = []
        self.fail = fail

    def update(self, obj: object) -> None:
        if self.fail:
            raise RuntimeError("no update_display here")
        self.updates.append(obj)


class FakeHTML:
    def __init__(self, data: str) -> None:
        self.data = data


def test_ipython_backend_displays_once_then_updates_in_place():
    shown: list[tuple[object, object]] = []
    handle = FakeHandle()

    def display(obj, display_id=None):
        shown.append((obj, display_id))
        return handle

    backend = IPythonDisplayBackend(display=display, html=FakeHTML, clear_output=lambda **_: None)
    backend.update("<p>1</p>", "s", final=False)
    backend.update("<p>2</p>", "s", final=True)

    assert len(shown) == 1
    assert shown[0][1] is True
    assert [h.data for h in handle.updates] == ["<p>2</p>"]  # type: ignore[attr-defined]


def test_ipython_backend_falls_back_to_clear_and_redisplay_when_update_fails():
    shown: list[str] = []
    cleared: list[bool] = []

    def display(obj, display_id=None):
        shown.append(obj.data)
        return FakeHandle(fail=True)

    backend = IPythonDisplayBackend(
        display=display, html=FakeHTML, clear_output=lambda wait=False: cleared.append(wait)
    )
    backend.update("<p>1</p>", "s", final=False)
    backend.update("<p>2</p>", "s", final=False)
    backend.update("<p>3</p>", "s", final=True)

    assert shown == ["<p>1</p>", "<p>2</p>", "<p>3</p>"]
    assert cleared == [True, True]


def test_ipython_backend_falls_back_when_display_returns_no_handle():
    shown: list[str] = []
    cleared: list[bool] = []
    backend = IPythonDisplayBackend(
        display=lambda obj, display_id=None: shown.append(obj.data),
        html=FakeHTML,
        clear_output=lambda wait=False: cleared.append(wait),
    )
    backend.update("a", "s", final=False)
    backend.update("b", "s", final=True)

    assert shown == ["a", "b"]
    assert cleared == [True]


def test_clear_output_backend_rerenders_the_cell():
    calls: list[str] = []
    backend = ClearOutputBackend(
        show_html=lambda html: calls.append(f"show:{html}"),
        clear_output=lambda wait=False: calls.append(f"clear:{wait}"),
    )
    backend.update("a", "s", final=False)
    backend.update("b", "s", final=True)

    assert calls == ["clear:True", "show:a", "clear:True", "show:b"]


def test_text_backend_prints_only_when_progress_changes_and_always_the_final_line():
    stream = io.StringIO()
    backend = TextBackend(stream=stream)

    backend.update("<x>", "1/3 done · 1 running [1.0s]", final=False)
    backend.update("<x>", "1/3 done · 1 running [2.0s]", final=False)
    backend.update("<x>", "2/3 done · 1 running [3.0s]", final=False)
    backend.update("<x>", "3/3 done [4.0s]", final=True)

    lines = stream.getvalue().splitlines()
    assert lines == [
        "[pipetree] 1/3 done · 1 running [1.0s]",
        "[pipetree] 2/3 done · 1 running [3.0s]",
        "[pipetree] 3/3 done [4.0s]",
    ]


def test_live_view_without_a_notebook_degrades_to_text_progress(monkeypatch, capsys):
    monkeypatch.setitem(sys.modules, "IPython", None)  # import IPython -> ImportError
    view = LiveGraphView(min_interval_s=0)

    run_pipeline(chain_graph(), FakeAdapter(), execution_id=5, observer=view)

    out = capsys.readouterr().out
    assert "[pipetree]" in out
    assert "3/3 done" in out
    assert "SUCCEEDED" in out
    assert "<" not in out


def fake_ipython(shell: object | None) -> types.ModuleType:
    module = types.ModuleType("IPython")
    module.get_ipython = lambda: shell  # type: ignore[attr-defined]
    return module


def test_detect_backend_picks_text_without_ipython_or_without_a_kernel(monkeypatch):
    monkeypatch.setitem(sys.modules, "IPython", None)
    assert isinstance(detect_backend(), TextBackend)

    monkeypatch.setitem(sys.modules, "IPython", fake_ipython(None))
    assert isinstance(detect_backend(), TextBackend)

    terminal = types.SimpleNamespace(user_ns={})  # plain terminal IPython: no kernel
    monkeypatch.setitem(sys.modules, "IPython", fake_ipython(terminal))
    assert isinstance(detect_backend(), TextBackend)


def test_detect_backend_picks_in_place_display_in_a_kernel(monkeypatch):
    jupyter = types.SimpleNamespace(kernel=object(), user_ns={})
    monkeypatch.setitem(sys.modules, "IPython", fake_ipython(jupyter))

    assert isinstance(detect_backend(), IPythonDisplayBackend)


def test_detect_backend_uses_databricks_displayhtml_for_the_fallback(monkeypatch):
    shown: list[str] = []
    databricks = types.SimpleNamespace(
        kernel=object(), user_ns={"displayHTML": lambda html: shown.append(html)}
    )
    monkeypatch.setitem(sys.modules, "IPython", fake_ipython(databricks))

    backend = detect_backend()

    assert isinstance(backend, IPythonDisplayBackend)
    assert backend.show_html is not None
    backend.show_html("<b>x</b>")
    assert shown == ["<b>x</b>"]


def test_named_backends_can_be_forced(monkeypatch):
    assert isinstance(LiveGraphView(backend="text").backend, TextBackend)
    with pytest.raises(ValueError, match="backend"):
        LiveGraphView(backend="hologram")
