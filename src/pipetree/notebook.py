"""Watching a run live: the dependency tree, redrawn as tables finish.

Two `RunObserver`s for `run_pipeline(..., observer=...)`:

- `LiveGraphView` - for a notebook cell (Databricks, Fabric, Jupyter).
  Draws the whole tree (all pending) when the run starts, redraws it as
  tables run/finish - at most once per `min_interval_s` - and always once
  more at the end, with the run digest under the graph.
- `HtmlFileObserver` - for anywhere else: atomically rewrites an HTML file
  a browser tab can sit on (it auto-refreshes until the run is over).

Both get every callback on the thread that called `run_pipeline` (the
executor guarantees that), so nothing here needs a lock, and no timer
thread is started: a held-back update is flushed from `on_idle`, which
the scheduler calls while it waits on running tables.

How a notebook redraws, per `detect_backend()`:

- an IPython kernel (Jupyter, VS Code, Fabric, Databricks Runtime 11.3+):
  `display(HTML(...), display_id=True)` once, then `handle.update(...)` -
  the output is replaced in place. If that isn't available (no handle, or
  `update` raises), it falls back to `clear_output(wait=True)` plus a fresh
  display - on Databricks via the notebook's own `displayHTML` - which
  re-renders the cell's output (and clears anything else the cell printed).
- no notebook at all (a script, a job without a kernel, a terminal):
  a plain-text progress line on stdout whenever the counts change.

The rendering and throttling are plain Python - every backend is a small
injectable object, so all of it is tested without a notebook.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Set
from pathlib import Path
from typing import Any, Protocol, TextIO

from pipetree.executor.events import ProgressStatus, TableEvent
from pipetree.executor.status import RunDigest
from pipetree.graph.builder import Graph
from pipetree.graph.html import NodeState, render_html, render_html_page, states_from_digest

__all__ = [
    "ClearOutputBackend",
    "DisplayBackend",
    "HtmlFileObserver",
    "IPythonDisplayBackend",
    "LiveGraphView",
    "TextBackend",
    "detect_backend",
]

_logger = logging.getLogger("pipetree.notebook")


# --- display backends ----------------------------------------------------------


class DisplayBackend(Protocol):
    """Where a `LiveGraphView` puts each redraw. `html` is the full
    rendering, `summary` a one-line plain-text equivalent; `final` marks
    the last update of the run."""

    def update(self, html: str, summary: str, *, final: bool) -> None: ...

    def close(self) -> None: ...


class IPythonDisplayBackend:
    """Update one output in place: `display(HTML(...), display_id=True)`,
    then `handle.update(HTML(...))`. Falls back - for good, after the first
    failure - to `clear_output(wait=True)` + a fresh display.

    `display`, `html` and `clear_output` default to IPython's (imported on
    first use); `show_html`, when given, is used for the fallback's fresh
    display instead of `display(html(...))` - Databricks' `displayHTML`.
    """

    def __init__(
        self,
        *,
        display: Callable[..., Any] | None = None,
        html: Callable[[str], Any] | None = None,
        clear_output: Callable[..., Any] | None = None,
        show_html: Callable[[str], Any] | None = None,
    ) -> None:
        self._display = display
        self._html = html
        self._clear_output = clear_output
        self.show_html = show_html
        self._handle: Any = None
        self._shown = False
        self._fallback = False

    def _ipython(self) -> None:
        if self._display is None or self._html is None or self._clear_output is None:
            from IPython.display import (  # pyright: ignore[reportMissingImports]
                HTML,
                clear_output,
                display,
            )

            self._display = self._display or display
            self._html = self._html or HTML
            self._clear_output = self._clear_output or clear_output

    def update(self, html: str, summary: str, *, final: bool) -> None:
        self._ipython()
        assert self._display is not None and self._html is not None

        if not self._shown:
            self._shown = True
            self._handle = self._display(self._html(html), display_id=True)
            if self._handle is None or not callable(getattr(self._handle, "update", None)):
                self._fallback = True
            return

        if not self._fallback:
            try:
                self._handle.update(self._html(html))
                return
            except Exception:  # noqa: BLE001 - degrade, don't fail the run
                _logger.warning(
                    "in-place display update failed; re-rendering the cell output instead",
                    exc_info=True,
                )
                self._fallback = True

        self._redisplay(html)

    def _redisplay(self, html: str) -> None:
        assert self._clear_output is not None and self._display is not None
        assert self._html is not None
        self._clear_output(wait=True)
        if self.show_html is not None:
            self.show_html(html)
        else:
            self._display(self._html(html))

    def close(self) -> None:
        self._handle = None


class ClearOutputBackend:
    """Re-render the cell's output on every update: `clear_output(wait=True)`
    then `show_html(html)`. Clears everything else the cell printed too, so
    it's the fallback, not the default."""

    def __init__(
        self,
        *,
        show_html: Callable[[str], Any] | None = None,
        clear_output: Callable[..., Any] | None = None,
    ) -> None:
        self._show_html = show_html
        self._clear_output = clear_output

    def update(self, html: str, summary: str, *, final: bool) -> None:
        if self._show_html is None or self._clear_output is None:
            from IPython.display import (  # pyright: ignore[reportMissingImports]
                HTML,
                clear_output,
                display,
            )

            self._clear_output = self._clear_output or clear_output
            self._show_html = self._show_html or (lambda h: display(HTML(h)))
        self._clear_output(wait=True)
        self._show_html(html)

    def close(self) -> None:
        pass


class TextBackend:
    """No notebook: a `[pipetree] ...` line whenever the counts change (the
    elapsed time alone doesn't count as a change), and always the last one."""

    def __init__(self, *, stream: TextIO | None = None) -> None:
        self._stream = stream
        self._last_key: str | None = None

    def update(self, html: str, summary: str, *, final: bool) -> None:
        key = summary.rsplit(" [", 1)[0]
        if key == self._last_key and not final:
            return
        self._last_key = key
        stream = self._stream if self._stream is not None else sys.stdout
        print(f"[pipetree] {summary}", file=stream, flush=True)

    def close(self) -> None:
        pass


def detect_backend() -> DisplayBackend:
    """In-place display in an IPython kernel; text progress anywhere else.

    A kernel is recognised by `get_ipython().kernel` (ipykernel: Jupyter,
    VS Code, Fabric, Databricks Runtime 11.3+). On Databricks the notebook's
    `displayHTML` global is used for the clear-and-redisplay fallback.
    """
    try:
        from IPython import get_ipython  # pyright: ignore[reportMissingImports]
    except ImportError:
        return TextBackend()

    shell = get_ipython()
    if shell is None or getattr(shell, "kernel", None) is None:
        return TextBackend()

    user_ns = getattr(shell, "user_ns", None) or {}
    display_html = user_ns.get("displayHTML")
    return IPythonDisplayBackend(show_html=display_html if callable(display_html) else None)


_NAMED_BACKENDS: dict[str, Callable[[], DisplayBackend]] = {
    "auto": detect_backend,
    "ipython": IPythonDisplayBackend,
    "clear": ClearOutputBackend,
    "text": TextBackend,
}


# --- the shared state machine -----------------------------------------------------


class _ThrottledGraphObserver:
    """Tracks every table's state through a run and publishes a redraw at
    most once per `min_interval_s` - plus always at the start and the end."""

    def __init__(
        self,
        *,
        title: str | None = None,
        min_interval_s: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        theme: str = "auto",
        max_rows: int = 30,
    ) -> None:
        self.title = title
        self.min_interval_s = min_interval_s
        self.theme = theme
        self.max_rows = max_rows
        self._clock = clock
        self.graph: Graph | None = None
        self.execution_id: int | None = None
        self.states: dict[str, NodeState] = {}
        self.digest: RunDigest | None = None
        self._started_at = 0.0
        self._ended_at: float | None = None
        self._last_publish: float | None = None
        self._dirty = False

    # RunObserver -------------------------------------------------------------

    def on_run_start(self, graph: Graph, run_set: Set[str], execution_id: int) -> None:
        self.graph = graph
        self.execution_id = execution_id
        self.states = {}
        self.digest = None
        self._started_at = self._clock()
        self._ended_at = None
        self._publish(final=False)

    def on_table_event(self, event: TableEvent) -> None:
        self.states[event.fqn] = NodeState.from_event(event)
        self._dirty = True
        self._maybe_publish()

    def on_idle(self) -> None:
        self._maybe_publish()

    def on_run_end(self, digest: RunDigest) -> None:
        self.digest = digest
        self.states = states_from_digest(digest)
        self._ended_at = self._clock()
        self._publish(final=True)

    # rendering ----------------------------------------------------------------

    @property
    def elapsed_s(self) -> float:
        end = self._ended_at if self._ended_at is not None else self._clock()
        return max(0.0, end - self._started_at)

    def _render_kwargs(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "title": self.title,
            "elapsed_s": self.elapsed_s,
            "digest": self.digest,
            "theme": self.theme,
            "max_rows": self.max_rows,
        }

    def html(self) -> str:
        """The current picture as an HTML fragment ("" before a run starts)."""
        if self.graph is None:
            return ""
        return render_html(self.graph, self.states, **self._render_kwargs())

    def summary(self) -> str:
        """One plain-text line: how far the run is."""
        if self.graph is None:
            return "no run yet"
        total = len(self.graph.tables)
        counts = Counter(self.states.get(fqn, NodeState()).status for fqn in self.graph.tables)
        done = sum(n for status, n in counts.items() if status.is_final)
        parts = [f"{done}/{total} done"]
        for label, statuses in (
            ("running", (ProgressStatus.RUNNING, ProgressStatus.RETRYING)),
            ("failed", (ProgressStatus.FAILED,)),
            ("upstream_failed", (ProgressStatus.UPSTREAM_FAILED,)),
            ("skipped", (ProgressStatus.SKIPPED,)),
            ("pending", (ProgressStatus.PENDING,)),
        ):
            n = sum(counts[s] for s in statuses)
            if n:
                parts.append(f"{n} {label}")
        noted = sum(bool(self.states.get(fqn, NodeState()).notes) for fqn in self.graph.tables)
        if noted:
            parts.append(f"⚠ {noted} with notes")
        line = " · ".join(parts)
        if self.digest is not None:
            outcome = "SUCCEEDED" if self.digest.succeeded else "FAILED"
            line = f"run {self.digest.execution_id} {outcome}: {line}"
        return f"{line} [{self.elapsed_s:.1f}s]"

    def _maybe_publish(self) -> None:
        if not self._dirty or self.graph is None:
            return
        if (
            self._last_publish is not None
            and self._clock() - self._last_publish < self.min_interval_s
        ):
            return
        self._publish(final=False)

    def _publish(self, *, final: bool) -> None:
        self._last_publish = self._clock()
        self._dirty = False
        self._emit(final=final)

    def _emit(self, *, final: bool) -> None:
        raise NotImplementedError


# --- the two observers ---------------------------------------------------------------


class LiveGraphView(_ThrottledGraphObserver):
    """The dependency tree in a notebook cell, redrawn while the run goes.

        from pipetree import run_pipeline
        from pipetree.notebook import LiveGraphView

        view = LiveGraphView(title="nightly")
        digest = run_pipeline("pipeline.yaml", observer=view)

    `backend` is "auto" (see `detect_backend`), "ipython" (in place),
    "clear" (clear-and-redisplay), "text" (stdout lines) or any object with
    `update(html, summary, *, final)` and `close()`.
    """

    def __init__(
        self,
        *,
        backend: DisplayBackend | str = "auto",
        title: str | None = None,
        min_interval_s: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        theme: str = "auto",
        max_rows: int = 30,
    ) -> None:
        super().__init__(
            title=title, min_interval_s=min_interval_s, clock=clock, theme=theme, max_rows=max_rows
        )
        if isinstance(backend, str):
            if backend not in _NAMED_BACKENDS:
                raise ValueError(
                    f"backend must be one of {sorted(_NAMED_BACKENDS)} or a DisplayBackend, "
                    f"got {backend!r}"
                )
            backend = _NAMED_BACKENDS[backend]()
        self.backend: DisplayBackend = backend
        self.renders = 0

    def _emit(self, *, final: bool) -> None:
        self.renders += 1
        self.backend.update(self.html(), self.summary(), final=final)

    def close(self) -> None:
        self.backend.close()

    def _repr_html_(self) -> str:
        # Evaluating `view` as a cell's last line shows the latest picture.
        return self.html()


class HtmlFileObserver(_ThrottledGraphObserver):
    """Rewrites `path` with the current picture - a complete HTML page -
    on every (throttled) update, atomically (temp file + rename), so a
    browser never reads half a file. While the run is going the page
    carries `<meta http-equiv="refresh" content="{refresh_s}">`, so an
    open tab reloads itself; the final write drops it."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        refresh_s: int = 2,
        title: str | None = None,
        min_interval_s: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        theme: str = "auto",
        max_rows: int = 30,
    ) -> None:
        super().__init__(
            title=title, min_interval_s=min_interval_s, clock=clock, theme=theme, max_rows=max_rows
        )
        self.path = Path(path)
        self.refresh_s = refresh_s
        self.writes = 0

    def page(self, *, final: bool) -> str:
        assert self.graph is not None
        return render_html_page(
            self.graph,
            self.states,
            refresh_s=None if final else self.refresh_s,
            **self._render_kwargs(),
        )

    def _emit(self, *, final: bool) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        content = self.page(final=final)
        fd, tmp = tempfile.mkstemp(
            dir=self.path.parent, prefix=f".{self.path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(content)
            os.chmod(tmp, 0o644)  # mkstemp's 0600 would make the page private to the writer
            os.replace(tmp, self.path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        self.writes += 1

    def close(self) -> None:
        pass
