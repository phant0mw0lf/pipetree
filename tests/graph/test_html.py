from __future__ import annotations

import os
import re
from html.parser import HTMLParser
from pathlib import Path

import pytest

from pipetree.executor.events import ProgressStatus, TableEvent
from pipetree.executor.status import RunDigest, TableResult, TableStatus
from pipetree.graph.html import (
    NodeState,
    render_html,
    render_html_page,
    states_from_digest,
)
from pipetree.graph.render import render_graph

from ..helpers import make_graph

SNAPSHOT = Path(__file__).parent / "snapshots" / "small_graph.html"


def small_graph():
    return make_graph(
        {
            "bronze.customer": "scd2",
            "bronze.orders": "scd1",
            "bronze.employee": "scd1",
            "silver.customer_enriched": "replace",
            "silver.orders": "replace",
            "gold.dim_customer": "scd2",
            "gold.fact_sales": "replace",
        },
        edges={
            "silver.customer_enriched": {"bronze.customer"},
            "silver.orders": {"bronze.orders"},
            "gold.dim_customer": {"silver.customer_enriched"},
            "gold.fact_sales": {"gold.dim_customer", "silver.orders"},
        },
    )


def midrun_states() -> dict[str, NodeState]:
    return {
        "bronze.customer": NodeState(ProgressStatus.SUCCEEDED, attempt=1, duration_ms=1200),
        "bronze.orders": NodeState(ProgressStatus.RETRIED_SUCCEEDED, attempt=2, duration_ms=3400),
        "bronze.employee": NodeState(
            ProgressStatus.FAILED,
            attempt=1,
            duration_ms=10,
            error_type="ValueError",
            error_message="boom",
        ),
        "silver.customer_enriched": NodeState(ProgressStatus.RUNNING, attempt=1),
        "silver.orders": NodeState(ProgressStatus.RETRYING, attempt=1),
    }


class _Collector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[tuple[str, dict[str, str | None]]] = []
        self.titles: list[str] = []
        self._in_title = False

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))
        if tag == "title":
            self._in_title = True
            self.titles.append("")

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False

    def handle_data(self, data):
        if self._in_title:
            self.titles[-1] += data


def parse(html: str) -> _Collector:
    collector = _Collector()
    collector.feed(html)
    return collector


def nodes(html: str) -> list[dict[str, str | None]]:
    return [a for t, a in parse(html).tags if t == "g" and "data-fqn" in a]


def edges(html: str) -> list[dict[str, str | None]]:
    return [a for t, a in parse(html).tags if t == "path" and "data-from" in a]


def test_every_table_is_drawn_once_and_every_dependency_is_an_edge():
    graph = small_graph()
    html = render_html(graph)

    drawn = [str(a["data-fqn"]) for a in nodes(html)]
    assert sorted(drawn) == sorted(graph.tables)
    pairs = sorted((str(a["data-from"]), str(a["data-to"])) for a in edges(html))
    expected = sorted((p, c) for c, parents in graph.edges.items() for p in parents)
    assert pairs == expected


def test_nodes_carry_wave_and_layer_and_waves_are_columns_left_to_right():
    html = render_html(small_graph())

    by_fqn = {a["data-fqn"]: a for a in nodes(html)}
    assert by_fqn["bronze.customer"]["data-wave"] == "0"
    assert by_fqn["silver.customer_enriched"]["data-wave"] == "1"
    assert by_fqn["gold.dim_customer"]["data-wave"] == "2"
    assert by_fqn["gold.fact_sales"]["data-wave"] == "3"
    assert by_fqn["gold.fact_sales"]["data-layer"] == "gold"

    def x(fqn: str) -> float:
        transform = by_fqn[fqn]["transform"] or ""
        match = re.match(r"translate\(([\d.]+),([\d.]+)\)", transform)
        assert match, transform
        return float(match.group(1))

    assert x("bronze.customer") < x("silver.orders") < x("gold.dim_customer") < x("gold.fact_sales")


def test_wave_headers_count_their_tables():
    html = render_html(small_graph())

    assert "wave 0 · 3 tables" in html
    assert "wave 1 · 2 tables" in html
    assert "wave 3 · 1 table<" in html


def test_all_pending_when_no_states_given():
    html = render_html(small_graph())

    assert {a["data-status"] for a in nodes(html)} == {"pending"}
    assert "7 pending" in html


def test_states_drive_node_status_and_header_counts():
    html = render_html(small_graph(), midrun_states(), execution_id=260927101500123, elapsed_s=75)

    status = {a["data-fqn"]: a["data-status"] for a in nodes(html)}
    assert status["bronze.customer"] == "succeeded"
    assert status["silver.orders"] == "retrying"
    assert status["gold.fact_sales"] == "pending"
    assert "260927101500123" in html
    assert "1m 15s" in html
    assert "2 succeeded" in html  # succeeded + retried_succeeded
    assert "2 running" in html  # running + retrying
    assert "1 failed" in html
    assert "2 pending" in html


def test_running_nodes_pulse_with_css_only_and_no_scripts_or_external_resources():
    html = render_html(small_graph(), midrun_states())

    assert "@keyframes" in html
    assert "<script" not in html.lower()
    assert "http://" not in html
    assert not re.search(r"https://(?!www\.w3\.org/2000/svg)", html)
    assert "@import" not in html
    assert "url(" not in html


def test_tooltips_carry_full_fqn_status_duration_and_error():
    html = render_html(small_graph(), midrun_states())

    titles = parse(html).titles
    employee = next(t for t in titles if t.startswith("bronze.employee"))
    assert "failed" in employee
    assert "ValueError: boom" in employee
    orders = next(t for t in titles if t.startswith("bronze.orders"))
    assert "3.4s" in orders
    assert "attempt 2" in orders


def test_long_names_are_truncated_in_the_cell_but_whole_in_the_tooltip():
    long = "bronze." + "x" * 80
    graph = make_graph({long: "scd1"})
    html = render_html(graph)

    assert "…" in html
    assert any(t.startswith(long) for t in parse(html).titles)


def test_render_is_deterministic():
    graph = small_graph()

    first = render_html(graph, midrun_states(), execution_id=1, elapsed_s=3.0)
    second = render_html(small_graph(), dict(midrun_states()), execution_id=1, elapsed_s=3.0)

    assert first == second


def test_snapshot_of_a_small_graph():
    html = render_html(
        small_graph(), midrun_states(), execution_id=1, title="snapshot", elapsed_s=3.0
    )
    if os.environ.get("PIPETREE_UPDATE_SNAPSHOTS"):
        SNAPSHOT.parent.mkdir(exist_ok=True)
        SNAPSHOT.write_text(html, encoding="utf-8")
    assert SNAPSHOT.read_text(encoding="utf-8") == html


EVIL = '"><script>alert(1)</script><img src=x onerror=alert(2)>'


def test_fqns_titles_and_error_messages_are_html_escaped_everywhere():
    evil_fqn = f"bronze.t{EVIL}"
    graph = make_graph({evil_fqn: "scd1", "silver.ok": "replace"}, edges={"silver.ok": {evil_fqn}})
    states = {
        evil_fqn: NodeState(
            ProgressStatus.FAILED, attempt=1, error_type=f"E{EVIL}", error_message=EVIL
        )
    }
    digest = RunDigest(
        execution_id=1,
        results={
            evil_fqn: TableResult(
                table_fqn=evil_fqn,
                status=TableStatus.FAILED,
                attempts=1,
                started_at=1,
                ended_at=1,
                duration_ms=1,
                error_type="E",
                error_message=EVIL,
            )
        },
    )

    html = render_html(graph, states, title=EVIL, digest=digest)
    page = render_html_page(graph, states, title=EVIL, refresh_s=2)

    for text in (html, page):
        assert "<script" not in text.lower()
        assert "<img" not in text.lower()
        assert "&lt;script&gt;" in text
    # attribute values stay intact after parsing: the evil fqn round-trips
    assert evil_fqn in [a["data-fqn"] for a in nodes(html)]
    assert any(a["data-from"] == evil_fqn for a in edges(html))


def test_page_is_a_full_document_with_optional_auto_refresh():
    graph = small_graph()

    live = render_html_page(graph, refresh_s=2, title="live")
    final = render_html_page(graph, title="final")

    assert live.startswith("<!DOCTYPE html>")
    assert '<meta http-equiv="refresh" content="2">' in live
    assert "http-equiv" not in final
    assert "<title>final</title>" in final


def test_digest_summary_table_lists_problems_first():
    digest = RunDigest(
        execution_id=9,
        results={
            "bronze.a": TableResult("bronze.a", TableStatus.SUCCEEDED, 1, 1, 1, 5),
            "bronze.b": TableResult(
                "bronze.b", TableStatus.FAILED, 1, 1, 1, 5, error_type="E", error_message="m"
            ),
        },
    )
    graph = make_graph({"bronze.a": "scd1", "bronze.b": "scd1"})

    html = render_html(graph, states_from_digest(digest), digest=digest)

    assert "Run digest" in html
    assert html.index(">bronze.b<") < html.index(">bronze.a<")
    assert "E: m" in html
    assert "FAILED" in html


def test_states_from_digest_and_from_event():
    result = TableResult("bronze.a", TableStatus.RETRIED_SUCCEEDED, 2, 1, 2, 30)
    digest = RunDigest(execution_id=1, results={"bronze.a": result})

    assert states_from_digest(digest)["bronze.a"] == NodeState(
        ProgressStatus.RETRIED_SUCCEEDED, attempt=2, duration_ms=30
    )
    event = TableEvent("bronze.a", ProgressStatus.RUNNING, 3, started_at=1)
    assert NodeState.from_event(event) == NodeState(ProgressStatus.RUNNING, attempt=3)


def test_plain_status_values_are_accepted_as_states():
    html = render_html(small_graph(), {"bronze.customer": ProgressStatus.SUCCEEDED})

    status = {a["data-fqn"]: a["data-status"] for a in nodes(html)}
    assert status["bronze.customer"] == "succeeded"


def test_states_for_unknown_tables_are_ignored():
    html = render_html(small_graph(), {"nope.x": NodeState(ProgressStatus.FAILED)})

    assert "nope.x" not in html


def test_wide_waves_wrap_into_sub_columns():
    strategies = {f"bronze.t{i:03d}": "scd1" for i in range(100)}
    graph = make_graph(strategies)  # type: ignore[arg-type]

    html = render_html(graph, max_rows=30)

    xs = {(a["transform"] or "").split(",")[0] for a in nodes(html)}
    assert len(xs) == 4  # 100 tables, at most 30 rows -> 4 balanced sub-columns
    assert "wave 0 · 100 tables" in html


def test_themes_are_explicit_and_validated():
    graph = small_graph()

    assert 'class="ptg ptg-dark"' in render_html(graph, theme="dark")
    assert 'class="ptg ptg-light"' in render_html(graph, theme="light")
    assert 'class="ptg ptg-auto"' in render_html(graph)
    with pytest.raises(ValueError, match="theme"):
        render_html(graph, theme="neon")


def test_render_graph_supports_html_format():
    assert render_graph(small_graph(), fmt="html").startswith("<!DOCTYPE html>")
