from pipetree.graph.render import render_graph

from ..helpers import make_graph


def diamond_graph():
    return make_graph(
        {
            "bronze.customer": "scd2",
            "silver.customer_enriched": "replace",
            "bronze.orders": "scd1",
            "silver.orders": "replace",
            "gold.dim_customer": "scd2",
            "gold.fact_sales": "replace",
            "bronze.employee": "scd1",
        },
        edges={
            "silver.customer_enriched": {"bronze.customer"},
            "silver.orders": {"bronze.orders"},
            "gold.dim_customer": {"silver.customer_enriched"},
            "gold.fact_sales": {"gold.dim_customer", "silver.orders"},
        },
    )


def test_render_text_lists_every_table_with_its_dependencies():
    text = render_graph(diamond_graph(), fmt="text")

    assert "bronze.customer" in text
    assert "silver.customer_enriched" in text
    assert "depends on: bronze.customer" in text
    assert "bronze.employee" in text  # isolated table still listed


def test_render_text_lists_a_table_with_no_dependencies_plainly():
    text = render_graph(diamond_graph(), fmt="text")

    lines = text.splitlines()
    employee_line = next(line for line in lines if "bronze.employee" in line)
    assert "depends on" not in employee_line


def test_render_mermaid_starts_with_the_graph_declaration():
    text = render_graph(diamond_graph(), fmt="mermaid")

    assert text.startswith("graph LR")


def test_render_mermaid_includes_an_edge_for_every_dependency():
    text = render_graph(diamond_graph(), fmt="mermaid")

    assert "bronze.customer" in text
    assert "silver.customer_enriched" in text
    assert "-->" in text
    # every edge from the diamond appears in some form
    edge_count = text.count("-->")
    assert edge_count == 5  # matches the 5 edges declared above


def test_render_mermaid_includes_an_isolated_node_with_no_arrow():
    text = render_graph(diamond_graph(), fmt="mermaid")

    lines = [line for line in text.splitlines() if "bronze.employee" in line]
    assert len(lines) == 1
    assert "-->" not in lines[0]


def test_render_graph_rejects_an_unknown_format():
    import pytest

    with pytest.raises(ValueError, match="text|mermaid"):
        render_graph(diamond_graph(), fmt="svg")
