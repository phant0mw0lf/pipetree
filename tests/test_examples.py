"""The example pipelines under examples/ stay loadable and keep the same
seven tables (no Spark needed)."""

from pathlib import Path

import pytest

from pipetree.config.loader import load_config
from pipetree.graph.builder import build_graph
from pipetree.model import PipelineConfig

EXAMPLES = Path(__file__).parent.parent / "examples"

BLOG_TABLES = {
    "bronze.customer",
    "bronze.orders",
    "bronze.employee",
    "silver.customer_enriched",
    "silver.orders",
    "gold.dim_customer",
    "gold.fact_sales",
}


@pytest.mark.parametrize("folder", ["", "with_problems"])
def test_example_pipeline_validates_and_has_the_blog_tables(folder: str):
    path = EXAMPLES / folder / "pipeline.yaml"
    raw = load_config(path)
    graph = build_graph(PipelineConfig.from_validated_raw(raw), base_dir=path.parent)
    assert set(graph.tables) == BLOG_TABLES


def test_the_example_pipeline_declares_tags_for_the_quickstart():
    path = EXAMPLES / "pipeline.yaml"
    raw = load_config(path)
    graph = build_graph(PipelineConfig.from_validated_raw(raw), base_dir=path.parent)

    from pipetree.graph.select import count_tags, resolve_run_set

    assert count_tags(graph) == {"daily": 7, "reporting": 2}
    assert resolve_run_set(graph, tags=["reporting"]) == {"gold.dim_customer", "gold.fact_sales"}
    assert resolve_run_set(graph, tags=["daily"], exclude_tags=["reporting"]) == {
        "bronze.customer",
        "bronze.orders",
        "bronze.employee",
        "silver.customer_enriched",
        "silver.orders",
    }
    assert resolve_run_set(graph, tags=["reporting"], with_ancestors=True) == set(graph.tables) - {
        "bronze.employee"
    }
