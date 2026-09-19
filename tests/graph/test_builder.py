from pathlib import Path

import pytest

from pipetree.graph.builder import build_graph
from pipetree.graph.errors import CycleError, UnresolvedDependencyError
from pipetree.model import PipelineConfig

BASE_RAW = {
    "defaults": {},
    "systems": {"crm": {"type": "synapse_link"}},
}


def config_with(layers: dict) -> PipelineConfig:
    return PipelineConfig.from_validated_raw({**BASE_RAW, **layers})


def test_builds_edges_from_explicit_depends_on_list(tmp_path: Path):
    config = config_with(
        {
            "bronze": {
                "tables": {
                    "customer": {
                        "source": {"system": "crm", "object": "account"},
                        "strategy": "scd1",
                    }
                }
            },
            "silver": {
                "tables": {
                    "customer_enriched": {
                        "logic": "silver/customer_enriched.sql",
                        "strategy": "replace",
                        "depends_on": ["customer"],
                    }
                }
            },
        }
    )

    graph = build_graph(config, tmp_path)

    assert graph.edges["silver.customer_enriched"] == frozenset({"bronze.customer"})
    assert graph.order.index("bronze.customer") < graph.order.index("silver.customer_enriched")


def test_builds_edges_from_auto_dependencies_via_logic_file(tmp_path: Path):
    (tmp_path / "silver").mkdir()
    (tmp_path / "silver" / "orders.sql").write_text("SELECT * FROM bronze.orders")

    config = config_with(
        {
            "bronze": {
                "tables": {
                    "orders": {
                        "source": {"system": "crm", "object": "SalesOrderHeader"},
                        "strategy": "scd1",
                    }
                }
            },
            "silver": {
                "tables": {
                    "orders": {
                        "logic": "silver/orders.sql",
                        "strategy": "replace",
                        "depends_on": "auto",
                    }
                }
            },
        }
    )

    graph = build_graph(config, tmp_path)

    assert graph.edges["silver.orders"] == frozenset({"bronze.orders"})


def test_ignores_self_reads_in_auto_dependencies(tmp_path: Path):
    (tmp_path / "silver").mkdir()
    (tmp_path / "silver" / "orders.sql").write_text(
        "MERGE INTO silver.orders USING (SELECT * FROM bronze.orders) s ON true"
    )

    config = config_with(
        {
            "bronze": {
                "tables": {
                    "orders": {
                        "source": {"system": "crm", "object": "SalesOrderHeader"},
                        "strategy": "scd1",
                    }
                }
            },
            "silver": {
                "tables": {
                    "orders": {
                        "logic": "silver/orders.sql",
                        "strategy": "replace",
                        "depends_on": "auto",
                    }
                }
            },
        }
    )

    graph = build_graph(config, tmp_path)

    assert graph.edges["silver.orders"] == frozenset({"bronze.orders"})


def test_resolves_short_name_dependency_by_table_name(tmp_path: Path):
    config = config_with(
        {
            "silver": {
                "tables": {
                    "customer_enriched": {
                        "logic": "silver/customer_enriched.sql",
                        "strategy": "replace",
                    }
                }
            },
            "gold": {
                "tables": {
                    "dim_customer": {
                        "logic": "gold/dim_customer.sql",
                        "strategy": "scd2",
                        "depends_on": ["customer_enriched"],
                    }
                }
            },
        }
    )

    graph = build_graph(config, tmp_path)

    assert graph.edges["gold.dim_customer"] == frozenset({"silver.customer_enriched"})


def test_raises_for_unresolved_explicit_dependency(tmp_path: Path):
    config = config_with(
        {
            "silver": {
                "tables": {
                    "orders": {
                        "logic": "silver/orders.sql",
                        "strategy": "replace",
                        "depends_on": ["does_not_exist"],
                    }
                }
            },
        }
    )

    with pytest.raises(UnresolvedDependencyError) as exc_info:
        build_graph(config, tmp_path)

    assert exc_info.value.table_fqn == "silver.orders"
    assert exc_info.value.dependency_name == "does_not_exist"


def test_raises_for_ambiguous_short_name_dependency(tmp_path: Path):
    config = config_with(
        {
            "silver": {"tables": {"customer": {"logic": "s.sql", "strategy": "replace"}}},
            "gold": {
                "tables": {
                    "customer": {
                        "logic": "g.sql",
                        "strategy": "replace",
                        "depends_on": [],
                    },
                    "fact_sales": {
                        "logic": "f.sql",
                        "strategy": "replace",
                        "depends_on": ["customer"],
                    },
                }
            },
        }
    )

    with pytest.raises(UnresolvedDependencyError):
        build_graph(config, tmp_path)


def test_topological_order_runs_parents_before_children(tmp_path: Path):
    config = config_with(
        {
            "bronze": {
                "tables": {
                    "customer": {"source": {"system": "crm", "object": "a"}, "strategy": "scd1"},
                    "orders": {"source": {"system": "crm", "object": "b"}, "strategy": "scd1"},
                }
            },
            "silver": {
                "tables": {
                    "customer_enriched": {
                        "logic": "x.sql",
                        "strategy": "replace",
                        "depends_on": ["customer"],
                    },
                    "orders": {
                        "logic": "y.sql",
                        "strategy": "replace",
                        "depends_on": ["bronze.orders"],
                    },
                }
            },
            "gold": {
                "tables": {
                    "dim_customer": {
                        "logic": "z.sql",
                        "strategy": "scd2",
                        "depends_on": ["customer_enriched"],
                    },
                    "fact_sales": {
                        "logic": "w.sql",
                        "strategy": "replace",
                        "depends_on": ["dim_customer", "silver.orders"],
                    },
                }
            },
        }
    )

    graph = build_graph(config, tmp_path)
    position = {fqn: i for i, fqn in enumerate(graph.order)}

    assert position["bronze.customer"] < position["silver.customer_enriched"]
    assert position["silver.customer_enriched"] < position["gold.dim_customer"]
    assert position["gold.dim_customer"] < position["gold.fact_sales"]
    assert position["silver.orders"] < position["gold.fact_sales"]
    # independent bronze.orders table has no dependents pulling it anywhere specific,
    # but it must still exist in the order exactly once
    assert graph.order.count("bronze.orders") == 1


def test_raises_cycle_error_with_the_cycle_path(tmp_path: Path):
    config = config_with(
        {
            "gold": {
                "tables": {
                    "a": {"logic": "a.sql", "strategy": "replace", "depends_on": ["b"]},
                    "b": {"logic": "b.sql", "strategy": "replace", "depends_on": ["a"]},
                }
            },
        }
    )

    with pytest.raises(CycleError) as exc_info:
        build_graph(config, tmp_path)

    cycle = exc_info.value.cycle
    assert cycle[0] == cycle[-1]
    assert set(cycle) == {"gold.a", "gold.b"}
