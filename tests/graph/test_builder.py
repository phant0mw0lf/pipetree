from pathlib import Path

import pytest

from pipetree.graph.builder import build_graph
from pipetree.graph.errors import (
    CycleError,
    DependencyParseError,
    GraphError,
    UnresolvedDependencyError,
)
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
        "MERGE INTO silver.orders USING (SELECT * FROM bronze.orders) s ON true "
        "WHEN MATCHED THEN DELETE"
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


def _auto_config(*, logic: str, tmp_path: Path, extra_layers: dict | None = None) -> PipelineConfig:
    (tmp_path / "gold").mkdir(exist_ok=True)
    (tmp_path / "gold" / "report.sql").write_text(logic)
    return config_with(
        {
            "bronze": {
                "tables": {
                    "orders": {
                        "source": {"system": "crm", "object": "SalesOrderHeader"},
                        "strategy": "scd1",
                    },
                    "customer": {
                        "source": {"system": "crm", "object": "account"},
                        "strategy": "scd1",
                    },
                }
            },
            "silver": {
                "tables": {
                    "orders": {
                        "source": {"system": "crm", "object": "SalesOrderHeader"},
                        "strategy": "scd1",
                    }
                }
            },
            "gold": {
                "tables": {
                    "report": {
                        "logic": "gold/report.sql",
                        "strategy": "replace",
                        "depends_on": "auto",
                    }
                }
            },
            **(extra_layers or {}),
        }
    )


def test_auto_matches_a_catalog_qualified_name_to_schema_table(tmp_path: Path):
    config = _auto_config(logic="SELECT * FROM main.bronze.customer", tmp_path=tmp_path)

    assert build_graph(config, tmp_path).edges["gold.report"] == frozenset({"bronze.customer"})


def test_auto_matches_names_regardless_of_case(tmp_path: Path):
    config = _auto_config(logic="SELECT * FROM `Main`.`Silver`.`Orders`", tmp_path=tmp_path)

    assert build_graph(config, tmp_path).edges["gold.report"] == frozenset({"silver.orders"})


def test_auto_matches_an_unambiguous_bare_name(tmp_path: Path):
    config = _auto_config(logic="SELECT * FROM customer", tmp_path=tmp_path)

    assert build_graph(config, tmp_path).edges["gold.report"] == frozenset({"bronze.customer"})


def test_auto_refuses_a_bare_name_that_several_tables_share(tmp_path: Path):
    config = _auto_config(logic="SELECT * FROM orders", tmp_path=tmp_path)

    with pytest.raises(DependencyParseError) as excinfo:
        build_graph(config, tmp_path)

    message = str(excinfo.value)
    assert "report.sql" in message
    assert "bronze.orders" in message
    assert "silver.orders" in message


def test_auto_ignores_names_that_match_no_table(tmp_path: Path):
    config = _auto_config(
        logic=(
            "SELECT * FROM raw.landing JOIN main.raw.landing2 ON true JOIN bronze.customer ON true"
        ),
        tmp_path=tmp_path,
    )

    assert build_graph(config, tmp_path).edges["gold.report"] == frozenset({"bronze.customer"})


def test_auto_does_not_count_a_cte_or_local_view_as_a_dependency(tmp_path: Path):
    config = _auto_config(
        logic=(
            "CREATE OR REPLACE TEMP VIEW customer AS SELECT * FROM silver.orders;"
            "WITH orders AS (SELECT * FROM bronze.customer) "
            "SELECT * FROM orders JOIN customer ON true"
        ),
        tmp_path=tmp_path,
    )

    assert build_graph(config, tmp_path).edges["gold.report"] == frozenset(
        {"silver.orders", "bronze.customer"}
    )


def test_auto_without_a_logic_file_is_refused(tmp_path: Path):
    config = config_with(
        {
            "bronze": {
                "tables": {
                    "customer": {
                        "source": {"system": "crm", "object": "account"},
                        "strategy": "scd1",
                        "depends_on": "auto",
                    }
                }
            }
        }
    )

    with pytest.raises(GraphError, match="depends_on: auto requires a 'logic' file"):
        build_graph(config, tmp_path)
