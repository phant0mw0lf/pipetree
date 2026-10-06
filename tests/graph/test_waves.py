from pipetree.graph.waves import compute_waves, layer_of, layer_order, wave_columns

from ..helpers import make_graph


def test_chain_gets_one_wave_per_step():
    graph = make_graph(
        {"bronze.a": "scd1", "silver.b": "replace", "gold.c": "replace"},
        edges={"silver.b": {"bronze.a"}, "gold.c": {"silver.b"}},
    )

    assert compute_waves(graph) == {"bronze.a": 0, "silver.b": 1, "gold.c": 2}


def test_diamond_child_waits_for_its_longest_parent_path():
    # a -> b -> d, a -> d directly: d's wave is set by the longer path.
    graph = make_graph(
        {"bronze.a": "scd1", "silver.b": "replace", "silver.c": "replace", "gold.d": "replace"},
        edges={
            "silver.b": {"bronze.a"},
            "silver.c": {"bronze.a"},
            "gold.d": {"silver.b", "bronze.a"},
        },
    )

    assert compute_waves(graph) == {
        "bronze.a": 0,
        "silver.b": 1,
        "silver.c": 1,
        "gold.d": 2,
    }


def test_wide_fan_out_puts_every_child_in_the_same_wave():
    strategies = {"bronze.root": "scd1"} | {f"silver.c{i}": "replace" for i in range(30)}
    edges = {f"silver.c{i}": {"bronze.root"} for i in range(30)}
    graph = make_graph(strategies, edges=edges)  # type: ignore[arg-type]

    waves = compute_waves(graph)

    assert waves["bronze.root"] == 0
    assert {waves[f"silver.c{i}"] for i in range(30)} == {1}


def test_isolated_tables_are_wave_zero():
    graph = make_graph({"bronze.x": "scd1", "gold.y": "replace"})

    assert compute_waves(graph) == {"bronze.x": 0, "gold.y": 0}


def test_deep_chain_does_not_hit_the_recursion_limit():
    n = 3000
    strategies = {f"l.t{i}": "replace" for i in range(n)}
    edges = {f"l.t{i}": {f"l.t{i - 1}"} for i in range(1, n)}
    graph = make_graph(strategies, edges=edges)  # type: ignore[arg-type]

    assert compute_waves(graph)[f"l.t{n - 1}"] == n - 1


def test_layer_is_the_fqn_prefix_and_layer_order_is_first_appearance():
    graph = make_graph({"silver.b": "replace", "bronze.a": "scd1", "silver.c": "replace"})

    assert layer_of("silver.b") == "silver"
    assert layer_of("plain") == "plain"
    assert layer_order(graph) == ["silver", "bronze"]


def test_wave_columns_group_by_layer_then_name():
    graph = make_graph(
        {
            "bronze.z": "scd1",
            "silver.m": "replace",
            "bronze.a": "scd1",
            "gold.k": "replace",
            "silver.b": "replace",
        },
        edges={"silver.m": {"bronze.z"}, "silver.b": {"bronze.a"}, "gold.k": {"bronze.a"}},
    )

    assert wave_columns(graph) == [["bronze.a", "bronze.z"], ["silver.b", "silver.m", "gold.k"]]
