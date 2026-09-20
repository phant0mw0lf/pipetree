import pytest

from pipetree.graph.errors import UnknownTableError
from pipetree.graph.select import resolve_selection

from ..helpers import make_graph


def test_resolve_selection_returns_none_when_nothing_is_selected():
    graph = make_graph({"bronze.orders": "scd1"})

    assert resolve_selection(graph, None, with_dependents=False) is None


def test_resolve_selection_returns_the_selected_fqns_by_exact_match():
    graph = make_graph({"bronze.orders": "scd1", "silver.orders": "replace"})

    result = resolve_selection(graph, ["bronze.orders"], with_dependents=False)

    assert result == {"bronze.orders"}


def test_resolve_selection_matches_an_unambiguous_bare_table_name():
    graph = make_graph({"bronze.orders": "scd1", "silver.customer": "replace"})

    result = resolve_selection(graph, ["orders"], with_dependents=False)

    assert result == {"bronze.orders"}


def test_resolve_selection_raises_for_an_unknown_table():
    graph = make_graph({"bronze.orders": "scd1"})

    with pytest.raises(UnknownTableError) as exc_info:
        resolve_selection(graph, ["does_not_exist"], with_dependents=False)

    assert exc_info.value.name == "does_not_exist"


def test_resolve_selection_raises_for_an_ambiguous_bare_name():
    graph = make_graph({"bronze.orders": "scd1", "silver.orders": "replace"})

    with pytest.raises(UnknownTableError):
        resolve_selection(graph, ["orders"], with_dependents=False)


def test_with_dependents_extends_to_the_full_descendant_closure():
    graph = make_graph(
        {
            "bronze.a": "scd1",
            "silver.b": "replace",
            "gold.c": "replace",
            "bronze.unrelated": "scd1",
        },
        edges={"silver.b": {"bronze.a"}, "gold.c": {"silver.b"}},
    )

    result = resolve_selection(graph, ["bronze.a"], with_dependents=True)

    assert result == {"bronze.a", "silver.b", "gold.c"}


def test_without_dependents_selection_is_exactly_the_given_tables():
    graph = make_graph(
        {"bronze.a": "scd1", "silver.b": "replace"}, edges={"silver.b": {"bronze.a"}}
    )

    result = resolve_selection(graph, ["bronze.a"], with_dependents=False)

    assert result == {"bronze.a"}


def test_with_dependents_supports_multiple_seeds():
    graph = make_graph(
        {"bronze.a": "scd1", "bronze.b": "scd1", "silver.x": "replace", "silver.y": "replace"},
        edges={"silver.x": {"bronze.a"}, "silver.y": {"bronze.b"}},
    )

    result = resolve_selection(graph, ["bronze.a", "bronze.b"], with_dependents=True)

    assert result == {"bronze.a", "bronze.b", "silver.x", "silver.y"}
