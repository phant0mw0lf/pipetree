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


def test_with_ancestors_extends_to_the_full_upstream_closure():
    graph = make_graph(
        {
            "bronze.a": "scd1",
            "silver.b": "replace",
            "gold.c": "replace",
            "gold.d": "replace",
            "bronze.unrelated": "scd1",
        },
        edges={"silver.b": {"bronze.a"}, "gold.c": {"silver.b"}, "gold.d": {"silver.b"}},
    )

    result = resolve_selection(graph, ["gold.c"], with_dependents=False, with_ancestors=True)

    # the parents all the way up; not the sibling gold.d (a child of silver.b, not an ancestor)
    assert result == {"gold.c", "silver.b", "bronze.a"}


def test_with_ancestors_follows_every_parent_of_a_table_with_two_parents():
    graph = make_graph(
        {"bronze.a": "scd1", "bronze.b": "scd1", "silver.x": "replace", "gold.c": "replace"},
        edges={"silver.x": {"bronze.a"}, "gold.c": {"silver.x", "bronze.b"}},
    )

    result = resolve_selection(graph, ["gold.c"], with_dependents=False, with_ancestors=True)

    assert result == {"gold.c", "silver.x", "bronze.a", "bronze.b"}


def test_with_both_flags_every_table_that_runs_has_all_its_ancestors_running():
    graph = make_graph(
        {"bronze.a": "scd1", "silver.b": "replace", "gold.c": "replace", "bronze.other": "scd1"},
        edges={"silver.b": {"bronze.a"}, "gold.c": {"silver.b", "bronze.other"}},
    )

    result = resolve_selection(graph, ["silver.b"], with_dependents=True, with_ancestors=True)

    # gold.c is a dependent; its other parent bronze.other must run too, or gold.c reads nothing
    assert result == {"bronze.a", "silver.b", "gold.c", "bronze.other"}


def test_with_ancestors_of_a_source_table_is_just_itself():
    graph = make_graph(
        {"bronze.a": "scd1", "silver.b": "replace"}, edges={"silver.b": {"bronze.a"}}
    )

    result = resolve_selection(graph, ["bronze.a"], with_dependents=False, with_ancestors=True)

    assert result == {"bronze.a"}
