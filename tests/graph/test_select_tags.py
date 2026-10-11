"""Tag selection as pure functions: every way tags combine with the
existing select / dependents / ancestors options."""

import pytest

from pipetree.graph.errors import (
    EmptySelectionError,
    GraphError,
    UnknownTableError,
    UnknownTagError,
)
from pipetree.graph.select import (
    count_tags,
    inputs_not_refreshed,
    resolve_run_set,
    split_names,
)

from ..helpers import make_graph


def pipeline():
    #   bronze.a -> silver.b -> gold.c
    #   bronze.x -> silver.b           (silver.b has two parents)
    #   bronze.lone                    (unconnected)
    return make_graph(
        {
            "bronze.a": "scd1",
            "bronze.x": "scd1",
            "silver.b": "replace",
            "gold.c": "replace",
            "bronze.lone": "scd1",
        },
        edges={"silver.b": {"bronze.a", "bronze.x"}, "gold.c": {"silver.b"}},
        tags={
            "bronze.a": ["daily", "crm"],
            "bronze.x": ["hourly"],
            "silver.b": ["daily"],
            "gold.c": ["reporting"],
            "bronze.lone": ["daily", "hourly"],
        },
    )


def run_set(**kwargs):
    return resolve_run_set(pipeline(), **kwargs)


def test_no_filter_at_all_means_everything():
    assert run_set() is None
    assert run_set(with_dependents=True) is None


def test_one_tag_selects_the_tables_carrying_it():
    assert run_set(tags=["daily"]) == {"bronze.a", "silver.b", "bronze.lone"}


def test_several_tags_are_an_or():
    assert run_set(tags=["crm", "reporting"]) == {"bronze.a", "gold.c"}


def test_tag_and_select_intersect():
    assert run_set(tags=["daily"], select=["bronze.a", "gold.c", "bronze.x"]) == {"bronze.a"}


def test_tag_and_select_without_overlap_is_an_empty_selection_error():
    with pytest.raises(EmptySelectionError):
        run_set(tags=["reporting"], select=["bronze.a"])


def test_tag_with_dependents_extends_downstream():
    assert run_set(tags=["crm"], with_dependents=True) == {"bronze.a", "silver.b", "gold.c"}


def test_tag_with_ancestors_extends_upstream():
    assert run_set(tags=["reporting"], with_ancestors=True) == {
        "gold.c",
        "silver.b",
        "bronze.a",
        "bronze.x",
    }


def test_tag_with_dependents_and_ancestors_pulls_in_the_other_parents_too():
    assert run_set(tags=["crm"], with_dependents=True, with_ancestors=True) == {
        "bronze.a",
        "bronze.x",
        "silver.b",
        "gold.c",
    }


def test_select_intersects_before_the_closure_extends():
    result = run_set(tags=["daily"], select=["bronze.a"], with_dependents=True)

    assert result == {"bronze.a", "silver.b", "gold.c"}


def test_exclude_tag_alone_runs_everything_else():
    assert run_set(exclude_tags=["hourly"]) == {"bronze.a", "silver.b", "gold.c"}


def test_exclude_tag_removes_from_a_tag_selection():
    assert run_set(tags=["daily"], exclude_tags=["hourly"]) == {"bronze.a", "silver.b"}


def test_exclude_wins_over_a_closure_that_pulled_the_table_in():
    result = run_set(tags=["crm"], with_dependents=True, exclude_tags=["reporting"])

    assert result == {"bronze.a", "silver.b"}


def test_exclude_wins_over_ancestors_too():
    result = run_set(tags=["reporting"], with_ancestors=True, exclude_tags=["hourly"])

    assert result == {"gold.c", "silver.b", "bronze.a"}


def test_exclude_wins_over_an_explicit_select():
    assert run_set(select=["bronze.a", "gold.c"], exclude_tags=["crm"]) == {"gold.c"}


def test_an_unknown_tag_is_an_error_that_lists_the_known_tags():
    with pytest.raises(UnknownTagError) as exc_info:
        run_set(tags=["dayly"])

    message = str(exc_info.value)
    assert "dayly" in message
    assert "crm, daily, hourly, reporting" in message
    assert exc_info.value.name == "dayly"
    assert isinstance(exc_info.value, GraphError)


def test_an_unknown_exclude_tag_is_an_error_too():
    with pytest.raises(UnknownTagError):
        run_set(exclude_tags=["nope"])


def test_an_unknown_tag_in_a_pipeline_without_tags_says_there_are_none():
    graph = make_graph({"bronze.a": "scd1"})

    with pytest.raises(UnknownTagError, match="no table has tags"):
        resolve_run_set(graph, tags=["daily"])


def test_excluding_everything_is_an_empty_selection_error():
    with pytest.raises(EmptySelectionError, match="no table"):
        run_set(tags=["crm"], exclude_tags=["crm"])


def test_an_explicitly_empty_select_is_an_empty_selection_error():
    with pytest.raises(EmptySelectionError):
        run_set(select=[])


def test_unknown_table_in_select_still_raises_unknown_table():
    with pytest.raises(UnknownTableError):
        run_set(tags=["daily"], select=["nope"])


def test_count_tags_counts_tables_per_tag_sorted_by_name():
    assert count_tags(pipeline()) == {"crm": 1, "daily": 3, "hourly": 2, "reporting": 1}


def test_split_names_trims_and_tolerates_commas_and_blanks():
    assert split_names("daily, crm") == ["daily", "crm"]
    assert split_names([" daily ,crm", "", "hourly,,"]) == ["daily", "crm", "hourly"]
    assert split_names("") == []
    assert split_names(None) == []
    assert split_names(" , ") == []


def test_split_names_removes_duplicates_keeping_order():
    assert split_names("b, a, b") == ["b", "a"]


def test_inputs_not_refreshed_lists_parents_outside_the_run_per_table():
    graph = pipeline()

    result = inputs_not_refreshed(graph, {"bronze.a", "silver.b", "gold.c"})

    assert result == {"silver.b": ["bronze.x"]}


def test_inputs_not_refreshed_is_empty_for_a_full_run_or_a_closed_subtree():
    graph = pipeline()

    assert inputs_not_refreshed(graph, set(graph.tables)) == {}
    assert inputs_not_refreshed(graph, {"bronze.a"}) == {}


def test_inputs_not_refreshed_for_a_selection_of_a_child_only():
    assert inputs_not_refreshed(pipeline(), {"gold.c"}) == {"gold.c": ["silver.b"]}
