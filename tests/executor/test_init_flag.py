from pipetree.executor.runner import run_pipeline

from ..helpers import FakeAdapter, make_graph


def test_init_false_by_default():
    graph = make_graph({"bronze.orders": "scd1"})
    adapter = FakeAdapter()

    run_pipeline(graph, adapter, execution_id=1)

    assert adapter.init_calls == []


def test_init_true_is_passed_to_every_table_run():
    graph = make_graph({"bronze.a": "scd1", "bronze.b": "scd1"})
    adapter = FakeAdapter()

    run_pipeline(graph, adapter, execution_id=1, init=True)

    assert set(adapter.init_calls) == {"bronze.a", "bronze.b"}
