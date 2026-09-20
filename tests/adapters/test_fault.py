import pytest

from pipetree.adapters.fault import FaultInjectingAdapter, FaultSpec
from pipetree.executor.retry import Throttled

from ..helpers import FakeAdapter, make_graph


def table_for(fqn: str):
    return make_graph({fqn: "scd1"}).tables[fqn]


def test_table_without_a_fault_spec_passes_through_untouched():
    wrapped = FakeAdapter()
    adapter = FaultInjectingAdapter(wrapped, faults={})

    result = adapter.run_table(table_for("bronze.orders"), execution_id=1)

    assert result == {}
    assert wrapped.calls == ["bronze.orders"]


def test_throttle_fault_raises_throttled_the_configured_number_of_times():
    wrapped = FakeAdapter()
    adapter = FaultInjectingAdapter(
        wrapped, faults={"bronze.orders": FaultSpec(kind="throttle", count=2)}
    )
    table = table_for("bronze.orders")

    with pytest.raises(Throttled):
        adapter.run_table(table, execution_id=1)
    with pytest.raises(Throttled):
        adapter.run_table(table, execution_id=1)

    result = adapter.run_table(table, execution_id=1)  # third call: throttling stops
    assert result == {}
    assert wrapped.calls == ["bronze.orders"]  # only the successful call reached the adapter


def test_fail_fault_always_raises():
    wrapped = FakeAdapter()
    adapter = FaultInjectingAdapter(
        wrapped, faults={"silver.bad": FaultSpec(kind="fail", message="schema drift")}
    )
    table = table_for("silver.bad")

    with pytest.raises(ValueError, match="schema drift"):
        adapter.run_table(table, execution_id=1)
    with pytest.raises(ValueError, match="schema drift"):
        adapter.run_table(table, execution_id=1)

    assert wrapped.calls == []  # never reaches the wrapped adapter


def test_slow_fault_delays_before_delegating(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("pipetree.adapters.fault.time.sleep", sleeps.append)

    wrapped = FakeAdapter()
    adapter = FaultInjectingAdapter(
        wrapped, faults={"gold.fact": FaultSpec(kind="slow", delay=5.0)}
    )

    adapter.run_table(table_for("gold.fact"), execution_id=1)

    assert sleeps == [5.0]
    assert wrapped.calls == ["gold.fact"]


def test_capabilities_pass_through_from_the_wrapped_adapter():
    wrapped = FakeAdapter()
    adapter = FaultInjectingAdapter(wrapped, faults={})

    assert adapter.capabilities is wrapped.capabilities


def test_delete_by_execution_id_delegates_to_the_wrapped_adapter():
    wrapped = FakeAdapter()
    adapter = FaultInjectingAdapter(wrapped, faults={})

    adapter.delete_by_execution_id(table_for("silver.appended"), execution_id=7)

    assert wrapped.deleted_execution_ids == [("silver.appended", 7)]
