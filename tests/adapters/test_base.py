from pipetree.adapters.base import Adapter, Capabilities


def test_capabilities_defaults_to_no_atomic_append_retry():
    assert Capabilities().supports_delete_by_execution_id is False


def test_capabilities_can_declare_atomic_append_support():
    assert Capabilities(supports_delete_by_execution_id=True).supports_delete_by_execution_id


class _FakeAdapter:
    """A minimal Adapter implementation, just to prove the Protocol is
    satisfiable by duck typing - no inheritance required."""

    capabilities = Capabilities()

    def run_table(self, table, *, execution_id):
        return {}

    def delete_by_execution_id(self, table, execution_id):
        pass


def test_adapter_protocol_is_satisfied_by_duck_typing():
    assert isinstance(_FakeAdapter(), Adapter)
