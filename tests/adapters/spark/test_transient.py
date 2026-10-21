"""Engine errors become the executor's transient types - without a JVM.

The errors below are hand-built in the shape Spark gives them to Python: a
Py4J error whose text is the Java stack trace, a pyspark `CapturedException`
whose first line is the Java exception, and Delta's own Python exception
classes. The adapter is built on a bare stand-in for the session, so nothing
here starts Spark.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from pipetree.adapters.spark.adapter import SparkAdapter
from pipetree.adapters.spark.merge import SnapshotGuardError, SurrogateKeyError
from pipetree.adapters.spark.transient import _translate
from pipetree.executor.retry import RetryPolicy, Throttled
from pipetree.model import System, Table


class FakePy4JError(Exception):
    """What `str()` of a Py4JJavaError gives: the call, then the Java trace."""

    def __init__(self, java_trace: str) -> None:
        super().__init__("An error occurred while calling o97.sql.\n: " + java_trace)


def py4j(java_class: str, message: str, *, caused_by: str | None = None) -> FakePy4JError:
    trace = "org.apache.spark.SparkException: Job aborted.\n"
    trace += "\tat org.apache.spark.Foo.bar(Foo.scala:1)\n"
    trace += f"Caused by: {java_class}: {message}\n\tat com.example.Baz.run(Baz.java:7)\n"
    if caused_by:
        trace += f"Caused by: {caused_by}: root cause\n"
    return FakePy4JError(trace)


def captured(java_class: str, message: str) -> Exception:
    """A pyspark CapturedException: the Java exception is the first line."""
    return Exception(
        f"{java_class}: {message}\n\nJVM stacktrace:\n{java_class}: {message}\n\tat a.B.c(B.java:1)"
    )


DELTA = "io.delta.exceptions"


@pytest.mark.parametrize(
    "name",
    [
        "ConcurrentAppendException",
        "ConcurrentDeleteReadException",
        "ConcurrentDeleteDeleteException",
        "ProtocolChangedException",
        "MetadataChangedException",
    ],
)
def test_delta_commit_conflicts_become_throttled(name):
    for error in (
        py4j(f"{DELTA}.{name}", "Files were added to the root of the table"),
        captured(f"{DELTA}.{name}", "Files were added to the root of the table"),
    ):
        translated = _translate(error)
        assert isinstance(translated, Throttled)
        assert f"{DELTA}.{name}" in str(translated)


def test_delta_python_exception_class_is_recognised_by_its_type():
    kind = type(
        "ConcurrentAppendException", (Exception,), {"__module__": "delta.exceptions.captured"}
    )

    assert isinstance(_translate(kind("no java text here")), Throttled)


@pytest.mark.parametrize(
    ("java_class", "message"),
    [
        (
            "org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException",
            'Operation failed: "The server is busy.", 503, PUT, '
            "https://x.dfs.core.windows.net/c/f, ServerBusy",
        ),
        (
            "org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException",
            "Operation failed: 429, HEAD, https://x.dfs.core.windows.net/c/f",
        ),
        (
            "com.azure.storage.blob.models.BlobStorageException",
            "Status code 503, ServerBusy: The server is currently unable to receive requests",
        ),
        (
            "com.azure.storage.blob.models.BlobStorageException",
            "Status code 429, TooManyRequests",
        ),
        (
            "com.azure.storage.file.datalake.models.DataLakeStorageException",
            "Status code 503, Server Busy",
        ),
    ],
)
def test_storage_throttling_becomes_throttled_even_when_wrapped(java_class, message):
    assert isinstance(_translate(py4j(java_class, message)), Throttled)
    assert isinstance(_translate(captured(java_class, message)), Throttled)


def test_a_storage_error_that_is_not_throttling_is_left_alone():
    error = py4j(
        "org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException",
        "Operation failed: 404, HEAD, PathNotFound",
    )

    assert _translate(error) is None


def test_a_status_code_in_text_without_a_storage_class_is_left_alone():
    error = py4j("org.apache.spark.sql.AnalysisException", "column order_503 cannot be resolved")

    assert _translate(error) is None


@pytest.mark.parametrize(
    "java_class",
    ["java.net.SocketTimeoutException", "org.apache.hadoop.net.ConnectTimeoutException"],
)
def test_read_and_connect_timeouts_become_timeout_error(java_class):
    assert isinstance(_translate(py4j(java_class, "Read timed out")), TimeoutError)


@pytest.mark.parametrize("java_class", ["java.net.SocketException", "java.net.ConnectException"])
def test_connection_resets_become_connection_error(java_class):
    assert isinstance(_translate(py4j(java_class, "Connection reset")), ConnectionError)


def test_a_jdbc_error_is_translated_by_the_socket_exception_underneath():
    error = py4j(
        "com.microsoft.sqlserver.jdbc.SQLServerException",
        "The driver could not establish a secure connection",
        caused_by="java.net.SocketException",
    )

    assert isinstance(_translate(error), ConnectionError)


def test_the_original_error_is_the_cause_and_its_class_is_in_the_message():
    error = py4j(f"{DELTA}.ConcurrentAppendException", "Files were added")
    adapter = adapter_failing_with(error)

    with pytest.raises(Throttled) as raised:
        adapter.run_table(table(), execution_id=1)

    assert raised.value.__cause__ is error
    assert f"{DELTA}.ConcurrentAppendException" in str(raised.value)


def test_a_translated_error_is_retried_by_the_default_policy():
    translated = _translate(py4j("java.net.SocketTimeoutException", "Read timed out"))

    assert translated is not None
    assert RetryPolicy().can_retry(1, translated)


@pytest.mark.parametrize(
    "error",
    [
        ValueError("malformed export file"),
        SnapshotGuardError("snapshot would delete 90% of the table"),
        SurrogateKeyError("surrogate_key needs a newer runtime"),
        py4j("org.apache.spark.sql.AnalysisException", "[UNRESOLVED_COLUMN] region"),
        py4j("org.apache.spark.SparkUnsupportedOperationException", "UNSUPPORTED_FEATURE"),
    ],
)
def test_other_errors_propagate_unchanged(error):
    adapter = adapter_failing_with(error)

    with pytest.raises(type(error)) as raised:
        adapter.run_table(table(), execution_id=1)

    assert raised.value is error


def test_an_error_that_is_already_transient_is_not_wrapped_again():
    error = Throttled("429 from a custom source")
    adapter = adapter_failing_with(error)

    with pytest.raises(Throttled) as raised:
        adapter.run_table(table(), execution_id=1)

    assert raised.value is error


def test_delete_by_execution_id_translates_too():
    error = py4j(f"{DELTA}.ConcurrentDeleteDeleteException", "deleted by a concurrent update")

    class Catalog:
        def tableExists(self, fqn: str) -> bool:  # noqa: N802 - the Spark API's name
            raise error

    adapter = SparkAdapter(SimpleNamespace(catalog=Catalog()), systems={}, base_dir=".")  # type: ignore[arg-type]

    with pytest.raises(Throttled) as raised:
        adapter.delete_by_execution_id(table(), 1)

    assert raised.value.__cause__ is error


def test_delete_by_execution_id_leaves_other_errors_alone():
    error = ValueError("not an engine problem")

    class Catalog:
        def tableExists(self, fqn: str) -> bool:  # noqa: N802 - the Spark API's name
            raise error

    adapter = SparkAdapter(SimpleNamespace(catalog=Catalog()), systems={}, base_dir=".")  # type: ignore[arg-type]

    with pytest.raises(ValueError) as raised:
        adapter.delete_by_execution_id(table(), 1)

    assert raised.value is error


def test_an_error_whose_text_cannot_be_read_is_named_by_its_type():
    class Unreadable(Exception):
        def __str__(self) -> str:
            raise RuntimeError("needs a live gateway")

    assert _translate(Unreadable()) is None


def test_a_chain_that_loops_back_on_itself_ends():
    first, second = Exception("a"), Exception("b")
    first.__cause__, second.__cause__ = second, first

    assert _translate(first) is None


def table() -> Table:
    return Table.model_validate(
        {
            "name": "orders",
            "layer": "bronze",
            "table_schema": "bronze",
            "fqn": "bronze.orders",
            "strategy": "scd1",
            "business_key": ["order_id"],
            "source": {"system": "erp", "object": "SalesOrderHeader"},
        }
    )


def adapter_failing_with(error: Exception) -> SparkAdapter:
    def read_source(table: Table, system: System):
        raise error

    return SparkAdapter(
        SimpleNamespace(),  # type: ignore[arg-type] - the read fails before the session is touched
        systems={"erp": System(type="csv")},
        base_dir=".",
        read_source=read_source,
    )
