"""Which engine errors are transient, in the executor's three words.

The executor retries `Throttled`, `TimeoutError` and `ConnectionError` and
nothing else (see `pipetree.executor.retry`). Spark does not raise those: a
Delta commit conflict or a throttled storage account reaches Python as a
`Py4JJavaError`, a pyspark `CapturedException` or a Delta Python exception,
and the only thing they share is the Java class names in their text. The
`SparkAdapter` translates them here, once, so the executor stays free of
engine error shapes.
"""

from __future__ import annotations

import re

from pipetree.executor.retry import Throttled

# Delta's optimistic-concurrency conflicts: the commit lost a race with another
# writer. Re-running the table re-reads the new state, so a retry is correct.
_COMMIT_CONFLICT = {
    "io.delta.exceptions.ConcurrentAppendException",
    "io.delta.exceptions.ConcurrentDeleteReadException",
    "io.delta.exceptions.ConcurrentDeleteDeleteException",
    "io.delta.exceptions.ProtocolChangedException",
    "io.delta.exceptions.MetadataChangedException",
}
# A throttled storage account has no class of its own: ADLS and Blob report it
# as a generic storage exception with the HTTP status or error code in the
# text. Only that text is searched, and only if one of these classes is present.
_STORAGE = {
    "org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException",
    "com.azure.storage.blob.models.BlobStorageException",
    "com.azure.storage.file.datalake.models.DataLakeStorageException",
}
_THROTTLE_TEXT = re.compile(r"\b(?:429|503)\b|ServerBusy|Server Busy|TooManyRequests", re.I)
_TIMEOUT = {
    "java.net.SocketTimeoutException",
    "org.apache.hadoop.net.ConnectTimeoutException",
    "java.util.concurrent.TimeoutException",
}
_CONNECTION = {"java.net.SocketException", "java.net.ConnectException"}

_ALREADY_TRANSIENT = (Throttled, TimeoutError, ConnectionError)

# "org.apache.spark.SparkException: ..." at the start of a line or after
# "Caused by: " or py4j's leading ": ". Stack frames ("at pkg.Class.method(")
# never end in ":" or a line break right after the class, so they don't match.
_JAVA_CLASS = re.compile(r"(?m)^(?:\s*:\s*|Caused by: )?((?:[a-z]\w*\.)+[A-Z][\w$]*)(?=:|$)")


def _translate(exc: BaseException) -> Exception | None:
    """The executor's transient type for an engine error, or None to leave it alone."""
    if isinstance(exc, _ALREADY_TRANSIENT):
        return None
    chain = _chain(exc)
    text = "\n".join(_text(e) for e in chain)
    names = set(_JAVA_CLASS.findall(text)) | {_delta_name(e) for e in chain}
    throttled = _COMMIT_CONFLICT | (_STORAGE if _THROTTLE_TEXT.search(text) else set())
    for kind, classes in (
        (Throttled, throttled),
        (TimeoutError, _TIMEOUT),
        (ConnectionError, _CONNECTION),
    ):
        if hit := names & classes:
            return kind(_line_naming(min(hit), text, exc))
    return None


def _chain(exc: BaseException) -> list[BaseException]:
    """`exc` and everything it was raised from, outermost first."""
    chain: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in chain:
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


def _line_naming(java_class: str, text: str, exc: BaseException) -> str:
    """The line of the Java trace that names the class: it carries the engine's own message."""
    line = next((ln for ln in text.splitlines() if java_class in ln), java_class)
    return line.strip(" :")[:300] or type(exc).__name__


def _text(exc: BaseException) -> str:
    try:
        return str(exc) or type(exc).__name__
    except Exception:  # noqa: BLE001 - str() of a Py4J error needs a live gateway
        return type(exc).__name__


def _delta_name(exc: BaseException) -> str:
    """Delta's Python exception classes carry the class name but not the text."""
    kind = type(exc)
    return f"io.delta.exceptions.{kind.__name__}" if kind.__module__.startswith("delta.") else ""
