from __future__ import annotations

import os

import pytest

pyspark = pytest.importorskip("pyspark")

from pipetree.adapters.spark.session import build_local_session  # noqa: E402


def _xdist_session_options(worker_id: str) -> dict:
    """Without xdist the session is the default `local[*]`. With N workers on one
    machine, N sessions of `local[*]` each oversubscribe every core (measured:
    `-n 4` gave 1.8x with `local[*]` and 2.7x with two threads per session), so a
    worker gets two task threads and a small shuffle."""
    if worker_id == "master":
        return {}
    return {"master": "local[2]", "conf": {"spark.sql.shuffle.partitions": "4"}}


@pytest.fixture(scope="session")
def spark(tmp_path_factory, worker_id):
    """One local Spark session per process.

    Under pytest-xdist every worker is its own process with its own JVM, so each
    gets its own session. The warehouse directory is named after the worker
    (`gw0`, `gw1`, ...; `master` without xdist) and lives under the worker's own
    temp base directory, so sessions never share on-disk state. The driver binds
    to loopback and the UI is off (see `build_local_session`), so ports are
    picked automatically and cannot collide.
    """
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    warehouse_dir = tmp_path_factory.mktemp(f"spark-warehouse-{worker_id}")
    try:
        session = build_local_session(
            app_name="pipetree-tests",
            warehouse_dir=str(warehouse_dir),
            **_xdist_session_options(worker_id),
        )
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"could not start a local Spark session (JVM missing?): {exc}")
        return

    yield session
    session.stop()
