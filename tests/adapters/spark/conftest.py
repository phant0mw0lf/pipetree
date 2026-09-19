from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pipetree.adapters.spark.session import build_local_session  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    try:
        session = build_local_session(app_name="pipetree-tests", warehouse_dir=str(warehouse_dir))
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"could not start a local Spark session (JVM missing?): {exc}")
        return

    yield session
    session.stop()
