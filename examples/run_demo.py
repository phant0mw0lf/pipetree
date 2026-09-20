"""Runs the example pipeline with fault injection, for the part-2 demo
transcript (examples/demo-run.txt captures this script's real output).

Scenario: bronze.orders gets throttled twice then recovers (retried ->
succeeded). bronze.customer fails outright, so its descendants
(silver.customer_enriched, gold.dim_customer, gold.fact_sales) are marked
upstream_failed and never start. bronze.employee has no fault and no
dependents - an independent branch that finishes regardless.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pipetree import run_pipeline
from pipetree.adapters.fault import FaultInjectingAdapter, FaultSpec
from pipetree.adapters.spark.adapter import SparkAdapter
from pipetree.adapters.spark.runlog_writer import DeltaRunLogWriter
from pipetree.adapters.spark.session import build_local_session
from pipetree.config.loader import load_config
from pipetree.model import PipelineConfig

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "pipeline.yaml"

FAULTS = {
    "bronze.orders": FaultSpec(kind="throttle", count=2, message="429 Too Many Requests"),
    "bronze.customer": FaultSpec(kind="fail", message="malformed export file"),
}


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    )

    raw = load_config(CONFIG_PATH)
    config = PipelineConfig.from_validated_raw(raw)

    spark = build_local_session(app_name="pipetree-demo")
    base_adapter = SparkAdapter(spark, systems=config.systems, base_dir=HERE)
    adapter = FaultInjectingAdapter(base_adapter, faults=FAULTS)

    digest = run_pipeline(
        CONFIG_PATH,
        adapter=adapter,
        run_log_writer=DeltaRunLogWriter(spark),
    )

    spark.stop()
    return digest.exit_code


if __name__ == "__main__":
    sys.exit(main())
