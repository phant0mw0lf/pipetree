"""Run the with_problems example the way the README describes: two runs
over a scratch copy of this folder, so the checked-in files stay untouched.

    uv run python examples/with_problems/run.py [--workdir DIR] [--html FILE]

Run 1 loads `data/`. Then `data_next/` replaces the source files (a second
delivery: duplicate rows, a NULL key, a new `channel` column) and run 2
loads them. `--html` writes the final graph page of run 2. The exit code is
1 on purpose: gold.dim_customer is broken, see README.md.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

from pipetree import run_pipeline
from pipetree.adapters.spark.session import build_local_session
from pipetree.notebook import HtmlFileObserver

HERE = Path(__file__).parent


def run(workdir: Path, html: Path | None) -> int:
    """Both runs inside `workdir`; returns the exit code of run 2."""
    work = workdir / "with_problems"
    shutil.copytree(HERE, work, ignore=shutil.ignore_patterns("__pycache__", "run.py"))
    os.chdir(workdir)  # Spark's metastore_db / derby.log land here, not in the repo
    spark = build_local_session(
        app_name="pipetree-with-problems", warehouse_dir=str(workdir / "wh")
    )
    config = work / "pipeline.yaml"
    try:
        print("--- run 1: first delivery ---")
        run_pipeline(config, execution_id=1)

        for new in (work / "data_next").glob("*.csv"):
            shutil.copy(new, work / "data" / new.name)

        print("--- run 2: second delivery ---")
        observer = (
            HtmlFileObserver(html, title="with_problems - second run", theme="light")
            if html
            else None
        )
        digest = run_pipeline(config, execution_id=2, observer=observer)
    finally:
        spark.stop()
    return digest.exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--workdir", type=Path, help="scratch directory (default: a temp dir)")
    parser.add_argument("--html", type=Path, help="write run 2's final graph page here")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)-7s %(name)s: %(message)s")
    html = args.html.resolve() if args.html else None
    if args.workdir:
        args.workdir.mkdir(parents=True, exist_ok=True)
        return run(args.workdir.resolve(), html)
    with tempfile.TemporaryDirectory(prefix="pipetree-with-problems-") as tmp:
        return run(Path(tmp), html)


if __name__ == "__main__":
    sys.exit(main())
