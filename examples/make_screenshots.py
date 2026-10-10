"""Regenerate the README's two pictures of the dependency graph:

- docs/images/graph-clean.png       - examples/pipeline.yaml, run for real
- docs/images/graph-with-notes.png  - examples/with_problems, second run

    uv run python examples/make_screenshots.py

Both pipelines run on local Spark (a JDK is required, see the README), in a
temp directory, so the repo stays clean. The final graph page of each run is
rendered by pipetree itself and photographed by headless Chromium. Set
CHROMIUM to a Chromium/Chrome binary to choose one explicitly.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
IMAGES = HERE.parent / "docs" / "images"
WIDTH = 1400
SCALE = 1.5
MEASURE = (
    "<script>(function(){var r=document.body.firstElementChild.getBoundingClientRect();"
    "document.documentElement.setAttribute('data-size',"
    "Math.ceil(r.right)+'x'+Math.ceil(r.bottom));})();</script>"
)


def find_chromium() -> str | None:
    """A browser that actually starts (a stale Homebrew wrapper does not)."""
    candidates = [os.environ.get("CHROMIUM"), "/opt/homebrew/bin/chromium"]
    candidates += [shutil.which(n) for n in ("chromium", "google-chrome", "chrome")]
    candidates.append("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    for candidate in candidates:
        if not candidate or not Path(candidate).exists():
            continue
        probe = subprocess.run([candidate, "--version"], capture_output=True, check=False)
        if probe.returncode == 0:
            return candidate
    return None


def run_clean(workdir: Path, html: Path) -> None:
    """examples/pipeline.yaml, run for real; writes the final graph page."""
    from pipetree import run_pipeline
    from pipetree.adapters.spark.session import build_local_session
    from pipetree.notebook import HtmlFileObserver

    work = workdir / "clean"
    shutil.copytree(HERE, work, ignore=shutil.ignore_patterns("__pycache__", "with_problems"))
    os.chdir(workdir)
    spark = build_local_session(app_name="pipetree-screenshot", warehouse_dir=str(workdir / "wh"))
    try:
        observer = HtmlFileObserver(html, title="pipeline.yaml - run", theme="light")
        digest = run_pipeline(work / "pipeline.yaml", execution_id=1, observer=observer)
    finally:
        spark.stop()
    if not digest.succeeded:
        raise SystemExit("examples/pipeline.yaml did not run clean")


def run_with_problems(workdir: Path, html: Path) -> None:
    # Own process: a second Spark session in this one would reuse the first.
    script = HERE / "with_problems" / "run.py"
    subprocess.run(
        [sys.executable, str(script), "--workdir", str(workdir / "problems"), "--html", str(html)],
        check=False,  # exit code 1 is expected, gold.dim_customer is broken on purpose
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if not html.exists():
        raise SystemExit("with_problems/run.py wrote no graph page; run it by hand to see why")


def screenshot(chromium: str, html: Path, png: Path, tmp: Path) -> None:
    flags = [
        "--headless",
        "--disable-gpu",
        "--hide-scrollbars",
        "--no-sandbox",
        "--blink-settings=preferredColorScheme=0",
    ]
    # Measure the page first, so the shot is cropped to the graph and nothing else.
    probe = tmp / f"probe-{html.name}"
    probe.write_text(html.read_text(encoding="utf-8").replace("</body>", MEASURE + "</body>"))
    dom = subprocess.run(
        [chromium, *flags, f"--window-size={WIDTH},2000", "--dump-dom", probe.as_uri()],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    found = re.search(r'data-size="(\d+)x(\d+)"', dom)
    width, height = (int(found[1]), int(found[2])) if found else (WIDTH, 900)
    png.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            chromium,
            *flags,
            f"--force-device-scale-factor={SCALE}",
            f"--window-size={max(width, WIDTH)},{height}",
            f"--screenshot={png}",
            html.as_uri(),
        ],
        capture_output=True,
        check=True,
    )
    print(f"{png.relative_to(HERE.parent)}  ({png.stat().st_size // 1024} KB)")


def main() -> int:
    chromium = find_chromium()
    if chromium is None:
        print(
            "No Chromium found. Install one (brew install --cask chromium), install Chrome, or point the "
            "CHROMIUM environment variable at a Chromium/Chrome binary.",
            file=sys.stderr,
        )
        return 1
    with tempfile.TemporaryDirectory(prefix="pipetree-shots-") as tmp_name:
        tmp = Path(tmp_name)
        clean_html, problems_html = tmp / "clean.html", tmp / "with-notes.html"
        run_clean(tmp, clean_html)
        run_with_problems(tmp, problems_html)
        screenshot(chromium, clean_html, IMAGES / "graph-clean.png", tmp)
        screenshot(chromium, problems_html, IMAGES / "graph-with-notes.png", tmp)
    return 0


if __name__ == "__main__":
    sys.exit(main())
