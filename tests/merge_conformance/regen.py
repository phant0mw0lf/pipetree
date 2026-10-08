"""Regenerate the seed lists of `known_divergences.py`.

    SPARK_LOCAL_IP=127.0.0.1 uv run python -m tests.merge_conformance.regen --seeds 300 \\
        [--strategy scd1 ...] [--write]

Runs every (strategy, seed) through Spark ONCE (`record_case`: the whole
history plus the idempotence re-run), then offline: a case that conforms to
the reference model is not listed; a diverging case gets the SMALLEST set of
quirk flags (`quirks.FLAGS`, subsets tried smallest first) whose `QuirkModel`
explains its whole recorded history (attribution mode, no divergence at all)
and in which every flag matters. A case no subset explains is printed as
UNEXPLAINED with its divergences (a new deviation) and makes the exit code 1.

Prints the generated block; `--write` replaces the block between the BEGIN/END
GENERATED markers in `known_divergences.py` and runs `ruff format` on it (with
`--strategy`, the entries of the other strategies are kept). Needs a local
Spark (about 6 s per case).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import textwrap
import time
from itertools import combinations
from pathlib import Path

from pipetree.testing.merge_gen import STRATEGIES, generate_case
from pipetree.testing.merge_harness import compare_recording, record_case
from tests.merge_conformance import known_divergences
from tests.merge_conformance.quirks import FLAGS, QuirkModel, quirk_problems

_FILE = Path(known_divergences.__file__)
_BEGIN = "# BEGIN GENERATED"
_END = "# END GENERATED"


def explain(case, rec) -> frozenset[str] | None:
    """Smallest flag set explaining the recording, or None."""
    names = sorted(FLAGS)
    for size in range(1, len(names) + 1):
        for subset in combinations(names, size):
            flags = frozenset(subset)
            if compare_recording(case, rec, QuirkModel(flags), attribution=True):
                continue
            if not quirk_problems(case, flags):
                return flags
    return None


def render(lists: dict[str, list[tuple[str, int]]]) -> str:
    def lit(seeds: list[int]) -> str:
        chunks = textwrap.wrap(" ".join(map(str, sorted(seeds))), 76)
        return "\n".join(f'            "{c} "' for c in chunks)

    out = [f"{_BEGIN} (tests.merge_conformance.regen) - do not edit by hand"]
    out.append("_LISTS: dict[str, list[tuple[str, int]]] = {")
    for f in sorted(lists):
        by: dict[str, list[int]] = {}
        for s, seed in lists[f]:
            by.setdefault(s, []).append(seed)
        if not by:
            continue
        parts = [
            f'_seeds(\n            "{s}",\n{lit(seeds)},\n        )'
            for s, seeds in sorted(by.items(), key=lambda kv: STRATEGIES.index(kv[0]))
        ]
        out.append(f'    "{f}": ' + "\n        + ".join(parts) + ",")
    out.append("}")
    out.append(_END)
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Regenerate the known-divergence seed lists.")
    ap.add_argument("--seeds", type=int, default=300, help="seeds 0..N-1 per strategy")
    ap.add_argument("--strategy", action="append", choices=STRATEGIES)
    ap.add_argument("--write", action="store_true", help="rewrite known_divergences.py")
    args = ap.parse_args(argv)
    strategies = args.strategy or list(STRATEGIES)

    from pipetree.adapters.spark.session import build_local_session

    spark = build_local_session("pipetree-regen", tempfile.mkdtemp(prefix="regen-wh-"))
    spark.sparkContext.setLogLevel("ERROR")

    lists: dict[str, list[tuple[str, int]]] = {f: [] for f in FLAGS}
    for f, (_, cases) in known_divergences.KNOWN_DIVERGENCES.items():
        lists.setdefault(f, []).extend(c for c in cases if c[0] not in strategies)
    unexplained = []
    for strategy in strategies:
        start, diverging = time.time(), 0
        for seed in range(args.seeds):
            case = generate_case(seed, strategy)
            rec = record_case(spark, case)
            if not compare_recording(case, rec):
                continue
            diverging += 1
            flags = explain(case, rec)
            if flags is None:
                divs = compare_recording(case, rec, attribution=True)
                unexplained.append((strategy, seed, divs))
                print(f"UNEXPLAINED {strategy} {seed}", file=sys.stderr)
                continue
            for f in flags:
                lists[f].append((strategy, seed))
        secs = time.time() - start
        print(
            f"{strategy}: {args.seeds} seeds, {diverging} diverging, "
            f"{secs:.0f} s ({secs / max(args.seeds, 1):.2f} s/case)",
            file=sys.stderr,
        )

    block = render(lists)
    print(block)
    for f in sorted(lists):
        counts = {s: sum(1 for c in lists[f] if c[0] == s) for s in STRATEGIES}
        print(f"{f}: {counts}", file=sys.stderr)
    for strategy, seed, divs in unexplained:
        print(f"\nUNEXPLAINED {strategy} {seed}:", file=sys.stderr)
        for d in divs:
            print(str(d), file=sys.stderr)
    if args.write:
        text = _FILE.read_text()
        head, rest = text.split(_BEGIN, 1)
        _, tail = rest.split(_END, 1)
        _FILE.write_text(head + block + tail)
        subprocess.run(["ruff", "format", str(_FILE)], check=False, capture_output=True)
        print(f"wrote {_FILE}", file=sys.stderr)
    return 1 if unexplained else 0


if __name__ == "__main__":
    sys.exit(main())
