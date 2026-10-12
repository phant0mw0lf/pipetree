"""The fixed seeds of the pull-request conformance run (`PIPETREE_CONFORMANCE_SEEDS=pr`).

The first 25 seeds of a strategy do not reach every coverage name (e.g. scd1
`ignored_only_change` and `resurrection`). So the PR run uses, per strategy, a
greedy set cover over seeds 0..299 (the nightly range): repeatedly the seed that
adds the most still-missing `coverage()` names, ties to the cheaper case (fewer
batches, then fewer rows), then the lower seed; padded with the first unused
seeds up to `TARGET` per strategy. Offline, no Spark.

Regenerate (after any change to the generator):

    uv run python -m tests.merge_conformance.pr_seeds --write
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import textwrap
from pathlib import Path

from pipetree.testing.merge_gen import STRATEGIES, coverage, generate_case

RANGE = 300  # the nightly seed range
TARGET = 25  # seeds per strategy in the PR run

_FILE = Path(__file__)
_BEGIN = "\n# BEGIN GENERATED"  # at a line start: the constant itself never matches
_END = "\n# END GENERATED"


# BEGIN GENERATED (tests.merge_conformance.pr_seeds) - do not edit by hand
# fmt: off
PR_SEEDS: dict[str, list[int]] = {
    "replace": [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
        22, 57, 227,
    ],
    "append": [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
        22, 76, 127,
    ],
    "scd1": [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 36,
        49, 63, 228,
    ],
    "scd2": [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
        78, 185, 259,
    ],
}
# fmt: on
# END GENERATED


def reachable(strategy: str, n: int = RANGE) -> set[str]:
    """Coverage names reached by some seed of 0..n-1 for the strategy."""
    out: set[str] = set()
    for seed in range(n):
        out |= coverage(generate_case(seed, strategy))
    return out


def derive(strategy: str, n: int = RANGE, target: int = TARGET) -> list[int]:
    info = {}
    for seed in range(n):
        case = generate_case(seed, strategy)
        cost = (len(case.batches), sum(len(b) for b in case.batches))
        info[seed] = (coverage(case), cost)
    missing = set().union(*(c for c, _ in info.values()))
    chosen: list[int] = []
    while missing:
        best = max(
            info, key=lambda s: (len(info[s][0] & missing), tuple(-x for x in info[s][1]), -s)
        )
        chosen.append(best)
        missing -= info[best][0]
    for seed in range(n):
        if len(chosen) >= target:
            break
        if seed not in chosen:
            chosen.append(seed)
    return sorted(chosen)


def render(seeds: dict[str, list[int]]) -> str:
    out = [f"{_BEGIN[1:]} (tests.merge_conformance.pr_seeds) - do not edit by hand"]
    out.append("# fmt: off")
    out.append("PR_SEEDS: dict[str, list[int]] = {")
    for s in STRATEGIES:
        body = textwrap.fill(
            ", ".join(map(str, seeds[s])), 88, initial_indent=" " * 8, subsequent_indent=" " * 8
        )
        out.append(f'    "{s}": [\n{body},\n    ],')
    out.append("}")
    out.append("# fmt: on")
    out.append(_END[1:])
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Derive the PR conformance seed lists.")
    ap.add_argument("--write", action="store_true", help="rewrite this module")
    args = ap.parse_args(argv)
    block = render({s: derive(s) for s in STRATEGIES})
    print(block)
    if args.write:
        text = _FILE.read_text()
        head, rest = text.split(_BEGIN, 1)
        _, tail = rest.split(_END, 1)
        _FILE.write_text(head + "\n" + block + tail)
        subprocess.run(["ruff", "format", str(_FILE)], check=False, capture_output=True)
        print(f"wrote {_FILE}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
