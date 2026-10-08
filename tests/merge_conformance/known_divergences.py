"""Known divergences between pipetree and the merge reference model (deviations).

`DEVIATIONS[id]` describes a deviation; `quirks.FLAGS` has one flag per
deviation that switches the model to today's pipetree behaviour.
`KNOWN_DIVERGENCES[id] = (description, [(strategy, seed), ...])` lists the
generated cases (seeds 0-299 per strategy) whose WHOLE history is explained by
exactly the deviations they are listed under: for every listed seed,
`test_conformance.py` asserts that the table matches `QuirkModel(S)` (S = all
deviations listing the seed) over all batches, that every flag in S matters and
that the real model differs; only then is the test reported as xfailed.
Anything else fails with the unexplained divergence. `HAND_REPROS[id[-variant]]`
are hand-minimised reproductions, checked the same way.

The lists are GENERATED. Regenerate them after EVERY fix (delete the fixed
deviation's flag from `quirks.py` first), and after any change to the generator,
the model or the harness:

    SPARK_LOCAL_IP=127.0.0.1 uv run python -m tests.merge_conformance.regen --seeds 300 --write
    (optionally --strategy scd1; prints the block, --write rewrites it below)

A seed that no subset of the quirk flags explains is printed as UNEXPLAINED: that is a
NEW deviation (add a `DEVIATIONS` entry, a quirk flag and a repro, then regenerate).

The lists are EMPTY today: every deviation is fixed and every generated history of the
four strategies conforms to the reference model. NULL keys on replace/append were never a
deviation: the NULL-key drop applies to scd1/scd2 only (an earlier model was wrong, not
pipetree).

To add a new deviation (a seed that diverges and that no pipetree fix is planned
for yet, or one regen prints as UNEXPLAINED):
  1. add `DEVIATIONS["Fn"]` (what is wrong, which rule it violates);
  2. add the flag to `quirks.FLAGS` and teach `QuirkModel` the deviation (cite
     the pipetree code that causes it); list the invariants it breaks in
     `quirks._TOLERATES`;
  3. add a minimal reproduction to `HAND_REPROS["Fn"]`;
  4. run regen with `--write`. When the deviation is fixed, undo all of this and
     turn the repro into a passing SMOKE case in `test_conformance.py`.
"""

from __future__ import annotations

from pipetree.testing.merge_gen import Case
from pipetree.testing.merge_model import ModelTable

COLS = ("v", "w", "seq", "op")


def _r(key: dict, v: str = "a", *, seq: int | None = None, op: str = "U", **extra) -> dict:
    return {**key, "v": v, "w": None, "seq": seq, "op": op, **extra}


def _case(strategy: str, batches: list[list[dict]], key=("id",), extra=(), **kw) -> Case:
    has_delete = kw.get("has_delete", False)
    table = ModelTable(strategy=strategy, key=key, columns=key + COLS + extra, **kw)
    return Case(
        seed=-1,
        table=table,
        delete_when="op = 'D'" if has_delete else None,
        surrogate_key=None,
        batches=batches,
        inits=frozenset(),
    )


def _id(i):
    return {"id": i}


def _seeds(strategy: str, seeds: str) -> list[tuple[str, int]]:
    return [(strategy, int(s)) for s in seeds.split()]


DEVIATIONS: dict[str, str] = {}

# BEGIN GENERATED (tests.merge_conformance.regen) - do not edit by hand
_LISTS: dict[str, list[tuple[str, int]]] = {}
# END GENERATED

KNOWN_DIVERGENCES: dict[str, tuple[str, list[tuple[str, int]]]] = {
    f: (desc, _LISTS.get(f, [])) for f, desc in DEVIATIONS.items()
}

# Hand-minimised reproductions, keyed "<deviation id>[-<variant>]" (run by
# test_deviation_minimal_repro).
HAND_REPROS: dict[str, Case] = {}


def deviations_for(strategy: str, seed: int) -> list[str]:
    return sorted(f for f, (_, cases) in KNOWN_DIVERGENCES.items() if (strategy, seed) in cases)


def deviation_of(repro_id: str) -> str:
    """`"null-key-kept-scd2"` -> `"null-key-kept"`."""
    return max((f for f in DEVIATIONS if repro_id == f or repro_id.startswith(f + "-")), key=len)
