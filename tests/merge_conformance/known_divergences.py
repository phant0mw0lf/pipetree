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

NULL keys on replace/append were never a deviation: the NULL-key drop applies
to scd1/scd2 only (an earlier model was wrong, not pipetree).
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


DEVIATIONS: dict[str, str] = {
    "revive-after-delete": "a soft-deleted scd1 key that returns stays `_is_deleted = "
    "true` (identical values: the row is untouched; changed "
    "values: the values and `_execution_id` move, but "
    "`_is_deleted` stays true)",
    "null-key-kept": "a NULL business key on an scd1/scd2 table without "
    "unknown_member is not dropped (no null_keys_dropped); it "
    "never matches, so it is re-inserted on every run",
    "composite-null-key": "a composite key with a NULL in one component is matched with"
    " `=` (not null-safe), so the row is re-inserted on every run",
    "ignore-mode-delete-rows": "delete_mode ignore treats delete rows as ordinary upserts "
    "(written, winning the dedupe by sequence, counted as "
    "duplicates) instead of removing them before the dedupe",
    "ignored-column-stale": "a change in an ignore_columns column alone is not written "
    "(scd1 and scd2)",
    "redelete-bump": "a repeated soft delete of an already deleted scd1 row bumps "
    "`_execution_id` and `_updated_at` again (it should be a "
    "no-op)",
}

# BEGIN GENERATED (tests.merge_conformance.regen) - do not edit by hand
_LISTS: dict[str, list[tuple[str, int]]] = {
    "revive-after-delete": _seeds(
        "scd1",
        "51 53 69 144 146 154 165 207 226 ",
    ),
    "null-key-kept": _seeds(
        "scd1",
        "0 1 4 10 11 20 24 28 45 48 49 56 62 64 67 68 69 73 82 83 85 87 88 100 103 "
        "105 110 113 117 121 125 126 132 135 139 141 148 149 151 152 154 156 160 162 "
        "165 170 173 180 181 197 200 206 207 215 219 221 227 230 231 238 242 244 247 "
        "253 280 281 284 294 ",
    )
    + _seeds(
        "scd2",
        "1 9 11 14 26 30 32 36 37 41 53 58 64 68 73 74 79 80 83 86 88 96 103 106 110 "
        "120 124 126 127 129 137 138 142 147 150 151 159 162 165 171 172 185 194 202 "
        "208 210 212 213 216 217 220 226 227 228 230 232 233 235 245 252 256 267 271 "
        "273 283 285 287 297 ",
    ),
    "composite-null-key": _seeds(
        "scd1",
        "254 ",
    )
    + _seeds(
        "scd2",
        "45 86 106 150 151 229 238 249 264 296 ",
    ),
    "ignore-mode-delete-rows": _seeds(
        "scd1",
        "5 6 8 9 12 20 25 28 29 33 34 35 38 39 41 42 49 73 77 88 93 97 101 113 118 "
        "119 120 123 131 141 153 170 171 172 175 177 180 193 194 218 220 225 234 236 "
        "242 243 244 256 273 280 282 288 289 295 297 ",
    )
    + _seeds(
        "scd2",
        "3 6 8 9 16 26 28 33 38 41 44 58 62 63 72 75 77 80 83 84 88 91 92 93 95 96 "
        "101 110 112 113 115 120 124 128 133 136 143 153 154 161 166 168 172 174 175 "
        "183 188 189 191 203 209 210 211 215 221 222 224 225 228 229 231 235 238 239 "
        "246 248 252 254 257 261 264 272 276 284 288 290 296 298 ",
    ),
    "ignored-column-stale": _seeds(
        "scd1",
        "76 79 93 103 111 118 132 136 148 194 206 215 217 228 283 298 ",
    )
    + _seeds(
        "scd2",
        "14 25 27 29 44 73 78 89 105 124 193 195 210 217 293 ",
    ),
    "redelete-bump": _seeds(
        "scd1",
        "51 53 58 69 80 139 146 154 164 165 206 214 231 249 284 ",
    ),
}
# END GENERATED

KNOWN_DIVERGENCES: dict[str, tuple[str, list[tuple[str, int]]]] = {
    f: (desc, _LISTS.get(f, [])) for f, desc in DEVIATIONS.items()
}

# Hand-minimised reproductions, keyed "<deviation id>[-<variant>]" (run by
# test_deviation_minimal_repro).
HAND_REPROS: dict[str, Case] = {
    "revive-after-delete": _case(
        "scd1", [[_r(_id(1))], [_r(_id(1), op="D")], [_r(_id(1))]], has_delete=True
    ),
    "revive-after-delete-changed-values": _case(
        "scd1", [[_r(_id(1))], [_r(_id(1), op="D")], [_r(_id(1), "b")]], has_delete=True
    ),
    "null-key-kept": _case("scd1", [[_r(_id(None))], [_r(_id(None))]]),
    "null-key-kept-scd2": _case("scd2", [[_r(_id(None))], [_r(_id(None))]]),
    "composite-null-key": _case(
        "scd1", [[_r({"a": None, "b": 1})], [_r({"a": None, "b": 1})]], key=("a", "b")
    ),
    "ignore-mode-delete-rows": _case(
        "scd1", [[_r(_id(1), op="D")]], has_delete=True, delete_mode="ignore"
    ),
    "ignore-mode-delete-rows-scd2-dedupe": _case(
        "scd2",
        [[_r(_id(1), seq=1)], [_r(_id(1), "b", seq=3, op="D"), _r(_id(1), "c", seq=2)]],
        sequence_by=("seq",),
        has_delete=True,
        delete_mode="ignore",
    ),
    "ignored-column-stale-scd2": _case(
        "scd2",
        [[_r(_id(1), seen=0)], [_r(_id(1), seen=1)]],
        extra=("seen",),
        ignore=("seen",),
    ),
    "ignored-column-stale": _case(
        "scd1",
        [[_r(_id(1), seen=0)], [_r(_id(1), seen=1)]],
        extra=("seen",),
        ignore=("seen",),
    ),
    "redelete-bump": _case(
        "scd1",
        [[_r(_id(1))], [_r(_id(1), op="D")], [_r(_id(1), op="D")]],
        has_delete=True,
    ),
}


def deviations_for(strategy: str, seed: int) -> list[str]:
    return sorted(f for f, (_, cases) in KNOWN_DIVERGENCES.items() if (strategy, seed) in cases)


def deviation_of(repro_id: str) -> str:
    """`"ignore-mode-delete-rows-scd2-dedupe"` -> `"ignore-mode-delete-rows"`."""
    return max((f for f in DEVIATIONS if repro_id == f or repro_id.startswith(f + "-")), key=len)
