"""Known divergences between pipetree and the merge reference model (deviations).

`KNOWN_DIVERGENCES[deviation_id] = (description, [(strategy, seed), ...])`: the
generated cases a deviation makes diverge (attributed by root cause; a case hit
by two deviations is listed under both). `HAND_REPROS[deviation_id]` is the
hand-minimised reproduction of the deviation. Both run as `xfail(strict=True)`
in `test_conformance.py`: when a fix lands, the xfail turns into a failure and
the entry must be removed.
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


KNOWN_DIVERGENCES: dict[str, tuple[str, list[tuple[str, int]]]] = {
    "revive-after-delete": (
        "scd1 soft-deleted key that returns stays _is_deleted=true "
        "(identical values: row untouched; changed values: values and _execution_id "
        "move but _is_deleted stays true)",
        _seeds(
            "scd1",
            "51 144 146 226 ",
        ),
    ),
    "null-key-kept": (
        "NULL business key on an scd1/scd2 table without unknown_member is "
        "not dropped (no null_keys_dropped); it never matches, so it is re-inserted "
        "on every run",
        _seeds(
            "scd1",
            "0 1 4 10 11 20 24 28 45 48 49 56 62 64 67 68 69 82 83 85 87 100 103 105 110 "
            "117 121 125 126 132 135 139 148 149 151 152 154 156 160 162 173 180 181 197 "
            "200 206 207 215 219 221 227 230 231 238 247 253 281 284 294 ",
        )
        + _seeds(
            "scd2",
            "1 11 14 30 32 36 37 53 64 68 73 74 79 80 86 88 103 106 124 126 127 129 137 "
            "138 142 147 150 159 162 165 171 185 194 202 208 212 213 216 217 220 226 227 "
            "230 232 233 245 252 256 267 271 273 283 285 287 297 ",
        ),
    ),
    "composite-null-key": (
        "composite key with a NULL in one component is matched with '=' "
        "(not null-safe), so the row is re-inserted on every run instead of being "
        "updated/left untouched",
        _seeds(
            "scd1",
            "254 ",
        )
        + _seeds(
            "scd2",
            "45 151 249 ",
        ),
    ),
    "F4": (
        "(scope to confirm): replace/append tables without unknown_member "
        "keep NULL-key rows; the model drops them as for scd1/scd2",
        _seeds(
            "append",
            "8 15 24 25 26 29 30 33 34 37 39 48 49 57 60 73 76 78 80 86 87 88 90 91 93 94 "
            "100 107 109 111 114 122 132 140 145 147 153 178 186 191 193 197 198 201 208 "
            "211 215 220 221 227 229 231 234 237 239 241 251 257 265 266 279 282 283 284 "
            "290 293 297 ",
        )
        + _seeds(
            "replace",
            "18 19 24 28 29 34 36 40 41 45 46 47 59 69 75 82 83 86 88 91 92 93 95 97 102 "
            "106 107 108 109 110 115 118 120 124 126 127 128 131 135 142 149 156 172 176 "
            "177 179 186 191 195 196 200 218 219 223 227 229 231 243 255 257 259 260 281 "
            "282 283 287 288 290 291 294 299 ",
        ),
    ),
    "ignore-mode-delete-rows": (
        "delete_mode ignore treats delete rows as ordinary upserts (they "
        "are written, win dedupe by sequence and count as duplicates) instead of "
        "removing them before dedupe",
        _seeds(
            "scd1",
            "5 6 8 9 12 25 28 29 33 34 35 38 39 41 42 49 73 77 88 93 97 101 113 118 119 "
            "120 123 131 141 153 170 171 172 175 177 180 193 194 218 220 225 234 236 242 "
            "243 244 256 273 280 282 288 289 295 297 ",
        )
        + _seeds(
            "scd2",
            "3 6 8 9 16 26 28 33 38 41 58 62 63 72 75 77 83 84 91 92 93 95 96 101 110 112 "
            "113 115 120 124 128 133 136 143 153 154 161 166 168 172 174 175 183 188 189 "
            "191 203 209 210 211 215 221 222 224 225 228 229 231 235 238 239 246 248 254 "
            "257 261 264 272 276 284 288 290 296 298 ",
        ),
    ),
    "ignored-column-stale": (
        "a change in ignore_columns alone is not written (scd1 and scd2); "
        "the model updates the current row in place without a bump",
        _seeds(
            "scd1",
            "76 79 111 132 136 148 215 217 228 283 298 ",
        )
        + _seeds(
            "scd2",
            "25 27 29 44 78 89 105 193 195 293 ",
        ),
    ),
    "redelete-bump": (
        "re-running a batch is a no-op: scd1 soft delete "
        "of an already soft-deleted row bumps _execution_id/_updated_at again",
        _seeds(
            "scd1",
            "53 58 80 164 165 214 249 ",
        ),
    ),
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
    "F4": _case("replace", [[_r(_id(None)), _r(_id(1))]]),
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
    return [f for f, (_, cases) in KNOWN_DIVERGENCES.items() if (strategy, seed) in cases]
