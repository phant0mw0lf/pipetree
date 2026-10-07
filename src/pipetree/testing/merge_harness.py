"""Run a generated ``Case`` through pipetree's real ``SparkAdapter.run_table`` and
compare the table with the reference model after every batch, plus invariants
that hold independently of the model (so a model bug cannot hide a table bug).

pyspark and the Spark adapter are imported lazily, inside the functions, so
this module can be imported without Spark.

Comparison (``normalise``): the business columns + ``_is_deleted`` +
``_execution_id`` (+ ``_is_current`` for scd2), rows in a canonical order
(key tuple with NULL first; scd2 versions by ``_valid_from``). Timestamps are
never compared by value, only by relation (invariants). The unknown-member
row is compared without its ``_execution_id``.
"""

from __future__ import annotations

import pprint
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from pipetree.testing.merge_gen import Case, generate_case
from pipetree.testing.merge_model import ModelTable, apply_batch, is_unknown_member

_INT_COLUMNS = {"a", "b", "w", "seq", "seen"}


@dataclass
class Divergence:
    case: Case
    batch_index: int
    kind: str  # rows | stats | invariant | error
    detail: str
    lines: list[str] = field(default_factory=list, repr=False)

    def __str__(self) -> str:
        i = self.batch_index
        init = " (init)" if i in self.case.inits else ""
        out = [
            f"DIVERGENCE [{self.kind}] at batch {i}, execution_id {i + 1}{init}",
            f"repro: {self.case.repr_line()}",
            "batches up to here:",
        ]
        for j, batch in enumerate(self.case.batches[: i + 1]):
            flag = " init" if j in self.case.inits else ""
            out.append(f"  [{j}]{flag} {batch}")
        out.append(self.detail)
        return "\n".join(out)


# ------------------------------------------------------------------ normalise


def _sort_value(v: Any) -> tuple:
    return (v is not None, 0 if v is None else v)


def _key_sort(t: ModelTable, row: dict) -> tuple:
    return tuple(_sort_value(row[c]) for c in t.key)


def _compared_columns(t: ModelTable) -> list[str]:
    cols = [*t.columns, "_is_deleted", "_execution_id"]
    if t.strategy == "scd2":
        cols.append("_is_current")
    return cols


def _project(t: ModelTable, row: dict) -> dict:
    out = {c: row[c] for c in _compared_columns(t)}
    if t.unknown_member and is_unknown_member(t, out):
        del out["_execution_id"]  # seeding time is not compared
    return out


def _normalise_raw(raw: list[dict], t: ModelTable) -> list[dict]:
    if t.strategy == "scd2":
        ordered = sorted(raw, key=lambda r: (_key_sort(t, r), _sort_value(r["_valid_from"])))
    else:
        ordered = sorted(raw, key=lambda r: (_key_sort(t, r), repr(_project(t, r))))
    return [_project(t, r) for r in ordered]


def normalise(df: Any, model_table: ModelTable) -> list[dict]:
    """The table (a Spark DataFrame) as comparable rows in canonical order."""
    return _normalise_raw([r.asDict() for r in df.collect()], model_table)


def normalise_model(state: list[dict], t: ModelTable) -> list[dict]:
    """The model state in the same canonical order (stable sort keeps the scd2
    version order of the model)."""
    if t.strategy == "scd2":
        ordered = sorted(state, key=lambda r: _key_sort(t, r))
    else:
        ordered = sorted(state, key=lambda r: (_key_sort(t, r), repr(_project(t, r))))
    return [_project(t, r) for r in ordered]


# ----------------------------------------------------------------- Spark glue


def _column_type(case: Case, col: str) -> str:
    if col in _INT_COLUMNS:
        return "int"
    if col != "id":
        return "string"
    if case.table.unknown_member and case.table.sentinels.get("id", 0) is None:
        return "string"
    if any(isinstance(r["id"], str) for b in case.batches for r in b):
        return "string"
    return "int"


def _ddl(case: Case) -> str:
    return ", ".join(f"`{c}` {_column_type(case, c)}" for c in case.table.columns)


def table_name(case: Case) -> str:
    seed = case.seed if case.seed >= 0 else "hand"
    return f"case_{case.table.strategy}_{seed}"


def build_table(case: Case, schema: str = "mc") -> Any:
    from pipetree.model import Table

    t = case.table
    name = table_name(case)
    raw: dict[str, Any] = {
        "name": name,
        "layer": schema,
        "table_schema": schema,
        "fqn": f"{schema}.{name}",
        "strategy": t.strategy,
        "business_key": list(t.key),
        "source": {"system": "gen", "object": name},
        "merge": {
            "sequence_by": list(t.sequence_by),
            "delete_when": case.delete_when,
            "delete_mode": t.delete_mode,
            "ignore_columns": list(t.ignore),
        },
        "unknown_member": t.unknown_member,
        "surrogate_key": case.surrogate_key,
    }
    return Table.model_validate(raw)


def _fmt(rows: Iterable[dict]) -> str:
    return "\n".join("    " + pprint.pformat(r, width=200, sort_dicts=False) for r in rows)


def _rows_detail(model_rows: list[dict], table_rows: list[dict]) -> str:
    only_model = [r for r in model_rows if r not in table_rows]
    only_table = [r for r in table_rows if r not in model_rows]
    return (
        f"model rows ({len(model_rows)}):\n{_fmt(model_rows)}\n"
        f"table rows ({len(table_rows)}):\n{_fmt(table_rows)}\n"
        f"only in model:\n{_fmt(only_model)}\nonly in table:\n{_fmt(only_table)}"
    )


# ----------------------------------------------------------------- invariants


def _identity(t: ModelTable, row: dict) -> tuple:
    k = tuple(row[c] for c in t.key)
    return (k, row["_valid_from"]) if t.strategy == "scd2" else k


def _unique_by_identity(t: ModelTable, raw: list[dict]) -> dict[tuple, dict]:
    """Rows by identity (scd1: key; scd2: key + _valid_from), only where unique."""
    out: dict[tuple, dict] = {}
    dup: set[tuple] = set()
    for r in raw:
        i = _identity(t, r)
        if i in out:
            dup.add(i)
        out[i] = r
    return {i: r for i, r in out.items() if i not in dup}


def _unknown_rows(t: ModelTable, raw: list[dict]) -> list[dict]:
    if not t.unknown_member:
        return []
    return [r for r in raw if all(r[c] == t.sentinels[c] for c in t.key)]


def check_invariants(
    case: Case,
    batch_index: int,
    prev: list[dict] | None,
    raw: list[dict],
    sids_seen: set[int],
) -> list[str]:
    """Model-independent invariants of one snapshot (and its predecessor)."""
    t = case.table
    problems: list[str] = []
    init = batch_index in case.inits
    rebuilt = init or t.strategy == "replace"  # the table legitimately starts over
    scd = t.strategy in ("scd1", "scd2")

    unknown = _unknown_rows(t, raw)
    if len(unknown) > 1:
        problems.append(f"unknown member exists {len(unknown)} times: {unknown}")
    if prev is not None and not rebuilt:
        before = _unknown_rows(t, prev)
        if len(before) == 1 and before != unknown:
            problems.append(f"unknown member changed: {before} -> {unknown}")

    if scd:
        groups: dict[tuple, int] = {}
        for r in raw:
            groups[_identity(t, r)] = groups.get(_identity(t, r), 0) + 1
            if r["_updated_at"] < r["_inserted_at"]:
                problems.append(f"_updated_at < _inserted_at: {r}")
        what = "key, _valid_from" if t.strategy == "scd2" else "key"
        problems += [f"{n} rows share ({what}) {i}" for i, n in groups.items() if n > 1]
    now_by_id = _unique_by_identity(t, raw) if scd else {}
    if scd and prev is not None and not init:
        for p in prev:
            n = now_by_id.get(_identity(t, p))
            if n is not None and n["_inserted_at"] != p["_inserted_at"]:
                problems.append(f"_inserted_at changed: {p} -> {n}")

    if t.strategy == "scd2":
        problems += _scd2_invariants(t, raw)
        if prev is not None and not init:
            for p in prev:
                if p["_is_current"]:
                    continue
                n = now_by_id.get(_identity(t, p))
                if n != p:
                    problems.append(f"closed version changed: {p} -> {n}")

    sk = case.surrogate_key
    if sk is not None:
        sids = [r[sk] for r in raw]
        if any(s is None for s in sids) or len(set(sids)) != len(sids):
            problems.append(f"surrogate keys not unique/non-NULL: {sorted(map(str, sids))}")
        for r in raw:
            is_unknown = r in unknown
            if is_unknown and r[sk] != -1:
                problems.append(f"unknown member sid is {r[sk]}, not -1")
            if not is_unknown and (r[sk] is None or r[sk] < 1):
                problems.append(f"generated sid {r[sk]} < 1: {r}")
        prev_sids = set((p[sk] for p in prev) if prev else ())
        if prev is not None and not init:
            for p in prev:
                n = now_by_id.get(_identity(t, p))
                if n is not None and n[sk] != p[sk]:
                    problems.append(f"sid changed {p[sk]} -> {n[sk]} for {_identity(t, p)}")
        for r in raw:
            s = r[sk]
            if s is not None and s != -1 and s not in prev_sids and s in sids_seen:
                problems.append(f"sid {s} reused for a new row: {r}")
        sids_seen.update(s for s in sids if s is not None)
    return problems


def _scd2_invariants(t: ModelTable, raw: list[dict]) -> list[str]:
    problems = []
    by_key: dict[tuple, list[dict]] = {}
    for r in raw:
        by_key.setdefault(tuple(r[c] for c in t.key), []).append(r)
    for k, versions in by_key.items():
        versions.sort(key=lambda r: _sort_value(r["_valid_from"]))
        if sum(bool(r["_is_current"]) for r in versions) > 1:
            problems.append(f"key {k}: more than one current version")
        for i, r in enumerate(versions):
            if r["_is_current"] != (r["_valid_to"] is None):
                problems.append(f"key {k}: _valid_to NULL iff current violated: {r}")
            if r["_valid_to"] is not None and r["_valid_to"] < r["_valid_from"]:
                problems.append(f"key {k}: _valid_to < _valid_from: {r}")
            if r["_is_current"] and r["_is_deleted"]:
                problems.append(f"key {k}: a current version is deleted: {r}")
            if i + 1 < len(versions):
                nxt = versions[i + 1]
                if r["_is_current"]:
                    problems.append(f"key {k}: current version is not the latest: {r}")
                elif r["_is_deleted"]:
                    # deleted, then returned (a returning scd2 key opens a new version): a gap is
                    # allowed, an overlap not
                    if r["_valid_to"] > nxt["_valid_from"]:
                        problems.append(f"key {k}: versions overlap: {r} / {nxt}")
                elif r["_valid_to"] != nxt["_valid_from"]:
                    problems.append(f"key {k}: gap/overlap between versions: {r} / {nxt}")
            elif not r["_is_current"] and not r["_is_deleted"]:
                problems.append(f"key {k}: latest version closed without a delete: {r}")
    return problems


# ----------------------------------------------------------------------- run


def run_case(spark: Any, case: Case, schema: str = "mc") -> list[Divergence]:
    """Run ``case`` batch by batch; return the divergences of the first diverging
    batch (later batches would only echo it), or of the idempotence re-run."""
    from pipetree.adapters.spark.adapter import SparkAdapter
    from pipetree.model import System

    t = case.table
    table = build_table(case, schema)
    ddl = _ddl(case)
    current: dict[str, Any] = {}
    adapter = SparkAdapter(
        spark,
        systems={"gen": System(type="custom")},
        base_dir=".",
        read_source=lambda _table, _system: current["df"],
    )

    def run(batch: list[dict], execution_id: int, init: bool) -> dict:
        rows = [tuple(r[c] for c in t.columns) for r in batch]
        current["df"] = spark.createDataFrame(rows, ddl)
        return adapter.run_table(table, execution_id=execution_id, init=init) or {}

    def snapshot() -> list[dict]:
        return [r.asDict() for r in spark.table(table.fqn).collect()]

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"DROP TABLE IF EXISTS {table.fqn}")
    try:
        state: list[dict] = []
        prev: list[dict] | None = None
        sids_seen: set[int] = set()
        for i, batch in enumerate(case.batches):
            init = i in case.inits
            try:
                result = run(batch, i + 1, init)
            except Exception as exc:  # reported, not raised: one case must not stop a run
                return [Divergence(case, i, "error", f"pipetree raised {exc!r}")]
            state, stats = apply_batch(t, state, batch, i + 1, init=init)
            raw = snapshot()
            divergences: list[Divergence] = []

            model_rows = normalise_model(state, t)
            table_rows = _normalise_raw(raw, t)
            if model_rows != table_rows:
                divergences.append(
                    Divergence(case, i, "rows", _rows_detail(model_rows, table_rows))
                )

            stat_problems = []
            if stats.duplicates_dropped is not None:
                got = result.get("duplicates_dropped")
                if got != stats.duplicates_dropped:
                    stat_problems.append(
                        f"duplicates_dropped: model {stats.duplicates_dropped}, table {got}"
                    )
            got_null = result.get("null_keys_dropped", 0)
            if got_null != stats.null_keys_dropped:
                stat_problems.append(
                    f"null_keys_dropped: model {stats.null_keys_dropped}, table {got_null}"
                )
            if stat_problems:
                divergences.append(Divergence(case, i, "stats", "\n".join(stat_problems)))

            problems = check_invariants(case, i, prev, raw, sids_seen)
            if problems:
                divergences.append(Divergence(case, i, "invariant", "\n".join(problems)))
            if divergences:
                return divergences
            prev = raw

        return _check_idempotence(case, run, snapshot, prev)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table.fqn}")


def _check_idempotence(case: Case, run: Any, snapshot: Any, before: list[dict] | None) -> list:
    """Re-running the last batch changes nothing (scd1/scd2: not one value, not
    even a timestamp; replace: the same rows). Skipped after an init batch: a
    plain re-run of an init batch is a different operation (a merge)."""
    t = case.table
    last = len(case.batches) - 1
    if t.strategy == "append" or last in case.inits or before is None:
        return []
    try:
        run(case.batches[last], last + 2, False)
    except Exception as exc:
        return [Divergence(case, last, "error", f"idempotence re-run raised {exc!r}")]
    after = snapshot()
    if t.strategy == "replace":
        cols = [*t.columns, "_is_deleted"]
        a = sorted(repr({c: r[c] for c in cols}) for r in before)
        b = sorted(repr({c: r[c] for c in cols}) for r in after)
        same = a == b
    else:
        same = sorted(map(repr, before)) == sorted(map(repr, after))
    if same:
        return []
    detail = (
        "re-running the last batch (execution_id "
        f"{last + 2}) changed the table:\nbefore:\n{_fmt(before)}\nafter:\n{_fmt(after)}"
    )
    return [Divergence(case, last, "invariant", detail)]


def run_many(spark: Any, strategy: str, seeds: Iterable[int]) -> list[Divergence]:
    out: list[Divergence] = []
    for seed in seeds:
        out += run_case(spark, generate_case(seed, strategy))
    return out
