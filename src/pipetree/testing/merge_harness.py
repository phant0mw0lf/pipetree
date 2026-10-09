"""Run a generated ``Case`` through pipetree's real ``SparkAdapter.run_table`` and
compare the table with a model after every batch, plus invariants that hold
independently of the model (so a model bug cannot hide a table bug).

pyspark and the Spark adapter are imported lazily, inside the functions, so
this module can be imported without Spark.

Comparison (``normalise``): the business columns + ``_is_deleted`` +
``_execution_id`` (+ ``_is_current`` for scd2), rows in a canonical order
(key tuple with NULL first; scd2 versions by ``_valid_from``). Timestamps are
never compared by value, only by relation (invariants). The unknown-member
row is compared without its ``_execution_id``.

The model is the reference model (``merge_model.apply_batch``) by default. Any
object with ``apply_batch(table, state, batch, execution_id, *, init)`` and
``tolerates(invariant_name) -> bool`` can be passed instead (the conformance
tests pass a model with today's known pipetree quirks switched on, to check
that a known divergence is explained by exactly the deviations it is listed
under).

Modes: by default ``run_case`` stops at the first diverging batch (later
batches would mostly echo it). With ``attribution=True`` it runs the whole
history: after a diverging batch the model state is reset to the table's
state (resync) and comparison goes on, so independent later divergences are
reported too instead of being masked by the first one.
"""

from __future__ import annotations

import contextlib
import pprint
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from pipetree.testing.merge_gen import Case, generate_case
from pipetree.testing.merge_model import ModelTable, apply_batch, is_unknown_member

_INT_COLUMNS = {"a", "b", "w", "seq", "seen"}

# Names of the model-independent invariants (see `check_invariants`).
INVARIANTS = (
    "unknown_member_once",
    "unknown_member_unchanged",
    "updated_before_inserted",
    "identity_unique",
    "inserted_at_changed",
    "scd2_one_current",
    "scd2_valid_to_iff_current",
    "scd2_valid_to_before_from",
    "scd2_current_deleted",
    "scd2_current_not_latest",
    "scd2_overlap",
    "scd2_gap",
    "scd2_latest_closed",
    "scd2_closed_changed",
    "sid_null_or_duplicate",
    "sid_unknown_member",
    "sid_range",
    "sid_changed",
    "sid_reused",
    "idempotence",
)


class Model(Protocol):
    def apply_batch(
        self,
        table: ModelTable,
        state: list[dict],
        batch: list[dict],
        execution_id: int,
        *,
        init: bool,
    ) -> tuple[list[dict], Any]: ...

    def tolerates(self, invariant: str) -> bool: ...


class ReferenceModel:
    """The reference model; tolerates no invariant violation."""

    def apply_batch(self, table, state, batch, execution_id, *, init=False):
        # Looked up at call time, so tests can monkeypatch `merge_harness.apply_batch`.
        return apply_batch(table, state, batch, execution_id, init=init)

    def tolerates(self, invariant: str) -> bool:
        return False


@dataclass
class Divergence:
    case: Case
    batch_index: int
    kind: str  # rows | stats | invariant | error
    detail: str
    rerun: bool = False  # found on the idempotence re-run of the last batch

    def __str__(self) -> str:
        i = self.batch_index
        init = " (init)" if i in self.case.inits else ""
        where = f"batch {i}, execution_id {i + 1}{init}"
        if self.rerun:
            where = f"idempotence re-run of batch {i} (execution_id {i + 2})"
        out = [
            f"DIVERGENCE [{self.kind}] at {where}",
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


def _version_order(t: ModelTable, row: dict) -> tuple:
    """scd2 version order within a key, a TOTAL order: `_valid_from`, then the sequence (a late
    version shares its `_valid_from` with the next later one), then `_execution_id` and
    `_inserted_at` (late versions of equal sequence: the later batch is the later version;
    a late version is never modified, so its audit values are stable)."""
    return (
        _sort_value(row["_valid_from"]),
        tuple(_sort_value(row[c]) for c in t.sequence_by),
        _sort_value(row["_execution_id"]),
        _sort_value(row["_inserted_at"]),
    )


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
        ordered = sorted(raw, key=lambda r: (_key_sort(t, r), _version_order(t, r)))
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


def state_from_table(raw: list[dict], t: ModelTable) -> list[dict]:
    """The table snapshot as a model state (the resync of attribution mode): the
    compared columns, scd2 versions in ``_valid_from`` order."""
    cols = _compared_columns(t)
    if t.strategy == "scd2":
        ordered = sorted(raw, key=lambda r: (_key_sort(t, r), _version_order(t, r)))
    else:
        ordered = list(raw)
    return [{c: r[c] for c in cols} for r in ordered]


# ----------------------------------------------------------------- invariants


def _identity(t: ModelTable, row: dict) -> tuple:
    k = tuple(row[c] for c in t.key)
    # A late version (sequence_by) has an empty interval at the next version's _valid_from, so
    # (key, _valid_from) alone can repeat; _inserted_at never changes and tells them apart.
    return (k, row["_valid_from"], row["_inserted_at"]) if t.strategy == "scd2" else k


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
    sid_owner: dict[int, tuple],
) -> list[tuple[str, str]]:
    """Model-independent invariants of one snapshot (and its predecessor), as
    ``(invariant name, message)``. ``sid_owner`` (surrogate key -> the identity
    that first got it) is carried across the batches of a case and updated."""
    t = case.table
    problems: list[tuple[str, str]] = []
    init = batch_index in case.inits
    rebuilt = init or t.strategy == "replace"  # the table legitimately starts over
    scd = t.strategy in ("scd1", "scd2")

    unknown = _unknown_rows(t, raw)
    if len(unknown) > 1:
        problems.append(("unknown_member_once", f"unknown member exists {len(unknown)} times"))
    if prev is not None and not rebuilt:
        before = _unknown_rows(t, prev)
        if len(before) == 1 and before != unknown:
            problems.append(
                ("unknown_member_unchanged", f"unknown member changed: {before} -> {unknown}")
            )

    now_by_id: dict[tuple, dict] = {}
    if scd:
        counts: dict[tuple, int] = {}
        for r in raw:
            counts[_identity(t, r)] = counts.get(_identity(t, r), 0) + 1
            if r["_updated_at"] < r["_inserted_at"]:
                problems.append(("updated_before_inserted", f"_updated_at < _inserted_at: {r}"))
        what = "key, _valid_from, _inserted_at" if t.strategy == "scd2" else "key"
        problems += [
            ("identity_unique", f"{n} rows share ({what}) {i}") for i, n in counts.items() if n > 1
        ]
        now_by_id = _unique_by_identity(t, raw)
        if prev is not None and not init:
            for p in prev:
                n = now_by_id.get(_identity(t, p))
                if n is not None and n["_inserted_at"] != p["_inserted_at"]:
                    problems.append(("inserted_at_changed", f"_inserted_at changed: {p} -> {n}"))

    if t.strategy == "scd2":
        problems += _scd2_invariants(t, raw)
        if prev is not None and not init:
            for p in prev:
                if p["_is_current"]:
                    continue
                n = now_by_id.get(_identity(t, p))
                if n != p:
                    problems.append(("scd2_closed_changed", f"closed version changed: {p} -> {n}"))

    sk = case.surrogate_key
    if sk is not None:
        sids = [r[sk] for r in raw]
        if any(s is None for s in sids) or len(set(sids)) != len(sids):
            problems.append(
                (
                    "sid_null_or_duplicate",
                    f"surrogate keys not unique/non-NULL: {sorted(map(str, sids))}",
                )
            )
        for r in raw:
            is_unknown = r in unknown
            if is_unknown and r[sk] != -1:
                problems.append(("sid_unknown_member", f"unknown member sid is {r[sk]}, not -1"))
            if not is_unknown and (r[sk] is None or r[sk] < 1):
                problems.append(("sid_range", f"generated sid {r[sk]} < 1: {r}"))
        if prev is not None and not init:
            for p in prev:
                n = now_by_id.get(_identity(t, p))
                if n is not None and n[sk] != p[sk]:
                    problems.append(
                        ("sid_changed", f"sid changed {p[sk]} -> {n[sk]} for {_identity(t, p)}")
                    )
        for r in raw:
            s = r[sk]
            if s is None or r in unknown:
                continue
            owner = sid_owner.setdefault(s, _identity(t, r))
            if owner != _identity(t, r):
                problems.append(
                    ("sid_reused", f"sid {s} first belonged to {owner}, now to {_identity(t, r)}")
                )
    return problems


def _scd2_invariants(t: ModelTable, raw: list[dict]) -> list[tuple[str, str]]:
    problems: list[tuple[str, str]] = []
    by_key: dict[tuple, list[dict]] = {}
    for r in raw:
        by_key.setdefault(tuple(r[c] for c in t.key), []).append(r)
    for k, versions in by_key.items():
        versions.sort(key=lambda r: _version_order(t, r))
        if sum(bool(r["_is_current"]) for r in versions) > 1:
            problems.append(("scd2_one_current", f"key {k}: more than one current version"))
        for i, r in enumerate(versions):
            if r["_is_current"] != (r["_valid_to"] is None):
                problems.append(("scd2_valid_to_iff_current", f"key {k}: {r}"))
            if r["_valid_to"] is not None and r["_valid_to"] < r["_valid_from"]:
                problems.append(("scd2_valid_to_before_from", f"key {k}: {r}"))
            if r["_is_current"] and r["_is_deleted"]:
                problems.append(("scd2_current_deleted", f"key {k}: {r}"))
            if i + 1 < len(versions):
                nxt = versions[i + 1]
                if r["_is_current"]:
                    problems.append(
                        (
                            "scd2_current_not_latest",
                            f"key {k}: current version is not the latest: {r}",
                        )
                    )
                elif r["_is_deleted"]:
                    # deleted, then returned (a returning scd2 key opens a new version): a gap is
                    # allowed, an overlap not
                    if r["_valid_to"] > nxt["_valid_from"]:
                        problems.append(("scd2_overlap", f"key {k}: {r} / {nxt}"))
                elif r["_valid_to"] > nxt["_valid_from"]:
                    problems.append(("scd2_overlap", f"key {k}: {r} / {nxt}"))
                elif r["_valid_to"] < nxt["_valid_from"]:
                    problems.append(("scd2_gap", f"key {k}: {r} / {nxt}"))
            elif not r["_is_current"] and not r["_is_deleted"]:
                problems.append(("scd2_latest_closed", f"key {k}: closed without a delete: {r}"))
    return problems


# --------------------------------------------------------------- comparison


def _has_rerun(case: Case) -> bool:
    """Idempotence re-run: scd1/scd2/replace, unless the last batch is an init
    (a plain re-run of an init batch is a different operation, a merge)."""
    last = len(case.batches) - 1
    return case.table.strategy != "append" and last >= 0 and last not in case.inits


class _Comparator:
    """Compares table snapshots with a model, batch by batch."""

    def __init__(self, case: Case, model: Model, attribution: bool) -> None:
        self.case = case
        self.model = model
        self.attribution = attribution
        self.state: list[dict] = []
        self.prev: list[dict] | None = None
        self.sid_owner: dict[int, tuple] = {}

    def _invariants(self, i: int, problems: list[tuple[str, str]], rerun=False) -> list:
        shown = [f"[{name}] {msg}" for name, msg in problems if not self.model.tolerates(name)]
        if not shown:
            return []
        return [Divergence(self.case, i, "invariant", "\n".join(shown), rerun=rerun)]

    def step(self, i: int, raw: list[dict], result: dict) -> list[Divergence]:
        case, t = self.case, self.case.table
        self.state, stats = self.model.apply_batch(
            t, self.state, case.batches[i], i + 1, init=i in case.inits
        )
        out: list[Divergence] = []
        model_rows = normalise_model(self.state, t)
        table_rows = _normalise_raw(raw, t)
        if model_rows != table_rows:
            out.append(Divergence(case, i, "rows", _rows_detail(model_rows, table_rows)))

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
            out.append(Divergence(case, i, "stats", "\n".join(stat_problems)))

        out += self._invariants(i, check_invariants(case, i, self.prev, raw, self.sid_owner))
        if out and self.attribution:
            self.state = state_from_table(raw, t)  # resync, keep comparing later batches
        self.prev = raw
        return out

    def rerun(self, after: list[dict]) -> list[Divergence]:
        """The last batch re-run (execution id n+2): the table must equal the model
        re-applying it, and (model-independent) must not have changed at all."""
        case, t = self.case, self.case.table
        last = len(case.batches) - 1
        before = self.prev or []
        state, _ = self.model.apply_batch(t, self.state, case.batches[last], last + 2, init=False)
        out: list[Divergence] = []
        model_rows = normalise_model(state, t)
        table_rows = _normalise_raw(after, t)
        if model_rows != table_rows:
            out.append(
                Divergence(case, last, "rows", _rows_detail(model_rows, table_rows), rerun=True)
            )
        if t.strategy == "replace":
            cols = [*t.columns, "_is_deleted"]
            a = sorted(repr({c: r[c] for c in cols}) for r in before)
            b = sorted(repr({c: r[c] for c in cols}) for r in after)
            same = a == b
        else:  # scd1/scd2: not one value may change, not even a timestamp
            same = sorted(map(repr, before)) == sorted(map(repr, after))
        if not same:
            detail = (
                "re-running the last batch changed the table:\n"
                f"before:\n{_fmt(before)}\nafter:\n{_fmt(after)}"
            )
            out += self._invariants(last, [("idempotence", detail)], rerun=True)
        return out


# ----------------------------------------------------------------- Spark run


class _Runner:
    """Runs the batches of one case against a fresh table."""

    def __init__(self, spark: Any, case: Case, schema: str) -> None:
        from pipetree.adapters.spark.adapter import SparkAdapter
        from pipetree.model import System

        self.spark = spark
        self.case = case
        self.table = build_table(case, schema)
        self.ddl = _ddl(case)
        self._df: Any = None
        self.adapter = SparkAdapter(
            spark,
            systems={"gen": System(type="custom")},
            base_dir=".",
            read_source=lambda _table, _system: self._df,
        )
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        self.drop()

    def run(self, i: int, execution_id: int, init: bool) -> dict:
        cols = self.case.table.columns
        rows = [tuple(r[c] for c in cols) for r in self.case.batches[i]]
        self._df = self.spark.createDataFrame(rows, self.ddl)
        return self.adapter.run_table(self.table, execution_id=execution_id, init=init) or {}

    def snapshot(self) -> list[dict]:
        return [r.asDict() for r in self.spark.table(self.table.fqn).collect()]

    def drop(self) -> None:
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table.fqn}")

    def release(self) -> None:
        """Drop the table and free what Spark/Delta keep cached for it: over a
        few hundred cases in one JVM, cached snapshot blocks of dropped tables
        exhausted the default 1 GB driver heap."""
        self.drop()
        self.spark.catalog.clearCache()
        with contextlib.suppress(Exception):  # not Delta / a different Delta version
            self.spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()


@dataclass
class Recording:
    """Everything a run of a case produced: per batch the raw table snapshot and
    the `run_table` result, the snapshot after the idempotence re-run (None if
    the case has none), and the error that stopped the run (if any)."""

    snapshots: list[list[dict]]
    results: list[dict]
    rerun: list[dict] | None
    error: tuple[int, str] | None = None


def record_case(spark: Any, case: Case, schema: str = "mc") -> Recording:
    """Run the WHOLE history (and the idempotence re-run) and record it, so it can
    be compared with any number of models offline (`compare_recording`)."""
    rec = Recording([], [], None)
    runner = _Runner(spark, case, schema)
    try:
        for i in range(len(case.batches)):
            try:
                rec.results.append(runner.run(i, i + 1, i in case.inits))
            except Exception as exc:  # recorded: one case must not stop a run
                rec.error = (i, f"pipetree raised {exc!r}")
                return rec
            rec.snapshots.append(runner.snapshot())
        if _has_rerun(case):
            last = len(case.batches) - 1
            try:
                runner.run(last, last + 2, False)
            except Exception as exc:
                rec.error = (last, f"idempotence re-run raised {exc!r}")
                return rec
            rec.rerun = runner.snapshot()
        return rec
    finally:
        runner.release()


def compare_recording(
    case: Case, rec: Recording, model: Model | None = None, *, attribution: bool = False
) -> list[Divergence]:
    """`run_case` over a `Recording` (no Spark)."""
    comp = _Comparator(case, model or ReferenceModel(), attribution)
    out: list[Divergence] = []
    for i, (raw, result) in enumerate(zip(rec.snapshots, rec.results, strict=True)):
        divs = comp.step(i, raw, result)
        out += divs
        if divs and not attribution:
            return out
    if rec.error is not None:
        return [*out, Divergence(case, rec.error[0], "error", rec.error[1])]
    if rec.rerun is not None:
        out += comp.rerun(rec.rerun)
    return out


def run_case(
    spark: Any,
    case: Case,
    schema: str = "mc",
    *,
    model: Model | None = None,
    attribution: bool = False,
) -> list[Divergence]:
    """Run ``case`` batch by batch against ``model`` (default: the reference
    model). Default mode returns the divergences of the first diverging batch
    (or of the idempotence re-run); ``attribution=True`` runs the whole history,
    resyncing the model to the table after each divergence, and returns all."""
    comp = _Comparator(case, model or ReferenceModel(), attribution)
    runner = _Runner(spark, case, schema)
    out: list[Divergence] = []
    try:
        for i in range(len(case.batches)):
            try:
                result = runner.run(i, i + 1, i in case.inits)
            except Exception as exc:  # reported, not raised: one case must not stop a run
                return [*out, Divergence(case, i, "error", f"pipetree raised {exc!r}")]
            divs = comp.step(i, runner.snapshot(), result)
            out += divs
            if divs and not attribution:
                return out
        if _has_rerun(case):
            last = len(case.batches) - 1
            try:
                runner.run(last, last + 2, False)
            except Exception as exc:
                return [*out, Divergence(case, last, "error", f"re-run raised {exc!r}", True)]
            out += comp.rerun(runner.snapshot())
        return out
    finally:
        runner.release()


def model_trajectory(case: Case, model: Model | None = None) -> list:
    """What ``model`` predicts for the whole history (no Spark): per batch the
    normalised rows and the two stats, then the normalised rows after the
    idempotence re-run. Two models with equal trajectories cannot be told apart
    by the table comparison of a case."""
    model = model or ReferenceModel()
    t = case.table
    state: list[dict] = []
    out: list = []
    for i, batch in enumerate(case.batches):
        state, stats = model.apply_batch(t, state, batch, i + 1, init=i in case.inits)
        out.append((normalise_model(state, t), stats.duplicates_dropped, stats.null_keys_dropped))
    if _has_rerun(case):
        last = len(case.batches) - 1
        state, _ = model.apply_batch(t, state, case.batches[last], last + 2, init=False)
        out.append(normalise_model(state, t))
    return out


def run_many(spark: Any, strategy: str, seeds: Iterable[int]) -> list[Divergence]:
    out: list[Divergence] = []
    for seed in seeds:
        out += run_case(spark, generate_case(seed, strategy))
    return out
