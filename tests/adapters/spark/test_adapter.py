import pytest

from pipetree.adapters.spark.adapter import SparkAdapter
from pipetree.model import System, Table

pytestmark = pytest.mark.spark


def make_table(**overrides) -> Table:
    defaults = {
        "name": "customer",
        "layer": "adaptertest",
        "table_schema": "adaptertest",
        "fqn": "adaptertest.customer",
        "strategy": "scd1",
        "business_key": ["id"],
    }
    defaults.update(overrides)
    return Table.model_validate(defaults)


class FakeCustomSourceReader:
    """A stand-in for a customer's own SourceReader (e.g. for Microsoft
    Graph), loaded by dotted path from `systems.<name>.class`."""

    def read(self, ctx):
        return ctx.spark.createDataFrame([(1, "FromCustomReader")], ["id", "name"])


class FakePlatform:
    """Records what it's asked to resolve, so tests can prove the adapter
    actually consults the platform rather than working around it."""

    name = "fake"

    def __init__(self, secrets: dict[str, str] | None = None) -> None:
        self._secrets = secrets or {}
        self.resolved_paths: list[tuple[str, object]] = []

    def resolve_secret(self, name: str) -> str:
        return self._secrets[name]

    def qualify_table_name(self, fqn: str) -> str:
        return fqn

    def resolve_path(self, relative_path, base_dir):
        self.resolved_paths.append((relative_path, base_dir))
        return str(base_dir / relative_path)

    def run_metadata(self) -> dict[str, str]:
        return {}

    def acquire_token(self, resource: str) -> str:
        return "fake-token"


def test_capabilities_declares_atomic_append_retry_support(spark, tmp_path):
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)
    assert adapter.capabilities.supports_delete_by_execution_id is True


def test_defaults_to_the_local_platform(spark, tmp_path):
    from pipetree.platform.local import LocalPlatform

    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    assert isinstance(adapter._platform, LocalPlatform)  # noqa: SLF001 - white-box sanity check


def test_read_local_file_source_resolves_the_path_through_the_platform(spark, tmp_path):
    (tmp_path / "customer.csv").write_text("id,name\n1,Alice\n")
    system = System.model_validate({"type": "csv", "path": "customer.csv"})
    table = make_table(
        source={"system": "crm", "object": "customer"}, strategy="scd1", fqn="bronze_f.customer"
    )
    platform = FakePlatform()
    adapter = SparkAdapter(spark, systems={"crm": system}, base_dir=tmp_path, platform=platform)

    adapter.run_table(table, execution_id=1)

    assert platform.resolved_paths == [("customer.csv", tmp_path)]


def test_read_local_file_source_resolves_a_secret_path(spark, tmp_path):
    (tmp_path / "customer.csv").write_text("id,name\n1,Alice\n")
    system = System.model_validate({"type": "csv", "path": {"secret": "customer-csv-path"}})
    table = make_table(
        source={"system": "crm", "object": "customer"}, strategy="scd1", fqn="bronze_g.customer"
    )
    platform = FakePlatform(secrets={"customer-csv-path": "customer.csv"})
    adapter = SparkAdapter(spark, systems={"crm": system}, base_dir=tmp_path, platform=platform)

    result = adapter.run_table(table, execution_id=1)

    assert result is not None
    assert result["rows_written"] == 1


def test_run_table_reads_a_source_table_via_read_source_and_merges(spark, tmp_path):
    system = System.model_validate({"type": "csv", "path": "customer.csv"})
    csv_path = tmp_path / "customer.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n")

    table = make_table(
        source={"system": "crm", "object": "customer"}, strategy="scd1", fqn="bronze_a.customer"
    )
    adapter = SparkAdapter(spark, systems={"crm": system}, base_dir=tmp_path)

    result = adapter.run_table(table, execution_id=1)

    assert result is not None
    written = spark.table(table.fqn)
    assert result["rows_written"] == 2
    assert {r["name"] for r in written.collect()} == {"Alice", "Bob"}
    assert {r["_source_system"] for r in written.collect()} == {"crm"}


def test_run_table_dispatches_a_custom_system_type_by_dotted_class(spark, tmp_path):
    system = System.model_validate(
        {
            "type": "custom",
            "class": "tests.adapters.spark.test_adapter.FakeCustomSourceReader",
        }
    )
    table = make_table(
        source={"system": "graph", "object": "whatever"},
        strategy="replace",
        fqn="bronze_custom.whatever",
    )
    adapter = SparkAdapter(spark, systems={"graph": system}, base_dir=tmp_path)

    result = adapter.run_table(table, execution_id=1)

    assert result is not None
    assert spark.table(table.fqn).collect()[0]["name"] == "FromCustomReader"


def test_run_table_raises_a_clear_error_for_an_unsupported_system_type(spark, tmp_path):
    system = System(type="carrier_pigeon")
    table = make_table(
        source={"system": "hr", "object": "employee"}, strategy="scd1", fqn="bronze_b.employee"
    )
    adapter = SparkAdapter(spark, systems={"hr": system}, base_dir=tmp_path)

    with pytest.raises(KeyError, match="carrier_pigeon"):
        adapter.run_table(table, execution_id=1)


def test_run_table_executes_a_sql_logic_file(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_c")
    spark.createDataFrame([(1, "Alice")], ["id", "name"]).write.format("delta").mode(
        "overwrite"
    ).saveAsTable("bronze_c.customer")

    logic_path = tmp_path / "silver.sql"
    logic_path.write_text("SELECT id, name FROM bronze_c.customer")
    table = make_table(
        logic="silver.sql", strategy="replace", fqn="silver_c.customer_enriched", source=None
    )
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    result = adapter.run_table(table, execution_id=1)

    assert result is not None
    assert result["rows_written"] == 1
    assert spark.table(table.fqn).collect()[0]["name"] == "Alice"


def test_run_table_executes_a_pyspark_logic_file_assigning_result(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_d")
    spark.createDataFrame([(1, "Bob")], ["id", "name"]).write.format("delta").mode(
        "overwrite"
    ).saveAsTable("bronze_d.customer")

    logic_path = tmp_path / "silver.py"
    logic_path.write_text(
        "customer = spark.table('bronze_d.customer')\n"
        "result = customer.selectExpr('id', 'upper(name) AS name')\n"
    )
    table = make_table(
        logic="silver.py", strategy="replace", fqn="silver_d.customer_enriched", source=None
    )
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    adapter.run_table(table, execution_id=1)

    assert spark.table(table.fqn).collect()[0]["name"] == "BOB"


def test_pyspark_logic_file_without_a_result_variable_raises_a_clear_error(spark, tmp_path):
    logic_path = tmp_path / "bad.py"
    logic_path.write_text("x = 1\n")
    table = make_table(logic="bad.py", strategy="replace", fqn="silver_e.x", source=None)
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    with pytest.raises(ValueError, match="result"):
        adapter.run_table(table, execution_id=1)


def test_run_table_seeds_unknown_member_when_requested(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold_a")
    logic_path = tmp_path / "dim.sql"
    logic_path.write_text("SELECT 1 AS id, 'Alice' AS name")
    table = make_table(
        logic="dim.sql",
        strategy="scd2",
        fqn="gold_a.dim_customer",
        source=None,
        unknown_member=True,
    )
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    adapter.run_table(table, execution_id=1)

    ids = {r["id"] for r in spark.table(table.fqn).collect()}
    assert ids == {1, -1}


def test_delete_by_execution_id_removes_only_rows_from_that_run(spark, tmp_path):
    table = make_table(strategy="append", fqn="applog.events")
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    from pipetree.adapters.spark.merge import merge_append

    merge_append(spark, table, spark.createDataFrame([(1, "a")], ["id", "name"]), 1, "sys")
    merge_append(spark, table, spark.createDataFrame([(2, "b")], ["id", "name"]), 2, "sys")

    adapter.delete_by_execution_id(table, execution_id=1)

    remaining = {r["id"] for r in spark.table(table.fqn).collect()}
    assert remaining == {2}


def test_delete_by_execution_id_is_a_noop_when_the_table_does_not_exist(spark, tmp_path):
    table = make_table(strategy="append", fqn="applog_missing.events")
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    adapter.delete_by_execution_id(table, execution_id=1)  # must not raise


def test_run_table_passes_init_through_to_the_merge_strategy(spark, tmp_path):
    table = make_table(strategy="scd1", fqn="initflag.customer")
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    from pipetree.adapters.spark.merge import merge_scd1

    merge_scd1(
        spark, table, spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"]), 1, "sys"
    )

    logic_path = tmp_path / "reload.sql"
    logic_path.write_text("SELECT 3 AS id, 'Carol' AS name")
    reload_table = make_table(
        strategy="scd1", fqn="initflag.customer", logic="reload.sql", source=None
    )

    adapter.run_table(reload_table, execution_id=2, init=True)

    ids = {r["id"] for r in spark.table(table.fqn).collect()}
    assert ids == {3}  # a full reload, not a merge against the existing rows


# ------------------------------------------------- reserved audit columns
#
# A downstream table whose logic is `SELECT *` from an upstream pipetree
# table inherits the upstream's audit columns. pipetree stamps its own, so
# without stripping them first every strategy failed with
# COLUMN_ALREADY_EXISTS (`_execution_id`) on the first write.

_RESERVED = {
    "_inserted_at",
    "_updated_at",
    "_is_deleted",
    "_execution_id",
    "_source_system",
    "_valid_from",
    "_valid_to",
    "_is_current",
}


def _write_upstream(spark, tmp_path, fqn: str, strategy: str, select_sql: str) -> SparkAdapter:
    """Write `fqn` through pipetree itself, so it carries real audit columns."""
    (tmp_path / "upstream.sql").write_text(select_sql)
    upstream = make_table(logic="upstream.sql", strategy=strategy, fqn=fqn, source=None)
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)
    adapter.run_table(upstream, execution_id=1)
    return adapter


@pytest.mark.parametrize("strategy", ["replace", "scd1", "scd2", "append"])
def test_select_star_from_a_pipetree_table_strips_its_audit_columns(spark, tmp_path, strategy):
    upstream_fqn = f"bronze_audit_{strategy}.customer"
    adapter = _write_upstream(
        spark, tmp_path, upstream_fqn, "scd1", "SELECT 1 AS id, 'Alice' AS name"
    )
    (tmp_path / "downstream.sql").write_text(f"SELECT * FROM {upstream_fqn}")
    downstream = make_table(
        logic="downstream.sql",
        strategy=strategy,
        fqn=f"silver_audit_{strategy}.customer",
        source=None,
    )

    # Twice: the first run seeds/overwrites, the second exercises the real
    # merge (scd1/scd2) or the append path against an existing table.
    adapter.run_table(downstream, execution_id=2)
    adapter.run_table(downstream, execution_id=3)

    written = spark.table(downstream.fqn)
    columns = written.columns
    assert len(columns) == len(set(columns))
    business = [c for c in columns if c not in _RESERVED]
    assert business == ["id", "name"]
    # The downstream table's stamps are its own, not the upstream's.
    assert {r["_execution_id"] for r in written.collect()} <= {2, 3}
    assert {r["_execution_id"] for r in written.collect()} != {1}


def test_scd2_reserved_columns_from_an_upstream_scd2_table_are_stripped_for_scd1(spark, tmp_path):
    upstream_fqn = "bronze_audit_scd2up.customer"
    adapter = _write_upstream(
        spark, tmp_path, upstream_fqn, "scd2", "SELECT 1 AS id, 'Alice' AS name"
    )
    assert {"_valid_from", "_valid_to", "_is_current"} <= set(spark.table(upstream_fqn).columns)

    (tmp_path / "downstream.sql").write_text(f"SELECT * FROM {upstream_fqn}")
    downstream = make_table(
        logic="downstream.sql", strategy="scd1", fqn="silver_audit_scd2up.customer", source=None
    )
    adapter.run_table(downstream, execution_id=2)
    adapter.run_table(downstream, execution_id=3)

    columns = spark.table(downstream.fqn).columns
    assert not {"_valid_from", "_valid_to", "_is_current"} & set(columns)
    assert len(columns) == len(set(columns))


def test_underscore_prefixed_user_columns_that_are_not_reserved_survive(spark, tmp_path):
    upstream_fqn = "bronze_audit_underscore.customer"
    adapter = _write_upstream(
        spark,
        tmp_path,
        upstream_fqn,
        "scd1",
        "SELECT 1 AS id, 'Alice' AS name, 'eu' AS _region, true AS _is_deleted_src",
    )
    (tmp_path / "downstream.sql").write_text(f"SELECT * FROM {upstream_fqn}")
    downstream = make_table(
        logic="downstream.sql",
        strategy="scd1",
        fqn="silver_audit_underscore.customer",
        source=None,
    )
    adapter.run_table(downstream, execution_id=2)
    adapter.run_table(downstream, execution_id=3)

    row = spark.table(downstream.fqn).collect()[0]
    assert row["_region"] == "eu"
    assert row["_is_deleted_src"] is True


def test_a_source_without_reserved_columns_is_unchanged(spark, tmp_path):
    (tmp_path / "plain.sql").write_text("SELECT 1 AS id, 'Alice' AS name")
    table = make_table(logic="plain.sql", strategy="scd1", fqn="silver_audit_plain.c", source=None)
    adapter = SparkAdapter(spark, systems={}, base_dir=tmp_path)

    adapter.run_table(table, execution_id=1)
    result = adapter.run_table(table, execution_id=2)

    assert result is not None
    assert not any("added" in c for c in result.get("schema_changes", []))
    assert spark.table(table.fqn).columns[:2] == ["id", "name"]
    written = spark.table(table.fqn).collect()
    assert [(r["id"], r["name"]) for r in written] == [(1, "Alice")]


def test_a_source_reader_returning_reserved_columns_is_stripped_too(spark, tmp_path):
    def read_source(table, system):
        return spark.createDataFrame(
            [(1, "Alice", 99, "elsewhere")], ["id", "name", "_execution_id", "_source_system"]
        )

    system = System.model_validate({"type": "csv", "path": "unused.csv"})
    table = make_table(
        source={"system": "crm", "object": "customer"},
        strategy="replace",
        fqn="bronze_audit_reader.customer",
    )
    adapter = SparkAdapter(
        spark, systems={"crm": system}, base_dir=tmp_path, read_source=read_source
    )

    adapter.run_table(table, execution_id=7)

    row = spark.table(table.fqn).collect()[0]
    assert row["_execution_id"] == 7
    assert row["_source_system"] == "crm"


def test_an_aliased_upstream_stamp_still_drives_delete_when(spark, tmp_path):
    upstream_fqn = "bronze_audit_alias.customer"
    adapter = _write_upstream(
        spark, tmp_path, upstream_fqn, "scd1", "SELECT 1 AS id, 'Alice' AS name"
    )
    spark.sql(f"INSERT INTO {upstream_fqn} SELECT 2, 'Bob', 0L, 0L, true, 1L, CAST(NULL AS STRING)")
    (tmp_path / "downstream.sql").write_text(
        f"SELECT *, _is_deleted AS upstream_is_deleted FROM {upstream_fqn}"
    )
    downstream = make_table(
        logic="downstream.sql",
        strategy="scd1",
        fqn="silver_audit_alias.customer",
        source=None,
        merge={"delete_when": "upstream_is_deleted = true", "delete_mode": "hard"},
    )

    adapter.run_table(downstream, execution_id=2)

    assert {r["id"] for r in spark.table(downstream.fqn).collect()} == {1}
