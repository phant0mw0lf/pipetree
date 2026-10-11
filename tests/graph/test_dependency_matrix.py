"""The specification of `depends_on: auto`, as one table.

Each row is (id, language, source text, expected). `expected` is the set of
names the parser reports, or `Err(fragment)` when the source must be refused
with a message containing the fragment. Names come back as written (quotes
stripped, parts joined by dots); matching them to configured tables is the
graph builder's job (see test_builder.py).
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from pipetree.graph.parsers import parse_pyspark_dependencies, parse_sql_dependencies


@dataclass(frozen=True)
class Err:
    fragment: str


def _row(case_id: str, language: str, source: str, expected: set[str] | Err):
    return pytest.param(language, source, expected, id=case_id)


def sql(case_id: str, source: str, *expected: str):
    return _row(f"sql-{case_id}", "sql", source, set(expected))


def sql_err(case_id: str, source: str, fragment: str):
    return _row(f"sql-{case_id}", "sql", source, Err(fragment))


def py(case_id: str, source: str, *expected: str):
    return _row(f"py-{case_id}", "py", source, set(expected))


def py_err(case_id: str, source: str, fragment: str):
    return _row(f"py-{case_id}", "py", source, Err(fragment))


CASES = [
    # -- plain reads and name shapes ------------------------------------
    sql("plain-from", "select * from a", "a"),
    sql("two-part", "select * from s.a", "s.a"),
    sql("three-part", "select * from c.s.a", "c.s.a"),
    sql("backticks", "select * from `s`.`a`", "s.a"),
    sql("backticks-three-part", "select * from `c`.`s`.`T`", "c.s.T"),
    sql("backticks-special-chars", "select * from `my-cat`.`s`.a", "my-cat.s.a"),
    sql("mixed-case", "SELECT * FROM Silver.Orders", "Silver.Orders"),
    sql("no-table", "select 1 as x"),
    sql("empty", ""),
    sql("only-comment", "-- nothing here"),
    # -- joins ------------------------------------------------------------
    sql("join-inner", "select * from a join b on a.id = b.id", "a", "b"),
    sql("join-inner-keyword", "select * from a inner join b on a.id = b.id", "a", "b"),
    sql("join-left", "select * from a left join b on a.id = b.id", "a", "b"),
    sql("join-left-outer", "select * from a left outer join b on a.id = b.id", "a", "b"),
    sql("join-right", "select * from a right join b on a.id = b.id", "a", "b"),
    sql("join-full", "select * from a full join b on a.id = b.id", "a", "b"),
    sql("join-full-outer", "select * from a full outer join b on a.id = b.id", "a", "b"),
    sql("join-cross", "select * from a cross join b", "a", "b"),
    sql("join-left-semi", "select * from a left semi join b on a.id = b.id", "a", "b"),
    sql("join-semi", "select * from a semi join b on a.id = b.id", "a", "b"),
    sql("join-left-anti", "select * from a left anti join b on a.id = b.id", "a", "b"),
    sql("join-anti", "select * from a anti join b on a.id = b.id", "a", "b"),
    sql("join-natural", "select * from a natural join b", "a", "b"),
    sql("join-using", "select * from a join b using (id)", "a", "b"),
    sql(
        "join-chain",
        "select * from a join b on 1=1 left join c on 1=1 join d using (k)",
        "a",
        "b",
        "c",
        "d",
    ),
    sql("comma-join", "select * from a, b", "a", "b"),
    sql("comma-join-aliases", "select * from a x, s.b as y, c", "a", "s.b", "c"),
    sql("self-join", "select * from a x join a y on x.id = y.parent", "a"),
    sql("alias-with-as", "select t.id from s.a as t", "s.a"),
    sql("alias-without-as", "select t.id from s.a t", "s.a"),
    sql("alias-equal-to-table-name", "select * from a a join b b on a.id = b.id", "a", "b"),
    # -- subqueries and set operations ---------------------------------------
    sql("subquery-in-from", "select * from (select * from a) q", "a"),
    sql("subquery-in-join", "select * from a join (select * from b) q on 1=1", "a", "b"),
    sql("scalar-subquery-select", "select (select max(x) from b) as m from a", "a", "b"),
    sql("scalar-subquery-where", "select * from a where x = (select max(y) from b)", "a", "b"),
    sql("in-subquery", "select * from a where x in (select y from b)", "a", "b"),
    sql("not-in-subquery", "select * from a where x not in (select y from b)", "a", "b"),
    sql(
        "exists-subquery",
        "select * from a where exists (select 1 from b where b.k = a.k)",
        "a",
        "b",
    ),
    sql(
        "not-exists-subquery",
        "select * from a where not exists (select 1 from b where b.k = a.k)",
        "a",
        "b",
    ),
    sql(
        "subquery-in-join-condition",
        "select * from a join b on exists (select 1 from c)",
        "a",
        "b",
        "c",
    ),
    sql(
        "subquery-in-having",
        "select k from a group by k having count(*) > (select count(*) from b)",
        "a",
        "b",
    ),
    sql(
        "subquery-in-case",
        "select case when x in (select y from b) then 1 else 0 end from a",
        "a",
        "b",
    ),
    sql(
        "nested-subqueries",
        "select * from (select * from (select * from (select * from a) x join b on 1=1) y) z",
        "a",
        "b",
    ),
    sql("union", "select * from a union select * from b", "a", "b"),
    sql("union-all", "select * from a union all select * from b", "a", "b"),
    sql("intersect", "select * from a intersect select * from b", "a", "b"),
    sql("except", "select * from a except select * from b", "a", "b"),
    sql(
        "set-operations-mixed",
        "select * from a union select * from b intersect select * from c except select * from d",
        "a",
        "b",
        "c",
        "d",
    ),
    sql(
        "union-in-subquery", "select * from (select * from a union all select * from b) u", "a", "b"
    ),
    # -- CTEs ---------------------------------------------------------------
    sql("cte-basic", "with c as (select * from a) select * from c", "a"),
    sql(
        "cte-chain",
        "with c1 as (select * from a), c2 as (select * from c1 join b on 1=1) select * from c2",
        "a",
        "b",
    ),
    sql("cte-name-is-not-a-table", "with orders as (select 1 as x) select * from orders"),
    sql(
        "cte-shadows-real-table",
        "with orders as (select * from bronze.orders) select * from orders",
        "bronze.orders",
    ),
    sql("cte-reads-table-of-same-name", "with a as (select * from a) select * from a", "a"),
    sql("cte-name-case-insensitive", "with C as (select * from a) select * from c", "a"),
    sql(
        "cte-recursive",
        "with recursive r as (select 1 as n union all select n + 1 from r where n < 5) "
        "select * from r",
    ),
    sql(
        "cte-recursive-with-table",
        "with recursive r as (select id from a union all select id from r join b on 1=1) "
        "select * from r",
        "a",
        "b",
    ),
    sql(
        "cte-with-set-operation",
        "with c as (select * from a union all select * from b) select * from c",
        "a",
        "b",
    ),
    sql(
        "cte-in-subquery",
        "select * from a where x in (with q as (select y from b) select y from q)",
        "a",
        "b",
    ),
    sql("cte-unused", "with c as (select * from a) select * from b", "a", "b"),
    sql(
        "cte-joined-with-alias", "with c as (select * from a) select * from c x join b y", "a", "b"
    ),
    sql("cte-before-insert", "with c as (select * from a) insert into x select * from c", "x", "a"),
    # -- lateral, table-valued functions, inline tables ----------------------
    sql("lateral-view", "select * from a lateral view explode(a.arr) t as x", "a"),
    sql("lateral-view-outer", "select * from a lateral view outer explode(a.arr) t as x", "a"),
    sql(
        "lateral-subquery", "select * from a, lateral (select * from b where b.k = a.k) l", "a", "b"
    ),
    sql(
        "join-lateral", "select * from a join lateral (select * from b where b.k = a.k) l", "a", "b"
    ),
    sql("tvf-range", "select * from range(10)"),
    sql("tvf-explode", "select * from explode(array(1, 2))"),
    sql("tvf-inline", "select * from inline(array(struct(1, 2)))"),
    sql("tvf-in-join", "select * from a join range(3) r on 1=1", "a"),
    sql("values", "select * from values (1), (2) as t(x)"),
    sql("values-in-join", "select * from a join (values (1)) v(x) on 1=1", "a"),
    sql("table-function-wrapper", "select * from table(a)", "a"),
    sql("stream-function", "select * from stream(a)", "a"),
    # -- table clauses and hints ---------------------------------------------
    sql("timestamp-as-of", "select * from a timestamp as of '2024-01-01'", "a"),
    sql("version-as-of", "select * from a version as of 3", "a"),
    sql("tablesample-percent", "select * from a tablesample (10 percent)", "a"),
    sql("tablesample-rows-alias", "select * from a tablesample (5 rows) t", "a"),
    sql("hint-broadcast", "select /*+ BROADCAST(b) */ * from a join b on a.id = b.id", "a", "b"),
    sql(
        "hint-names-are-not-tables",
        "select /*+ BROADCAST(x), MERGE(y) */ * from a join b on a.id = b.id",
        "a",
        "b",
    ),
    # -- path tables are not dependencies ------------------------------------
    sql("path-delta", "select * from delta.`/mnt/lake/x`"),
    sql("path-parquet", "select * from parquet.`/mnt/lake/x`"),
    sql("path-abfss", "select * from delta.`abfss://c@acct.dfs.core.windows.net/x`"),
    sql("path-joined-with-table", "select * from a join delta.`/mnt/x` p on 1=1", "a"),
    # -- comments, strings, statements ---------------------------------------
    sql("line-comment", "select * from a -- join b", "a"),
    sql("line-comment-before", "-- from z\nselect * from a", "a"),
    sql("block-comment", "select * from a /* from b join c */", "a"),
    sql("block-comment-middle", "select * /* join z */ from a", "a"),
    sql("string-literal-keywords", "select 'from x join y' as s from a", "a"),
    sql("string-literal-escaped-quote", "select 'it''s from x' as s from a", "a"),
    sql("string-literal-cross-apply", "select 'cross apply b' as s from a", "a"),
    sql("multi-statement", "select * from a; select * from b", "a", "b"),
    sql("multi-statement-trailing-semicolon", "select * from a;\nselect * from b;\n", "a", "b"),
    sql("multi-statement-empty-statements", "select * from a;; ;select * from b", "a", "b"),
    sql("set-statement", "set spark.sql.shuffle.partitions = 8; select * from a", "a"),
    sql(
        "window-group-order",
        "select a.x, row_number() over (partition by a.k order by a.d) as rn "
        "from a group by a.x, a.k, a.d order by a.x",
        "a",
    ),
    sql("column-references", "select a.col, b.col from a join b on a.id = b.id", "a", "b"),
    sql("struct-field-access", "select a.col.sub from a", "a"),
    sql("function-calls", "select coalesce(a.x, 0), upper(a.y) from a", "a"),
    # -- views local to the file ---------------------------------------------
    sql(
        "temp-view-is-local",
        "create or replace temp view v as select * from a; select * from v",
        "a",
    ),
    sql(
        "temporary-view-is-local",
        "create or replace temporary view v as select * from a; select * from v",
        "a",
    ),
    sql("view-is-local", "create view v as select * from a; select * from v", "a"),
    sql(
        "view-chain",
        "create temp view v1 as select * from a; create temp view v2 as select * from v1 "
        "join b on 1=1; select * from v2",
        "a",
        "b",
    ),
    sql(
        "view-read-before-definition",
        "select * from v; create temp view v as select * from a",
        "v",
        "a",
    ),
    sql(
        "view-name-case-insensitive", "create temp view V as select * from a; select * from v", "a"
    ),
    sql(
        "view-redefinition-reads-table-of-same-name",
        "create or replace temp view a as select * from a; select * from a",
        "a",
    ),
    # -- writes: the block's own table is reported, the builder ignores it ---------
    sql("ctas", "create or replace table x as select * from a", "x", "a"),
    sql("ctas-if-not-exists", "create table if not exists s.x as select * from a", "s.x", "a"),
    sql(
        "ctas-using-delta",
        "create or replace table x using delta partitioned by (d) as select * from a",
        "x",
        "a",
    ),
    sql("insert-overwrite", "insert overwrite x select * from a", "x", "a"),
    sql("insert-overwrite-table", "insert overwrite table x select * from a", "x", "a"),
    sql("insert-into", "insert into x select * from a", "x", "a"),
    sql("insert-into-columns", "insert into x (c1, c2) select c1, c2 from a", "x", "a"),
    sql("insert-values", "insert into x values (1, 2)", "x"),
    sql(
        "merge-using-table",
        "merge into x using s on x.id = s.id when matched then update set * "
        "when not matched then insert *",
        "x",
        "s",
    ),
    sql(
        "merge-using-alias",
        "merge into x t using s on t.id = s.id when matched then delete",
        "x",
        "s",
    ),
    sql(
        "merge-using-subquery",
        "merge into x using (select * from s) u on x.id = u.id when matched then delete",
        "x",
        "s",
    ),
    sql("create-table-like", "create table t like src", "t", "src"),
    sql("create-table-shallow-clone", "create table t shallow clone src", "t", "src"),
    sql("create-table-deep-clone", "create table t deep clone src", "t", "src"),
    sql("drop-then-create", "drop table if exists x; create table x as select * from a", "x", "a"),
    # -- not Spark SQL, or not SQL -------------------------------------------
    sql_err("cross-apply", "select * from a cross apply b", "not Spark SQL"),
    sql_err("outer-apply", "select * from a outer apply (select 1 as x) b", "not Spark SQL"),
    sql_err("unparseable", "select from where", "depends_on"),
    sql_err("unclosed-paren", "select * from (select * from a", "depends_on"),
    sql_err("garbage", "this is not sql at all", "depends_on"),
    # -- PySpark: reads -------------------------------------------------------
    py("table-literal", 'df = spark.table("silver.orders")', "silver.orders"),
    py("table-single-quotes", "df = spark.table('silver.orders')", "silver.orders"),
    py("table-bare-name", 'df = spark.table("orders")', "orders"),
    py("table-three-part", 'df = spark.table("main.silver.orders")', "main.silver.orders"),
    py("read-table", 'df = spark.read.table("silver.orders")', "silver.orders"),
    py(
        "read-format-table",
        'df = spark.read.format("delta").table("silver.orders")',
        "silver.orders",
    ),
    py("readstream-table", 'df = spark.readStream.table("silver.orders")', "silver.orders"),
    py("deltatable-forname", 'dt = DeltaTable.forName(spark, "silver.orders")', "silver.orders"),
    py(
        "deltatable-forname-qualified",
        'dt = delta.tables.DeltaTable.forName(spark, "silver.orders")',
        "silver.orders",
    ),
    py("receiver-attribute-named-spark", 'df = self.spark.table("silver.orders")', "silver.orders"),
    py("several-reads", 'a = spark.table("x.a")\nb = spark.table("x.b")', "x.a", "x.b"),
    py("same-table-twice", 'a = spark.table("x.a")\nb = spark.table("x.a")', "x.a"),
    py("read-in-function", 'def load():\n    return spark.table("x.a")', "x.a"),
    py("read-in-loop", 'for _ in range(2):\n    df = spark.table("x.a")', "x.a"),
    py("read-in-comprehension", 'dfs = [spark.table("x.a") for _ in range(2)]', "x.a"),
    py(
        "read-in-class-method",
        'class C:\n    def m(self):\n        return spark.table("x.a")',
        "x.a",
    ),
    py(
        "read-after-other-code",
        'import os\nx = 1\ndef f():\n    pass\ndf = spark.table("x.a").filter("a > 1")',
        "x.a",
    ),
    py("read-literal-concatenation", 'df = spark.table("silver." + "orders")', "silver.orders"),
    py("no-spark-calls", "x = 1 + 2"),
    py("empty-file", ""),
    # -- PySpark: spark.sql ---------------------------------------------------
    py("sql-literal", 'df = spark.sql("select * from silver.orders")', "silver.orders"),
    py(
        "sql-triple-quoted",
        'df = spark.sql("""\n    select * from silver.orders o\n    join gold.dim d on 1=1\n""")',
        "silver.orders",
        "gold.dim",
    ),
    py(
        "sql-implicit-concatenation", 'df = spark.sql("select * from a " "join b on 1=1")', "a", "b"
    ),
    py(
        "sql-parenthesised-concatenation",
        'df = spark.sql(\n    "select * from a "\n    "join b on 1=1"\n)',
        "a",
        "b",
    ),
    py("sql-plus-concatenation", 'df = spark.sql("select * from " + "a")', "a"),
    py("sql-keyword-argument", 'df = spark.sql(sqlQuery="select * from a")', "a"),
    py("sql-with-cte", 'df = spark.sql("with c as (select * from a) select * from c")', "a"),
    py("sql-chained-write", 'spark.sql("select * from a").write.saveAsTable("x.out")', "a"),
    py("sql-module-constant", 'Q = "select * from a"\ndf = spark.sql(Q)', "a"),
    py("sql-in-function", 'def f():\n    return spark.sql("select * from a")', "a"),
    py("sql-in-loop", 'for _ in range(2):\n    spark.sql("select * from a")', "a"),
    py("sql-in-comprehension", 'x = [spark.sql("select * from a") for _ in range(2)]', "a"),
    py("sql-multi-statement-string", 'spark.sql("select * from a; select * from b")', "a", "b"),
    py("sql-literal-without-tables", 'spark.sql("select 1")'),
    # -- PySpark: constants and f-strings --------------------------------------
    py(
        "fstring-module-constant",
        'LAYER = "silver"\ndf = spark.sql(f"select * from {LAYER}.orders")',
        "silver.orders",
    ),
    py(
        "fstring-constant-in-function",
        'LAYER = "silver"\ndef f():\n    return spark.sql(f"select * from {LAYER}.orders")',
        "silver.orders",
    ),
    py(
        "fstring-two-constants",
        'L = "silver"\nT = "orders"\ndf = spark.table(f"{L}.{T}")',
        "silver.orders",
    ),
    py("fstring-without-placeholders", 'df = spark.table(f"silver.orders")', "silver.orders"),
    py("table-module-constant", 'SRC = "silver.orders"\ndf = spark.table(SRC)', "silver.orders"),
    py(
        "table-annotated-constant",
        'SRC: str = "silver.orders"\ndf = spark.table(SRC)',
        "silver.orders",
    ),
    py(
        "table-concatenated-constant",
        'SRC = "silver." + "orders"\ndf = spark.table(SRC)',
        "silver.orders",
    ),
    py(
        "constant-built-from-constant",
        'L = "silver"\nSRC = f"{L}.orders"\ndf = spark.table(SRC)',
        "silver.orders",
    ),
    # -- PySpark: files that look like reads but are not ------------------------
    py("other-receiver", 'df = obj.table("x")'),
    py("other-receiver-read", 'df = loader.read.table("x")'),
    py("other-method-named-sql", 'df = db.sql("select * from a")'),
    py("comment", '# df = spark.table("x")\ny = 1'),
    py("docstring", '"""Reads spark.table("x") and spark.sql("select * from y")."""'),
    py("function-docstring", 'def f():\n    """spark.table("x")"""\n    return 1'),
    py("call-inside-string", "s = 'spark.table(\"x\")'"),
    py("path-parquet", 'df = spark.read.parquet("/mnt/lake/x")'),
    py("path-load", 'df = spark.read.format("delta").load("/mnt/lake/x")'),
    py("path-csv", 'df = spark.read.option("header", True).csv("/mnt/lake/x.csv")'),
    py("path-delta-forpath", 'dt = DeltaTable.forPath(spark, "/mnt/lake/x")'),
    py("write-target-is-not-a-read", 'df.write.saveAsTable("x.out")'),
    # -- PySpark: temp views local to the file ----------------------------------
    py("temp-view-is-local", 'df.createOrReplaceTempView("v")\nout = spark.table("v")'),
    py("temp-view-create", 'df.createTempView("v")\nout = spark.table("v")'),
    py("global-temp-view", 'df.createGlobalTempView("v")\nout = spark.table("global_temp.v")'),
    py(
        "temp-view-used-in-sql",
        'df.createOrReplaceTempView("v")\nout = spark.sql("select * from v join a on 1=1")',
        "a",
    ),
    py(
        "temp-view-read-before-definition",
        'x = spark.table("v")\ndf.createOrReplaceTempView("v")',
        "v",
    ),
    py(
        "temp-view-from-sql-statement",
        'spark.sql("create or replace temp view v as select * from a")\nout = spark.table("v")',
        "a",
    ),
    py(
        "temp-view-name-case-insensitive", 'df.createOrReplaceTempView("V")\nout = spark.table("v")'
    ),
    # -- PySpark: names that cannot be resolved ---------------------------------
    py_err(
        "dynamic-fstring-parameter",
        'def f(layer):\n    return spark.table(f"{layer}.orders")',
        "depends_on",
    ),
    py_err(
        "dynamic-fstring-sql",
        'name = get_name()\ndf = spark.sql(f"select * from {name}")',
        "depends_on",
    ),
    py_err("dynamic-function-call", "df = spark.table(some_function())", "depends_on"),
    py_err("dynamic-variable", "df = spark.table(table_name)", "depends_on"),
    py_err("dynamic-concatenation", 'df = spark.table("silver." + name)', "depends_on"),
    py_err("dynamic-subscript", "df = spark.table(sys.argv[1])", "depends_on"),
    py_err("dynamic-loop-variable", 'for t in ["a", "b"]:\n    df = spark.table(t)', "depends_on"),
    py_err("dynamic-format", 'df = spark.sql("select * from {}".format(name))', "depends_on"),
    py_err(
        "dynamic-format-literal-args",
        'df = spark.sql("select * from {}".format("a"))',
        "depends_on",
    ),
    py_err("dynamic-percent", 'df = spark.sql("select * from %s" % name)', "depends_on"),
    py_err("dynamic-conditional", 'df = spark.table("a" if flag else "b")', "depends_on"),
    py_err(
        "dynamic-constant-reassigned", 'SRC = "a"\nSRC = "b"\ndf = spark.table(SRC)', "depends_on"
    ),
    py_err(
        "dynamic-constant-shadowed-by-parameter",
        'SRC = "a"\ndef f(SRC):\n    return spark.table(SRC)',
        "depends_on",
    ),
    py_err("dynamic-fstring-format-spec", 'N = "a"\ndf = spark.table(f"{N:>5}")', "depends_on"),
    py_err("dynamic-forname", "dt = DeltaTable.forName(spark, name)", "depends_on"),
    py_err("dynamic-sql-argument", "df = spark.sql(query)", "depends_on"),
    py_err("dynamic-sql-literal-unparseable", 'df = spark.sql("select from where")', "depends_on"),
    py_err("syntax-error", "df = spark.table(", "syntax error"),
    py_err("syntax-error-message-points-to-fix", "def f(:\n    pass", "depends_on"),
]


def test_matrix_has_enough_cases():
    assert len(CASES) >= 70


@pytest.mark.parametrize(("language", "source", "expected"), CASES)
def test_dependency_matrix(language: str, source: str, expected: set[str] | Err):
    parse = parse_sql_dependencies if language == "sql" else parse_pyspark_dependencies

    if isinstance(expected, Err):
        with pytest.raises(ValueError) as excinfo:
            parse(source)
        assert expected.fragment in str(excinfo.value)
    else:
        assert parse(source) == expected
