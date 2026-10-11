"""Finds the tables a logic file reads, for `depends_on: auto`.

SQL is parsed with sqlglot (Spark dialect) and its scopes, so a CTE, a view
created in the same file, a lateral alias or a table-valued function is never
mistaken for a table. PySpark is read with `ast`: `spark.table`,
`spark.read.table`, `spark.readStream.table`, `DeltaTable.forName` and
`spark.sql` calls anywhere in the file, with the name given as a string
literal, an implicit or `+` concatenation, or an f-string over module-level
string constants.

A table name that is only known at run time, a SQL string that does not
parse, or a file that is not valid Python raises `DependencyParseError`
instead of silently dropping an edge. Declare those parents with an explicit
`depends_on` list.

Names come back as written (quotes stripped, parts joined by dots): `t`,
`schema.t` or `catalog.schema.t`. Matching them to configured tables is the
graph builder's job. Tables the block writes (`CREATE TABLE x AS`,
`INSERT INTO x`, `MERGE INTO x`) are reported too; the builder ignores a
table's reference to itself.
"""

from __future__ import annotations

import ast
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.scope import Scope, traverse_scope

from pipetree.graph.errors import DependencyParseError

# --------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------

# Statements that never read a pipeline table: skipped (a Command is a
# statement sqlglot has no grammar for, only its first word is known).
_SKIPPED_STATEMENTS = (
    exp.Use,
    exp.Set,
    exp.Drop,
    exp.Alter,
    exp.Describe,
    exp.Analyze,
    exp.Refresh,
    exp.TruncateTable,
)
_SKIPPED_COMMANDS = frozenset(
    {
        "ALTER",
        "ANALYZE",
        "DESCRIBE",
        "DROP",
        "GRANT",
        "MSCK",
        "OPTIMIZE",
        "REFRESH",
        "REVOKE",
        "SET",
        "SHOW",
        "TRUNCATE",
        "USE",
        "VACUUM",
    }
)


_DEEP_CLONE_RE = re.compile(r"\bDEEP\s+CLONE\b", re.IGNORECASE)


def parse_sql_dependencies(sql: str, *, file: str = "<sql>") -> set[str]:
    """Tables a (multi-statement) Spark SQL text reads or writes."""
    return _sql_dependencies(sql, file, local=set())


def _sql_dependencies(sql: str, file: str, local: set[str]) -> set[str]:
    """`local` holds the (case-folded) names of views created earlier in the
    same file; views this text creates are added to it."""
    try:
        statements = sqlglot.parse(sql, read="spark")
    except SqlglotError as exc:
        reason = str(exc).splitlines()[0] if str(exc) else "invalid syntax"
        raise DependencyParseError(file, f"cannot parse the SQL ({reason})") from exc

    deps: set[str] = set()
    for statement in statements:
        if statement is None:
            continue
        try:
            deps |= _statement_dependencies(statement, file, local)
        except SqlglotError as exc:
            raise DependencyParseError(file, f"cannot analyse the SQL ({exc})") from exc
    return deps


def _statement_dependencies(statement: exp.Expression, file: str, local: set[str]) -> set[str]:
    if isinstance(statement, exp.Command):
        word = str(statement.this).split(maxsplit=1)[0].upper() if statement.this else ""
        if word in _SKIPPED_COMMANDS:
            return set()
        if word == "CREATE":
            # sqlglot only knows SHALLOW CLONE; DEEP CLONE reads the same table.
            text = f"CREATE {statement.text('expression')}"
            retry = _DEEP_CLONE_RE.sub("SHALLOW CLONE", text)
            if retry != text:
                reparsed = sqlglot.parse_one(retry, read="spark")
                if not isinstance(reparsed, exp.Command):
                    return _statement_dependencies(reparsed, file, local)
        raise DependencyParseError(
            file,
            f"cannot analyse the {word or 'unknown'} statement",
            expression=statement.sql(dialect="spark")[:100],
        )
    if isinstance(statement, _SKIPPED_STATEMENTS):
        return set()

    for lateral in statement.find_all(exp.Lateral):
        if lateral.args.get("cross_apply") is not None:
            raise DependencyParseError(
                file,
                "CROSS APPLY / OUTER APPLY is T-SQL, not Spark SQL "
                "(use JOIN LATERAL or LATERAL VIEW)",
            )

    view_target: exp.Table | None = None
    if isinstance(statement, exp.Create):
        kind = str(statement.args.get("kind") or "").upper()
        if kind not in ("TABLE", "VIEW"):
            return set()  # schema, function, ...: no table is read
        if kind == "VIEW" and statement.this is not None:
            view_target = statement.this.find(exp.Table)

    cte_names = {cte.alias_or_name.casefold() for cte in statement.find_all(exp.CTE)}
    accounted: set[int] = set()
    deps: set[str] = set()

    def add(parts: list[str]) -> None:
        if not parts or "/" in parts[-1]:
            return  # a path table (delta.`/mnt/x`) is not a pipeline table
        name = ".".join(parts)
        if name.casefold() not in local:
            deps.add(name)

    for scope in traverse_scope(statement):
        for table in scope.tables or []:
            accounted.add(id(table))
            if _is_cte_reference(table, scope):
                continue
            parts = _table_parts(table)
            if parts is not None:
                add(parts)

    # Tables outside any query scope: the target of INSERT / CREATE / MERGE,
    # the MERGE source, the LIKE / CLONE source.
    for table in statement.find_all(exp.Table):
        if id(table) in accounted or table is view_target or _inside_hint(table):
            continue
        parts = _table_parts(table)
        if parts is None:
            continue
        if len(parts) == 1 and parts[0].casefold() in cte_names:
            continue
        add(parts)

    # TABLE(name) passes a table to a table-valued function.
    for call in statement.find_all(exp.Anonymous):
        if str(call.this).upper() == "TABLE" and len(call.expressions) == 1:
            argument = call.expressions[0]
            if isinstance(argument, exp.Column):
                add([part.name for part in argument.parts])

    if view_target is not None:
        parts = _table_parts(view_target)
        if parts is not None:
            local.add(".".join(parts).casefold())
    return deps


def _is_cte_reference(table: exp.Table, scope: Scope) -> bool:
    if table.args.get("db"):
        return False
    wanted = table.alias_or_name.casefold()
    return any(
        isinstance(source, Scope) and name.casefold() == wanted
        for name, source in scope.sources.items()
    )


def _inside_hint(node: exp.Expression) -> bool:
    return node.find_ancestor(exp.Hint) is not None


def _table_parts(table: exp.Table) -> list[str] | None:
    """Name parts of a plain table reference; None for a table-valued
    function, VALUES or other source that is not a named table."""
    if not isinstance(table.this, exp.Identifier):
        return None
    return [
        part.name
        for part in (table.args.get("catalog"), table.args.get("db"), table.this)
        if isinstance(part, exp.Identifier)
    ]


# --------------------------------------------------------------------------
# PySpark
# --------------------------------------------------------------------------

_READER_STEPS = frozenset({"format", "option", "options", "schema"})
_VIEW_DEFINITIONS = frozenset(
    {
        "createOrReplaceTempView",
        "createTempView",
        "createGlobalTempView",
        "createOrReplaceGlobalTempView",
    }
)
_GLOBAL_VIEW_DEFINITIONS = frozenset({"createGlobalTempView", "createOrReplaceGlobalTempView"})


@dataclass(frozen=True)
class _Event:
    position: tuple[int, int]
    kind: str  # "view", "table" or "sql"
    value: str


def parse_pyspark_dependencies(code: str, *, file: str = "<python>") -> set[str]:
    """Tables a PySpark file reads, from its `spark.table(...)`,
    `spark.read.table(...)`, `spark.readStream.table(...)`,
    `DeltaTable.forName(spark, ...)` and `spark.sql(...)` calls."""
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        raise DependencyParseError(file, f"syntax error at line {exc.lineno}: {exc.msg}") from exc

    constants = _module_constants(tree)
    events: list[_Event] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        attr = node.func.attr
        receiver = node.func.value
        start = (node.lineno, node.col_offset)
        end = (node.end_lineno or node.lineno, node.end_col_offset or node.col_offset)

        if attr in _VIEW_DEFINITIONS:
            name = _resolve(_argument(node, 0, "viewName"), constants)
            if name is not None:
                events.append(_Event(end, "view", name))
                if attr in _GLOBAL_VIEW_DEFINITIONS:
                    events.append(_Event(end, "view", f"global_temp.{name}"))
        elif attr == "table" and (_is_spark(receiver) or _is_reader(receiver)):
            events.append(_read_event("table", start, node, 0, "tableName", code, file, constants))
        elif attr == "sql" and _is_spark(receiver):
            events.append(_read_event("sql", start, node, 0, "sqlQuery", code, file, constants))
        elif attr == "forName" and _is_delta_table(receiver):
            events.append(
                _read_event("table", start, node, 1, "tableOrViewName", code, file, constants)
            )

    local: set[str] = set()
    deps: set[str] = set()
    for event in sorted(events, key=lambda e: (e.position, e.kind != "view")):
        if event.kind == "view":
            local.add(event.value.casefold())
        elif event.kind == "table":
            parts = _name_parts(event.value, file)
            name = ".".join(parts)
            if name.casefold() not in local and "/" not in parts[-1]:
                deps.add(name)
        else:
            deps |= _sql_dependencies(event.value, file, local)
    return deps


def _read_event(
    kind: str,
    position: tuple[int, int],
    call: ast.Call,
    index: int,
    keyword: str,
    code: str,
    file: str,
    constants: dict[str, str],
) -> _Event:
    value = _resolve(_argument(call, index, keyword), constants)
    if value is None:
        source = ast.get_source_segment(code, call) or ast.unparse(call)
        raise DependencyParseError(
            file,
            f"line {call.lineno}: the name is not a string literal or module-level "
            "string constant, so it is only known at run time",
            expression=" ".join(source.split())[:100],
        )
    return _Event(position, kind, value)


def _name_parts(name: str, file: str) -> list[str]:
    try:
        parts = _table_parts(exp.to_table(name, dialect="spark"))
    except SqlglotError as exc:
        raise DependencyParseError(file, f"cannot read the table name {name!r}") from exc
    if parts is None:
        raise DependencyParseError(file, f"cannot read the table name {name!r}")
    return parts


def _argument(call: ast.Call, index: int, keyword: str) -> ast.expr | None:
    if len(call.args) > index and not isinstance(call.args[index], ast.Starred):
        return call.args[index]
    for kw in call.keywords:
        if kw.arg == keyword:
            return kw.value
    return None


def _is_spark(node: ast.expr) -> bool:
    return (isinstance(node, ast.Name) and node.id == "spark") or (
        isinstance(node, ast.Attribute) and node.attr == "spark"
    )


def _is_reader(node: ast.expr) -> bool:
    """`spark.read` / `spark.readStream`, optionally with .format()/.option()/..."""
    while (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _READER_STEPS
    ):
        node = node.func.value
    return (
        isinstance(node, ast.Attribute)
        and node.attr in ("read", "readStream")
        and _is_spark(node.value)
    )


def _is_delta_table(node: ast.expr) -> bool:
    return (isinstance(node, ast.Name) and node.id == "DeltaTable") or (
        isinstance(node, ast.Attribute) and node.attr == "DeltaTable"
    )


def _module_constants(tree: ast.Module) -> dict[str, str]:
    """Module-level names assigned exactly once, to a string made of literals
    and other such constants. A name that is also a loop variable, parameter
    or assigned again elsewhere could hold anything, so it is not one."""
    stores = Counter(
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name) and not isinstance(node.ctx, ast.Load)
    )
    parameters = {node.arg for node in ast.walk(tree) if isinstance(node, ast.arg)}

    constants: dict[str, str] = {}
    for statement in tree.body:
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target, value = statement.targets[0], statement.value
        elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
            target, value = statement.target, statement.value
        else:
            continue
        if not isinstance(target, ast.Name) or stores[target.id] != 1 or target.id in parameters:
            continue
        resolved = _resolve(value, constants)
        if resolved is not None:
            constants[target.id] = resolved
    return constants


def _resolve(node: ast.expr | None, constants: dict[str, str]) -> str | None:
    """The string an expression evaluates to, or None if it depends on
    anything only known at run time."""
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _resolve(node.left, constants), _resolve(node.right, constants)
        return left + right if left is not None and right is not None else None
    if isinstance(node, ast.JoinedStr):
        return _join(node.values, constants)
    return None


def _join(values: Iterable[ast.expr], constants: dict[str, str]) -> str | None:
    pieces: list[str] = []
    for value in values:
        if isinstance(value, ast.FormattedValue):
            if value.conversion != -1 or value.format_spec is not None:
                return None
            piece = _resolve(value.value, constants)
        else:
            piece = _resolve(value, constants)
        if piece is None:
            return None
        pieces.append(piece)
    return "".join(pieces)


# --------------------------------------------------------------------------
# Dispatch
# --------------------------------------------------------------------------


def parse_logic_dependencies(path: Path) -> set[str]:
    """Parse a logic file's dependencies, dispatching on its extension."""
    if path.suffix == ".sql":
        return parse_sql_dependencies(path.read_text(), file=str(path))
    if path.suffix == ".py":
        return parse_pyspark_dependencies(path.read_text(), file=str(path))
    raise ValueError(
        f"{path}: don't know how to parse dependencies from a {path.suffix!r} file "
        "(expected one of ['.py', '.sql'])"
    )
