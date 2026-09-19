"""Deliberately simple dependency parsers.

Per part 1: for `depends_on: auto`, SQL files are scanned for `FROM`/`JOIN`
clauses and PySpark files for `spark.table("...")` / `spark.sql("...")`
calls with **literal** strings. This won't catch a table name built at
runtime (an f-string, a variable) or every statement in a multi-statement
file - that's intentional. Declare those with an explicit `depends_on` and
move on.
"""

from __future__ import annotations

import re
from pathlib import Path

_SQL_FROM_JOIN_RE = re.compile(
    r"\b(?:from|join)\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)",
    re.IGNORECASE,
)

_SPARK_TABLE_RE = re.compile(r"""spark\.table\(\s*(['"])([^'"]+)\1\s*\)""")

# A literal string argument to spark.sql(...): triple- or single-quoted,
# not an f-string/variable (an f-prefixed or interpolated call simply won't
# match, which is the point).
_SPARK_SQL_RE = re.compile(
    r'''spark\.sql\(\s*(?:"""(?P<dq3>.*?)"""|\'\'\'(?P<sq3>.*?)\'\'\''''
    r"""|"(?P<dq1>[^"]*)"|\'(?P<sq1>[^\']*)\')\s*\)""",
    re.DOTALL,
)


def parse_sql_dependencies(sql: str) -> set[str]:
    """Table names referenced in FROM/JOIN clauses, anywhere in the text."""
    return {match.group(1) for match in _SQL_FROM_JOIN_RE.finditer(sql)}


def parse_pyspark_dependencies(code: str) -> set[str]:
    """Table names from literal spark.table(...) and spark.sql(...) calls."""
    deps = {match.group(2) for match in _SPARK_TABLE_RE.finditer(code)}

    for match in _SPARK_SQL_RE.finditer(code):
        sql_literal = next(g for g in match.groups() if g is not None)
        deps |= parse_sql_dependencies(sql_literal)

    return deps


_PARSERS_BY_SUFFIX = {
    ".sql": parse_sql_dependencies,
    ".py": parse_pyspark_dependencies,
}


def parse_logic_dependencies(path: Path) -> set[str]:
    """Parse a logic file's dependencies, dispatching on its extension."""
    parser = _PARSERS_BY_SUFFIX.get(path.suffix)
    if parser is None:
        raise ValueError(
            f"{path}: don't know how to parse dependencies from a {path.suffix!r} file "
            f"(expected one of {sorted(_PARSERS_BY_SUFFIX)})"
        )
    return parser(path.read_text())
