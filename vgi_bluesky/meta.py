"""Catalog-metadata helpers: column comments, result schemas, doc tags.

VGI publishes documentation as ``vgi.*`` tags on catalog objects, and the
`vgi-lint <https://github.com/Query-farm/vgi-lint-check>`_ rules check them. The
tags are strings carrying JSON, which is easy to get subtly wrong by hand, so
everything here builds them from the same Arrow schemas the functions already
return — a column documented once in :mod:`vgi_bluesky.schemas` shows up in
``DESCRIBE``, in ``duckdb_columns()``, and in the function's declared result
schema, and cannot drift between them.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

#: Arrow field-metadata key DuckDB reads column comments from.
_COMMENT_KEY = b"comment"


def field(name: str, type: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field:
    """A ``pa.Field`` carrying its column comment as Arrow field metadata.

    This is the single source for a column's documentation: DuckDB surfaces it
    through ``duckdb_columns()`` and ``DESCRIBE``, and :func:`result_columns_schema`
    reads the same string back out when declaring a function's result shape.
    """
    return pa.field(name, type, nullable=nullable, metadata={_COMMENT_KEY: comment.encode()})


def comment_of(f: pa.Field) -> str:
    """The comment attached to ``f``, or an empty string when it has none."""
    if f.metadata and _COMMENT_KEY in f.metadata:
        return str(f.metadata[_COMMENT_KEY].decode())
    return ""


def column_comments(schema: pa.Schema) -> dict[str, str]:
    """Every documented column in ``schema``, as the mapping ``Table`` wants."""
    return {f.name: comment_of(f) for f in schema if comment_of(f)}


def _sql_type(kind: pa.DataType) -> str:
    """Render an Arrow type as the DuckDB type name a consumer will actually see."""
    if pa.types.is_decimal(kind):
        return f"DECIMAL({kind.precision},{kind.scale})"
    if pa.types.is_timestamp(kind):
        return "TIMESTAMP WITH TIME ZONE" if kind.tz else "TIMESTAMP"
    if pa.types.is_boolean(kind):
        return "BOOLEAN"
    if pa.types.is_int64(kind):
        return "BIGINT"
    if pa.types.is_int32(kind):
        return "INTEGER"
    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return "VARCHAR"
    if pa.types.is_map(kind):
        return f"MAP({_sql_type(kind.key_type)}, {_sql_type(kind.item_type)})"
    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        return f"{_sql_type(kind.value_type)}[]"
    if pa.types.is_struct(kind):
        inner = ", ".join(f'"{f.name}" {_sql_type(f.type)}' for f in kind)
        return f"STRUCT({inner})"
    raise ValueError(f"no DuckDB type mapping for {kind}")


def result_columns_schema(schema: pa.Schema) -> str:
    """Render a table function's static result shape as ``vgi.result_columns_schema``.

    DuckDB cannot see a VGI table function's columns until it binds, so the
    worker declares them up front. Every column must carry a description, which
    is why :func:`field` is used throughout :mod:`vgi_bluesky.schemas`.
    """
    return json.dumps(
        [{"name": f.name, "type": _sql_type(f.type), "description": comment_of(f)} for f in schema]
    )


def examples(*pairs: tuple[str, str]) -> str:
    """Render ``(description, sql)`` pairs as a ``vgi.example_queries`` tag."""
    return json.dumps([{"description": description, "sql": sql} for description, sql in pairs])


def keywords(*terms: str) -> str:
    """Render search terms as a ``vgi.keywords`` tag."""
    return json.dumps(list(terms))


def docs(
    *,
    llm: str,
    md: str,
    category: str | None = None,
    result_schema: pa.Schema | None = None,
    example_queries: str | None = None,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    """Assemble the ``vgi.*`` documentation tags for one catalog object.

    ``llm`` is the narrative an agent reads when deciding whether this object is
    the right tool; ``md`` is the longer human-facing page. Both must add detail
    the object's one-line ``comment`` does not already carry.
    """
    tags: dict[str, Any] = {"vgi.doc_llm": llm.strip(), "vgi.doc_md": md.strip()}
    if category is not None:
        tags["vgi.category"] = category
    if result_schema is not None:
        tags["vgi.result_columns_schema"] = result_columns_schema(result_schema)
    if example_queries is not None:
        tags["vgi.example_queries"] = example_queries
    if extra:
        tags.update(extra)
    return tags
