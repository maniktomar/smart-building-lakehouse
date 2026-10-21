"""Turn a config type ('double', 'timestamp', ...) into a safe Spark SQL cast expression.

`try_cast` / `try_to_timestamp` return NULL instead of raising on bad input. That matters
because Spark 4 and Databricks serverless run with ANSI mode on, where a plain cast of
'abc' to double fails the whole job.
"""

from __future__ import annotations


def cast_expr(column: str, type_: str, fmt: str | None = None) -> str:
    col = f"nullif(trim(`{column}`), '')"   # blank strings count as missing
    if type_ == "string":
        return col
    if type_ == "double":
        return f"try_cast({col} AS DOUBLE)"
    if type_ == "int":
        # Source files write years as '1976.0', which does not cast straight to INT.
        return f"CAST(try_cast({col} AS DOUBLE) AS INT)"
    if type_ == "timestamp":
        # Formats can contain quotes (yyyy-MM-dd'T'HH:mm), so escape them inside the SQL literal.
        escaped = fmt.replace("\\", "\\\\").replace("'", "\\'")
        return f"try_to_timestamp({col}, '{escaped}')"
    raise ValueError(f"unsupported type: {type_}")
