"""Data-quality rules from quality.yml. Every result is logged to ops.dq_results;
any failed `error` rule then fails the run (after all rules are logged, so you see them all)."""

from __future__ import annotations

from datetime import timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from sbl.audit import audited, utc_now
from sbl.lakehouse import Lakehouse

DQ_TABLE = "dq_results"
DQ_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("checked_at", TimestampType(), False),
    StructField("table_name", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("rule_type", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("status", StringType(), False),
    StructField("failed_rows", LongType(), False),
    StructField("total_rows", LongType(), False),
    StructField("details", StringType(), True),
])


class DataQualityError(Exception):
    """At least one rule with severity `error` failed."""


def evaluate(lh: Lakehouse, df: DataFrame, rule: dict) -> tuple[int, int, str]:
    """Return (failed_rows, total_rows, details) for one rule. failed_rows > 0 means FAIL."""
    total = df.count()
    kind = rule["type"]

    if kind == "not_null":
        cond = " OR ".join(f"`{c}` IS NULL" for c in rule["columns"])
        return df.filter(cond).count(), total, f"columns {rule['columns']}"

    if kind == "unique":
        distinct = df.select(*rule["columns"]).distinct().count()
        return total - distinct, total, f"{total - distinct} duplicate row(s) on {rule['columns']}"

    if kind == "range":
        col = F.col(rule["column"])
        bad = df.filter((col < rule["min"]) | (col > rule["max"])).count()
        return bad, total, f"{rule['column']} outside [{rule['min']}, {rule['max']}]"

    if kind == "accepted_values":
        bad = df.filter(~F.col(rule["column"]).isin(rule["values"])).count()
        return bad, total, f"{rule['column']} not in {rule['values']}"

    if kind == "references":
        ref = lh.read(*lh.split(rule["ref_table"])).select(
            F.col(rule["ref_column"]).alias("_ref")).distinct()
        # NULL keys are the not_null rule's job; here only non-NULL keys without a match count.
        keyed = df.filter(F.col(rule["column"]).isNotNull())
        bad = keyed.join(ref, F.col(rule["column"]) == F.col("_ref"), "left_anti").count()
        return bad, total, f"{rule['column']} missing from {rule['ref_table']}.{rule['ref_column']}"

    if kind == "row_count_min":
        return (1 if total < rule["min"] else 0), total, f"needs at least {rule['min']} rows"

    if kind == "max_share":
        matching = df.filter(rule["expr"]).count()
        share = matching / total if total else 0.0
        failed = matching if share > rule["max_share"] else 0
        return failed, total, f"{share:.1%} of rows match `{rule['expr']}` (max {rule['max_share']:.0%})"

    if kind == "freshness":
        latest = df.agg(F.max(rule["column"])).first()[0]
        limit = utc_now() - timedelta(hours=rule["max_age_hours"])
        stale = latest is None or latest < limit
        return (1 if stale else 0), total, f"latest {rule['column']} = {latest}"

    raise ValueError(f"unknown rule type {kind}")


def run_rules(lh: Lakehouse, quality_cfg: dict, run_id: str) -> list[tuple]:
    results = []
    with audited(lh, run_id, "quality", "ops.dq_results") as audit:
        for table, rules in quality_cfg["rules"].items():
            layer, name = lh.split(table)
            df = lh.read(layer, name) if lh.exists(layer, name) else None
            for rule in rules:
                if df is None:
                    failed, total, details = 1, 0, "table does not exist"
                else:
                    failed, total, details = evaluate(lh, df, rule)
                status = "PASS" if failed == 0 else "FAIL"
                results.append((run_id, utc_now(), table, rule["name"], rule["type"],
                                rule["severity"], status, int(failed), int(total), details))

        lh.write(lh.spark.createDataFrame(results, DQ_SCHEMA), "ops", DQ_TABLE)
        errors = [r for r in results if r[6] == "FAIL" and r[5] == "error"]
        warnings = [r for r in results if r[6] == "FAIL" and r[5] == "warn"]
        audit.rows_read = len(results)
        audit.rows_written = len(results)
        audit.message = (f"{len(results)} rules: {len(results) - len(errors) - len(warnings)} passed, "
                         f"{len(warnings)} warning(s), {len(errors)} error(s)")
        for r in errors + warnings:
            print(f"[dq] {r[6]} {r[5]:<5} {r[2]}.{r[3]}: {r[7]} of {r[8]} rows; {r[9]}")
        if errors:
            names = ", ".join(f"{r[2]}.{r[3]}" for r in errors)
            raise DataQualityError(f"{len(errors)} error rule(s) failed: {names}")
    return results
