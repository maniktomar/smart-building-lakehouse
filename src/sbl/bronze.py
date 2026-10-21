"""Bronze: append raw landing files to Delta tables, with lineage columns and a quarantine.

For one source (an entry in sources.yml):
  1. find landing files not ingested before (ops.ingested_files)  -> re-runs skip old files
  2. parse them into string columns (sbl.parsers)
  3. compare columns with config and the existing table             -> schema drift handling
  4. check required / castable values                               -> bad rows to ops.quarantine
  5. append good rows with _ingested_at, _source_file, _batch_id     -> bronze is append-only
"""

from __future__ import annotations

import glob
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from sbl.audit import audited, utc_now
from sbl.casts import cast_expr
from sbl.lakehouse import Lakehouse, cache_if_possible
from sbl.parsers import CORRUPT, PARSERS

META_COLUMNS = ["_ingested_at", "_source_file", "_batch_id"]
QUARANTINE_TABLE = "quarantine"
FILE_LOG_TABLE = "ingested_files"

FILE_LOG_SCHEMA = StructType([
    StructField("source", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("rows_read", LongType(), False),
    StructField("ingested_at", TimestampType(), False),
])


class SchemaDriftError(Exception):
    """A file's columns do not match what the config and the bronze table allow."""


def list_new_files(lh: Lakehouse, source: str, src: dict) -> list[str]:
    pattern = os.path.join(lh.landing_root, src["landing_dir"], src["file_glob"])
    files = sorted(os.path.abspath(p) for p in glob.glob(pattern, recursive=True))
    if not lh.exists("ops", FILE_LOG_TABLE):
        return files
    done = {
        r.file_path
        for r in lh.read("ops", FILE_LOG_TABLE).filter(F.col("source") == source)
        .select("file_path").collect()
    }
    return [f for f in files if f not in done]


def check_schema(lh: Lakehouse, src: dict, incoming: list[str]) -> list[str]:
    """Raise on breaking drift; return messages describing allowed (additive) drift."""
    missing = [c["name"] for c in src["columns"] if c["name"] not in incoming]
    if missing:
        raise SchemaDriftError(f"configured columns missing from files: {missing}")

    if not lh.exists("bronze", src["bronze_table"]):
        return []
    existing = set(lh.read("bronze", src["bronze_table"]).columns)
    new = [c for c in incoming if c not in existing]
    if not new:
        return []
    if src["schema_drift"]["new_columns"] == "fail":
        raise SchemaDriftError(f"new columns not allowed by config: {new}")
    return [f"schema drift: added columns {new}"]


def with_quarantine_reason(df: DataFrame, src: dict) -> DataFrame:
    """Add `_quarantine_reason`: NULL for good rows, else every problem found, '; '-joined."""
    checks = [F.when(F.col(CORRUPT).isNotNull(), F.lit("malformed record"))]
    for col in src["columns"]:
        name, type_ = col["name"], col["type"]
        raw_missing = F.expr(f"nullif(trim(`{name}`), '') IS NULL")
        if col.get("required"):
            checks.append(F.when(raw_missing, F.lit(f"{name}: missing")))
        if type_ != "string":
            not_castable = F.expr(f"{cast_expr(name, type_, col.get('format'))} IS NULL")
            checks.append(F.when(~raw_missing & not_castable, F.lit(f"{name}: invalid {type_}")))
    reason = F.concat_ws("; ", *checks)   # concat_ws skips NULLs
    return df.withColumn("_quarantine_reason", F.when(reason != "", reason))


def ingest_source(lh: Lakehouse, source: str, src: dict, run_id: str) -> None:
    table = src["bronze_table"]
    with audited(lh, run_id, "bronze", f"bronze.{table}") as audit:
        files = list_new_files(lh, source, src)
        if not files:
            audit.message = "no new files"
            return

        raw = PARSERS[src["format"]](lh.spark, files, src)
        data_cols = [c for c in raw.columns if c not in ("_source_file", CORRUPT)]
        drift = check_schema(lh, src, data_cols)

        ingested_at = utc_now()
        checked = cache_if_possible(
            with_quarantine_reason(raw, src)
            .withColumn("_ingested_at", F.lit(ingested_at))
            .withColumn("_batch_id", F.lit(run_id))
        )

        good = checked.filter(F.col("_quarantine_reason").isNull()).select(
            *data_cols, *META_COLUMNS
        )
        bad = checked.filter(F.col("_quarantine_reason").isNotNull()).select(
            F.lit(source).alias("source"),
            "_source_file", "_batch_id", "_ingested_at",
            F.col("_quarantine_reason").alias("reason"),
            F.coalesce(F.col(CORRUPT), F.to_json(F.struct(*data_cols))).alias("raw_record"),
        )

        audit.rows_read = checked.count()
        audit.rows_written = good.count()
        audit.rows_quarantined = bad.count()

        lh.write(good, "bronze", table, merge_schema=bool(drift))
        if audit.rows_quarantined:
            lh.write(bad, "ops", QUARANTINE_TABLE)

        # Record files last: if anything above fails, the files are retried next run.
        per_file = {r["_source_file"]: r["count"]
                    for r in checked.groupBy("_source_file").count().collect()}
        log = [(source, f, run_id, int(per_file.get(f, 0)), ingested_at) for f in files]
        lh.write(lh.spark.createDataFrame(log, FILE_LOG_SCHEMA), "ops", FILE_LOG_TABLE)

        notes = [f"{len(files)} file(s)"] + drift
        audit.message = "; ".join(notes)
