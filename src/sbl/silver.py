"""Silver: typed, deduplicated tables kept up to date with MERGE INTO (upsert).

For one table (an entry in silver.yml):
  1. read bronze rows ingested after the table's watermark (ops.watermarks)  -> incremental
  2. cast to the configured types and rename columns
  3. keep the latest row per business key                                    -> deduplication
  4. MERGE on the business key: update matches, insert the rest              -> idempotent
  5. move the watermark forward
Re-running with nothing new changes nothing. Re-processing everything (reset_watermark=True)
updates rows in place, so the row count stays the same: that is the idempotency guarantee.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from sbl.audit import audited, utc_now
from sbl.casts import cast_expr
from sbl.lakehouse import Lakehouse

WATERMARK_TABLE = "watermarks"
WATERMARK_SCHEMA = StructType([
    StructField("table_name", StringType(), False),
    StructField("watermark_ts", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
])


# ---- watermarks ---------------------------------------------------------------------------
def get_watermark(lh: Lakehouse, table_name: str):
    if not lh.exists("ops", WATERMARK_TABLE):
        return None
    row = (lh.read("ops", WATERMARK_TABLE)
           .filter(F.col("table_name") == table_name).select("watermark_ts").first())
    return row[0] if row else None


def set_watermark(lh: Lakehouse, table_name: str, value) -> None:
    update = lh.spark.createDataFrame([(table_name, value, utc_now())], WATERMARK_SCHEMA)
    if not lh.exists("ops", WATERMARK_TABLE):
        lh.write(update, "ops", WATERMARK_TABLE)
        return
    (lh.delta("ops", WATERMARK_TABLE).alias("t")
     .merge(update.alias("s"), "t.table_name = s.table_name")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


# ---- transformation (pure DataFrame -> DataFrame, unit-tested) -----------------------------
def transform(bronze: DataFrame, cfg: dict) -> DataFrame:
    """Cast and rename per config, then keep the most recently ingested row per key."""
    typed = bronze.select(
        *[F.expr(cast_expr(spec["from"], spec["type"], spec.get("format"))).alias(col)
          for col, spec in cfg["columns"].items()],
        F.col("_ingested_at").alias("_source_ingested_at"),
    )
    latest_first = Window.partitionBy(*cfg["keys"]).orderBy(F.col("_source_ingested_at").desc())
    return (typed.withColumn("_rn", F.row_number().over(latest_first))
            .filter("_rn = 1").drop("_rn"))


def _create_table(lh: Lakehouse, name: str, cfg: dict, sample: DataFrame) -> None:
    lh.write(sample.limit(0), "silver", name)
    for constraint, expr in cfg.get("constraints", {}).items():
        lh.sql(f"ALTER TABLE {lh.ref('silver', name)} ADD CONSTRAINT {constraint} CHECK ({expr})")
    lh.set_comments("silver", name, cfg.get("description", ""))


def build_table(lh: Lakehouse, name: str, cfg: dict, run_id: str,
                reset_watermark: bool = False) -> None:
    wm_name = f"silver.{name}"
    with audited(lh, run_id, "silver", wm_name) as audit:
        bronze = lh.read("bronze", cfg["source"])
        watermark = None if reset_watermark else get_watermark(lh, wm_name)
        if watermark is not None:
            bronze = bronze.filter(F.col("_ingested_at") > F.lit(watermark))

        new_watermark = bronze.agg(F.max("_ingested_at")).first()[0]
        if new_watermark is None:
            audit.message = f"no new bronze rows after watermark {watermark}"
            return

        updates = (transform(bronze, cfg)
                   .withColumn("_batch_id", F.lit(run_id))
                   .withColumn("_updated_at", F.lit(utc_now())))
        audit.rows_read = bronze.count()

        if not lh.exists("silver", name):
            _create_table(lh, name, cfg, updates)

        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in cfg["keys"])
        (lh.delta("silver", name).alias("t")
         .merge(updates.alias("s"), on)
         # Only overwrite with data that is at least as new as what is already there.
         .whenMatchedUpdateAll(condition="s._source_ingested_at >= t._source_ingested_at")
         .whenNotMatchedInsertAll()
         .execute())

        metrics = lh.delta("silver", name).history(1).select("operationMetrics").first()[0]
        inserted = int(metrics.get("numTargetRowsInserted", 0))
        updated = int(metrics.get("numTargetRowsUpdated", 0))
        audit.rows_written = inserted + updated
        audit.message = (f"inserted={inserted} updated={updated} "
                         f"watermark {watermark} -> {new_watermark}")
        set_watermark(lh, wm_name, new_watermark)
