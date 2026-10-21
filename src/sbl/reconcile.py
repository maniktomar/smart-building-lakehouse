"""Reconciliation: do row counts and kWh totals agree from source files to gold?

  source files  -> bronze + quarantine   every landed row is either in bronze or quarantined
  bronze        -> silver                distinct business keys and kWh totals match
  silver        -> gold                  kWh totals and building-day counts match
The landing files are re-read independently here, not taken from the ingestion log.
Results go to ops.reconciliation; any FAIL fails the run.
"""

from __future__ import annotations

import glob
import os

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from sbl.audit import audited, utc_now
from sbl.lakehouse import Lakehouse
from sbl.silver import transform

RECON_TABLE = "reconciliation"
RECON_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("checked_at", TimestampType(), False),
    StructField("check_name", StringType(), False),
    StructField("left_side", StringType(), False),
    StructField("left_value", DoubleType(), True),
    StructField("right_side", StringType(), False),
    StructField("right_value", DoubleType(), True),
    StructField("relative_diff", DoubleType(), True),
    StructField("tolerance", DoubleType(), False),
    StructField("status", StringType(), False),
])


class ReconciliationError(Exception):
    """A reconciliation check is outside its tolerance."""


def relative_diff(left: float, right: float) -> float:
    if left == right:
        return 0.0
    return abs(left - right) / max(abs(left), abs(right))


def landing_rows(lh: Lakehouse, src: dict) -> int:
    """Count data rows in the landing files, independently of the bronze code."""
    files = glob.glob(os.path.join(lh.landing_root, src["landing_dir"], src["file_glob"]),
                      recursive=True)
    if not files:
        return 0
    if src["format"] == "csv":
        return lh.spark.read.option("header", "true").csv(files).count()
    raw = lh.spark.read.option("multiLine", "true").json(files)
    if "hourly" not in raw.columns:
        return raw.count()
    # A corrupt file is one quarantined row; a good file is one row per hour.
    return raw.select(F.coalesce(F.size("hourly.time"), F.lit(1)).alias("n")).agg(
        F.sum("n")).first()[0] or 0


def collect_checks(lh: Lakehouse, cfg) -> list[tuple[str, str, float, str, float, str]]:
    """(check, left side, left value, right side, right value, 'count' | 'sum')."""
    checks = []
    quarantine = (lh.read("ops", "quarantine") if lh.exists("ops", "quarantine") else None)

    for name, src in cfg.sources.items():
        bronze = lh.read("bronze", src["bronze_table"])
        quarantined = (quarantine.filter(F.col("source") == name).count()
                       if quarantine is not None else 0)
        checks.append((f"{name}: source rows = bronze + quarantine",
                       "landing files", landing_rows(lh, src),
                       "bronze + quarantine", bronze.count() + quarantined, "count"))

    for name, tbl in cfg.silver.items():
        expected = transform(lh.read("bronze", tbl["source"]), tbl)
        silver = lh.read("silver", name)
        checks.append((f"{name}: distinct keys bronze = silver rows",
                       "bronze (deduplicated)", expected.count(), "silver", silver.count(), "count"))

    bronze_kwh = transform(lh.read("bronze", "meter_readings"), cfg.silver["meter_readings"])
    silver_kwh = lh.read("silver", "meter_readings")
    gold_fact = lh.read("gold", "fact_daily_energy")
    total = lambda df, col: float(df.agg(F.sum(col)).first()[0] or 0.0)  # noqa: E731

    checks.append(("meter_readings: kWh bronze = silver",
                   "bronze (deduplicated)", total(bronze_kwh, "reading_kwh"),
                   "silver", total(silver_kwh, "reading_kwh"), "sum"))
    checks.append(("energy: kWh silver = gold",
                   "silver.meter_readings", total(silver_kwh, "reading_kwh"),
                   "gold.fact_daily_energy", total(gold_fact, "energy_kwh"), "sum"))
    building_days = silver_kwh.select("building_id", F.to_date("reading_ts")).distinct().count()
    checks.append(("energy: building-days silver = gold rows",
                   "silver.meter_readings", building_days,
                   "gold.fact_daily_energy", gold_fact.count(), "count"))
    checks.append(("weather: hours silver = gold hours_observed",
                   "silver.weather_hourly", lh.read("silver", "weather_hourly").count(),
                   "gold.fact_daily_weather",
                   total(lh.read("gold", "fact_daily_weather"), "hours_observed"), "count"))
    return checks


def run(lh: Lakehouse, cfg, run_id: str) -> list[tuple]:
    tol = cfg.quality["reconciliation"]
    with audited(lh, run_id, "reconcile", "ops.reconciliation") as audit:
        results = []
        for check, left_name, left, right_name, right, kind in collect_checks(lh, cfg):
            tolerance = tol["count_tolerance"] if kind == "count" else tol["sum_tolerance"]
            diff = relative_diff(float(left), float(right))
            status = "PASS" if diff <= tolerance else "FAIL"
            results.append((run_id, utc_now(), check, left_name, float(left), right_name,
                            float(right), diff, float(tolerance), status))
            print(f"[recon] {status} {check}: {left:,.3f} vs {right:,.3f} (diff {diff:.2e})")

        lh.write(lh.spark.createDataFrame(results, RECON_SCHEMA), "ops", RECON_TABLE)
        failed = [r for r in results if r[-1] == "FAIL"]
        audit.rows_read = audit.rows_written = len(results)
        audit.message = f"{len(results) - len(failed)} of {len(results)} checks passed"
        if failed:
            raise ReconciliationError("; ".join(r[2] for r in failed))
    return results
