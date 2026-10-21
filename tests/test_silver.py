"""Silver: typing, deduplication, MERGE upserts, watermarks, idempotency and constraints."""

import os
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from sbl.bronze import ingest_source
from sbl.silver import build_table, get_watermark, transform
from tests.helpers import METER_HEADER, meter_rows, write_csv


def land(lh, name, rows):
    return write_csv(os.path.join(lh.landing_root, "meters", name), METER_HEADER, rows)


def load(lh, cfg, run_id, reset=False):
    ingest_source(lh, "meter_readings", cfg.sources["meter_readings"], run_id)
    build_table(lh, "meter_readings", cfg.silver["meter_readings"], run_id, reset)
    return lh.read("silver", "meter_readings")


def test_transform_casts_renames_and_keeps_latest_duplicate(spark, cfg):
    bronze = spark.createDataFrame(
        [("2016-01-01 00:00:00", "B1", "electricity", "1.0", datetime(2026, 1, 1)),
         ("2016-01-01 00:00:00", "B1", "electricity", "9.0", datetime(2026, 1, 2)),  # newer
         ("2016-01-01 01:00:00", "B1", "electricity", "2.0", datetime(2026, 1, 1))],
        ["timestamp", "building_id", "meter", "meter_reading", "_ingested_at"])
    out = transform(bronze, cfg.silver["meter_readings"]).orderBy("reading_ts").collect()

    assert [r.reading_kwh for r in out] == [9.0, 2.0]
    assert out[0].reading_ts == datetime(2016, 1, 1, 0, 0)
    assert set(out[0].asDict()) == {"reading_ts", "building_id", "meter", "reading_kwh",
                                    "_source_ingested_at"}


def test_merge_inserts_then_updates_in_place(lh, cfg):
    land(lh, "m1.csv", meter_rows("B1", "2016-01-01 00:00:00", 3, 1.0))
    assert load(lh, cfg, "run-1").count() == 3

    # A corrected re-delivery of one hour plus one new hour.
    land(lh, "m1_fix.csv", [["2016-01-01 01:00:00", "B1", "electricity", "5.0"],
                            ["2016-01-01 03:00:00", "B1", "electricity", "1.0"]])
    silver = load(lh, cfg, "run-2")
    assert silver.count() == 4
    fixed = silver.filter(F.hour("reading_ts") == 1).first()
    assert (fixed.reading_kwh, fixed._batch_id) == (5.0, "run-2")

    msg = (lh.read("ops", "audit_runs")
           .filter((F.col("run_id") == "run-2") & (F.col("step") == "silver")).first().message)
    assert msg.startswith("inserted=1 updated=1")


def test_watermark_limits_work_to_new_bronze_rows(lh, cfg):
    land(lh, "m1.csv", meter_rows("B1", "2016-01-01 00:00:00", 5, 1.0))
    load(lh, cfg, "run-1")
    first_wm = get_watermark(lh, "silver.meter_readings")

    land(lh, "m2.csv", meter_rows("B1", "2016-01-02 00:00:00", 2, 1.0))
    load(lh, cfg, "run-2")
    audit = (lh.read("ops", "audit_runs")
             .filter((F.col("run_id") == "run-2") & (F.col("step") == "silver")).first())
    assert audit.rows_read == 2                  # only the new file's rows
    assert get_watermark(lh, "silver.meter_readings") > first_wm


def test_rerun_and_full_reprocess_do_not_duplicate(lh, cfg):
    land(lh, "m1.csv", meter_rows("B1", "2016-01-01 00:00:00", 24, 1.0))
    before = load(lh, cfg, "run-1").count()

    load(lh, cfg, "run-2")                       # nothing new
    load(lh, cfg, "run-3", reset=True)           # reprocess every bronze row via MERGE
    after = lh.read("silver", "meter_readings")
    assert before == after.count() == 24
    assert after.agg(F.sum("reading_kwh")).first()[0] == 24.0


def test_check_constraint_rejects_negative_readings(lh, cfg):
    land(lh, "m1.csv", meter_rows("B1", "2016-01-01 00:00:00", 2, 1.0))
    load(lh, cfg, "run-1")
    land(lh, "m2.csv", [["2016-01-02 00:00:00", "B1", "electricity", "-3.0"]])

    with pytest.raises(Exception, match="reading_not_negative"):
        load(lh, cfg, "run-2")
    assert lh.read("silver", "meter_readings").count() == 2
    failed = lh.read("ops", "audit_runs").filter(F.col("status") == "FAILED").first()
    assert failed.target_table == "silver.meter_readings"
