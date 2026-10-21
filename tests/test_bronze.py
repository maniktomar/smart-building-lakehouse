"""Bronze ingestion: quarantine, schema drift, lineage columns and re-run safety."""

import os

import pytest
from pyspark.sql import functions as F

from sbl.bronze import SchemaDriftError, ingest_source
from tests.helpers import METER_HEADER, meter_rows, write_csv, write_text


def meters_dir(lh):
    return os.path.join(lh.landing_root, "meters")


def test_good_rows_land_with_lineage_columns(lh, cfg):
    path = write_csv(os.path.join(meters_dir(lh), "m1.csv"), METER_HEADER,
                     meter_rows("B1", "2016-01-01 00:00:00", 3, 1.5))
    ingest_source(lh, "meter_readings", cfg.sources["meter_readings"], "run-1")

    bronze = lh.read("bronze", "meter_readings")
    assert bronze.count() == 3
    row = bronze.first()
    assert row["_batch_id"] == "run-1"
    assert row["_source_file"] == os.path.abspath(path)
    assert row["_ingested_at"] is not None
    assert row["meter_reading"] == "1.5"          # bronze keeps the raw string


def test_bad_rows_go_to_quarantine_with_a_reason(lh, cfg):
    rows = meter_rows("B1", "2016-01-01 00:00:00", 2, 1.0) + [
        ["not-a-date", "B1", "electricity", "1.0"],
        ["2016-01-01 05:00:00", "", "electricity", "1.0"],
        ["2016-01-01 06:00:00", "B1", "electricity", "abc"],
        ["2016-01-01 07:00:00", "B1", "electricity", ""],
    ]
    write_csv(os.path.join(meters_dir(lh), "m1.csv"), METER_HEADER, rows)
    ingest_source(lh, "meter_readings", cfg.sources["meter_readings"], "run-1")

    assert lh.read("bronze", "meter_readings").count() == 2
    reasons = {r.reason for r in lh.read("ops", "quarantine").collect()}
    assert reasons == {"timestamp: invalid timestamp", "building_id: missing",
                       "meter_reading: invalid double", "meter_reading: missing"}


def test_malformed_csv_row_is_quarantined_not_dropped(lh, cfg):
    text = ("timestamp,building_id,meter,meter_reading\n"
            "2016-01-01 00:00:00,B1,electricity,1.0\n"
            "2016-01-01 01:00:00,B1\n")
    write_text(os.path.join(meters_dir(lh), "m1.csv"), text)
    ingest_source(lh, "meter_readings", cfg.sources["meter_readings"], "run-1")

    assert lh.read("bronze", "meter_readings").count() == 1
    bad = lh.read("ops", "quarantine").collect()
    assert len(bad) == 1
    assert "malformed record" in bad[0].reason
    assert "B1" in bad[0].raw_record


def test_rerun_skips_files_already_ingested(lh, cfg):
    write_csv(os.path.join(meters_dir(lh), "m1.csv"), METER_HEADER,
              meter_rows("B1", "2016-01-01 00:00:00", 4, 1.0))
    src = cfg.sources["meter_readings"]
    ingest_source(lh, "meter_readings", src, "run-1")
    ingest_source(lh, "meter_readings", src, "run-2")
    assert lh.read("bronze", "meter_readings").count() == 4

    write_csv(os.path.join(meters_dir(lh), "m2.csv"), METER_HEADER,
              meter_rows("B1", "2016-01-02 00:00:00", 2, 1.0))
    ingest_source(lh, "meter_readings", src, "run-3")
    bronze = lh.read("bronze", "meter_readings")
    assert bronze.count() == 6
    assert bronze.filter(F.col("_batch_id") == "run-3").count() == 2


def test_new_column_is_added_when_config_allows_it(lh, cfg):
    src = cfg.sources["meter_readings"]
    write_csv(os.path.join(meters_dir(lh), "m1.csv"), METER_HEADER,
              meter_rows("B1", "2016-01-01 00:00:00", 2, 1.0))
    ingest_source(lh, "meter_readings", src, "run-1")

    write_csv(os.path.join(meters_dir(lh), "m2.csv"), METER_HEADER + ["unit"],
              [r + ["kWh"] for r in meter_rows("B1", "2016-01-02 00:00:00", 2, 1.0)])
    ingest_source(lh, "meter_readings", src, "run-2")

    bronze = lh.read("bronze", "meter_readings")
    assert "unit" in bronze.columns
    assert bronze.filter(F.col("unit") == "kWh").count() == 2
    assert bronze.filter(F.col("unit").isNull()).count() == 2   # older rows: NULL
    msg = lh.read("ops", "audit_runs").filter(F.col("run_id") == "run-2").first().message
    assert "schema drift: added columns ['unit']" in msg


def test_new_column_fails_the_batch_when_config_forbids_it(lh, cfg):
    src = cfg.sources["meter_readings"]
    src["schema_drift"]["new_columns"] = "fail"
    write_csv(os.path.join(meters_dir(lh), "m1.csv"), METER_HEADER,
              meter_rows("B1", "2016-01-01 00:00:00", 2, 1.0))
    ingest_source(lh, "meter_readings", src, "run-1")
    write_csv(os.path.join(meters_dir(lh), "m2.csv"), METER_HEADER + ["unit"],
              [r + ["kWh"] for r in meter_rows("B1", "2016-01-02 00:00:00", 2, 1.0)])

    with pytest.raises(SchemaDriftError, match="new columns not allowed"):
        ingest_source(lh, "meter_readings", src, "run-2")
    assert lh.read("bronze", "meter_readings").count() == 2


def test_missing_configured_column_fails_and_is_audited(lh, cfg):
    write_csv(os.path.join(meters_dir(lh), "m1.csv"), ["timestamp", "building_id", "meter"],
              [["2016-01-01 00:00:00", "B1", "electricity"]])
    with pytest.raises(SchemaDriftError, match="meter_reading"):
        ingest_source(lh, "meter_readings", cfg.sources["meter_readings"], "run-1")

    audit = lh.read("ops", "audit_runs").first()
    assert audit.status == "FAILED"
    assert "SchemaDriftError" in audit.message
    assert not lh.exists("ops", "ingested_files")    # the file will be retried next run


def test_no_new_files_is_a_successful_no_op(lh, cfg):
    ingest_source(lh, "meter_readings", cfg.sources["meter_readings"], "run-1")
    audit = lh.read("ops", "audit_runs").first()
    assert (audit.status, audit.message) == ("SUCCESS", "no new files")
