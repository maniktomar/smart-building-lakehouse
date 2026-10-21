"""End-to-end: a tiny landing dataset through every stage, checked against hand-computed gold.

Data: 2 buildings on site S1.
  B1: 1.0 kWh every hour on 1-2 Jan, 1.5 kWh every hour on 1 Feb  (floor area 100 m²)
  B2: 2.0 kWh every hour on 1-2 Jan                                 (floor area 200 m²)
  plus 3 bad meter rows that must be quarantined.
Weather: 10 °C all day 1 Jan, 20 °C on 2 Jan, 18 °C on 1 Feb (degree-day base 18 °C).
"""

import os

import pytest
from pyspark.sql import functions as F

from sbl import report, restore_demo
from sbl.config import load_config
from sbl.lakehouse import Lakehouse
from sbl.reconcile import ReconciliationError
from sbl.reconcile import run as reconcile
from sbl.run import run_stage
from tests.helpers import (
    BUILDING_HEADER,
    METER_HEADER,
    meter_rows,
    open_meteo_payload,
    write_csv,
    write_weather,
)


def build_landing(landing):
    write_csv(os.path.join(landing, "buildings", "buildings.csv"), BUILDING_HEADER,
              [["B1", "S1", "Office", "100", "50.0", "-1.0", "Europe/London", "2000"],
               ["B2", "S1", "Education", "200", "50.0", "-1.0", "Europe/London", "1990.0"]])
    write_csv(os.path.join(landing, "meters", "electricity_2016-01.csv"), METER_HEADER,
              meter_rows("B1", "2016-01-01 00:00:00", 48, 1.0)
              + meter_rows("B2", "2016-01-01 00:00:00", 48, 2.0)
              + [["not-a-date", "B1", "electricity", "1.0"],
                 ["2016-01-03 00:00:00", "B1", "electricity", ""],
                 ["2016-01-03 01:00:00", "B2", "electricity", "abc"]])
    write_csv(os.path.join(landing, "meters", "electricity_2016-02.csv"), METER_HEADER,
              meter_rows("B1", "2016-02-01 00:00:00", 24, 1.5))
    write_weather(landing, "S1", "2016-01",
                  open_meteo_payload("2016-01-01 00:00:00", [10.0] * 24 + [20.0] * 24))
    write_weather(landing, "S1", "2016-02",
                  open_meteo_payload("2016-02-01 00:00:00", [18.0] * 24))


def run_all(lh, cfg, run_id, reset_watermark=False):
    for stage in cfg.stages:
        run_stage(lh, cfg, stage, run_id, reset_watermark)


@pytest.fixture(scope="module")
def pipeline(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("e2e")
    cfg = load_config("local", storage_root=str(root / "lakehouse"),
                      landing_root=str(root / "landing"))
    # Weather files are already in landing, so the extract step finds nothing to download.
    cfg.sources["weather_hourly"]["api"].update(start_month="2016-01", end_month="2016-02")
    lh = Lakehouse(spark, cfg.env)
    build_landing(lh.landing_root)
    run_all(lh, cfg, "run-1")
    return lh, cfg


def counts(lh, *tables):
    return {t: lh.read(*lh.split(t)).count() for t in tables}


ALL_TABLES = ["bronze.buildings", "bronze.meter_readings", "bronze.weather_hourly",
              "silver.buildings", "silver.meter_readings", "silver.weather_hourly",
              "gold.dim_building", "gold.dim_date", "gold.fact_daily_weather",
              "gold.fact_daily_energy", "ops.quarantine"]


def test_row_counts_through_every_layer(pipeline):
    lh, _ = pipeline
    assert counts(lh, *ALL_TABLES) == {
        "bronze.buildings": 2, "bronze.meter_readings": 120, "bronze.weather_hourly": 72,
        "silver.buildings": 2, "silver.meter_readings": 120, "silver.weather_hourly": 72,
        "gold.dim_building": 2, "gold.dim_date": 32, "gold.fact_daily_weather": 3,
        "gold.fact_daily_energy": 5, "ops.quarantine": 3,
    }


def test_gold_fact_values(pipeline):
    lh, _ = pipeline
    fact = {(r.building_id, r.date_key): r for r in lh.read("gold", "fact_daily_energy").collect()}
    b1_jan1, b2_jan2, b1_feb1 = fact[("B1", 20160101)], fact[("B2", 20160102)], fact[("B1", 20160201)]

    assert (b1_jan1.energy_kwh, b1_jan1.energy_kwh_per_sqm) == (24.0, 0.24)
    assert (b2_jan2.energy_kwh, b2_jan2.energy_kwh_per_sqm) == (48.0, 0.24)
    assert b1_feb1.energy_kwh == 36.0
    assert (b1_jan1.heating_degree_days, b1_jan1.cooling_degree_days) == (8.0, 0.0)
    assert (b2_jan2.heating_degree_days, b2_jan2.cooling_degree_days) == (0.0, 2.0)
    assert all(r.completeness_pct == 100.0 and r.data_quality_flag == "ok" for r in fact.values())
    assert not any(r.is_anomaly for r in fact.values())


def test_audit_quality_and_reconciliation_all_pass(pipeline):
    lh, _ = pipeline
    audit = lh.read("ops", "audit_runs").filter(F.col("run_id") == "run-1")
    assert audit.filter(F.col("status") != "SUCCESS").count() == 0
    bronze_meters = audit.filter(F.col("target_table") == "bronze.meter_readings").first()
    assert (bronze_meters.rows_read, bronze_meters.rows_written,
            bronze_meters.rows_quarantined) == (123, 120, 3)

    dq = lh.read("ops", "dq_results").filter(F.col("run_id") == "run-1")
    assert dq.filter((F.col("status") == "FAIL") & (F.col("severity") == "error")).count() == 0
    recon = lh.read("ops", "reconciliation").filter(F.col("run_id") == "run-1")
    assert recon.count() == 10
    assert recon.filter(F.col("status") == "FAIL").count() == 0


def test_rerun_and_full_reprocess_are_idempotent(pipeline):
    lh, cfg = pipeline
    before = counts(lh, *ALL_TABLES)
    run_all(lh, cfg, "run-2")
    run_all(lh, cfg, "run-3", reset_watermark=True)
    assert counts(lh, *ALL_TABLES) == before


def test_restore_demo_undoes_a_bad_load(pipeline):
    lh, cfg = pipeline
    result = restore_demo.demo(lh, cfg, "run-restore", month="2016-01")
    assert result["bad_rows"] == 48                       # B1, January
    assert result["rule_failed_rows"] == 0                # 1 kWh x 1000 = 1000, still in range
    assert result["after_bad_load"][1] == pytest.approx(result["before"][1] + 48 * 999)
    assert result["after_restore"] == result["before"]
    assert result["history"][0][1] == "RESTORE"


def test_reconciliation_catches_a_lost_row(pipeline):
    lh, cfg = pipeline
    version = lh.delta("silver", "meter_readings").history(1).first().version
    lh.delta("silver", "meter_readings").delete("building_id = 'B2' AND hour(reading_ts) = 5")
    try:
        with pytest.raises(ReconciliationError, match="distinct keys bronze = silver"):
            reconcile(lh, cfg, "run-broken")
    finally:
        lh.sql(f"RESTORE TABLE {lh.ref('silver', 'meter_readings')} TO VERSION AS OF {version}")


def test_report_renders_every_query(pipeline):
    lh, cfg = pipeline
    text = report.build_report(lh, cfg)
    for name in report.load_queries():
        assert f"## {name}" in text
    assert "_Query failed" not in text
    assert "| gold.fact_daily_energy | 5 |" in text
