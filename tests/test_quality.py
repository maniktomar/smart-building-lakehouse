"""Data-quality rules: each rule type, logging, and fail-loudly behaviour."""

from datetime import timedelta

import pytest

from sbl.audit import utc_now
from sbl.quality import DataQualityError, evaluate, run_rules


@pytest.fixture
def readings(spark):
    return spark.createDataFrame(
        [("B1", 1, 5.0, "electricity"), ("B1", 1, 0.0, "electricity"),
         ("B2", 2, -1.0, "gas"), (None, 3, 20000.0, "electricity")],
        ["building_id", "hour", "kwh", "meter"])


@pytest.mark.parametrize("rule, failed", [
    ({"type": "not_null", "columns": ["building_id"]}, 1),
    ({"type": "unique", "columns": ["building_id", "hour"]}, 1),
    ({"type": "range", "column": "kwh", "min": 0, "max": 10000}, 2),
    ({"type": "accepted_values", "column": "meter", "values": ["electricity"]}, 1),
    ({"type": "row_count_min", "min": 5}, 1),
    ({"type": "row_count_min", "min": 4}, 0),
    ({"type": "max_share", "expr": "kwh = 0", "max_share": 0.2}, 1),   # 25% > 20%
    ({"type": "max_share", "expr": "kwh = 0", "max_share": 0.3}, 0),
])
def test_rule_types(lh, readings, rule, failed):
    assert evaluate(lh, readings, rule)[0] == failed


def test_references_rule_finds_orphans(lh, spark, readings):
    lh.write(spark.createDataFrame([("B1",)], ["building_id"]), "silver", "buildings")
    rule = {"type": "references", "column": "building_id",
            "ref_table": "silver.buildings", "ref_column": "building_id"}
    assert evaluate(lh, readings, rule)[0] == 1          # B2 (the NULL key is not an orphan)


def test_freshness_rule(lh, spark):
    rule = {"type": "freshness", "column": "ts", "max_age_hours": 48}
    fresh = spark.createDataFrame([(utc_now(),)], ["ts"])
    stale = spark.createDataFrame([(utc_now() - timedelta(days=3),)], ["ts"])
    assert evaluate(lh, fresh, rule)[0] == 0
    assert evaluate(lh, stale, rule)[0] == 1


def test_error_rules_fail_the_run_after_everything_is_logged(lh, spark):
    lh.write(spark.createDataFrame([("B1",), ("B1",)], ["building_id"]), "silver", "buildings")
    cfg = {"rules": {"silver.buildings": [
        {"name": "dupes", "type": "unique", "columns": ["building_id"], "severity": "error"},
        {"name": "count", "type": "row_count_min", "min": 10, "severity": "warn"},
        {"name": "not_null", "type": "not_null", "columns": ["building_id"], "severity": "error"},
    ]}}
    with pytest.raises(DataQualityError, match="silver.buildings.dupes"):
        run_rules(lh, cfg, "run-1")

    logged = {r.rule_name: r.status for r in lh.read("ops", "dq_results").collect()}
    assert logged == {"dupes": "FAIL", "count": "FAIL", "not_null": "PASS"}
    assert lh.read("ops", "audit_runs").first().status == "FAILED"


def test_warnings_alone_do_not_fail_the_run(lh, spark):
    lh.write(spark.createDataFrame([("B1",)], ["building_id"]), "silver", "buildings")
    cfg = {"rules": {"silver.buildings": [
        {"name": "count", "type": "row_count_min", "min": 10, "severity": "warn"}]}}
    results = run_rules(lh, cfg, "run-1")
    assert results[0][6] == "FAIL"
    assert lh.read("ops", "audit_runs").first().status == "SUCCESS"


def test_missing_table_is_a_failure_not_a_crash(lh):
    cfg = {"rules": {"gold.fact_daily_energy": [
        {"name": "rows", "type": "row_count_min", "min": 1, "severity": "warn"}]}}
    results = run_rules(lh, cfg, "run-1")
    assert results[0][6] == "FAIL" and results[0][9] == "table does not exist"


def test_unknown_rule_type_raises(lh, readings):
    with pytest.raises(ValueError):
        evaluate(lh, readings, {"type": "telepathy"})

