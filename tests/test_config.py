"""Config loading and validation (no Spark needed)."""

import copy

import pytest

from sbl.config import known_tables, load_config, validate


@pytest.mark.parametrize("env", ["local", "databricks"])
def test_repo_config_is_valid(env):
    assert validate(load_config(env)) == []


def test_catalog_override_also_moves_the_landing_volume():
    cfg = load_config("databricks", catalog="workspace")
    assert cfg.env["catalog"] == "workspace"
    assert cfg.env["landing_root"] == "/Volumes/workspace/bronze/landing"


def test_known_tables_cover_every_layer():
    tables = known_tables(load_config("local"))
    assert {"bronze.meter_readings", "silver.meter_readings", "gold.fact_daily_energy"} <= tables


def test_validation_reports_each_broken_setting():
    cfg = copy.deepcopy(load_config("local"))
    cfg.sources["meter_readings"]["columns"][3]["type"] = "decimal"
    cfg.sources["meter_readings"]["schema_drift"]["new_columns"] = "ignore"
    cfg.silver["meter_readings"]["keys"].append("not_a_column")
    cfg.silver["weather_hourly"]["source"] = "missing_source"
    cfg.quality["rules"]["silver.meter_readings"][4]["ref_table"] = "silver.nope"
    cfg.quality["rules"]["silver.buildings"][0]["severity"] = "fatal"

    problems = "\n".join(validate(cfg))
    assert "meter_reading: type must be one of" in problems
    assert "schema_drift.new_columns" in problems
    assert "key 'not_a_column'" in problems
    assert "source 'missing_source' is not defined" in problems
    assert "ref_table 'silver.nope' is unknown" in problems
    assert "severity must be" in problems


def test_timestamp_columns_need_a_format():
    cfg = copy.deepcopy(load_config("local"))
    del cfg.sources["meter_readings"]["columns"][0]["format"]
    assert any("timestamp columns need a format" in p for p in validate(cfg))
