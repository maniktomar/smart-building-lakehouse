"""Gold builders: dimensions, degree days, daily energy and the weather model."""

from datetime import date, datetime, timedelta

import pytest

from sbl.gold import (
    build_dim_date,
    build_fact_daily_energy,
    build_fact_daily_weather,
    daily_energy,
)

PARAMS = {"degree_day_base_c": 18.0, "min_completeness": 0.9, "anomaly_z_threshold": 3.0}


@pytest.fixture
def buildings(spark):
    return spark.createDataFrame([("B1", "S1", 100.0)], ["building_id", "site_id", "floor_area_sqm"])


def test_dim_date_covers_every_day_and_flags_weekends(spark):
    readings = spark.createDataFrame([(datetime(2016, 1, 1, 5),), (datetime(2016, 1, 10, 3),)],
                                     ["reading_ts"])
    days = {r.date: r for r in build_dim_date(readings).collect()}
    assert len(days) == 10
    assert days[date(2016, 1, 1)].date_key == 20160101
    assert days[date(2016, 1, 1)].day_of_week == "Friday"
    assert days[date(2016, 1, 2)].is_weekend and not days[date(2016, 1, 4)].is_weekend


def test_daily_weather_degree_days(spark):
    weather = spark.createDataFrame(
        [("S1", datetime(2016, 1, 1, h), 10.0) for h in range(24)]
        + [("S1", datetime(2016, 1, 2, h), 20.0 + (h % 2)) for h in range(24)],
        ["site_id", "observed_ts", "temperature_c"])
    rows = {r.date: r for r in build_fact_daily_weather(weather, 18.0).collect()}
    cold, warm = rows[date(2016, 1, 1)], rows[date(2016, 1, 2)]
    assert (cold.heating_degree_days, cold.cooling_degree_days, cold.hours_observed) == (8.0, 0.0, 24)
    assert (warm.heating_degree_days, warm.cooling_degree_days) == (0.0, 2.5)
    assert (warm.min_temperature_c, warm.max_temperature_c) == (20.0, 21.0)


def test_daily_energy_totals_completeness_and_intensity(spark, buildings):
    readings = spark.createDataFrame(
        [("B1", datetime(2016, 1, 1, h), 0.0 if h < 6 else 2.0) for h in range(24)],
        ["building_id", "reading_ts", "reading_kwh"])
    row = daily_energy(readings, buildings).first()
    assert (row.energy_kwh, row.hours_reported, row.zero_hours) == (36.0, 24, 6)
    assert row.completeness_pct == 75.0
    assert row.energy_kwh_per_sqm == pytest.approx(0.36)


def synthetic_days(n=60, spike_day=40):
    """energy = 100 + 5*HDD + 3*CDD with small noise; one day 4x too high."""
    rows, weather = [], []
    for i in range(n):
        d = date(2016, 1, 1) + timedelta(days=i)
        hdd, cdd = (i % 10) * 1.0, (i % 7) * 1.0
        noise = [0.5, -0.5, 0.25, -0.25][i % 4]
        energy = 100 + 5 * hdd + 3 * cdd + noise
        if i == spike_day:
            energy *= 4
        rows.append(("B1", "S1", d, energy, 24, 0, 100.0, energy / 100))
        weather.append(("S1", d, 10.0, hdd, cdd))
    return rows, weather


def test_weather_model_recovers_coefficients_and_flags_spike(spark):
    rows, weather = synthetic_days()
    energy = spark.createDataFrame(rows, ["building_id", "site_id", "date", "energy_kwh",
                                          "hours_reported", "zero_hours", "completeness_pct",
                                          "energy_kwh_per_sqm"])
    weather_df = spark.createDataFrame(weather, ["site_id", "date", "avg_temperature_c",
                                                 "heating_degree_days", "cooling_degree_days"])
    fact = {r.date: r for r in build_fact_daily_energy(energy, weather_df, PARAMS).collect()}

    spike = fact[date(2016, 1, 1) + timedelta(days=40)]
    normal = fact[date(2016, 1, 1) + timedelta(days=3)]
    assert spike.is_anomaly and spike.residual_z > 3
    assert not normal.is_anomaly
    assert sum(r.is_anomaly for r in fact.values()) == 1
    # Expected energy for a normal day is close to the true formula 100 + 5*3 + 3*3 = 124.
    assert normal.expected_kwh == pytest.approx(124, rel=0.1)
    assert normal.weather_normalised_ratio == pytest.approx(1.0, abs=0.1)


def test_incomplete_days_are_not_modelled_or_flagged(spark):
    energy = spark.createDataFrame(
        [("B1", "S1", date(2016, 1, 1), 5.0, 24, 20, 16.67, 0.05)],
        ["building_id", "site_id", "date", "energy_kwh", "hours_reported", "zero_hours",
         "completeness_pct", "energy_kwh_per_sqm"])
    weather = spark.createDataFrame([("S1", date(2016, 1, 1), 10.0, 8.0, 0.0)],
                                    ["site_id", "date", "avg_temperature_c",
                                     "heating_degree_days", "cooling_degree_days"])
    row = build_fact_daily_energy(energy, weather, PARAMS).first()
    assert row.data_quality_flag == "incomplete"
    assert row.expected_kwh is None and row.is_anomaly is False


def test_too_few_days_gives_no_z_score_rather_than_a_false_anomaly(spark):
    """Guards the NaN case: Spark treats NaN > 3 as true."""
    rows, weather = synthetic_days(n=3, spike_day=99)
    energy = spark.createDataFrame(rows, ["building_id", "site_id", "date", "energy_kwh",
                                          "hours_reported", "zero_hours", "completeness_pct",
                                          "energy_kwh_per_sqm"])
    weather_df = spark.createDataFrame(weather, ["site_id", "date", "avg_temperature_c",
                                                 "heating_degree_days", "cooling_degree_days"])
    out = build_fact_daily_energy(energy, weather_df, PARAMS).collect()
    assert all(r.residual_z is None and r.is_anomaly is False for r in out)
