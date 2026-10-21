"""Gold: star schema for Power BI, rebuilt from silver on every run.

  dim_building        one row per building
  dim_date            one row per date
  fact_daily_weather  site x date: temperatures and degree days
  fact_daily_energy   building x date: kWh, kWh/m², weather-normalised use, anomaly flag

Simplification: gold is small (a few thousand rows), so it is fully overwritten each run.
A production system with large gold tables would MERGE only the affected dates instead.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sbl.audit import audited
from sbl.lakehouse import Lakehouse

MODEL_SCHEMA = ("building_id string, intercept double, coef_hdd double, coef_cdd double, "
                "residual_std double, n_days long")


def date_key(col: str) -> F.Column:
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


# ---- builders (pure DataFrame -> DataFrame, unit-tested) ----------------------------------
def build_dim_building(buildings: DataFrame) -> DataFrame:
    return buildings.select("building_id", "site_id", "primary_use", "floor_area_sqm",
                            "year_built", "latitude", "longitude", "timezone")


def build_dim_date(readings: DataFrame) -> DataFrame:
    bounds = readings.agg(F.min(F.to_date("reading_ts")).alias("lo"),
                          F.max(F.to_date("reading_ts")).alias("hi"))
    days = bounds.select(F.explode(F.sequence("lo", "hi")).alias("date"))
    return days.select(
        date_key("date").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "EEEE").alias("day_of_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),   # 1 = Sunday, 7 = Saturday
    )


def build_fact_daily_weather(weather: DataFrame, base_c: float) -> DataFrame:
    daily = (weather.withColumn("date", F.to_date("observed_ts"))
             .groupBy("site_id", "date")
             .agg(F.avg("temperature_c").alias("avg_temperature_c"),
                  F.min("temperature_c").alias("min_temperature_c"),
                  F.max("temperature_c").alias("max_temperature_c"),
                  F.count("*").alias("hours_observed")))
    return daily.select(
        "site_id", date_key("date").alias("date_key"), "date",
        "avg_temperature_c", "min_temperature_c", "max_temperature_c",
        F.greatest(F.lit(0.0), F.lit(base_c) - F.col("avg_temperature_c")).alias("heating_degree_days"),
        F.greatest(F.lit(0.0), F.col("avg_temperature_c") - F.lit(base_c)).alias("cooling_degree_days"),
        "hours_observed",
    )


def daily_energy(readings: DataFrame, buildings: DataFrame) -> DataFrame:
    daily = (readings.withColumn("date", F.to_date("reading_ts"))
             .groupBy("building_id", "date")
             .agg(F.sum("reading_kwh").alias("energy_kwh"),
                  F.count("*").alias("hours_reported"),
                  F.sum(F.when(F.col("reading_kwh") == 0, 1).otherwise(0)).alias("zero_hours")))
    return (daily.join(buildings.select("building_id", "site_id", "floor_area_sqm"), "building_id")
            .withColumn("completeness_pct",
                        F.round((F.col("hours_reported") - F.col("zero_hours")) / 24 * 100, 2))
            .withColumn("energy_kwh_per_sqm",
                        F.try_divide(F.col("energy_kwh"), F.col("floor_area_sqm"))))


def _fit(pdf: pd.DataFrame) -> pd.DataFrame:
    """Ordinary least squares per building: energy_kwh = a + b*HDD + c*CDD."""
    x = np.column_stack([np.ones(len(pdf)), pdf["heating_degree_days"], pdf["cooling_degree_days"]])
    y = pdf["energy_kwh"].to_numpy()
    coef, *_ = np.linalg.lstsq(x, y, rcond=None)
    residuals = y - x @ coef
    return pd.DataFrame([{
        "building_id": pdf["building_id"].iloc[0],
        "intercept": float(coef[0]), "coef_hdd": float(coef[1]), "coef_cdd": float(coef[2]),
        "residual_std": float(residuals.std(ddof=3)) if len(pdf) > 3 else float("nan"),
        "n_days": len(pdf),
    }])


def build_fact_daily_energy(energy: DataFrame, weather: DataFrame, params: dict) -> DataFrame:
    """Join weather, fit a per-building weather model on complete days, flag outliers."""
    min_pct = params["min_completeness"] * 100
    joined = (energy.join(weather.select("site_id", "date", "avg_temperature_c",
                                         "heating_degree_days", "cooling_degree_days"),
                          ["site_id", "date"], "left")
              .withColumn("data_quality_flag",
                          F.when(F.col("completeness_pct") >= min_pct, "ok").otherwise("incomplete")))

    training = joined.filter((F.col("data_quality_flag") == "ok")
                             & F.col("heating_degree_days").isNotNull())
    models = (training.select("building_id", "energy_kwh", "heating_degree_days",
                              "cooling_degree_days")
              .groupBy("building_id").applyInPandas(_fit, MODEL_SCHEMA))

    # Spark sorts NaN above every number (so NaN > 3 is true): turn NaN/0 spreads into NULL.
    models = models.withColumn(
        "residual_std",
        F.when(~F.isnan("residual_std") & (F.col("residual_std") > 0), F.col("residual_std")))
    modelled = joined.join(models, "building_id", "left")
    expected = (F.col("intercept") + F.col("coef_hdd") * F.col("heating_degree_days")
                + F.col("coef_cdd") * F.col("cooling_degree_days"))
    is_ok = F.col("data_quality_flag") == "ok"
    out = (modelled
           .withColumn("expected_kwh", F.when(is_ok, expected))
           .withColumn("weather_normalised_ratio",
                       F.try_divide(F.col("energy_kwh"), F.col("expected_kwh")))
           .withColumn("residual_z",
                       F.try_divide(F.col("energy_kwh") - F.col("expected_kwh"),
                                    F.col("residual_std")))
           .withColumn("is_anomaly",
                       F.coalesce(is_ok & (F.abs("residual_z") > params["anomaly_z_threshold"]),
                                  F.lit(False))))
    return out.select(
        "building_id", "site_id", date_key("date").alias("date_key"), "date",
        "energy_kwh", "hours_reported", "zero_hours", "completeness_pct", "energy_kwh_per_sqm",
        "avg_temperature_c", "heating_degree_days", "cooling_degree_days",
        "expected_kwh", "weather_normalised_ratio", "residual_z", "data_quality_flag", "is_anomaly",
    )


# ---- orchestration ------------------------------------------------------------------------
def build_all(lh: Lakehouse, gold_cfg: dict, run_id: str) -> None:
    params, docs = gold_cfg["params"], gold_cfg["tables"]
    buildings = lh.read("silver", "buildings")
    readings = lh.read("silver", "meter_readings")
    weather_hourly = lh.read("silver", "weather_hourly")

    weather = build_fact_daily_weather(weather_hourly, params["degree_day_base_c"])
    tables = {
        "dim_building": (build_dim_building(buildings), buildings),
        "dim_date": (build_dim_date(readings), readings),
        "fact_daily_weather": (weather, weather_hourly),
        "fact_daily_energy": (build_fact_daily_energy(daily_energy(readings, buildings),
                                                      weather, params), readings),
    }
    for name, (df, source) in tables.items():
        with audited(lh, run_id, "gold", f"gold.{name}") as audit:
            audit.rows_read = source.count()
            lh.write(df, "gold", name, mode="overwrite")
            audit.rows_written = lh.read("gold", name).count()
            lh.set_comments("gold", name, docs[name]["description"], docs[name]["columns"])

