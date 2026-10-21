"""Open-Meteo JSON parsing and the safe cast expressions."""

import os

from pyspark.sql import functions as F

from sbl.bronze import ingest_source
from sbl.casts import cast_expr
from sbl.parsers import read_open_meteo_json
from tests.helpers import open_meteo_payload, write_text, write_weather


def test_open_meteo_response_explodes_to_one_row_per_hour(spark, lh, cfg):
    path = write_weather(lh.landing_root, "S1", "2016-01",
                         open_meteo_payload("2016-01-01 00:00:00", [1.0, 2.5, 3.0]))
    df = read_open_meteo_json(spark, [path], cfg.sources["weather_hourly"])
    rows = df.orderBy("time").collect()

    assert [r.time for r in rows] == ["2016-01-01T00:00", "2016-01-01T01:00", "2016-01-01T02:00"]
    assert [r.temperature_2m for r in rows] == ["1.0", "2.5", "3.0"]
    assert {r.site_id for r in rows} == {"S1"}             # taken from the folder name
    assert all(r._corrupt_record is None for r in rows)


def test_corrupt_json_file_is_quarantined(lh, cfg):
    write_weather(lh.landing_root, "S1", "2016-01",
                  open_meteo_payload("2016-01-01 00:00:00", [1.0, 2.0]))
    write_text(os.path.join(lh.landing_root, "weather", "site_id=S1", "2016-02.json"),
               '{"hourly": {"time": ["2016-02-01T00:00"')       # truncated download
    ingest_source(lh, "weather_hourly", cfg.sources["weather_hourly"], "run-1")

    assert lh.read("bronze", "weather_hourly").count() == 2
    bad = lh.read("ops", "quarantine").collect()
    assert len(bad) == 1 and bad[0].reason.startswith("malformed record")


def test_cast_expressions_return_null_instead_of_failing(spark):
    df = spark.createDataFrame(
        [(" 1.5 ", "1976.0", "2016-01-01 00:00:00", "  "),
         ("abc", "x", "01/01/2016", "B1")],
        ["d", "i", "t", "s"])
    out = df.select(
        F.expr(cast_expr("d", "double")).alias("d"),
        F.expr(cast_expr("i", "int")).alias("i"),
        F.expr(cast_expr("t", "timestamp", "yyyy-MM-dd HH:mm:ss")).alias("t"),
        F.expr(cast_expr("s", "string")).alias("s"),
    ).collect()
    assert (out[0].d, out[0].i, out[0].s) == (1.5, 1976, None)
    assert out[0].t is not None
    assert (out[1].d, out[1].i, out[1].t, out[1].s) == (None, None, None, "B1")
