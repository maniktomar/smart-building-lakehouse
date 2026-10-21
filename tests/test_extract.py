"""Open-Meteo extract: month ranges, URLs, retries and skip-existing (no network)."""

import io
import json
import os
from urllib.parse import parse_qs, urlparse

import pytest

from sbl.extract import build_url, extract_weather, fetch, month_range
from tests.helpers import open_meteo_payload


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def fake_opener(calls, fail_first=0):
    def opener(url, timeout):
        calls.append(url)
        if len(calls) <= fail_first:
            raise OSError("temporary network error")
        return FakeResponse(json.dumps(open_meteo_payload("2016-01-01 00:00:00", [5.0])).encode())
    return opener


def test_month_range_handles_year_end_and_leap_years():
    months = month_range("2015-12", "2016-02")
    assert months == [("2015-12", "2015-12-01", "2015-12-31"),
                      ("2016-01", "2016-01-01", "2016-01-31"),
                      ("2016-02", "2016-02-01", "2016-02-29")]


def test_build_url(cfg):
    api = cfg.sources["weather_hourly"]["api"]
    url = build_url(api, 28.517689, -81.379039, "US/Eastern", "2016-01-01", "2016-01-31")
    query = parse_qs(urlparse(url).query)
    assert url.startswith("https://archive-api.open-meteo.com/v1/archive?")
    assert query["latitude"] == ["28.5177"] and query["timezone"] == ["US/Eastern"]
    assert query["hourly"] == ["temperature_2m,relative_humidity_2m"]


def test_fetch_retries_then_succeeds():
    calls = []
    body = fetch("http://x", fake_opener(calls, fail_first=2), backoff_s=0)
    assert len(calls) == 3 and b"hourly" in body


def test_fetch_gives_up_loudly():
    with pytest.raises(OSError):
        fetch("http://x", fake_opener([], fail_first=99), retries=2, backoff_s=0)


def test_fetch_rejects_a_response_without_data():
    def opener(url, timeout):
        return FakeResponse(b'{"error": true, "reason": "bad date"}')
    with pytest.raises(ValueError, match="no 'hourly' block"):
        fetch("http://x", opener, retries=1)


def test_extract_writes_one_file_per_site_month_and_skips_existing(lh, spark, cfg):
    lh.write(spark.createDataFrame([("B1", "S1", 50.0, -1.0, "Europe/London")],
                                   ["building_id", "site_id", "latitude", "longitude", "timezone"]),
             "silver", "buildings")
    src = cfg.sources["weather_hourly"]
    src["api"].update(start_month="2016-01", end_month="2016-03")

    calls = []
    extract_weather(lh, "weather_hourly", src, "run-1", opener=fake_opener(calls))
    folder = os.path.join(lh.landing_root, "weather", "site_id=S1")
    assert sorted(os.listdir(folder)) == ["2016-01.json", "2016-02.json", "2016-03.json"]
    assert len(calls) == 3

    extract_weather(lh, "weather_hourly", src, "run-2", opener=fake_opener(calls))
    assert len(calls) == 3                       # nothing fetched again
