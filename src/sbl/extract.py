"""Extract: batch-download hourly weather from the Open-Meteo archive API into the landing zone.

One request per site and month, saved unchanged as landing/weather/site_id=<id>/<YYYY-MM>.json.
Months already on disk are skipped, so re-running only fetches what is missing.
Site coordinates and timezones come from silver.buildings.
"""

from __future__ import annotations

import calendar
import json
import os
import time
from urllib.parse import urlencode
from urllib.request import urlopen

from pyspark.sql import functions as F

from sbl.audit import audited
from sbl.lakehouse import Lakehouse


def month_range(start: str, end: str) -> list[tuple[str, str, str]]:
    """('2016-01', '2016-02') -> [('2016-01', '2016-01-01', '2016-01-31'), ('2016-02', ...)]"""
    year, month = map(int, start.split("-"))
    end_year, end_month = map(int, end.split("-"))
    months = []
    while (year, month) <= (end_year, end_month):
        last = calendar.monthrange(year, month)[1]
        months.append((f"{year}-{month:02d}", f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last}"))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def build_url(api: dict, lat: float, lng: float, tz: str, start: str, end: str) -> str:
    query = {
        "latitude": round(lat, 4), "longitude": round(lng, 4),
        "start_date": start, "end_date": end,
        "hourly": ",".join(api["hourly_variables"]),
        "timezone": tz,       # local time, to match the meter timestamps
    }
    return f"{api['url']}?{urlencode(query)}"


def fetch(url: str, opener=urlopen, retries: int = 3, backoff_s: float = 2.0) -> bytes:
    """GET with simple retries. Raises after the last attempt, so the job fails loudly."""
    for attempt in range(1, retries + 1):
        try:
            with opener(url, timeout=60) as resp:
                body = resp.read()
            payload = json.loads(body)
            if "hourly" not in payload:
                raise ValueError(f"response has no 'hourly' block: {str(payload)[:200]}")
            return body
        except Exception:
            if attempt == retries:
                raise
            time.sleep(backoff_s * attempt)
    raise RuntimeError("unreachable")


def extract_weather(lh: Lakehouse, source: str, src: dict, run_id: str, opener=urlopen) -> None:
    api = src["api"]
    with audited(lh, run_id, "extract", f"landing.{src['landing_dir']}") as audit:
        sites = (lh.read("silver", api["sites_from_silver_table"])
                 .groupBy("site_id")
                 .agg(F.first("latitude").alias("lat"), F.first("longitude").alias("lng"),
                      F.first("timezone").alias("tz"))
                 .orderBy("site_id").collect())
        fetched = skipped = 0
        for site in sites:
            folder = os.path.join(lh.landing_root, src["landing_dir"], f"site_id={site.site_id}")
            os.makedirs(folder, exist_ok=True)
            for label, first_day, last_day in month_range(api["start_month"], api["end_month"]):
                path = os.path.join(folder, f"{label}.json")
                if os.path.exists(path):
                    skipped += 1
                    continue
                body = fetch(build_url(api, site.lat, site.lng, site.tz, first_day, last_day), opener)
                with open(path, "wb") as f:
                    f.write(body)
                fetched += 1
        audit.rows_read = len(sites)
        audit.rows_written = fetched
        audit.message = f"{len(sites)} site(s): fetched {fetched} file(s), skipped {skipped} existing"
