"""Small builders for test landing files."""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timedelta

METER_HEADER = ["timestamp", "building_id", "meter", "meter_reading"]
BUILDING_HEADER = ["building_id", "site_id", "primaryspaceusage", "sqm", "lat", "lng",
                   "timezone", "yearbuilt"]


def write_csv(path, header, rows) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    return str(path)


def write_text(path, text) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return str(path)


def hourly(start: str, hours: int) -> list[str]:
    t0 = datetime.fromisoformat(start)
    return [(t0 + timedelta(hours=h)).strftime("%Y-%m-%d %H:%M:%S") for h in range(hours)]


def meter_rows(building: str, start: str, hours: int, value: float) -> list[list]:
    return [[ts, building, "electricity", value] for ts in hourly(start, hours)]


def open_meteo_payload(start: str, temps: list[float], humidity: float = 80.0) -> dict:
    times = [t.replace(" ", "T")[:16] for t in hourly(start, len(temps))]
    return {"latitude": 50.0, "longitude": -1.0, "timezone": "Europe/London",
            "hourly": {"time": times, "temperature_2m": temps,
                       "relative_humidity_2m": [humidity] * len(temps)}}


def write_weather(landing, site, month, payload) -> str:
    return write_text(os.path.join(landing, "weather", f"site_id={site}", f"{month}.json"),
                      json.dumps(payload))
