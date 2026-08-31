"""
AERIS - Fetch Historical Weather Data from Open-Meteo
=====================================================
Lokasi file: pipeline/fetch/openmeteo_historical.py
Output: 
data/raw/openmeteo/jakarta_pusat.csv
data/raw/openmeteo/jakarta_selatan.csv
data/raw/openmeteo/jakarta_barat.csv
data/raw/openmeteo/jakarta_timur.csv
data/raw/openmeteo/jakarta_utara.csv
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
import requests

TARGET_STATIONS = [
    {"region": "jakarta_pusat", "name": "US Embassy / Bunderan HI (DKI1)", "lat": -6.182536, "lon": 106.828236},
    {"region": "jakarta_selatan", "name": "Jagakarsa (DKI3)", "lat": -6.325500, "lon": 106.814400},
    {"region": "jakarta_barat", "name": "Kebon Jeruk (DKI5)", "lat": -6.194900, "lon": 106.764500},
    {"region": "jakarta_timur", "name": "Lubang Buaya (DKI4)", "lat": -6.289600, "lon": 106.900300},
    {"region": "jakarta_utara", "name": "Kelapa Gading (DKI2)", "lat": -6.155300, "lon": 106.892300},
]

OPEN_METEO_HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"
OUTPUT_DIR = Path("data/raw/openmeteo")

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
]


def fetch_weather_for_station(station: dict, date_from: str, date_to: str) -> list[dict]:
    params = {
        "latitude": station["lat"],
        "longitude": station["lon"],
        "start_date": date_from,
        "end_date": date_to,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "UTC",
    }

    resp = requests.get(OPEN_METEO_HISTORICAL_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])

    rows = []
    for i, t in enumerate(times):
        rows.append(
            {
                "datetime_utc": t,  # Menyimpan waktu dalam standar UTC (ISO 8601)
                "target_region": station["region"],
                "target_lat": station["lat"],
                "target_lon": station["lon"],
                "temperature_2m": hourly.get("temperature_2m", [])[i],
                "relative_humidity_2m": hourly.get("relative_humidity_2m", [])[i],
                "precipitation": hourly.get("precipitation", [])[i],
                "surface_pressure": hourly.get("surface_pressure", [])[i],
                "wind_speed_10m": hourly.get("wind_speed_10m", [])[i],
                "wind_direction_10m": hourly.get("wind_direction_10m", [])[i],
            }
        )
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="Fetch historical Weather from Open-Meteo for 5 Jakarta Regions")
    
    # Default diset ke awal tahun 2024
    parser.add_argument("--date-from", type=str, default="2024-01-01", help="Format YYYY-MM-DD")
    parser.add_argument("--date-to", type=str, default=None, help="Format YYYY-MM-DD")
    parser.add_argument("--out-dir", type=str, default=str(OUTPUT_DIR))
    args = parser.parse_args()

    # Open-Meteo Archive API memiliki jeda beberapa hari, set default ke 3 hari sebelum hari ini
    date_to = args.date_to or (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
    date_from = args.date_from
    out_dir = Path(args.out_dir)

    print(f"Menjalankan pipeline Open-Meteo: {date_from} s/d {date_to}")

    total_rows = 0
    for station in TARGET_STATIONS:
        print(f"Fetching weather: {station['region']} ({station['name']}) ...")
        try:
            rows = fetch_weather_for_station(station, date_from, date_to)
            
            # Format nama file CSV disamakan persis dengan OpenAQ
            filename = f"{station['region']}.csv"
            write_csv(rows, out_dir / filename)
            
            print(f"  Disimpan {len(rows)} baris -> {out_dir / filename}")
            total_rows += len(rows)
            time.sleep(0.5)  # Jeda aman agar tidak terkena limit API sementara
            
        except requests.exceptions.HTTPError as e:
            print(f"  [ERROR] Gagal mengambil data untuk {station['region']}: {e}")
            if e.response.status_code == 400:
                print("  Pastikan rentang tanggal tidak terlalu dekat dengan hari ini.")

    print(f"\nSelesai! Total {total_rows} baris disimpan ke {out_dir}/")


if __name__ == "__main__":
    main()