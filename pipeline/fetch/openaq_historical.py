"""
AERIS - Fetch Historical Air Quality Data from OpenAQ v3
==========================================================

Lokasi file ini di project: pipeline/fetch/openaq_historical.py

Mengambil data historis PM2.5 dan PM10 dari stasiun-stasiun OpenAQ
di wilayah Jakarta, lalu menyimpannya sebagai CSV mentah ke data/raw/.

Cara pakai
----------
1. Daftar API key gratis di https://explore.openaq.org (Settings > API Keys)
2. Simpan ke file .env di root project:
       OPENAQ_API_KEY=xxxxxxxxxxxxxxxx
3. Install dependency:
       pip install requests python-dotenv
4. Jalankan DARI ROOT PROJECT (bukan dari dalam folder pipeline/fetch/),
   supaya path output data/raw/ mengarah ke tempat yang benar:
       python -m pipeline.fetch.openaq_historical --days 30
   (atau atur --date-from / --date-to manual, lihat --help)

Output
------
Untuk tiap sensor PM2.5/PM10 yang ditemukan di dalam bounding box Jakarta,
script akan membuat satu file CSV di:
    data/raw/openaq/{location_name}__sensor{sensor_id}__{parameter}.csv

Kolom: datetime_utc, value, unit, parameter, sensor_id,
       location_id, location_name, latitude, longitude
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv opsional, bisa juga set env var manual

API_BASE = "https://api.openaq.org/v3"

# Bounding box kasar untuk DKI Jakarta (min_lon, min_lat, max_lon, max_lat).
# Perluas sendiri kalau mau mencakup Bogor/Depok/Tangerang/Bekasi.
JAKARTA_BBOX = (106.65, -6.40, 106.98, -6.05)

TARGET_PARAMETERS = {"pm25", "pm10"}

OUTPUT_DIR = Path("data/raw/openaq")


def api_key() -> str:
    key = os.getenv("OPENAQ_API_KEY")
    if not key:
        sys.exit(
            "ERROR: OPENAQ_API_KEY belum diset. "
            "Daftar di https://explore.openaq.org lalu simpan ke .env"
        )
    return key


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"X-API-Key": api_key()})
    return session


def request_with_retry(session: requests.Session, url: str, params: dict, max_retries: int = 5):
    """GET dengan retry sederhana untuk menangani rate limit (429), timeout (408) & error transient."""
    for attempt in range(max_retries):
        resp = session.get(url, params=params, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 5))
            print(f"  Rate limited, menunggu {wait}s ...")
            time.sleep(wait)
            continue
        if resp.status_code == 408:
            # Server OpenAQ kewalahan mengagregasi rentang yang diminta.
            # Retry dengan backoff -- kalau tetap gagal, caller yang akan
            # memperkecil ukuran chunk tanggalnya.
            wait = 2 ** attempt
            print(f"  Server timeout (408), retry dalam {wait}s ...")
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            wait = 2 ** attempt
            print(f"  Server error {resp.status_code}, retry dalam {wait}s ...")
            time.sleep(wait)
            continue
        # error lain (400/401/404) - langsung berhenti, biar kelihatan errornya
        resp.raise_for_status()
    return None  # gagal setelah semua retry -- caller yang menentukan tindakan


def get_jakarta_locations(session: requests.Session) -> list[dict]:
    """Ambil semua lokasi monitoring di dalam bbox Jakarta."""
    print("Mengambil daftar lokasi monitoring di Jakarta ...")
    locations = []
    page = 1
    while True:
        data = request_with_retry(
            session,
            f"{API_BASE}/locations",
            params={
                "bbox": ",".join(str(v) for v in JAKARTA_BBOX),
                "limit": 1000,
                "page": page,
            },
        )
        results = data.get("results", [])
        if not results:
            break
        locations.extend(results)
        if len(results) < 1000:
            break
        page += 1
    print(f"  Ditemukan {len(locations)} lokasi.")
    return locations


def extract_target_sensors(locations: list[dict]) -> list[dict]:
    """Filter sensor PM2.5 / PM10 dari daftar lokasi."""
    sensors = []
    for loc in locations:
        coords = loc.get("coordinates") or {}
        for sensor in loc.get("sensors", []):
            param_name = (sensor.get("parameter") or {}).get("name", "").lower()
            if param_name not in TARGET_PARAMETERS:
                continue
            sensors.append(
                {
                    "sensor_id": sensor["id"],
                    "parameter": param_name,
                    "unit": (sensor.get("parameter") or {}).get("units", ""),
                    "location_id": loc.get("id"),
                    "location_name": loc.get("name", "unknown"),
                    "latitude": coords.get("latitude"),
                    "longitude": coords.get("longitude"),
                }
            )
    print(f"  Ditemukan {len(sensors)} sensor PM2.5/PM10.")
    return sensors


def safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", name).strip("_")


def month_chunks(date_from: str, date_to: str, chunk_days: int = 30):
    """Pecah rentang tanggal jadi potongan-potongan kecil (default 30 hari),
    supaya tiap request ke OpenAQ lebih ringan dan tidak timeout (408)."""
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d")
    chunks = []
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=chunk_days), end)
        chunks.append((cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cursor = chunk_end
    return chunks


def fetch_sensor_hours_chunk(
    session: requests.Session,
    sensor: dict,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """Ambil data satu chunk tanggal (sudah dipecah kecil) untuk satu sensor,
    dengan pagination di dalam chunk itu."""
    rows = []
    page = 1
    # Untuk satu chunk 30 hari, maksimal 30*24/1000 = ~1 halaman biasanya.
    # Kasih ruang lebih untuk jaga-jaga tapi tetap ada batas wajar.
    max_pages = 5
    while True:
        if page > max_pages:
            print(
                f"    PERINGATAN: chunk {date_from}..{date_to} sudah {max_pages} "
                f"halaman ({len(rows)} baris), dihentikan untuk jaga-jaga."
            )
            break
        data = request_with_retry(
            session,
            f"{API_BASE}/sensors/{sensor['sensor_id']}/hours",
            params={
                "datetime_from": date_from,
                "datetime_to": date_to,
                "limit": 1000,
                "page": page,
            },
        )
        if data is None:
            print(f"    Gagal ambil chunk {date_from}..{date_to} setelah beberapa percobaan, dilewati.")
            break
        results = data.get("results", [])
        if not results:
            break
        for r in results:
            period = r.get("period", {})
            rows.append(
                {
                    "datetime_utc": (period.get("datetimeFrom") or {}).get("utc"),
                    "value": r.get("value"),
                    "unit": sensor["unit"],
                    "parameter": sensor["parameter"],
                    "sensor_id": sensor["sensor_id"],
                    "location_id": sensor["location_id"],
                    "location_name": sensor["location_name"],
                    "latitude": sensor["latitude"],
                    "longitude": sensor["longitude"],
                }
            )
        if len(results) < 1000:
            break
        page += 1
        time.sleep(0.2)
    return rows


def fetch_sensor_hours(
    session: requests.Session,
    sensor: dict,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """Ambil data rata-rata per jam untuk satu sensor, dipecah per chunk 30 hari
    supaya tidak membebani API dalam satu request besar (menghindari 408)."""
    all_rows = []
    chunks = month_chunks(date_from, date_to, chunk_days=30)
    for i, (chunk_from, chunk_to) in enumerate(chunks, start=1):
        rows = fetch_sensor_hours_chunk(session, sensor, chunk_from, chunk_to)
        all_rows.extend(rows)
        time.sleep(0.2)
    return all_rows


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="Fetch data historis PM2.5/PM10 Jakarta dari OpenAQ")
    parser.add_argument("--days", type=int, default=30, help="Ambil N hari terakhir (default 30)")
    parser.add_argument("--date-from", type=str, default=None, help="Override tanggal awal, format YYYY-MM-DD")
    parser.add_argument("--date-to", type=str, default=None, help="Override tanggal akhir, format YYYY-MM-DD")
    parser.add_argument("--out-dir", type=str, default=str(OUTPUT_DIR), help="Folder output CSV")
    args = parser.parse_args()

    if args.date_to:
        date_to = args.date_to
    else:
        date_to = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if args.date_from:
        date_from = args.date_from
    else:
        date_from = (datetime.now(timezone.utc) - timedelta(days=args.days)).strftime("%Y-%m-%d")

    out_dir = Path(args.out_dir)

    print(f"Rentang tanggal: {date_from} s/d {date_to}")

    session = make_session()
    locations = get_jakarta_locations(session)
    sensors = extract_target_sensors(locations)

    if not sensors:
        print("Tidak ada sensor PM2.5/PM10 ditemukan di bbox ini. Cek JAKARTA_BBOX.")
        return

    total_rows = 0
    summary = []
    for i, sensor in enumerate(sensors, start=1):
        label = f"{sensor['location_name']} ({sensor['parameter']}, sensor {sensor['sensor_id']})"
        print(f"[{i}/{len(sensors)}] Mengambil {label} ...")
        rows = fetch_sensor_hours(session, sensor, date_from, date_to)
        if not rows:
            print("  Tidak ada data untuk sensor ini di rentang tanggal tsb, dilewati.")
            summary.append({**sensor, "rows": 0, "first": None, "last": None})
            continue
        filename = f"{safe_filename(sensor['location_name'])}__sensor{sensor['sensor_id']}__{sensor['parameter']}.csv"
        write_csv(rows, out_dir / filename)
        print(f"  Disimpan {len(rows)} baris -> {out_dir / filename}")
        total_rows += len(rows)
        timestamps = sorted(r["datetime_utc"] for r in rows if r["datetime_utc"])
        summary.append(
            {
                **sensor,
                "rows": len(rows),
                "first": timestamps[0] if timestamps else None,
                "last": timestamps[-1] if timestamps else None,
            }
        )

    print(f"\nSelesai. Total {total_rows} baris data disimpan ke {out_dir}/")

    print("\nRingkasan per sensor (diurutkan dari histori terbanyak):")
    print(f"{'Lokasi':40s} {'Param':6s} {'Baris':>8s}  {'Dari':20s} {'Sampai':20s}")
    for s in sorted(summary, key=lambda x: x["rows"], reverse=True):
        print(
            f"{s['location_name'][:40]:40s} {s['parameter']:6s} {s['rows']:8d}  "
            f"{str(s['first'] or '-'):20s} {str(s['last'] or '-'):20s}"
        )


if __name__ == "__main__":
    main()