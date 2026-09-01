"""
AERIS - Fetch Historical Air Quality (PM2.5 & PM10) from OpenAQ v3
===================================================================
Lokasi file: pipeline/fetch/openaq_historical.py
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
# Tambahan import untuk menangani error timeout dari library requests
from requests.exceptions import Timeout, ConnectionError, RequestException

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

API_BASE = "https://api.openaq.org/v3"

TARGET_STATIONS = [
    {"region": "jakarta_pusat", "name": "Bunderan HI / US Embassy", "lat": -6.182536, "lon": 106.828236},
    {"region": "jakarta_selatan", "name": "Jagakarsa", "lat": -6.325500, "lon": 106.814400},
    {"region": "jakarta_barat", "name": "Kebon Jeruk", "lat": -6.194900, "lon": 106.764500},
    {"region": "jakarta_timur", "name": "Jatinegara", "lat": -6.212000, "lon": 106.883000},
    {"region": "jakarta_utara", "name": "Kelapa Gading", "lat": -6.155300, "lon": 106.892300},
]

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
    """Fungsi yang sudah diperkuat dengan try-except dan Rate Limit Tracking"""
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=60)
            
            # --- MULAI TRACKING RATE LIMIT ---
            # OpenAQ mengembalikan header menggunakan huruf kecil (lowercase)
            limit = resp.headers.get('x-ratelimit-limit', 'N/A')
            remaining = resp.headers.get('x-ratelimit-remaining', 'N/A')
            
            # (Opsional) Tampilkan sisa limit jika sudah menipis (misal di bawah 100)
            if remaining != 'N/A' and int(remaining) < 100:
                print(f"    [WARNING] Sisa kuota API OpenAQ menipis: {remaining}/{limit}")
            # --- SELESAI TRACKING RATE LIMIT ---

            if resp.status_code == 200:
                # Jika ingin memantau TERUS-MENERUS di setiap request, buka komentar (uncomment) baris di bawah:
                # print(f"    [API] Sisa request bulan ini: {remaining}/{limit}")
                return resp.json()
            
            if resp.status_code == 429:
                # Jika terkena limit (kuota habis atau request terlalu cepat)
                wait = int(resp.headers.get("Retry-After", 10))
                print(f"    [429] Rate limited! Sisa kuota: {remaining}. Istirahat {wait}s ...")
                time.sleep(wait)
                continue
                
            if resp.status_code in (408, 500, 502, 503, 504):
                wait = 2 ** attempt
                print(f"    [{resp.status_code}] Server sibuk, retry dalam {wait}s ...")
                time.sleep(wait)
                continue
                
            resp.raise_for_status()

        except Timeout:
            wait = 2 ** attempt
            print(f"    [Timeout] OpenAQ tidak merespon dalam 60s, coba lagi dalam {wait}s ...")
            time.sleep(wait)
        except ConnectionError:
            wait = 2 ** attempt
            print(f"    [Koneksi Putus] Jaringan terganggu, coba lagi dalam {wait}s ...")
            time.sleep(wait)
        except RequestException as e:
            print(f"    [Error Fatal] {e}")
            break
            
    return None

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def get_jakarta_locations(session: requests.Session) -> list[dict]:
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
        if not data:
            break
        results = data.get("results", [])
        if not results:
            break
        locations.extend(results)
        if len(results) < 1000:
            break
        page += 1
    print(f"  Ditemukan {len(locations)} lokasi stasiun OpenAQ.")
    return locations


def map_sensors_to_target_stations(locations: list[dict]) -> list[dict]:
    sensors = []
    for loc in locations:
        coords = loc.get("coordinates") or {}
        lat = coords.get("latitude")
        lon = coords.get("longitude")
        if lat is None or lon is None:
            continue

        nearest_target = min(
            TARGET_STATIONS,
            key=lambda t: haversine_distance(lat, lon, t["lat"], t["lon"]),
        )
        dist_km = haversine_distance(lat, lon, nearest_target["lat"], nearest_target["lon"])

        for sensor in loc.get("sensors", []):
            param_name = (sensor.get("parameter") or {}).get("name", "").lower()
            if param_name not in TARGET_PARAMETERS:
                continue

            sensors.append(
                {
                    "sensor_id": sensor["id"],
                    "parameter": param_name,
                    "unit": (sensor.get("parameter") or {}).get("units", "µg/m³"),
                    "target_region": nearest_target["region"],
                    "target_station_name": nearest_target["name"],
                    "target_lat": nearest_target["lat"],
                    "target_lon": nearest_target["lon"],
                    "actual_location_name": loc.get("name", "unknown"),
                    "actual_lat": lat,
                    "actual_lon": lon,
                    "distance_km": round(dist_km, 2),
                }
            )
    print(f"  Ditemukan {len(sensors)} sensor (PM2.5 / PM10) terpetakan.")
    return sensors


def month_chunks(date_from: str, date_to: str, chunk_days: int = 7):
    """PERUBAHAN: Diperkecil menjadi 7 hari (seminggu) agar request lebih ringan"""
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d")
    chunks = []
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=chunk_days), end)
        chunks.append((cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cursor = chunk_end
    return chunks


def fetch_sensor_hours(session: requests.Session, sensor: dict, date_from: str, date_to: str) -> list[dict]:
    all_rows = []
    # Memotong periode panjang menjadi potongan kecil 7 hari
    chunks = month_chunks(date_from, date_to, chunk_days=7)
    
    for chunk_from, chunk_to in chunks:
        page = 1
        while page <= 5: 
            data = request_with_retry(
                session,
                f"{API_BASE}/sensors/{sensor['sensor_id']}/hours",
                params={
                    "datetime_from": chunk_from,
                    "datetime_to": chunk_to,
                    "limit": 1000,
                    "page": page,
                },
            )
            
            # Jika retries sudah diusahakan tapi tetap None (gagal), lewati chunk ini
            if not data:
                print(f"    ! Gagal ambil periode {chunk_from} s/d {chunk_to}, melewati ke minggu berikutnya...")
                break
                
            results = data.get("results", [])
            if not results:
                break
                
            for r in results:
                period = r.get("period", {})
                all_rows.append(
                    {
                        "datetime_utc": (period.get("datetimeFrom") or {}).get("utc"),
                        "value": r.get("value"),
                        "unit": sensor["unit"],
                        "parameter": sensor["parameter"],
                        "target_region": sensor["target_region"],
                        "target_lat": sensor["target_lat"],
                        "target_lon": sensor["target_lon"],
                        "sensor_id": sensor["sensor_id"],
                        "source_station": sensor["actual_location_name"],
                    }
                )
            if len(results) < 1000:
                break
            page += 1
            time.sleep(0.5) # Jeda antar request (sopan ke server API)
            
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
    parser = argparse.ArgumentParser(description="Fetch historical PM2.5 & PM10 from OpenAQ for 5 Jakarta Regions")
    parser.add_argument("--date-from", type=str, default="2024-01-01", help="Format YYYY-MM-DD")
    parser.add_argument("--date-to", type=str, default=None, help="Format YYYY-MM-DD")
    parser.add_argument("--out-dir", type=str, default=str(OUTPUT_DIR))
    args = parser.parse_args()

    date_to = args.date_to or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_from = args.date_from
    out_dir = Path(args.out_dir)

    print(f"Menjalankan pipeline OpenAQ: {date_from} s/d {date_to}")
    session = make_session()
    locations = get_jakarta_locations(session)
    sensors = map_sensors_to_target_stations(locations)

    data_per_region = defaultdict(list)
    total_rows = 0

    for i, sensor in enumerate(sensors, start=1):
        label = f"{sensor['target_region']} | {sensor['parameter']} | Stasiun: {sensor['actual_location_name']}"
        print(f"[{i}/{len(sensors)}] Fetching {label} ...")
        
        rows = fetch_sensor_hours(session, sensor, date_from, date_to)
        if not rows:
            print("  Tidak ada data.")
            continue
            
        data_per_region[sensor['target_region']].extend(rows)
        total_rows += len(rows)

    print(f"\nSemua data berhasil ditarik. Mulai menyimpan ke dalam CSV per wilayah...")

    for region, region_rows in data_per_region.items():
        # Urutkan berdasarkan waktu
        region_rows.sort(key=lambda x: (x["datetime_utc"] or "", x["parameter"]))
        
        filename = f"{region}.csv"
        write_csv(region_rows, out_dir / filename)
        print(f"  Disimpan {len(region_rows)} baris -> {out_dir / filename}")

    print(f"\nSelesai! Total {total_rows} baris disimpan ke {out_dir}/")


if __name__ == "__main__":
    main()