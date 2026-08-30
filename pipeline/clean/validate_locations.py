"""
AERIS - Validasi Lokasi Sensor terhadap Batas Administratif DKI Jakarta
=========================================================================

Lokasi file ini di project: pipeline/clean/validate_locations.py

Bounding box (kotak koordinat) yang dipakai di tahap fetch itu kasar --
bisa saja menangkap sensor yang sebenarnya di luar Jakarta (Depok, Bekasi,
bahkan di laut karena koordinat GPS yang salah/placeholder dari sensor
komunitas). Script ini melakukan pengecekan yang lebih presisi: point-in-
polygon terhadap batas administratif DKI Jakarta yang sebenarnya.

Cara pakai
----------
1. Download GeoJSON batas wilayah DKI Jakarta. Dua sumber yang bisa dipakai:
   a. https://github.com/Alf-Anas/batas-administrasi-indonesia
      (cari folder Kabupaten/Kota level, filter untuk 5 kota DKI Jakarta:
       Jakarta Pusat, Jakarta Selatan, Jakarta Barat, Jakarta Timur, Jakarta Utara)
   b. http://gis.bpbd.jakarta.go.id/layers/geonode:dki_kecamatan
      (klik "Download Layer" > GeoJSON -- ini sumber resmi Pemprov DKI)

2. Simpan file GeoJSON itu, misal ke: data/raw/boundaries/dki_jakarta.geojson

3. Install dependency:
       pip install geopandas shapely pandas

4. Jalankan dari root project:
       python -m pipeline.clean.validate_locations \\
           --boundary data/raw/boundaries/dki_jakarta.geojson \\
           --sensors-dir data/raw/openaq

Output
------
- Ringkasan di terminal: sensor mana yang valid (di dalam Jakarta) dan
  mana yang di luar (termasuk kemungkinan di laut / koordinat salah).
- File data/raw/openaq/_location_validation_report.csv berisi detail
  per sensor: status valid/invalid dan alasannya.
"""

from __future__ import annotations

import argparse
import glob
import os
from pathlib import Path

import pandas as pd

try:
    import geopandas as gpd
    from shapely.geometry import Point
except ImportError:
    raise SystemExit(
        "Perlu geopandas & shapely. Install dengan:\n"
        "  pip install geopandas shapely"
    )


def load_boundary(path: str) -> "gpd.GeoDataFrame":
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        # Asumsikan WGS84 (lat/lon biasa) kalau tidak ada info CRS di file-nya.
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    # Gabungkan semua polygon (kecamatan/kelurahan) jadi satu wilayah utuh,
    # supaya titik yang di dalam kecamatan manapun tetap dianggap valid.
    union_geom = gdf.geometry.union_all() if hasattr(gdf.geometry, "union_all") else gdf.geometry.unary_union
    return union_geom


def find_sensor_csvs(sensors_dir: str) -> list[Path]:
    pattern = os.path.join(sensors_dir, "*.csv")
    files = [Path(p) for p in glob.glob(pattern)]
    # Skip file laporan validasi kalau dijalankan berkali-kali
    return [f for f in files if not f.name.startswith("_")]


def main():
    parser = argparse.ArgumentParser(
        description="Validasi apakah koordinat sensor OpenAQ masuk ke dalam batas DKI Jakarta"
    )
    parser.add_argument("--boundary", required=True, help="Path ke file GeoJSON batas DKI Jakarta")
    parser.add_argument("--sensors-dir", default="data/raw/openaq", help="Folder berisi CSV hasil fetch OpenAQ")
    args = parser.parse_args()

    print(f"Memuat batas wilayah dari {args.boundary} ...")
    jakarta_union = load_boundary(args.boundary)

    csv_files = find_sensor_csvs(args.sensors_dir)
    if not csv_files:
        print(f"Tidak ada file CSV ditemukan di {args.sensors_dir}")
        return

    print(f"Memeriksa {len(csv_files)} file sensor ...\n")

    results = []
    for csv_path in csv_files:
        df = pd.read_csv(csv_path, nrows=1)
        if df.empty:
            continue
        lat = df["latitude"].iloc[0]
        lon = df["longitude"].iloc[0]
        location_name = df["location_name"].iloc[0]
        sensor_id = df["sensor_id"].iloc[0]
        parameter = df["parameter"].iloc[0]

        point = Point(lon, lat)  # Shapely: urutannya (x=lon, y=lat)
        is_valid = jakarta_union.contains(point)

        results.append(
            {
                "file": csv_path.name,
                "location_name": location_name,
                "sensor_id": sensor_id,
                "parameter": parameter,
                "latitude": lat,
                "longitude": lon,
                "valid_dki_jakarta": is_valid,
            }
        )

        status = "VALID" if is_valid else "DI LUAR JAKARTA"
        print(f"  [{status:16s}] {location_name} (sensor {sensor_id}, {parameter}) -- {lat:.4f}, {lon:.4f}")

    report_df = pd.DataFrame(results)
    report_path = Path(args.sensors_dir) / "_location_validation_report.csv"
    report_df.to_csv(report_path, index=False)

    n_valid = report_df["valid_dki_jakarta"].sum()
    n_invalid = len(report_df) - n_valid

    print(f"\nRingkasan: {n_valid} sensor valid di dalam DKI Jakarta, {n_invalid} sensor di luar.")
    print(f"Detail lengkap disimpan di: {report_path}")

    if n_invalid > 0:
        print(
            "\nSensor yang di luar Jakarta sebaiknya di-exclude dari training model, "
            "atau ditandai terpisah kalau memang mau dipakai untuk memperkaya konteks "
            "regional (misal Depok/Bekasi/Tangerang untuk pengembangan Jabodetabek nanti)."
        )


if __name__ == "__main__":
    main()
