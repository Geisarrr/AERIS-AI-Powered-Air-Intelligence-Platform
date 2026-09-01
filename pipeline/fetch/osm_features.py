"""
AERIS - OSM Spatial Feature Extraction
======================================

Output:
    data/raw/osm/aeris_osm_features.csv

Features:
    - road_density_1km
    - major_road_distance_m
    - intersection_density_1km
    - traffic_signal_count_1km
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from pathlib import Path
from collections import defaultdict

import pandas as pd
import requests

from requests.exceptions import (
    Timeout,
    ConnectionError,
    RequestException,
)

from shapely.geometry import Point, LineString
from shapely.ops import transform
from pyproj import Transformer


# ============================================================
# 1. CONFIGURATION
# ============================================================

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

OUTPUT_DIR = Path("data/raw/osm")

OUTPUT_FILE = OUTPUT_DIR / "aeris_osm_features.csv"

ROAD_RADIUS_M = 1000
SIGNAL_RADIUS_M = 1000
MAJOR_ROAD_RADIUS_M = 5000

REQUEST_TIMEOUT = 180
MAX_RETRIES = 5

# JANGAN diubah jika semua lokasi berada di Jakarta
# Jakarta berada pada UTM Zone 48S
TRANSFORMER = Transformer.from_crs(
    "EPSG:4326",
    "EPSG:32748",
    always_xy=True,
)


# ============================================================
# 2. LOCATIONS
# ============================================================

# Masukkan koordinat representatif yang ingin digunakan
# untuk masing-masing wilayah Jakarta.

LOCATIONS = {
    "Jakarta Barat": {
        "lat": -6.167,
        "lon": 106.763,
    },

    "Jakarta Pusat": {
        "lat": -6.186,
        "lon": 106.834,
    },

    "Jakarta Selatan": {
        "lat": -6.261,
        "lon": 106.810,
    },

    "Jakarta Timur": {
        "lat": -6.225,
        "lon": 106.900,
    },

    "Jakarta Utara": {
        "lat": -6.138,
        "lon": 106.864,
    },
}


# ============================================================
# 3. CREATE SESSION
# ============================================================

def make_session() -> requests.Session:

    session = requests.Session()

    session.headers.update({
        "User-Agent": (
            "AERIS-Research/1.0 "
            "(OpenStreetMap Overpass API client)"
        )
    })

    return session


# ============================================================
# 4. OVERPASS API WITH ERROR HANDLING
# ============================================================

def request_with_retry(
    session: requests.Session,
    query: str,
    max_retries: int = MAX_RETRIES,
):

    for attempt in range(max_retries):

        try:

            response = session.post(
                OVERPASS_URL,
                data=query,
                timeout=REQUEST_TIMEOUT,
            )

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            if response.status_code == 200:

                print(
                    f"    [API] Request berhasil "
                    f"(attempt {attempt + 1}/{max_retries})"
                )

                try:

                    return response.json()

                except ValueError as e:

                    print(
                        f"    [JSON Error] "
                        f"Response bukan JSON valid: {e}"
                    )

                    return None

            # ------------------------------------------------
            # RATE LIMIT
            # ------------------------------------------------

            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                if retry_after:

                    try:
                        wait = int(retry_after)
                    except ValueError:
                        wait = 30

                else:

                    wait = min(
                        60,
                        5 * (2 ** attempt)
                    )

                print(
                    f"    [429] Rate limit. "
                    f"Retry dalam {wait} detik..."
                )

                time.sleep(wait)

                continue

            # ------------------------------------------------
            # SERVER ERROR
            # ------------------------------------------------

            if response.status_code in (
                408,
                500,
                502,
                503,
                504,
            ):

                wait = min(
                    60,
                    5 * (2 ** attempt)
                )

                print(
                    f"    [{response.status_code}] "
                    f"Overpass server error. "
                    f"Retry dalam {wait} detik..."
                )

                time.sleep(wait)

                continue

            # ------------------------------------------------
            # OTHER HTTP ERROR
            # ------------------------------------------------

            print(
                f"    [HTTP {response.status_code}] "
                f"Request gagal."
            )

            try:

                print(
                    f"    Response: "
                    f"{response.text[:300]}"
                )

            except Exception:
                pass

            response.raise_for_status()

        # ----------------------------------------------------
        # TIMEOUT
        # ----------------------------------------------------

        except Timeout:

            wait = min(
                60,
                5 * (2 ** attempt)
            )

            print(
                f"    [Timeout] Request melebihi "
                f"{REQUEST_TIMEOUT} detik."
            )

            print(
                f"    Retry dalam {wait} detik..."
            )

            time.sleep(wait)

        # ----------------------------------------------------
        # CONNECTION ERROR
        # ----------------------------------------------------

        except ConnectionError:

            wait = min(
                60,
                5 * (2 ** attempt)
            )

            print(
                "    [Connection Error] "
                "Koneksi ke Overpass gagal."
            )

            print(
                f"    Retry dalam {wait} detik..."
            )

            time.sleep(wait)

        # ----------------------------------------------------
        # REQUEST ERROR
        # ----------------------------------------------------

        except RequestException as e:

            print(
                f"    [Request Error] {e}"
            )

            if attempt < max_retries - 1:

                wait = min(
                    60,
                    5 * (2 ** attempt)
                )

                print(
                    f"    Retry dalam {wait} detik..."
                )

                time.sleep(wait)

            else:

                print(
                    "    Retry habis."
                )

        # ----------------------------------------------------
        # UNEXPECTED ERROR
        # ----------------------------------------------------

        except Exception as e:

            print(
                f"    [Unexpected Error] {e}"
            )

            break

    return None


# ============================================================
# 5. GET ROADS
# ============================================================

def get_roads(
    session,
    lat,
    lon,
    radius=ROAD_RADIUS_M,
):

    query = f"""
    [out:json][timeout:120];

    way
      ["highway"]
      (around:{radius},{lat},{lon});

    out body geom;
    """

    return request_with_retry(
        session,
        query,
    )


# ============================================================
# 6. GET MAJOR ROADS
# ============================================================

def get_major_roads(
    session,
    lat,
    lon,
    radius=MAJOR_ROAD_RADIUS_M,
):

    query = f"""
    [out:json][timeout:120];

    way
      ["highway"~"^(motorway|trunk|primary|secondary)$"]
      (around:{radius},{lat},{lon});

    out body geom;
    """

    return request_with_retry(
        session,
        query,
    )


# ============================================================
# 7. GET TRAFFIC SIGNALS
# ============================================================

def get_traffic_signals(
    session,
    lat,
    lon,
    radius=SIGNAL_RADIUS_M,
):

    query = f"""
    [out:json][timeout:120];

    node
      ["highway"="traffic_signals"]
      (around:{radius},{lat},{lon});

    out body;
    """

    return request_with_retry(
        session,
        query,
    )


# ============================================================
# 8. COORDINATE TRANSFORMATION
# ============================================================

def project_geometry(geometry):

    return transform(
        TRANSFORMER.transform,
        geometry,
    )


def project_point(lat, lon):

    point = Point(
        lon,
        lat,
    )

    return project_geometry(point)


# ============================================================
# 9. ROAD DENSITY
# ============================================================

def calculate_road_density(
    road_json,
    lat,
    lon,
):

    if not road_json:
        return None

    target = project_point(
        lat,
        lon,
    )

    buffer_1km = target.buffer(
        ROAD_RADIUS_M
    )

    total_road_length_m = 0.0

    for element in road_json.get(
        "elements",
        [],
    ):

        if element.get("type") != "way":
            continue

        geometry = element.get(
            "geometry"
        )

        if not geometry:
            continue

        coords = [
            (
                p["lon"],
                p["lat"],
            )
            for p in geometry
            if "lon" in p
            and "lat" in p
        ]

        if len(coords) < 2:
            continue

        try:

            line = LineString(
                coords
            )

            line_projected = (
                project_geometry(line)
            )

            clipped = (
                line_projected
                .intersection(
                    buffer_1km
                )
            )

            total_road_length_m += (
                clipped.length
            )

        except Exception as e:

            print(
                f"    [Warning] "
                f"Road geometry gagal diproses: "
                f"{e}"
            )

    # Area lingkaran radius 1 km
    area_km2 = (
        math.pi
        * (ROAD_RADIUS_M / 1000) ** 2
    )

    road_density = (
        total_road_length_m / 1000
    ) / area_km2

    return road_density


# ============================================================
# 10. MAJOR ROAD DISTANCE
# ============================================================

def calculate_major_road_distance(
    road_json,
    lat,
    lon,
):

    if not road_json:
        return None

    target = project_point(
        lat,
        lon,
    )

    min_distance_m = float("inf")

    for element in road_json.get(
        "elements",
        [],
    ):

        if element.get("type") != "way":
            continue

        geometry = element.get(
            "geometry"
        )

        if not geometry:
            continue

        coords = [
            (
                p["lon"],
                p["lat"],
            )
            for p in geometry
            if "lon" in p
            and "lat" in p
        ]

        if len(coords) < 2:
            continue

        try:

            line = project_geometry(
                LineString(coords)
            )

            distance = target.distance(
                line
            )

            if distance < min_distance_m:
                min_distance_m = distance

        except Exception as e:

            print(
                f"    [Warning] "
                f"Major road gagal diproses: "
                f"{e}"
            )

    if math.isinf(min_distance_m):
        return None

    return min_distance_m


# ============================================================
# 11. TRAFFIC SIGNAL COUNT
# ============================================================

def calculate_traffic_signal_count(
    signal_json,
):

    if not signal_json:
        return None

    return sum(
        1
        for element in signal_json.get(
            "elements",
            [],
        )
        if element.get("type") == "node"
    )


# ============================================================
# 12. INTERSECTION DENSITY
# ============================================================

def calculate_intersection_density(
    road_json,
    lat,
    lon,
):

    if not road_json:
        return None

    target = project_point(
        lat,
        lon,
    )

    buffer_1km = target.buffer(
        ROAD_RADIUS_M
    )

    node_usage = defaultdict(int)

    node_coordinates = {}

    for element in road_json.get(
        "elements",
        [],
    ):

        if element.get("type") != "way":
            continue

        geometry = element.get(
            "geometry"
        )

        nodes = element.get(
            "nodes",
            []
        )

        if not geometry:
            continue

        # Simpan koordinat node
        for point in geometry:

            if (
                "lat" not in point
                or "lon" not in point
            ):
                continue

            node_id = point.get(
                "id"
            )

            if node_id is None:
                continue

            node_coordinates[node_id] = (
                point["lat"],
                point["lon"],
            )

        # Hitung penggunaan node
        for node_id in nodes:

            node_usage[node_id] += 1

    intersection_count = 0

    for node_id, count in node_usage.items():

        if count < 3:
            continue

        coords = node_coordinates.get(
            node_id
        )

        if not coords:
            continue

        try:

            node_point = project_point(
                coords[0],
                coords[1],
            )

            if buffer_1km.contains(
                node_point
            ):

                intersection_count += 1

        except Exception:
            continue

    area_km2 = (
        math.pi
        * (ROAD_RADIUS_M / 1000) ** 2
    )

    return (
        intersection_count
        / area_km2
    )


# ============================================================
# 13. MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "AERIS OSM Feature Extraction"
        )
    )

    parser.add_argument(
        "--out-dir",
        type=str,
        default=str(OUTPUT_DIR),
        help="Output directory",
    )

    args = parser.parse_args()

    output_dir = Path(
        args.out_dir
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        output_dir
        / "aeris_osm_features.csv"
    )

    print("=" * 70)
    print("AERIS - OSM FEATURE EXTRACTION")
    print("=" * 70)

    print(
        f"Output directory : "
        f"{output_dir.resolve()}"
    )

    print(
        f"Output CSV       : "
        f"{output_file.resolve()}"
    )

    print(
        f"Jumlah lokasi    : "
        f"{len(LOCATIONS)}"
    )

    print()

    session = make_session()

    results = []

    total_locations = len(
        LOCATIONS
    )

    # ========================================================
    # PROCESS LOCATIONS
    # ========================================================

    for index, (
        region,
        coordinate,
    ) in enumerate(
        LOCATIONS.items(),
        start=1,
    ):

        lat = coordinate["lat"]
        lon = coordinate["lon"]

        print("-" * 70)

        print(
            f"[{index}/{total_locations}] "
            f"{region}"
        )

        print(
            f"Coordinate: "
            f"{lat}, {lon}"
        )

        # ----------------------------------------------------
        # ROADS
        # ----------------------------------------------------

        print(
            "\n  → Mengambil road data..."
        )

        roads = get_roads(
            session,
            lat,
            lon,
        )

        if roads is None:

            print(
                "  [FAILED] "
                "Road data tidak tersedia."
            )

            road_density = None
            intersection_density = None

        else:

            print(
                f"  [OK] "
                f"{len(roads.get('elements', []))} "
                f"road objects"
            )

            road_density = (
                calculate_road_density(
                    roads,
                    lat,
                    lon,
                )
            )

            intersection_density = (
                calculate_intersection_density(
                    roads,
                    lat,
                    lon,
                )
            )

        # ----------------------------------------------------
        # MAJOR ROADS
        # ----------------------------------------------------

        print(
            "\n  → Mengambil major road data..."
        )

        major_roads = get_major_roads(
            session,
            lat,
            lon,
        )

        if major_roads is None:

            print(
                "  [FAILED] "
                "Major road data tidak tersedia."
            )

            major_distance = None

        else:

            print(
                f"  [OK] "
                f"{len(major_roads.get('elements', []))} "
                f"major road objects"
            )

            major_distance = (
                calculate_major_road_distance(
                    major_roads,
                    lat,
                    lon,
                )
            )

        # ----------------------------------------------------
        # TRAFFIC SIGNALS
        # ----------------------------------------------------

        print(
            "\n  → Mengambil traffic signals..."
        )

        signals = get_traffic_signals(
            session,
            lat,
            lon,
        )

        if signals is None:

            print(
                "  [FAILED] "
                "Traffic signal data tidak tersedia."
            )

            signal_count = None

        else:

            signal_count = (
                calculate_traffic_signal_count(
                    signals
                )
            )

            print(
                f"  [OK] "
                f"{signal_count} "
                f"traffic signals"
            )

        # ----------------------------------------------------
        # RESULT
        # ----------------------------------------------------

        result = {

            "target_region": region,

            "target_lat": lat,

            "target_lon": lon,

            "road_density_1km":
                road_density,

            "major_road_distance_m":
                major_distance,

            "intersection_density_1km":
                intersection_density,

            "traffic_signal_count_1km":
                signal_count,
        }

        results.append(
            result
        )

        # ----------------------------------------------------
        # PRINT RESULT
        # ----------------------------------------------------

        print("\n  Feature hasil:")

        print(
            f"    road_density_1km       : "
            f"{road_density}"
        )

        print(
            f"    major_road_distance_m  : "
            f"{major_distance}"
        )

        print(
            f"    intersection_density   : "
            f"{intersection_density}"
        )

        print(
            f"    traffic_signal_count   : "
            f"{signal_count}"
        )

        # ----------------------------------------------------
        # DELAY
        # ----------------------------------------------------

        if index < total_locations:

            print(
                "\n  → Menunggu 3 detik..."
            )

            time.sleep(3)

    # ========================================================
    # SAVE CSV
    # ========================================================

    if not results:

        print(
            "\nERROR: Tidak ada data yang "
            "berhasil diperoleh."
        )

        sys.exit(1)

    osm_features = pd.DataFrame(
        results
    )

    osm_features.to_csv(
        output_file,
        index=False,
    )

    # ========================================================
    # VALIDATE OUTPUT
    # ========================================================

    if not output_file.exists():

        print(
            "\nERROR: CSV gagal dibuat."
        )

        sys.exit(1)

    # ========================================================
    # FINAL SUMMARY
    # ========================================================

    print()
    print("=" * 70)
    print("SELESAI")
    print("=" * 70)

    print(
        f"\nFile berhasil dibuat:"
    )

    print(
        output_file.resolve()
    )

    print(
        f"\nJumlah baris: "
        f"{len(osm_features)}"
    )

    print(
        f"Jumlah kolom: "
        f"{len(osm_features.columns)}"
    )

    print("\nDataset OSM:")

    print(
        osm_features.to_string(
            index=False
        )
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()