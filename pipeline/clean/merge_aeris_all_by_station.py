from __future__ import annotations

from pathlib import Path

import pandas as pd


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(
    "/Users/geisarrampan/Project/"
    "AERIS-AI-Powered-Air-Intelligence-Platform"
)

BASE_PATH = (
    PROJECT_ROOT
    / "data/processed/dataset_aeris_final.csv"
)

WEATHER_PATH = (
    PROJECT_ROOT
    / "data/processed/weather_enriched_master.parquet"
)

ISPU_PATH = (
    PROJECT_ROOT
    / "data/processed/ispu_master.parquet"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data/processed/dataset_aeris_final_merged_all.csv"
)


# ============================================================
# COLUMN DEFINITIONS
# ============================================================

BASE_WEATHER_COLUMNS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
]

WEATHER_ENRICHMENT_COLUMNS = [
    "boundary_layer_height",
    "dew_point_2m",
    "vapour_pressure_deficit",
    "cloud_cover",
    "cloud_cover_low",
    "shortwave_radiation",
    "wind_gusts_10m",
    "wind_speed_100m",
    "wind_direction_100m",
]

ISPU_POLLUTANTS = [
    "pm10_ispu",
    "pm25_ispu",
    "so2_ispu",
    "co_ispu",
    "o3_ispu",
    "no2_ispu",
    "hc_ispu",
]


# ============================================================
# HELPERS
# ============================================================

def normalize_region(s: pd.Series) -> pd.Series:
    """
    Example:
    Jakarta Barat -> jakarta_barat
    jakarta-barat -> jakarta_barat
    """
    return (
        s.astype("string")
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
    )


def normalize_station(s: pd.Series) -> pd.Series:
    """
    Normalized station key.

    Example:
    Kelapa Gading   -> kelapa_gading
    KELAPA-GADING   -> kelapa_gading
    """
    return (
        s.astype("string")
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
    )


def to_utc_hour(s: pd.Series) -> pd.Series:
    """
    Convert datetime into canonical UTC hourly timestamp.
    """
    return (
        pd.to_datetime(
            s,
            utc=True,
            errors="raise",
        )
        .dt.floor("h")
    )


def require_columns(
    df: pd.DataFrame,
    required: list[str],
    label: str,
) -> None:

    missing = [
        col
        for col in required
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"{label} missing columns: {missing}\n"
            f"Available columns:\n{df.columns.tolist()}"
        )


def assert_unique_key(
    df: pd.DataFrame,
    keys: list[str],
    label: str,
) -> None:

    duplicate_mask = df.duplicated(
        subset=keys,
        keep=False,
    )

    if duplicate_mask.any():

        examples = (
            df.loc[
                duplicate_mask,
                keys
            ]
            .sort_values(keys)
            .head(20)
        )

        raise ValueError(
            f"{label} has duplicate merge keys: {keys}\n\n"
            f"Examples:\n"
            f"{examples.to_string(index=False)}"
        )


# ============================================================
# CHECK FILES
# ============================================================

print("Checking input files...")

for path in [
    BASE_PATH,
    WEATHER_PATH,
    ISPU_PATH,
]:

    if not path.exists():
        raise FileNotFoundError(
            f"File not found:\n{path}"
        )

    print(f"OK -> {path}")


# ============================================================
# LOAD DATA
# ============================================================

print("\nLoading dataset_aeris_final...")

base = pd.read_csv(
    BASE_PATH,
    low_memory=False,
)

print(
    f"BASE    : "
    f"{len(base):,} rows x "
    f"{len(base.columns):,} columns"
)


print("\nLoading weather_enriched_master...")

weather = pd.read_parquet(
    WEATHER_PATH
)

print(
    f"WEATHER : "
    f"{len(weather):,} rows x "
    f"{len(weather.columns):,} columns"
)


print("\nLoading ispu_master...")

ispu = pd.read_parquet(
    ISPU_PATH
)

print(
    f"ISPU    : "
    f"{len(ispu):,} rows x "
    f"{len(ispu.columns):,} columns"
)


# ============================================================
# VALIDATE REQUIRED COLUMNS
# ============================================================

require_columns(
    base,
    [
        "datetime_utc",
        "target_region",
        "target_lat",
        "target_lon",
        *BASE_WEATHER_COLUMNS,
    ],
    "BASE",
)

require_columns(
    weather,
    [
        "datetime_utc",
        "target_region",
        "target_name",
        *BASE_WEATHER_COLUMNS,
        *WEATHER_ENRICHMENT_COLUMNS,
        "weather_source",
        "source_timezone",
    ],
    "WEATHER",
)

require_columns(
    ispu,
    [
        "observed_at_utc",
        "target_region",
        "target_name",
        "observed_at_local",
        "source_date_local",
        "source_hour_local",
        "source_station_id",
        "source_station_code",
        "source_station_name",
        *ISPU_POLLUTANTS,
        "category",
        "quality_flag",
        "source_timezone",
        "source",
        "source_url",
    ],
    "ISPU",
)


# ============================================================
# NORMALIZE REGION
# ============================================================

base["target_region"] = normalize_region(
    base["target_region"]
)

weather["target_region"] = normalize_region(
    weather["target_region"]
)

ispu["target_region"] = normalize_region(
    ispu["target_region"]
)


# ============================================================
# NORMALIZE DATETIME TO UTC HOUR
# ============================================================

base["_join_time"] = to_utc_hour(
    base["datetime_utc"]
)

weather["_join_time"] = to_utc_hour(
    weather["datetime_utc"]
)

ispu["_join_time"] = to_utc_hour(
    ispu["observed_at_utc"]
)


# ============================================================
# BUILD TARGET STATION MAPPING
# ============================================================
#
# dataset_aeris_final does not contain target_name.
# weather master does.
#
# Therefore:
#
# target_region
#       ↓
# target_name
#
# is added to dataset_aeris_final first.
#
# ============================================================

station_map = (
    weather[
        [
            "target_region",
            "target_name",
        ]
    ]
    .dropna()
    .drop_duplicates()
    .sort_values(
        [
            "target_region",
            "target_name",
        ]
    )
)


# ============================================================
# CHECK ONE TARGET STATION PER REGION
# ============================================================

station_count = (
    station_map
    .groupby("target_region")["target_name"]
    .nunique()
)

ambiguous_regions = station_count[
    station_count > 1
]

if not ambiguous_regions.empty:

    raise ValueError(
        "There is more than one target station "
        "for some target_region:\n\n"
        f"{ambiguous_regions}"
    )


# ============================================================
# ADD target_name INTO BASE DATASET
# ============================================================

base = base.merge(
    station_map,
    on="target_region",
    how="left",
    validate="many_to_one",
)


if base["target_name"].isna().any():

    missing_regions = (
        base.loc[
            base["target_name"].isna(),
            "target_region",
        ]
        .drop_duplicates()
        .tolist()
    )

    raise ValueError(
        "Some base rows could not be mapped "
        "to target_name.\n"
        f"Regions: {missing_regions}"
    )


# ============================================================
# NORMALIZE TARGET STATION FOR JOIN
# ============================================================

base["_join_station"] = normalize_station(
    base["target_name"]
)

weather["_join_station"] = normalize_station(
    weather["target_name"]
)

ispu["_join_station"] = normalize_station(
    ispu["target_name"]
)


# ============================================================
# FINAL MERGE KEYS
# ============================================================
#
# DATE/TIME
# +
# TARGET REGION
# +
# TARGET STATION
#
# ============================================================

KEYS = [
    "_join_time",
    "target_region",
    "_join_station",
]


# ============================================================
# UNIQUE KEY CHECK
# ============================================================

print("\nChecking duplicate merge keys...")

assert_unique_key(
    base,
    KEYS,
    "BASE",
)

assert_unique_key(
    weather,
    KEYS,
    "WEATHER",
)

assert_unique_key(
    ispu,
    KEYS,
    "ISPU",
)

print("No duplicate merge keys found.")


# ============================================================
# VALIDATE EXISTING BASIC WEATHER
# ============================================================
#
# dataset_aeris_final already has:
#
# temperature
# humidity
# precipitation
# pressure
# wind speed
# wind direction
#
# Therefore we do NOT add them again.
#
# But we validate that values in weather_master
# correspond to the same station + timestamp.
#
# ============================================================

print("\nValidating existing basic weather data...")


weather_check = weather[
    KEYS + BASE_WEATHER_COLUMNS
].copy()


check = base[
    KEYS + BASE_WEATHER_COLUMNS
].merge(
    weather_check,
    on=KEYS,
    how="left",
    validate="one_to_one",
    suffixes=(
        "_base",
        "_weather",
    ),
    indicator=True,
)


weather_key_matches = int(
    (check["_merge"] == "both").sum()
)


if weather_key_matches != len(base):

    raise RuntimeError(
        "Weather coverage is incomplete.\n"
        f"Matched: "
        f"{weather_key_matches:,}/{len(base):,}"
    )


weather_differences = {}


for column in BASE_WEATHER_COLUMNS:

    base_col = f"{column}_base"
    weather_col = f"{column}_weather"

    valid = (
        check[base_col].notna()
        & check[weather_col].notna()
    )

    differences = (
        (
            check.loc[
                valid,
                base_col
            ].astype(float)
            -
            check.loc[
                valid,
                weather_col
            ].astype(float)
        )
        .abs()
        > 1e-6
    )

    difference_count = int(
        differences.sum()
    )

    weather_differences[column] = (
        difference_count
    )


print(
    "Basic weather validation complete."
)

for column, difference_count in (
    weather_differences.items()
):

    print(
        f"{column:<25}: "
        f"{difference_count:,} differences"
    )


# ============================================================
# PREPARE WEATHER ENRICHMENT
# ============================================================

weather_cols = [
    *KEYS,
    *WEATHER_ENRICHMENT_COLUMNS,
    "weather_source",
    "source_timezone",
]


weather_merge = (
    weather[
        weather_cols
    ]
    .copy()
    .rename(
        columns={
            "source_timezone":
                "weather_source_timezone",
        }
    )
)


# ============================================================
# PREPARE ISPU DATA
# ============================================================

ispu_cols = [
    *KEYS,

    "observed_at_local",
    "source_date_local",
    "source_hour_local",

    "source_station_id",
    "source_station_code",
    "source_station_name",

    *ISPU_POLLUTANTS,

    "category",
    "quality_flag",

    "source_timezone",
    "source",
    "source_url",
]


ispu_merge = (
    ispu[
        ispu_cols
    ]
    .copy()
    .rename(
        columns={

            "observed_at_local":
                "ispu_observed_at_local",

            "source_date_local":
                "ispu_source_date_local",

            "source_hour_local":
                "ispu_source_hour_local",

            "source_station_id":
                "ispu_source_station_id",

            "source_station_code":
                "ispu_source_station_code",

            "source_station_name":
                "ispu_source_station_name",

            "category":
                "ispu_category",

            "quality_flag":
                "ispu_quality_flag",

            "source_timezone":
                "ispu_source_timezone",

            "source":
                "ispu_source",

            "source_url":
                "ispu_source_url",
        }
    )
)


# ============================================================
# LEFT JOIN WEATHER INTO BASE
# ============================================================

print("\nMerging weather enrichment...")

original_rows = len(base)


merged = base.merge(
    weather_merge,
    on=KEYS,
    how="left",
    validate="one_to_one",
    indicator="_weather_merge",
)


weather_matched = int(
    (
        merged["_weather_merge"]
        == "both"
    ).sum()
)


merged = merged.drop(
    columns=[
        "_weather_merge"
    ]
)


# ============================================================
# LEFT JOIN ISPU INTO BASE
# ============================================================

print("Merging ISPU...")

merged = merged.merge(
    ispu_merge,
    on=KEYS,
    how="left",
    validate="one_to_one",
    indicator="_ispu_merge",
)


ispu_matched = int(
    (
        merged["_ispu_merge"]
        == "both"
    ).sum()
)


merged = merged.drop(
    columns=[
        "_ispu_merge"
    ]
)


# ============================================================
# SAFETY CHECK
# ============================================================

if len(merged) != original_rows:

    raise RuntimeError(
        "Row count changed after merge!\n"
        f"Before : {original_rows:,}\n"
        f"After  : {len(merged):,}"
    )


# ============================================================
# DROP INTERNAL JOIN KEYS
# ============================================================

merged = merged.drop(
    columns=[
        "_join_time",
        "_join_station",
    ]
)


# ============================================================
# REORDER IMPORTANT COLUMNS
# ============================================================

first_columns = [
    "datetime_utc",
    "target_region",
    "target_name",
    "target_lat",
    "target_lon",
]


remaining_columns = [
    col
    for col in merged.columns
    if col not in first_columns
]


merged = merged[
    first_columns
    + remaining_columns
]


# ============================================================
# REPORT
# ============================================================

print("\n")
print("=" * 60)
print("                 AERIS MERGE REPORT")
print("=" * 60)

print(
    f"Base rows          : "
    f"{original_rows:,}"
)

print(
    f"Weather matched    : "
    f"{weather_matched:,} "
    f"({weather_matched / original_rows * 100:.2f}%)"
)

print(
    f"ISPU matched       : "
    f"{ispu_matched:,} "
    f"({ispu_matched / original_rows * 100:.2f}%)"
)

print(
    f"Final rows         : "
    f"{len(merged):,}"
)

print(
    f"Final columns      : "
    f"{len(merged.columns):,}"
)

print("=" * 60)


# ============================================================
# TARGET STATION REPORT
# ============================================================

print("\nTarget station mapping:")

print(
    station_map.to_string(
        index=False
    )
)


# ============================================================
# SAVE
# ============================================================

OUTPUT_PATH.parent.mkdir(
    parents=True,
    exist_ok=True,
)

merged.to_csv(
    OUTPUT_PATH,
    index=False,
)


print(
    f"\nSaved successfully ->\n"
    f"{OUTPUT_PATH}"
)