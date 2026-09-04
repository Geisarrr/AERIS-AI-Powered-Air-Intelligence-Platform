#!/usr/bin/env python3
"""
AERIS — Historical Weather Forecast Archive Builder V3 (Rate-Safe)
======================================================

Purpose
-------
Build a leakage-aware RAW archive of historical ECMWF forecasts for AERIS
using Open-Meteo Single Runs API.

This V2 fixes the main design issues from V1:

1. Tries all ECMWF cycles: 00 / 06 / 12 / 18 UTC.
2. "Model run is not available" is treated as SKIPPED_UNAVAILABLE,
   not as a retryable failure.
3. Stores the FULL HOURLY forecast trajectory for every available run,
   not only +1/+6/+12/+24/+48 from the run-availability time.
4. Stores model lead 0..72h by default. 72h is intentionally larger than
   the AERIS maximum horizon (+48h), because an AERIS forecast origin can
   occur many hours after the latest available ECMWF run.
5. Uses per-run Parquet chunks instead of one giant append-only CSV.
   This makes resume safer and finalization more memory efficient.
6. Separates:
      forecast_run_initialized_utc
      forecast_available_utc
      forecast_valid_utc
      model_lead_h
   so future AERIS modeling can use an as-of join without look-ahead leakage.

IMPORTANT
---------
This output is a RAW FORECAST ARCHIVE, not yet the final AERIS modeling table.

Later V3 should do:

AERIS origin time t
    -> choose latest forecast_available_utc <= t
    -> from that run, choose forecast_valid_utc == t + horizon
    -> use those weather forecast values as future-weather features

Default source:
    data/processed/weather_enriched_master.parquet

Default output:
    data/processed/weather_forecast_historical_raw.parquet

Default chunk dir:
    data/interim/weather_forecast_raw_chunks/

Default state:
    data/interim/weather_forecast_historical_raw.state.json
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------
# API / MODEL CONFIG
# ---------------------------------------------------------------------

API_URL = "https://single-runs-api.open-meteo.com/v1/forecast"

# Open-Meteo Single Runs archive:
# ECMWF IFS HRES is available from 2024-03-14.
DEFAULT_START_DATE = "2024-03-14"
DEFAULT_MODEL = "ecmwf_ifs"

# Try all documented ECMWF cycles.
DEFAULT_RUN_HOURS = (0, 6, 12, 18)

# Conservative assumption:
# global-model output is typically distributed several hours after init.
DEFAULT_AVAILABILITY_DELAY_H = 6

# AERIS max target horizon is +48h.
# We store 0..72h because an AERIS origin may be 6–12+ hours after
# the run initialization / availability cycle.
DEFAULT_MAX_MODEL_LEAD_H = 72

# Keep schema close to current AERIS historical weather master.
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
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


# ---------------------------------------------------------------------
# CUSTOM EXCEPTIONS
# ---------------------------------------------------------------------

class RunUnavailableError(RuntimeError):
    """Historical model run is absent from the archive."""


class RateLimitError(RuntimeError):
    """Open-Meteo returned HTTP 429."""

    def __init__(self, message: str, retry_after: str | None = None):
        super().__init__(message)
        self.retry_after = retry_after


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build raw historical ECMWF forecast archive for AERIS "
            "using Open-Meteo Single Runs API."
        )
    )

    parser.add_argument(
        "--station-source",
        type=Path,
        default=Path("data/processed/weather_enriched_master.parquet"),
        help=(
            "Parquet containing target_region, target_name, "
            "target_lat, target_lon, datetime_utc."
        ),
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "data/processed/weather_forecast_historical_raw.parquet"
        ),
        help="Final raw historical forecast archive.",
    )

    parser.add_argument(
        "--chunk-dir",
        type=Path,
        default=Path("data/interim/weather_forecast_raw_chunks"),
        help="Per-run Parquet chunk directory.",
    )

    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path(
            "data/interim/weather_forecast_historical_raw.state.json"
        ),
        help="Resume / unavailable / failure state file.",
    )

    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="First run date YYYY-MM-DD.",
    )

    parser.add_argument(
        "--end-date",
        default=None,
        help=(
            "Last historical date YYYY-MM-DD. "
            "Default = max datetime_utc in station source."
        ),
    )

    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help="Open-Meteo model id. Default: ecmwf_ifs",
    )

    parser.add_argument(
        "--run-hours",
        default="0,6,12,18",
        help="UTC model cycles to try, comma separated.",
    )

    parser.add_argument(
        "--availability-delay-hours",
        type=int,
        default=DEFAULT_AVAILABILITY_DELAY_H,
        help=(
            "Conservative run initialization -> forecast availability delay."
        ),
    )

    parser.add_argument(
        "--max-model-lead-hours",
        type=int,
        default=DEFAULT_MAX_MODEL_LEAD_H,
        help=(
            "Maximum hourly model lead saved per run. "
            "Default 72h; recommended >= 66h for AERIS +48h."
        ),
    )

    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=3.0,
        help=(
            "Pause between candidate requests. Default 3s is deliberately "
            "more conservative for the Open-Meteo free endpoint."
        ),
    )

    parser.add_argument(
        "--max-pending-runs",
        type=int,
        default=None,
        help=(
            "Process at most N currently-pending candidate runs in this "
            "execution. Useful for rate-safe batches, e.g. 400."
        ),
    )

    parser.add_argument(
        "--request-timeout",
        type=int,
        default=90,
        help="HTTP request timeout seconds.",
    )

    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=20,
        help="Save state after this many processed runs.",
    )

    parser.add_argument(
        "--max-runs",
        type=int,
        default=None,
        help="Optional test limit, e.g. --max-runs 8.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Delete prior output/chunks/state and restart extraction."
        ),
    )

    parser.add_argument(
        "--no-finalize",
        action="store_true",
        help=(
            "Fetch chunks only; do not build final parquet at the end."
        ),
    )

    return parser.parse_args()


# ---------------------------------------------------------------------
# BASIC HELPERS
# ---------------------------------------------------------------------

def parse_int_tuple(value: str) -> tuple[int, ...]:
    result = tuple(
        int(item.strip())
        for item in value.split(",")
        if item.strip()
    )

    if not result:
        raise ValueError("Expected at least one integer value.")

    return result


def build_session() -> requests.Session:
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        status=6,
        backoff_factor=1.0,
        # 429 is handled explicitly by the AERIS circuit breaker below.
        # Keep automatic retries only for transient server-side 5xx errors.
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )

    session = requests.Session()

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=4,
        pool_maxsize=4,
    )

    session.mount("https://", adapter)

    session.headers.update(
        {
            "User-Agent": (
                "AERIS-Air-Intelligence/3.0 "
                "historical-forecast-archive"
            )
        }
    )

    return session


def load_stations(station_source: Path) -> pd.DataFrame:
    if not station_source.exists():
        raise FileNotFoundError(
            f"Station source not found: {station_source}"
        )

    required = [
        "target_region",
        "target_name",
        "target_lat",
        "target_lon",
    ]

    df = pd.read_parquet(
        station_source,
        columns=required,
    )

    missing = [column for column in required if column not in df.columns]

    if missing:
        raise ValueError(
            f"Station source missing required columns: {missing}"
        )

    stations = (
        df[required]
        .dropna()
        .drop_duplicates()
        .sort_values(["target_region", "target_name"])
        .reset_index(drop=True)
    )

    if stations.empty:
        raise ValueError("No station rows found.")

    return stations


def infer_dataset_end(
    station_source: Path,
    explicit_end: str | None,
) -> pd.Timestamp:
    if explicit_end:
        return (
            pd.Timestamp(explicit_end, tz="UTC").normalize()
            + pd.Timedelta(hours=23)
        )

    dt = pd.read_parquet(
        station_source,
        columns=["datetime_utc"],
    )["datetime_utc"]

    dt = pd.to_datetime(
        dt,
        utc=True,
        errors="raise",
    )

    return dt.max()


def generate_run_times(
    start_date: str,
    end_utc: pd.Timestamp,
    run_hours: tuple[int, ...],
) -> list[pd.Timestamp]:
    start = pd.Timestamp(start_date, tz="UTC").normalize()

    days = pd.date_range(
        start=start,
        end=end_utc.normalize(),
        freq="D",
        tz="UTC",
    )

    runs: list[pd.Timestamp] = []

    for day in days:
        for hour in run_hours:
            run_time = day + pd.Timedelta(hours=hour)

            if run_time <= end_utc:
                runs.append(run_time)

    return runs


def run_key(run_time: pd.Timestamp) -> str:
    return run_time.strftime("%Y-%m-%dT%H:%MZ")


def chunk_path_for_run(
    chunk_dir: Path,
    run_time: pd.Timestamp,
) -> Path:
    return chunk_dir / (
        "run_"
        + run_time.strftime("%Y%m%d_%H")
        + ".parquet"
    )


# ---------------------------------------------------------------------
# STATE / RESUME
# ---------------------------------------------------------------------

def empty_state() -> dict[str, Any]:
    return {
        "completed_runs": [],
        "unavailable_runs": {},
        "failed_runs": {},
        "row_counts": {},
        "last_rate_limit": None,
    }


def load_state(state_file: Path) -> dict[str, Any]:
    if not state_file.exists():
        return empty_state()

    with state_file.open("r", encoding="utf-8") as handle:
        state = json.load(handle)

    template = empty_state()

    for key, default_value in template.items():
        state.setdefault(key, default_value)

    return state


def save_state(
    state_file: Path,
    state: dict[str, Any],
) -> None:
    state_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = state_file.with_suffix(
        state_file.suffix + ".tmp"
    )

    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(
            state,
            handle,
            indent=2,
            sort_keys=True,
        )

    temporary.replace(state_file)


def reset_outputs(
    output: Path,
    chunk_dir: Path,
    state_file: Path,
) -> None:
    if output.exists():
        output.unlink()

    if state_file.exists():
        state_file.unlink()

    if chunk_dir.exists():
        shutil.rmtree(chunk_dir)


# ---------------------------------------------------------------------
# API
# ---------------------------------------------------------------------

def normalize_payload(
    payload: Any,
    expected_locations: int,
) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        items = payload

    elif isinstance(payload, dict):
        items = [payload]

    else:
        raise TypeError(
            f"Unexpected API payload type: {type(payload)!r}"
        )

    if len(items) != expected_locations:
        raise ValueError(
            f"API returned {len(items)} locations; "
            f"expected {expected_locations}."
        )

    return items


def fetch_run(
    session: requests.Session,
    run_time: pd.Timestamp,
    stations: pd.DataFrame,
    model: str,
    max_model_lead_h: int,
    timeout: int,
) -> list[dict[str, Any]]:
    """
    Fetch one model run for all AERIS stations in a single request.
    """

    params = {
        "latitude": ",".join(
            stations["target_lat"].astype(str)
        ),
        "longitude": ",".join(
            stations["target_lon"].astype(str)
        ),
        "run": run_time.strftime("%Y-%m-%dT%H:%M"),
        "hourly": ",".join(HOURLY_VARIABLES),
        "models": model,
        # Includes lead 0, so request N+1 timestamps.
        "forecast_hours": max_model_lead_h + 1,
        "timezone": "GMT",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }

    response = session.get(
        API_URL,
        params=params,
        timeout=timeout,
    )

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        text = response.text[:2000]

        raise RateLimitError(
            (
                f"HTTP 429 rate limit for run {run_time}. "
                f"Retry-After={retry_after!r}. Response={text}"
            ),
            retry_after=retry_after,
        )

    if response.status_code >= 400:
        text = response.text[:2000]

        unavailable_phrase = (
            "requested model run is not available"
        )

        if (
            response.status_code == 400
            and unavailable_phrase in text.lower()
        ):
            raise RunUnavailableError(text)

        raise RuntimeError(
            f"HTTP {response.status_code} "
            f"for run {run_time}: {text}"
        )

    payload = response.json()

    if isinstance(payload, dict) and payload.get("error"):
        reason = str(payload.get("reason", ""))

        if (
            "requested model run is not available"
            in reason.lower()
        ):
            raise RunUnavailableError(reason)

        raise RuntimeError(
            f"Open-Meteo error for run {run_time}: {reason}"
        )

    return normalize_payload(
        payload,
        expected_locations=len(stations),
    )


# ---------------------------------------------------------------------
# PAYLOAD -> RAW FORECAST ROWS
# ---------------------------------------------------------------------

def extract_hourly_archive_rows(
    payloads: list[dict[str, Any]],
    stations: pd.DataFrame,
    run_time: pd.Timestamp,
    model: str,
    availability_delay_h: int,
    max_model_lead_h: int,
) -> pd.DataFrame:
    """
    Convert one archived model run into a row-per-station-per-valid-hour table.
    """

    available_time = (
        run_time
        + pd.Timedelta(hours=availability_delay_h)
    )

    records: list[dict[str, Any]] = []

    for station_index, payload in enumerate(payloads):
        station = stations.iloc[station_index]

        hourly = payload.get("hourly") or {}

        if "time" not in hourly:
            raise ValueError(
                f"No hourly.time returned for "
                f"{station['target_name']} run={run_time}"
            )

        valid_times = pd.to_datetime(
            hourly["time"],
            utc=True,
            errors="raise",
        )

        for array_index, valid_time in enumerate(valid_times):
            lead_h = int(
                round(
                    (
                        valid_time - run_time
                    ).total_seconds()
                    / 3600
                )
            )

            if lead_h < 0:
                continue

            if lead_h > max_model_lead_h:
                continue

            record: dict[str, Any] = {
                "forecast_run_initialized_utc": run_time,
                "forecast_available_utc": available_time,
                "forecast_valid_utc": valid_time,
                "model_lead_h": lead_h,
                "hours_after_available": (
                    lead_h - availability_delay_h
                ),
                "target_region": station["target_region"],
                "target_name": station["target_name"],
                "target_lat": float(station["target_lat"]),
                "target_lon": float(station["target_lon"]),
                "forecast_model": model,
                "forecast_source": "open_meteo_single_runs",
                "forecast_source_timezone": "UTC",
                "availability_delay_assumption_h": (
                    availability_delay_h
                ),
            }

            for variable in HOURLY_VARIABLES:
                values = hourly.get(variable)

                record[f"{variable}_forecast"] = (
                    values[array_index]
                    if (
                        values is not None
                        and array_index < len(values)
                    )
                    else None
                )

            records.append(record)

    frame = pd.DataFrame.from_records(records)

    if frame.empty:
        raise ValueError(
            f"No hourly forecast rows extracted for {run_time}"
        )

    datetime_columns = [
        "forecast_run_initialized_utc",
        "forecast_available_utc",
        "forecast_valid_utc",
    ]

    for column in datetime_columns:
        frame[column] = pd.to_datetime(
            frame[column],
            utc=True,
            errors="raise",
        )

    key = [
        "target_name",
        "forecast_run_initialized_utc",
        "forecast_valid_utc",
        "forecast_model",
    ]

    duplicates = int(
        frame.duplicated(key).sum()
    )

    if duplicates:
        raise ValueError(
            f"{duplicates} duplicate rows inside run {run_time}"
        )

    # Basic timing integrity.
    bad_lead = (
        (
            frame["forecast_valid_utc"]
            - frame["forecast_run_initialized_utc"]
        )
        .dt.total_seconds()
        .div(3600)
        .round()
        .astype(int)
        != frame["model_lead_h"]
    )

    if bad_lead.any():
        raise ValueError(
            f"Lead-time integrity failed for run {run_time}"
        )

    return frame.sort_values(
        [
            "target_name",
            "forecast_valid_utc",
        ]
    ).reset_index(drop=True)


# ---------------------------------------------------------------------
# CHUNKS / FINALIZATION
# ---------------------------------------------------------------------

def write_run_chunk(
    frame: pd.DataFrame,
    chunk_path: Path,
) -> None:
    chunk_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = chunk_path.with_suffix(
        ".parquet.tmp"
    )

    frame.to_parquet(
        temporary,
        index=False,
    )

    temporary.replace(chunk_path)


def list_chunk_files(
    chunk_dir: Path,
) -> list[Path]:
    if not chunk_dir.exists():
        return []

    return sorted(
        chunk_dir.glob("run_*.parquet")
    )


def reconcile_state_from_chunks(
    chunk_dir: Path,
    state: dict[str, Any],
) -> int:
    """
    Recover completed-run state from existing run_YYYYMMDD_HH.parquet files.

    This protects progress when Ctrl+C happens after a chunk is written but
    before the periodic JSON checkpoint is flushed.
    """
    completed = set(state.get("completed_runs", []))
    recovered = 0

    for path in list_chunk_files(chunk_dir):
        stem = path.stem  # run_YYYYMMDD_HH
        parts = stem.split("_")

        if len(parts) != 3 or parts[0] != "run":
            continue

        try:
            ts = pd.Timestamp(
                f"{parts[1]} {parts[2]}:00",
                tz="UTC",
            )
        except Exception:
            continue

        key = run_key(ts)

        if key not in completed:
            completed.add(key)
            state["completed_runs"].append(key)
            recovered += 1

        # A real chunk always wins over an older failed marker.
        state.get("failed_runs", {}).pop(key, None)

    return recovered


def finalize_chunks(
    chunk_dir: Path,
    output: Path,
) -> tuple[int, list[str]]:
    """
    Stream all run chunks into one Parquet file without loading
    the entire archive into pandas memory.
    """

    chunks = list_chunk_files(chunk_dir)

    if not chunks:
        raise FileNotFoundError(
            f"No run chunks found in {chunk_dir}"
        )

    output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_output = output.with_suffix(
        output.suffix + ".tmp"
    )

    if temporary_output.exists():
        temporary_output.unlink()

    writer: pq.ParquetWriter | None = None
    total_rows = 0
    schema_names: list[str] = []

    try:
        for chunk in chunks:
            table = pq.read_table(chunk)

            if writer is None:
                writer = pq.ParquetWriter(
                    temporary_output,
                    table.schema,
                    compression="snappy",
                )
                schema_names = table.schema.names

            elif table.schema != writer.schema:
                table = table.cast(
                    writer.schema
                )

            writer.write_table(table)
            total_rows += table.num_rows

    finally:
        if writer is not None:
            writer.close()

    temporary_output.replace(output)

    return total_rows, schema_names


# ---------------------------------------------------------------------
# SUMMARY / QA
# ---------------------------------------------------------------------

def summarize_archive(
    output: Path,
    state: dict[str, Any],
) -> None:
    columns = [
        "forecast_run_initialized_utc",
        "forecast_available_utc",
        "forecast_valid_utc",
        "model_lead_h",
        "target_name",
        "forecast_model",
    ]

    summary_df = pd.read_parquet(
        output,
        columns=columns,
    )

    print("\n" + "=" * 82)
    print("AERIS RAW HISTORICAL WEATHER FORECAST ARCHIVE")
    print("=" * 82)

    print(f"Rows               : {len(summary_df):,}")
    print(
        "Available run files :",
        f"{summary_df['forecast_run_initialized_utc'].nunique():,}",
    )
    print(
        "Unavailable runs     :",
        f"{len(state.get('unavailable_runs', {})):,}",
    )
    print(
        "Retryable failures   :",
        f"{len(state.get('failed_runs', {})):,}",
    )
    print(
        "Stations             :",
        summary_df["target_name"].nunique(),
    )
    print(
        "Model leads          :",
        f"{summary_df['model_lead_h'].min()}h "
        f"-> {summary_df['model_lead_h'].max()}h",
    )
    print(
        "Run initialized      :",
        summary_df["forecast_run_initialized_utc"].min(),
        "->",
        summary_df["forecast_run_initialized_utc"].max(),
    )
    print(
        "Forecast valid time  :",
        summary_df["forecast_valid_utc"].min(),
        "->",
        summary_df["forecast_valid_utc"].max(),
    )

    print("\nRows per station:")
    print(
        summary_df["target_name"]
        .value_counts()
        .sort_index()
        .to_string()
    )

    print("\nAvailable run cycles:")
    run_cycle_counts = (
        summary_df[
            [
                "forecast_run_initialized_utc",
            ]
        ]
        .drop_duplicates()
        .assign(
            run_hour=lambda x:
                x["forecast_run_initialized_utc"].dt.hour
        )
        ["run_hour"]
        .value_counts()
        .sort_index()
    )

    print(run_cycle_counts.to_string())


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | %(message)s"
        ),
    )

    if args.max_model_lead_hours < 66:
        logging.warning(
            "max-model-lead-hours=%d may be too short for "
            "AERIS +48h when the latest available run is old. "
            "72h is recommended.",
            args.max_model_lead_hours,
        )

    if args.overwrite:
        reset_outputs(
            output=args.output,
            chunk_dir=args.chunk_dir,
            state_file=args.state_file,
        )

    stations = load_stations(
        args.station_source
    )

    dataset_end = infer_dataset_end(
        station_source=args.station_source,
        explicit_end=args.end_date,
    )

    run_hours = parse_int_tuple(
        args.run_hours
    )

    invalid_run_hours = [
        hour for hour in run_hours
        if hour not in (0, 6, 12, 18)
    ]

    if invalid_run_hours:
        logging.warning(
            "Non-standard ECMWF run hours requested: %s",
            invalid_run_hours,
        )

    all_runs = generate_run_times(
        start_date=args.start_date,
        end_utc=dataset_end,
        run_hours=run_hours,
    )

    if args.max_runs is not None:
        all_runs = all_runs[: args.max_runs]

    state = load_state(
        args.state_file
    )

    recovered = reconcile_state_from_chunks(
        args.chunk_dir,
        state,
    )

    if recovered:
        logging.info(
            "Recovered from chunks  : %s completed runs",
            f"{recovered:,}",
        )
        save_state(
            args.state_file,
            state,
        )

    completed = set(
        state.get("completed_runs", [])
    )

    unavailable = set(
        state.get("unavailable_runs", {}).keys()
    )

    pending_all = [
        run_time
        for run_time in all_runs
        if (
            run_key(run_time) not in completed
            and run_key(run_time) not in unavailable
        )
    ]

    if args.max_pending_runs is not None:
        pending = pending_all[: args.max_pending_runs]
    else:
        pending = pending_all

    logging.info(
        "Station count          : %d",
        len(stations),
    )
    logging.info(
        "Dataset end            : %s",
        dataset_end,
    )
    logging.info(
        "Requested run cycles   : %s",
        run_hours,
    )
    logging.info(
        "Max model lead         : %dh",
        args.max_model_lead_hours,
    )
    logging.info(
        "Total candidate runs   : %s",
        f"{len(all_runs):,}",
    )
    logging.info(
        "Completed / unavailable: %s / %s",
        f"{len(completed):,}",
        f"{len(unavailable):,}",
    )
    logging.info(
        "Pending total          : %s",
        f"{len(pending_all):,}",
    )
    logging.info(
        "Pending this execution : %s",
        f"{len(pending):,}",
    )

    session = build_session()

    processed_since_checkpoint = 0
    rate_limited = False

    for position, run_time in enumerate(
        pending,
        start=1,
    ):
        key = run_key(run_time)

        try:
            payloads = fetch_run(
                session=session,
                run_time=run_time,
                stations=stations,
                model=args.model,
                max_model_lead_h=(
                    args.max_model_lead_hours
                ),
                timeout=args.request_timeout,
            )

            frame = extract_hourly_archive_rows(
                payloads=payloads,
                stations=stations,
                run_time=run_time,
                model=args.model,
                availability_delay_h=(
                    args.availability_delay_hours
                ),
                max_model_lead_h=(
                    args.max_model_lead_hours
                ),
            )

            chunk_path = chunk_path_for_run(
                args.chunk_dir,
                run_time,
            )

            write_run_chunk(
                frame,
                chunk_path,
            )

            if key not in state["completed_runs"]:
                state["completed_runs"].append(key)

            state["failed_runs"].pop(
                key,
                None,
            )

            state["row_counts"][key] = int(
                len(frame)
            )

            state["last_rate_limit"] = None

            logging.info(
                "[%s/%s] OK          run=%s rows=%s lead=%d..%d",
                f"{position:,}",
                f"{len(pending):,}",
                run_time,
                f"{len(frame):,}",
                int(frame["model_lead_h"].min()),
                int(frame["model_lead_h"].max()),
            )

        except RateLimitError as exc:
            # Do NOT mark this run as failed/completed. Keep it pending so
            # the next execution resumes exactly here.
            state["last_rate_limit"] = {
                "run": key,
                "retry_after": exc.retry_after,
                "message": str(exc),
                "detected_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
            }

            save_state(
                args.state_file,
                state,
            )

            logging.error(
                "[%s/%s] RATE LIMITED run=%s — checkpoint saved; "
                "stopping cleanly. Retry-After=%r",
                f"{position:,}",
                f"{len(pending):,}",
                run_time,
                exc.retry_after,
            )

            rate_limited = True
            break

        except RunUnavailableError as exc:
            # Archive gap is normal and permanent for this historical run.
            state["unavailable_runs"][key] = str(exc)

            state["failed_runs"].pop(
                key,
                None,
            )

            logging.warning(
                "[%s/%s] UNAVAILABLE run=%s — skipped permanently",
                f"{position:,}",
                f"{len(pending):,}",
                run_time,
            )

        except Exception as exc:
            # Keep as retryable failure.
            state["failed_runs"][key] = (
                f"{type(exc).__name__}: {exc}"
            )

            logging.exception(
                "[%s/%s] FAILED      run=%s",
                f"{position:,}",
                f"{len(pending):,}",
                run_time,
            )

        processed_since_checkpoint += 1

        if (
            processed_since_checkpoint
            >= args.checkpoint_every
        ):
            save_state(
                args.state_file,
                state,
            )

            processed_since_checkpoint = 0

        time.sleep(
            args.sleep_seconds
        )

    save_state(
        args.state_file,
        state,
    )

    # Recompute unresolved candidates after this execution.
    completed_now = set(state.get("completed_runs", []))
    unavailable_now = set(state.get("unavailable_runs", {}).keys())

    unresolved = [
        rt for rt in all_runs
        if (
            run_key(rt) not in completed_now
            and run_key(rt) not in unavailable_now
        )
    ]

    if rate_limited:
        print("\nAERIS fetch paused safely because Open-Meteo returned HTTP 429.")
        print("Progress is preserved.")
        print("Chunks:", args.chunk_dir)
        print("State :", args.state_file)
        print(f"Remaining candidate runs: {len(unresolved):,}")
        print(
            "Re-run the SAME command later; do not use --overwrite. "
            "The rate-limited run remains pending."
        )
        return

    if args.no_finalize or unresolved:
        print("\nFetch batch finished; finalization skipped because archive is incomplete.")
        print("Chunks:", args.chunk_dir)
        print("State :", args.state_file)
        print(f"Remaining candidate runs: {len(unresolved):,}")
        print(
            "Continue with the same command without --overwrite. "
            "When no unresolved runs remain, the final parquet will be built."
        )
        return

    total_rows, _ = finalize_chunks(
        chunk_dir=args.chunk_dir,
        output=args.output,
    )

    logging.info(
        "Final archive rows     : %s",
        f"{total_rows:,}",
    )
    logging.info(
        "Final archive          : %s",
        args.output,
    )

    summarize_archive(
        output=args.output,
        state=state,
    )

    print("\nFiles:")
    print("  chunks :", args.chunk_dir)
    print("  state  :", args.state_file)
    print("  output :", args.output)

    print("\nNext V3 step:")
    print(
        "  AS-OF JOIN each AERIS hourly forecast origin "
        "to the latest available ECMWF run, then select "
        "weather at origin + {1,6,12,24,48} hours."
    )


if __name__ == "__main__":
    main()
