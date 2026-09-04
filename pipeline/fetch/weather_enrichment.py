from __future__ import annotations

import argparse
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import polars as pl
import requests


OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_MAX_WORKERS = 3

TARGET_STATIONS = [
    {
        "region": "jakarta_pusat",
        "name": "Bunderan HI / US Embassy",
        "lat": -6.182536,
        "lon": 106.828236,
    },
    {
        "region": "jakarta_selatan",
        "name": "Jagakarsa",
        "lat": -6.325500,
        "lon": 106.814400,
    },
    {
        "region": "jakarta_barat",
        "name": "Kebon Jeruk",
        "lat": -6.194900,
        "lon": 106.764500,
    },
    {
        "region": "jakarta_timur",
        "name": "Jatinegara",
        "lat": -6.212000,
        "lon": 106.883000,
    },
    {
        "region": "jakarta_utara",
        "name": "Kelapa Gading",
        "lat": -6.155300,
        "lon": 106.892300,
    },
]

# Existing AERIS weather variables + requested enrichment.
HOURLY_VARIABLES = [
    # Existing
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    # Enrichment
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

OUTPUT_COLUMNS = [
    "datetime_utc",
    "target_region",
    "target_name",
    "target_lat",
    "target_lon",
    *HOURLY_VARIABLES,
    "weather_source",
    "source_timezone",
]


@dataclass(frozen=True)
class FetchBlock:
    station: dict[str, Any]
    start_date: date
    end_date: date


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def format_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "--:--"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


@dataclass
class ProgressTracker:
    label: str
    total: int
    completed: int = 0
    succeeded: int = 0
    failed: int = 0
    rows: int = 0
    started_at: float = 0.0

    def __post_init__(self) -> None:
        self.started_at = time.monotonic()

    def update(
        self,
        *,
        success: bool,
        detail: str,
        rows: int = 0,
    ) -> None:
        self.completed += 1
        self.rows += rows
        if success:
            self.succeeded += 1
        else:
            self.failed += 1

        elapsed = time.monotonic() - self.started_at
        percent = (self.completed / self.total * 100.0) if self.total else 100.0
        rate = self.completed / elapsed if elapsed > 0 else 0.0
        remaining = max(self.total - self.completed, 0)
        eta = remaining / rate if rate > 0 else None

        logging.info(
            "[%s] %d/%d (%.1f%%) | OK=%d FAIL=%d | rows=%d | elapsed=%s | ETA=%s | %s",
            self.label,
            self.completed,
            self.total,
            percent,
            self.succeeded,
            self.failed,
            self.rows,
            format_duration(elapsed),
            format_duration(eta),
            detail,
        )


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def iter_year_blocks(start: date, end: date) -> Iterable[tuple[date, date]]:
    current_year = start.year
    while current_year <= end.year:
        block_start = max(start, date(current_year, 1, 1))
        block_end = min(end, date(current_year, 12, 31))
        yield block_start, block_end
        current_year += 1


def request_json_with_retry(
    url: str,
    params: dict[str, Any],
    *,
    max_attempts: int = 6,
    connect_timeout: float = 15.0,
    read_timeout: float = 90.0,
) -> dict[str, Any]:
    """
    Robust HTTP request:
    - retry timeout / connection errors
    - retry 429 and 5xx
    - exponential backoff + jitter
    - raises only after all attempts are exhausted
    """
    headers = {
        "User-Agent": "AERIS-Air-Intelligence/1.0 (+research; Open-Meteo historical enrichment)",
        "Accept": "application/json",
    }

    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=(connect_timeout, read_timeout),
            )

            if response.status_code == 429 or 500 <= response.status_code < 600:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_seconds = float(retry_after)
                    except ValueError:
                        wait_seconds = 0.0
                else:
                    wait_seconds = 0.0

                if attempt == max_attempts:
                    response.raise_for_status()

                backoff = min(60.0, 2 ** (attempt - 1)) + random.uniform(0.0, 1.0)
                wait_seconds = max(wait_seconds, backoff)
                logging.warning(
                    "HTTP %s. Retry %s/%s in %.1fs",
                    response.status_code,
                    attempt,
                    max_attempts,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            payload = response.json()

            if "error" in payload and payload.get("error"):
                raise RuntimeError(
                    f"Open-Meteo API error: {payload.get('reason', payload)}"
                )

            return payload

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt == max_attempts:
                break

            wait_seconds = min(60.0, 2 ** (attempt - 1)) + random.uniform(0.0, 1.0)
            logging.warning(
                "Network error: %s. Retry %s/%s in %.1fs",
                exc,
                attempt,
                max_attempts,
                wait_seconds,
            )
            time.sleep(wait_seconds)

        except requests.RequestException as exc:
            # Non-transient 4xx should fail immediately.
            raise RuntimeError(f"Open-Meteo request failed: {exc}") from exc

    raise RuntimeError(
        f"Open-Meteo request failed after {max_attempts} attempts: {last_error}"
    )


def fetch_weather_block(block: FetchBlock) -> pl.DataFrame:
    station = block.station
    params = {
        "latitude": station["lat"],
        "longitude": station["lon"],
        "start_date": block.start_date.isoformat(),
        "end_date": block.end_date.isoformat(),
        "hourly": ",".join(HOURLY_VARIABLES),
        # Keep AERIS canonical timeline in UTC.
        "timezone": "UTC",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
    }

    logging.info(
        "Fetching weather %s | %s -> %s",
        station["region"],
        block.start_date,
        block.end_date,
    )

    payload = request_json_with_retry(OPEN_METEO_ARCHIVE_URL, params)

    hourly = payload.get("hourly")
    if not hourly or "time" not in hourly:
        raise ValueError(
            f"Missing hourly data for {station['region']} "
            f"{block.start_date}..{block.end_date}"
        )

    times = hourly["time"]
    row_count = len(times)

    data: dict[str, Any] = {
        "datetime_utc": times,
        "target_region": [station["region"]] * row_count,
        "target_name": [station["name"]] * row_count,
        "target_lat": [station["lat"]] * row_count,
        "target_lon": [station["lon"]] * row_count,
    }

    for variable in HOURLY_VARIABLES:
        values = hourly.get(variable)
        if values is None:
            logging.warning(
                "Variable %s unavailable for %s %s..%s; filling null.",
                variable,
                station["region"],
                block.start_date,
                block.end_date,
            )
            values = [None] * row_count

        if len(values) != row_count:
            raise ValueError(
                f"Length mismatch for {variable}: {len(values)} != {row_count}"
            )
        data[variable] = values

    data["weather_source"] = ["open_meteo_historical_weather"] * row_count
    data["source_timezone"] = ["UTC"] * row_count

    df = pl.DataFrame(data).with_columns(
        pl.col("datetime_utc")
        .str.to_datetime(strict=False)
        .dt.replace_time_zone("UTC")
        .alias("datetime_utc")
    )

    # Never retain future hours if the API returns a full current-day series.
    current_hour_utc = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    df = df.filter(pl.col("datetime_utc") <= current_hour_utc)

    return (
        df.select(OUTPUT_COLUMNS)
        .unique(subset=["datetime_utc", "target_region"], keep="last")
        .sort(["target_region", "datetime_utc"])
    )


def block_output_path(raw_dir: Path, block: FetchBlock) -> Path:
    region = block.station["region"]
    year = block.start_date.year
    return raw_dir / region / f"weather_enriched_{region}_{year}.csv.gz"


def is_existing_block_usable(path: Path, expected_end: date) -> bool:
    """
    Past-year files can be reused if they already contain the enriched schema.
    Current-year files are refreshed by the caller.
    """
    if not path.exists():
        return False

    try:
        sample = pl.read_csv(path, n_rows=2)
    except Exception:
        return False

    return set(OUTPUT_COLUMNS).issubset(set(sample.columns))


def atomic_write_csv_gz(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Keep the temporary filename ending in .gz so Polars applies gzip compression.
    tmp = path.with_name(path.stem + ".tmp" + path.suffix)
    df.write_csv(tmp, compression="gzip")
    tmp.replace(path)


def write_master_dataset(raw_dir: Path, processed_path: Path) -> pl.DataFrame:
    files = sorted(raw_dir.glob("*/weather_enriched_*.csv.gz"))
    if not files:
        raise FileNotFoundError(f"No enriched weather files found under {raw_dir}")

    frames = [pl.read_csv(path, try_parse_dates=True) for path in files]
    master = (
        pl.concat(frames, how="diagonal_relaxed")
        .with_columns(
            pl.col("datetime_utc")
            .cast(pl.Datetime, strict=False)
            .dt.replace_time_zone("UTC")
        )
        .unique(subset=["datetime_utc", "target_region"], keep="last")
        .sort(["target_region", "datetime_utc"])
    )

    processed_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = processed_path.with_suffix(processed_path.suffix + ".tmp")
    master.write_parquet(tmp, compression="zstd")
    tmp.replace(processed_path)

    return master


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch AERIS historical weather enrichment from Open-Meteo "
            "for the five target Jakarta stations."
        )
    )
    parser.add_argument(
        "--start-date",
        type=parse_iso_date,
        default=DEFAULT_START_DATE,
        help="Inclusive start date, default: 2024-01-01",
    )
    parser.add_argument(
        "--end-date",
        type=parse_iso_date,
        default=None,
        help="Inclusive end date, default: today",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw/openmeteo_enriched"),
    )
    parser.add_argument(
        "--master-output",
        type=Path,
        default=Path("data/processed/weather_enriched_master.parquet"),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Refetch past-year files even when the enriched schema already exists.",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)

    if not 1 <= args.max_workers <= 3:
        raise ValueError("--max-workers must be between 1 and 3.")

    end_date = args.end_date or datetime.now(ZoneInfo("Asia/Jakarta")).date()
    if args.start_date > end_date:
        raise ValueError("start-date cannot be after end-date.")

    current_year = end_date.year
    blocks: list[FetchBlock] = []

    for station in TARGET_STATIONS:
        for block_start, block_end in iter_year_blocks(args.start_date, end_date):
            block = FetchBlock(station, block_start, block_end)
            output_path = block_output_path(args.raw_dir, block)

            # Always refresh current year so "until now" really updates.
            should_fetch = (
                args.force
                or block_start.year == current_year
                or not is_existing_block_usable(output_path, block_end)
            )

            if should_fetch:
                blocks.append(block)
            else:
                logging.info("Reuse existing: %s", output_path)

    failures: list[str] = []
    progress = ProgressTracker(label="WEATHER", total=len(blocks))

    if blocks:
        logging.info(
            "[WEATHER] Starting %d fetch block(s) with %d worker(s).",
            len(blocks),
            args.max_workers,
        )
    else:
        logging.info("[WEATHER] Nothing to fetch; all reusable blocks already exist.")

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_map = {
            executor.submit(fetch_weather_block, block): block for block in blocks
        }

        for future in as_completed(future_map):
            block = future_map[future]
            path = block_output_path(args.raw_dir, block)
            detail = (
                f"{block.station['region']} "
                f"{block.start_date}..{block.end_date}"
            )
            try:
                df = future.result()
                atomic_write_csv_gz(df, path)
                logging.info("Saved %s rows -> %s", df.height, path)
                progress.update(success=True, detail=detail, rows=df.height)
            except Exception as exc:
                message = f"{detail}: {exc}"
                failures.append(message)
                logging.exception("Weather block failed: %s", message)
                progress.update(success=False, detail=detail)

    master = write_master_dataset(args.raw_dir, args.master_output)
    logging.info(
        "Master weather dataset: %s rows, %s cols -> %s",
        master.height,
        master.width,
        args.master_output,
    )

    if failures:
        logging.warning("Completed with %s failed block(s):", len(failures))
        for failure in failures:
            logging.warning("  - %s", failure)
    else:
        logging.info("Weather enrichment completed without failed blocks.")


if __name__ == "__main__":
    main()
