from __future__ import annotations

import argparse
import gzip
import io
import logging
import random
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
import requests
from bs4 import BeautifulSoup


BASE_URL = "https://rendahemisi.jakarta.go.id"
SOURCE_TIMEZONE = ZoneInfo("Asia/Jakarta")
DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_MAX_WORKERS = 3
DEFAULT_REFRESH_DAYS = 0

# IMPORTANT:
# target_* follows the AERIS logical region/station registry requested by the user.
# source_* follows the Jakarta Rendah Emisi historical page.
#
# Jakarta Timur:
# target station remains Jatinegara, while the historical full-pollutant source
# is DKI4 Lubang Buaya. Keep both identities in the output; do not silently
# pretend DKI4 is physically located at the Jatinegara target coordinate.
TARGET_STATIONS = [
    {
        "region": "jakarta_pusat",
        "name": "Bunderan HI / US Embassy",
        "lat": -6.182536,
        "lon": 106.828236,
        "source_station_id": 4,
        "source_station_code": "DKI1",
        "source_station_name": "DKI1 Bundaran HI",
        "source_slug": "dki1-bundaran-hi",
    },
    {
        "region": "jakarta_selatan",
        "name": "Jagakarsa",
        "lat": -6.325500,
        "lon": 106.814400,
        "source_station_id": 6,
        "source_station_code": "DKI3",
        "source_station_name": "DKI3 Jagakarsa",
        "source_slug": "dki3-jagakarsa",
    },
    {
        "region": "jakarta_barat",
        "name": "Kebon Jeruk",
        "lat": -6.194900,
        "lon": 106.764500,
        "source_station_id": 8,
        "source_station_code": "DKI5",
        "source_station_name": "DKI5 Kebun Jeruk",
        "source_slug": "dki5-kebun-jeruk",
    },
    {
        "region": "jakarta_timur",
        "name": "Jatinegara",
        "lat": -6.212000,
        "lon": 106.883000,
        "source_station_id": 7,
        "source_station_code": "DKI4",
        "source_station_name": "DKI4 Lubang Buaya",
        "source_slug": "dki4-lubang-buaya",
    },
    {
        "region": "jakarta_utara",
        "name": "Kelapa Gading",
        "lat": -6.155300,
        "lon": 106.892300,
        "source_station_id": 5,
        "source_station_code": "DKI2",
        "source_station_name": "DKI2 Kelapa Gading",
        "source_slug": "dki2-kelapa-gading",
    },
]

POLLUTANT_COLUMNS = [
    "pm10_ispu",
    "pm25_ispu",
    "so2_ispu",
    "co_ispu",
    "o3_ispu",
    "no2_ispu",
    "hc_ispu",
]

OUTPUT_COLUMNS = [
    "observed_at_utc",
    "observed_at_local",
    "source_date_local",
    "source_hour_local",
    "target_region",
    "target_name",
    "target_lat",
    "target_lon",
    "source_station_id",
    "source_station_code",
    "source_station_name",
    *POLLUTANT_COLUMNS,
    "category",
    "quality_flag",
    "source_timezone",
    "source",
    "source_url",
]


OUTPUT_SCHEMA = {
    "observed_at_utc": pl.Utf8,
    "observed_at_local": pl.Utf8,
    "source_date_local": pl.Utf8,
    "source_hour_local": pl.Utf8,
    "target_region": pl.Utf8,
    "target_name": pl.Utf8,
    "target_lat": pl.Float64,
    "target_lon": pl.Float64,
    "source_station_id": pl.Int64,
    "source_station_code": pl.Utf8,
    "source_station_name": pl.Utf8,
    "pm10_ispu": pl.Float64,
    "pm25_ispu": pl.Float64,
    "so2_ispu": pl.Float64,
    "co_ispu": pl.Float64,
    "o3_ispu": pl.Float64,
    "no2_ispu": pl.Float64,
    "hc_ispu": pl.Float64,
    "category": pl.Utf8,
    "quality_flag": pl.Utf8,
    "source_timezone": pl.Utf8,
    "source": pl.Utf8,
    "source_url": pl.Utf8,
}


@dataclass
class DayFetchResult:
    station: dict[str, Any]
    day: date
    status: str
    rows: list[dict[str, Any]]
    error: str | None = None
    attempts: int = 0


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
    report_every: int = 10
    completed: int = 0
    succeeded: int = 0
    empty: int = 0
    failed: int = 0
    rows: int = 0
    started_at: float = 0.0

    def __post_init__(self) -> None:
        self.started_at = time.monotonic()

    def update(
        self,
        *,
        status: str,
        rows: int,
        detail: str,
        force_report: bool = False,
    ) -> None:
        self.completed += 1
        self.rows += rows

        if status == "ok":
            self.succeeded += 1
        elif status == "empty":
            self.empty += 1
        else:
            self.failed += 1

        should_report = (
            force_report
            or status == "error"
            or self.completed == 1
            or self.completed == self.total
            or self.completed % max(self.report_every, 1) == 0
        )
        if not should_report:
            return

        elapsed = time.monotonic() - self.started_at
        percent = (self.completed / self.total * 100.0) if self.total else 100.0
        rate = self.completed / elapsed if elapsed > 0 else 0.0
        remaining = max(self.total - self.completed, 0)
        eta = remaining / rate if rate > 0 else None

        logging.info(
            "[%s] %d/%d (%.1f%%) | OK=%d EMPTY=%d FAIL=%d | rows=%d | elapsed=%s | ETA=%s | %s",
            self.label,
            self.completed,
            self.total,
            percent,
            self.succeeded,
            self.empty,
            self.failed,
            self.rows,
            format_duration(elapsed),
            format_duration(eta),
            detail,
        )


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def iter_dates(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def build_source_url(station: dict[str, Any], day: date) -> str:
    # Historical route observed on Jakarta Rendah Emisi:
    # /ispu-detail/{station_id}/{slug}/DD-MM-YYYY
    date_path = day.strftime("%d-%m-%Y")
    return (
        f"{BASE_URL}/ispu-detail/"
        f"{station['source_station_id']}/"
        f"{station['source_slug']}/"
        f"{date_path}"
    )


def get_html_with_retry(
    url: str,
    *,
    max_attempts: int = 7,
    connect_timeout: float = 15.0,
    read_timeout: float = 60.0,
) -> tuple[str, int]:
    """
    Retry policy intentionally catches timeout and transient HTTP errors so
    a single bad date never terminates the whole historical collection.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; AERIS-Air-Intelligence/1.0; "
            "+research data collection)"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
    }

    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=(connect_timeout, read_timeout),
                allow_redirects=True,
            )

            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt == max_attempts:
                    response.raise_for_status()

                retry_after = response.headers.get("Retry-After")
                try:
                    retry_after_seconds = float(retry_after) if retry_after else 0.0
                except ValueError:
                    retry_after_seconds = 0.0

                backoff = min(90.0, 2 ** (attempt - 1)) + random.uniform(0.0, 1.5)
                wait_seconds = max(retry_after_seconds, backoff)

                logging.warning(
                    "Transient HTTP %s | retry %s/%s in %.1fs | %s",
                    response.status_code,
                    attempt,
                    max_attempts,
                    wait_seconds,
                    url,
                )
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()

            text = response.text
            if not text or len(text) < 500:
                raise ValueError(f"Unexpectedly short HTML response ({len(text)} bytes)")

            return text, attempt

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt == max_attempts:
                break

            wait_seconds = min(90.0, 2 ** (attempt - 1)) + random.uniform(0.0, 1.5)
            logging.warning(
                "Network error | retry %s/%s in %.1fs | %s | %s",
                attempt,
                max_attempts,
                wait_seconds,
                url,
                exc,
            )
            time.sleep(wait_seconds)

        except requests.RequestException as exc:
            raise RuntimeError(f"HTTP request failed: {exc}") from exc

        except ValueError as exc:
            last_error = exc
            if attempt == max_attempts:
                break

            wait_seconds = min(90.0, 2 ** (attempt - 1)) + random.uniform(0.0, 1.5)
            logging.warning(
                "Invalid response | retry %s/%s in %.1fs | %s | %s",
                attempt,
                max_attempts,
                wait_seconds,
                url,
                exc,
            )
            time.sleep(wait_seconds)

    raise RuntimeError(
        f"Request failed after {max_attempts} attempts: {last_error}"
    )


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def parse_nullable_float(value: str) -> float | None:
    value = normalize_text(value)
    if value in {"", "-", "–", "—", "null", "None"}:
        return None

    value = value.replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", value)
    if not match:
        return None
    return float(match.group(0))


def normalized_header(value: str) -> str:
    compact = normalize_text(value).upper().replace("_", " ")
    compact = re.sub(r"\s+", " ", compact)

    mapping = {
        "WAKTU": "source_hour_local",
        "PM 10": "pm10_ispu",
        "PM10": "pm10_ispu",
        "PM 2.5": "pm25_ispu",
        "PM2.5": "pm25_ispu",
        "PM 2,5": "pm25_ispu",
        "SO2": "so2_ispu",
        "SO 2": "so2_ispu",
        "CO": "co_ispu",
        "O3": "o3_ispu",
        "O 3": "o3_ispu",
        "NO2": "no2_ispu",
        "NO 2": "no2_ispu",
        "HC": "hc_ispu",
        "KATEGORI": "category",
    }
    return mapping.get(compact, compact.lower().replace(" ", "_"))


def find_history_table(soup: BeautifulSoup):
    candidates = []
    for table in soup.find_all("table"):
        headers = [
            normalize_text(cell.get_text(" ", strip=True))
            for cell in table.find_all("th")
        ]
        if not headers:
            first_row = table.find("tr")
            if first_row:
                headers = [
                    normalize_text(cell.get_text(" ", strip=True))
                    for cell in first_row.find_all(["th", "td"])
                ]

        joined = " | ".join(headers).upper()
        if "WAKTU" in joined and ("PM 10" in joined or "PM10" in joined):
            candidates.append(table)

    if not candidates:
        raise ValueError("Historical ISPU table was not found in HTML.")

    return candidates[0]


def parse_history_html(
    html: str,
    station: dict[str, Any],
    day: date,
    source_url: str,
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    table = find_history_table(soup)

    rows_html = table.find_all("tr")
    if not rows_html:
        raise ValueError("Historical table has no rows.")

    header_cells = rows_html[0].find_all(["th", "td"])
    headers = [normalized_header(cell.get_text(" ", strip=True)) for cell in header_cells]

    # Some HTML versions may use THEAD separately. If the first row is data,
    # fall back to the canonical legacy table order.
    if "source_hour_local" not in headers:
        headers = [
            "source_hour_local",
            "pm10_ispu",
            "pm25_ispu",
            "so2_ispu",
            "co_ispu",
            "o3_ispu",
            "no2_ispu",
            "hc_ispu",
            "category",
        ]
        data_rows = rows_html
    else:
        data_rows = rows_html[1:]

    parsed: list[dict[str, Any]] = []

    for tr in data_rows:
        cells = tr.find_all(["td", "th"])
        values = [normalize_text(cell.get_text(" ", strip=True)) for cell in cells]
        if not values:
            continue

        # Defensive handling for malformed row lengths.
        if len(values) < len(headers):
            values += [""] * (len(headers) - len(values))
        elif len(values) > len(headers):
            values = values[: len(headers)]

        record = dict(zip(headers, values))
        hour_text = record.get("source_hour_local", "")

        if not re.fullmatch(r"\d{2}:\d{2}", hour_text):
            continue

        hour, minute = map(int, hour_text.split(":"))
        local_dt = datetime(
            day.year,
            day.month,
            day.day,
            hour,
            minute,
            tzinfo=SOURCE_TIMEZONE,
        )
        utc_dt = local_dt.astimezone(timezone.utc)

        pollutant_values = {
            column: parse_nullable_float(record.get(column, ""))
            for column in POLLUTANT_COLUMNS
        }

        non_null_count = sum(v is not None for v in pollutant_values.values())
        if non_null_count == 0:
            quality_flag = "source_missing_all_pollutants"
        elif non_null_count < len(POLLUTANT_COLUMNS):
            quality_flag = "source_partial_pollutants"
        else:
            quality_flag = "ok"

        parsed.append(
            {
                "observed_at_utc": utc_dt.isoformat(),
                "observed_at_local": local_dt.isoformat(),
                "source_date_local": day.isoformat(),
                "source_hour_local": hour_text,
                "target_region": station["region"],
                "target_name": station["name"],
                "target_lat": station["lat"],
                "target_lon": station["lon"],
                "source_station_id": station["source_station_id"],
                "source_station_code": station["source_station_code"],
                "source_station_name": station["source_station_name"],
                **pollutant_values,
                "category": normalize_text(record.get("category", "")) or None,
                "quality_flag": quality_flag,
                "source_timezone": "Asia/Jakarta",
                "source": "jakarta_rendah_emisi_legacy_ispu",
                "source_url": source_url,
            }
        )

    if not parsed:
        raise ValueError("No hourly ISPU rows could be parsed.")

    return parsed


def fetch_day(station: dict[str, Any], day: date) -> DayFetchResult:
    url = build_source_url(station, day)

    try:
        html, attempts = get_html_with_retry(url)
        rows = parse_history_html(html, station, day, url)

        all_missing = all(
            all(row[col] is None for col in POLLUTANT_COLUMNS)
            for row in rows
        )
        status = "empty" if all_missing else "ok"

        return DayFetchResult(
            station=station,
            day=day,
            status=status,
            rows=rows,
            attempts=attempts,
        )

    except Exception as exc:
        logging.error(
            "Failed %s | %s | %s",
            station["region"],
            day,
            exc,
        )
        return DayFetchResult(
            station=station,
            day=day,
            status="error",
            rows=[],
            error=str(exc),
        )


def station_year_path(raw_dir: Path, station: dict[str, Any], year: int) -> Path:
    region = station["region"]
    return raw_dir / region / f"ispu_{region}_{year}.csv.gz"


def read_existing_station_year(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()

    try:
        return pl.read_csv(
            path,
            try_parse_dates=False,
            schema_overrides=OUTPUT_SCHEMA,
            infer_schema_length=0,
        )
    except Exception as exc:
        logging.warning("Cannot read existing %s: %s", path, exc)
        return pl.DataFrame()


def dates_already_present(existing: pl.DataFrame) -> set[str]:
    if existing.is_empty() or "source_date_local" not in existing.columns:
        return set()

    return set(
        existing.select(pl.col("source_date_local").cast(pl.Utf8))
        .unique()
        .to_series()
        .to_list()
    )


def atomic_write_csv_gz(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Keep the temporary filename ending in .gz so Polars applies gzip compression.
    tmp = path.with_name(path.stem + ".tmp" + path.suffix)
    df.write_csv(tmp, compression="gzip")
    tmp.replace(path)


def normalize_output_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    # Add any newly introduced columns to older existing files.
    for column in OUTPUT_COLUMNS:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None).alias(column))

    return (
        df.select(OUTPUT_COLUMNS)
        .unique(
            subset=["target_region", "observed_at_utc"],
            keep="last",
        )
        .sort(["target_region", "observed_at_utc"])
    )


def read_ispu_zip_member(zip_path: Path, member_name: str) -> pl.DataFrame:
    """Read one .csv.gz file stored inside the supplied ISPU zip."""
    with zipfile.ZipFile(zip_path) as zf:
        compressed = zf.read(member_name)

    decompressed = gzip.decompress(compressed)
    return pl.read_csv(
        io.BytesIO(decompressed),
        try_parse_dates=False,
        schema_overrides=OUTPUT_SCHEMA,
        infer_schema_length=0,
    )


def import_existing_zip(zip_path: Path, raw_dir: Path) -> dict[str, int]:
    """
    Merge already-downloaded yearly ISPU files from a zip into raw_dir.

    Existing local rows win on duplicate timestamps, so this never destroys
    newer local progress. The fetch plan is based on actual rows present in
    these CSVs, not on the status log.
    """
    if not zip_path.exists():
        raise FileNotFoundError(f"Existing ISPU zip not found: {zip_path}")

    imported_files = 0
    imported_dates = 0

    with zipfile.ZipFile(zip_path) as zf:
        members = [
            name
            for name in zf.namelist()
            if name.endswith(".csv.gz")
            and "/status/" not in name
            and "/ispu_" in name
        ]

    for member in sorted(members):
        filename = Path(member).name
        match = re.fullmatch(r"ispu_(jakarta_[a-z]+)_(\d{4})\.csv\.gz", filename)
        if not match:
            logging.warning("Skip unrecognized zip member: %s", member)
            continue

        region = match.group(1)
        zip_df = normalize_output_df(read_ispu_zip_member(zip_path, member))
        output_path = raw_dir / region / filename
        local_df = read_existing_station_year(output_path)

        if local_df.is_empty():
            combined = zip_df
        else:
            combined = normalize_output_df(
                pl.concat([zip_df, local_df], how="diagonal_relaxed")
            )

        atomic_write_csv_gz(combined, output_path)
        imported_files += 1
        imported_dates += len(dates_already_present(zip_df))
        logging.info(
            "Imported existing ZIP data: %s | rows=%s | dates=%s",
            output_path,
            combined.height,
            len(dates_already_present(combined)),
        )

    return {"files": imported_files, "dates": imported_dates}


def chunked(values: list[date], size: int):
    for start in range(0, len(values), size):
        yield values[start : start + size]


def pending_dates_for_station_year(
    *,
    station: dict[str, Any],
    year: int,
    start_date: date,
    end_date: date,
    raw_dir: Path,
    refresh_days: int,
    force: bool,
) -> tuple[Path, pl.DataFrame, list[date]]:
    year_start = max(start_date, date(year, 1, 1))
    year_end = min(end_date, date(year, 12, 31))
    output_path = station_year_path(raw_dir, station, year)

    existing = read_existing_station_year(output_path)
    present_dates = dates_already_present(existing)

    if refresh_days > 0:
        refresh_cutoff = end_date - timedelta(days=refresh_days - 1)
    else:
        refresh_cutoff = None

    dates_to_fetch: list[date] = []
    for day in iter_dates(year_start, year_end):
        should_refresh_recent = (
            refresh_cutoff is not None and day >= refresh_cutoff
        )
        if force or day.isoformat() not in present_dates or should_refresh_recent:
            dates_to_fetch.append(day)

    return output_path, existing, dates_to_fetch


def load_status_log(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    try:
        return pl.read_csv(path)
    except Exception:
        return pl.DataFrame()


def update_status_log(
    path: Path,
    results: list[DayFetchResult],
) -> None:
    records = []
    now_utc = datetime.now(timezone.utc).isoformat()

    for result in results:
        records.append(
            {
                "target_region": result.station["region"],
                "source_station_code": result.station["source_station_code"],
                "source_date_local": result.day.isoformat(),
                "status": result.status,
                "row_count": len(result.rows),
                "attempts": result.attempts,
                "error": result.error,
                "checked_at_utc": now_utc,
            }
        )

    if not records:
        return

    new_df = pl.DataFrame(records)
    existing = load_status_log(path)

    if existing.is_empty():
        combined = new_df
    else:
        combined = pl.concat([existing, new_df], how="diagonal_relaxed")

    combined = (
        combined.unique(
            subset=["target_region", "source_date_local"],
            keep="last",
        )
        .sort(["target_region", "source_date_local"])
    )

    atomic_write_csv_gz(combined, path)


def build_master_dataset(raw_dir: Path, output_path: Path) -> pl.DataFrame:
    files = sorted(raw_dir.glob("*/ispu_*.csv.gz"))
    if not files:
        raise FileNotFoundError(f"No ISPU files found under {raw_dir}")

    frames = []
    for path in files:
        try:
            frames.append(read_existing_station_year(path))
        except Exception as exc:
            logging.warning("Skip unreadable file %s: %s", path, exc)

    if not frames:
        raise RuntimeError("No readable ISPU files found.")

    master = normalize_output_df(pl.concat(frames, how="diagonal_relaxed"))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    master.write_parquet(tmp, compression="zstd")
    tmp.replace(output_path)

    return master


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Resume historical hourly ISPU fetching for AERIS. "
            "It can import already-downloaded files from ispu.zip and only "
            "request dates that are still missing."
        )
    )
    parser.add_argument(
        "--start-date",
        type=parse_iso_date,
        default=DEFAULT_START_DATE,
    )
    parser.add_argument(
        "--end-date",
        type=parse_iso_date,
        default=None,
        help="Default: today's date in Asia/Jakarta",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw/ispu"),
    )
    parser.add_argument(
        "--status-log",
        type=Path,
        default=Path("data/raw/ispu/status/ispu_fetch_status.csv.gz"),
    )
    parser.add_argument(
        "--master-output",
        type=Path,
        default=Path("data/processed/ispu_master.parquet"),
    )
    parser.add_argument(
        "--existing-zip",
        type=Path,
        default=None,
        help=(
            "Optional existing archive such as ispu.zip. Yearly CSVs in the zip "
            "are merged into --raw-dir before the missing-date plan is built."
        ),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
    )
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=DEFAULT_REFRESH_DAYS,
        help=(
            "Refetch the most recent N local dates even if already present. "
            "Default: 0, so existing ZIP/local data is never requested again."
        ),
    )
    parser.add_argument(
        "--checkpoint-size",
        type=int,
        default=25,
        help=(
            "Persist successful results after every N requested dates. "
            "Default: 25. This avoids losing a whole year after a late crash."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Refetch every requested date, including dates already present.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=10,
        help="Print progress every N completed dates. Default: 10.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Import/inspect existing data and print the fetch plan without HTTP requests.",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)

    if not 1 <= args.max_workers <= 3:
        raise ValueError("--max-workers must be between 1 and 3.")
    if args.progress_every < 1:
        raise ValueError("--progress-every must be >= 1.")
    if args.checkpoint_size < 1:
        raise ValueError("--checkpoint-size must be >= 1.")
    if args.refresh_days < 0:
        raise ValueError("--refresh-days must be >= 0.")

    today_jakarta = datetime.now(SOURCE_TIMEZONE).date()
    end_date = args.end_date or today_jakarta
    if args.start_date > end_date:
        raise ValueError("start-date cannot be after end-date.")

    if args.existing_zip is not None:
        summary = import_existing_zip(args.existing_zip, args.raw_dir)
        logging.info(
            "Existing ZIP import complete | files=%s | date-entries=%s",
            summary["files"],
            summary["dates"],
        )

    plan: list[tuple[dict[str, Any], int, Path, pl.DataFrame, list[date]]] = []
    total_expected_dates = 0
    total_existing_dates = 0
    total_pending_dates = 0

    logging.info("========== ISPU RESUME PLAN ==========")
    for station in TARGET_STATIONS:
        for year in range(args.start_date.year, end_date.year + 1):
            year_start = max(args.start_date, date(year, 1, 1))
            year_end = min(end_date, date(year, 12, 31))
            expected_count = sum(1 for _ in iter_dates(year_start, year_end))

            output_path, existing, dates_to_fetch = pending_dates_for_station_year(
                station=station,
                year=year,
                start_date=args.start_date,
                end_date=end_date,
                raw_dir=args.raw_dir,
                refresh_days=args.refresh_days,
                force=args.force,
            )

            present_count = len(
                {
                    d
                    for d in dates_already_present(existing)
                    if year_start.isoformat() <= d <= year_end.isoformat()
                }
            )
            pending_count = len(dates_to_fetch)

            total_expected_dates += expected_count
            total_existing_dates += present_count
            total_pending_dates += pending_count

            logging.info(
                "%-16s %s | expected=%3d existing=%3d pending=%3d",
                station["region"],
                year,
                expected_count,
                present_count,
                pending_count,
            )

            if dates_to_fetch:
                plan.append((station, year, output_path, existing, dates_to_fetch))

    pct_done = (
        total_existing_dates / total_expected_dates * 100.0
        if total_expected_dates
        else 100.0
    )
    logging.info(
        "TOTAL | expected=%s existing=%s pending=%s | %.1f%% already present",
        total_expected_dates,
        total_existing_dates,
        total_pending_dates,
        pct_done,
    )
    logging.info("======================================")

    if args.dry_run:
        logging.info("Dry run complete. No HTTP requests were sent.")
        return

    if not plan:
        logging.info("No missing ISPU dates. Nothing to fetch.")
        master = build_master_dataset(args.raw_dir, args.master_output)
        logging.info(
            "Master ISPU dataset: %s rows, %s cols -> %s",
            master.height,
            master.width,
            args.master_output,
        )
        return

    failures = 0

    for station, year, output_path, existing, dates_to_fetch in plan:
        logging.info(
            "Station %s | source=%s | year=%s | pending=%s",
            station["region"],
            station["source_station_name"],
            year,
            len(dates_to_fetch),
        )

        progress = ProgressTracker(
            label=f"ISPU {station['region']} {year}",
            total=len(dates_to_fetch),
            report_every=args.progress_every,
        )

        for batch_number, batch_dates in enumerate(
            chunked(dates_to_fetch, args.checkpoint_size),
            start=1,
        ):
            logging.info(
                "[%s] checkpoint batch %s | %s date(s) | %s..%s",
                progress.label,
                batch_number,
                len(batch_dates),
                batch_dates[0],
                batch_dates[-1],
            )

            batch_results: list[DayFetchResult] = []
            fetched_rows: list[dict[str, Any]] = []

            with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
                future_map = {
                    executor.submit(fetch_day, station, day): day
                    for day in batch_dates
                }

                for future in as_completed(future_map):
                    result = future.result()
                    batch_results.append(result)

                    progress.update(
                        status=result.status,
                        rows=len(result.rows),
                        detail=(
                            f"date={result.day.isoformat()} "
                            f"attempts={result.attempts}"
                        ),
                    )

                    if result.status == "error":
                        failures += 1
                        continue

                    fetched_rows.extend(result.rows)

            replace_dates = [
                result.day.isoformat()
                for result in batch_results
                if result.status != "error"
            ]

            if not existing.is_empty() and replace_dates:
                existing = existing.filter(
                    ~pl.col("source_date_local")
                    .cast(pl.Utf8)
                    .is_in(replace_dates)
                )

            new_df = (
                pl.from_dicts(
                    fetched_rows,
                    schema=OUTPUT_SCHEMA,
                    strict=False,
                    infer_schema_length=None,
                )
                if fetched_rows
                else pl.DataFrame(schema=OUTPUT_SCHEMA)
            )

            if existing.is_empty():
                combined = new_df
            elif new_df.is_empty():
                combined = existing
            else:
                combined = pl.concat(
                    [existing, new_df],
                    how="diagonal_relaxed",
                )

            if not combined.is_empty():
                combined = normalize_output_df(combined)
                atomic_write_csv_gz(combined, output_path)
                existing = combined
                logging.info(
                    "[%s] CHECKPOINT SAVED | rows=%s dates=%s -> %s",
                    progress.label,
                    combined.height,
                    len(dates_already_present(combined)),
                    output_path,
                )

            update_status_log(args.status_log, batch_results)

        logging.info(
            "[%s] station-year finished | stored dates=%s | rows=%s",
            progress.label,
            len(dates_already_present(existing)),
            existing.height,
        )

    master = build_master_dataset(args.raw_dir, args.master_output)
    logging.info(
        "Master ISPU dataset: %s rows, %s cols -> %s",
        master.height,
        master.width,
        args.master_output,
    )

    if failures:
        logging.warning(
            "Collection finished with %s failed date request(s). "
            "Those dates were not marked as present and will be retried on the next run.",
            failures,
        )
    else:
        logging.info("ISPU resume collection completed without failed date requests.")


if __name__ == "__main__":
    main()
