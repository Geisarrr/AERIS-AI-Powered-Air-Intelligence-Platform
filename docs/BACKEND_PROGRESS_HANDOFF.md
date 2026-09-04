# AERIS Backend Progress Handoff

> Snapshot diverifikasi: 4 September 2026, sekitar 21:03 WIB  
> Repository: `/Users/geisarrampan/Project/AERIS-AI-Powered-Air-Intelligence-Platform`  
> Branch/commit tracked: `main` @ `2e6b25e` (`initial file for calling osm data`)

## Instruksi untuk chat baru

Dokumen ini adalah ringkasan kondisi backend dan data pipeline aktual. Sebelum mengubah apa pun, baca file ini, jalankan `git status --short`, lalu cocokkan state pipeline terbaru karena pengambilan historical weather forecast bersifat resumable dan dapat berubah setelah snapshot ini.

Pertahankan semua file untracked. Jangan memakai `--overwrite`, menghapus chunk/state, atau melakukan reset Git tanpa instruksi eksplisit pengguna.

## Ringkasan eksekutif

Backend aplikasi masih berupa fondasi FastAPI: hanya `GET /` dan `GET /health`. PostgreSQL/PostGIS dan Redis sudah diprovision melalui Docker Compose, tetapi belum ada schema, migration, repository/service layer, autentikasi, cache/queue, maupun endpoint produk.

Kemajuan terbesar berada di data backend:

- Historical OpenAQ, weather observation, enriched weather, dan ISPU sudah dikumpulkan.
- Dataset gabungan observation + enriched weather + ISPU sudah dibuat: 62.103 baris dan 42 kolom.
- Pipeline historical ECMWF weather forecast yang leakage-aware sudah ditulis dan sedang dikumpulkan secara resumable.
- Saat snapshot, archive forecast sudah memiliki 2.018 chunk/run atau 736.570 baris, lalu berhenti aman karena batas kuota harian Open-Meteo.

Status keseluruhan: **data engineering aktif dan cukup maju, tetapi backend produk belum memiliki vertical slice end-to-end**. Dataset gabungan belum layak disebut canonical/training-ready karena bug ISPU master, missingness tinggi, dan quality gate/test otomatis belum tersedia.

## 1. Backend API aktual

Entry point: `backend/app/main.py`

Endpoint yang benar-benar tersedia:

| Method | Path | Hasil |
|---|---|---|
| GET | `/` | `{"message": "AERIS API is running", "version": "0.1.0"}` |
| GET | `/health` | `{"status": "healthy"}` |

Dokumentasi bawaan FastAPI tersedia pada `/docs` dan `/redoc` ketika service hidup.

Yang belum ada:

- Konfigurasi aplikasi berbasis `pydantic-settings`.
- Koneksi database dari kode aplikasi.
- SQLAlchemy models dan session management.
- Alembic configuration/migrations.
- API router terpisah, schema request/response, service, dan repository layer.
- Endpoint station, latest air quality, historical series, forecast, anomaly, atau alert.
- Pemakaian Redis.
- Authentication/authorization.
- Automated tests dan CI.

Verifikasi snapshot:

- Import aplikasi berhasil.
- Route FastAPI yang terdaftar hanya route dokumentasi, `/`, dan `/health`.
- Fungsi root dan health mengembalikan payload yang sesuai README.
- `docker compose config --quiet` berhasil, sehingga konfigurasi Compose valid secara sintaksis.

## 2. Infrastruktur

`docker-compose.yml` mendefinisikan empat service:

```text
Next.js :3000
    -> FastAPI :8000
        -> PostgreSQL/PostGIS :5432
        -> Redis :6379
```

PostgreSQL/PostGIS dan Redis mempunyai health check. Backend menunggu keduanya sehat, menerima `DATABASE_URL` serta `REDIS_URL`, dan menjalankan Uvicorn dengan reload. Namun kedua dependency tersebut belum dipakai oleh kode aplikasi.

Docker Compose baru divalidasi konfigurasinya; snapshot ini tidak membuktikan semua container sedang berjalan atau integration test antarlayanan lolos.

## 3. Pipeline dan dataset aktual

### 3.1 Dataset observation utama

| Artefak | Ukuran logis | Periode | Status |
|---|---:|---|---|
| `data/processed/dataset_aeris_final.csv` | 62.103 × 12 | 2024-01-01 00:00–2026-08-29 00:00 UTC | Legacy base, key region-hour unik |
| `data/processed/weather_enriched_master.parquet` | 116.640 × 22 | 2024-01-01 00:00–2026-08-29 23:00 UTC | 5 wilayah × 23.328 jam; key unik |
| `data/processed/ispu_master.parquet` | 117.245 × 23 | 2023-12-31 17:00–2026-09-03 16:00 UTC | Masih mengandung 5 malformed rows |
| `data/processed/dataset_aeris_final_merged_all.csv` | 62.103 × 42 | 2024-01-01 00:00–2026-08-29 00:00 UTC | Merge sudah dieksekusi, key unik, masih ada caveat kualitas |

Hasil validasi dataset gabungan:

- Jumlah baris tetap 62.103 setelah merge.
- Tidak ada duplicate key pada `target_region + target_name + datetime_utc`.
- Seluruh 62.103 key base memperoleh pasangan enriched weather.
- Enam fitur weather dasar sama persis antara base dan weather master pada seluruh baris yang dibandingkan.
- Sembilan fitur enriched weather masuk ke output.
- `boundary_layer_height` hanya terisi pada 41.641 dari 62.103 baris gabungan; 20.462 baris kosong.
- 35.129 baris mempunyai minimal satu nilai polutan ISPU.
- 26.974 baris mempunyai key ISPU tetapi semua nilai polutannya kosong.
- `hc_ispu` kosong pada seluruh output.

Utility merge berada di `pipeline/clean/merge_aeris_all_by_station.py`. Script memiliki required-column checks, normalisasi key, unique-key guards, one-to-one merge validation, dan row-count guard. Kekurangannya:

- Path proyek masih hard-coded ke mesin lokal.
- Seluruh pekerjaan dijalankan di top level, bukan fungsi/CLI yang mudah diuji.
- Output hanya CSV dan belum memiliki quality-report sidecar.
- Nama `final_merged_all` terlalu kuat karena input masih memiliki masalah kualitas.
- Historical forecast archive belum dan memang tidak boleh di-join secara naive ke tabel ini.

### 3.2 Bug ISPU master yang belum diperbaiki

`build_master_dataset()` pada `pipeline/fetch/ispu.py` dan `pipeline/fetch/ispu_resume.py` memakai:

```python
raw_dir.glob("*/ispu_*.csv.gz")
```

Pattern ini ikut membaca `data/raw/ispu/status/ispu_fetch_status.csv.gz`. Efek aktual:

- Master berisi 117.245 baris, sedangkan grid valid seharusnya 117.240.
- Ada 5 baris tanpa `observed_at_utc`, satu per target region.
- Lima baris tersebut juga tidak memiliki quality flag dan metadata observasi yang valid.
- Dari 117.240 baris valid, 65.190 mempunyai minimal satu nilai polutan dan 52.050 kehilangan seluruh nilai polutan.
- HC kosong pada seluruh dataset.

Perbaikan wajib: batasi file master hanya ke folder lima region, exclude `status`, lalu assert timestamp/region non-null dan key region-hour unik sebelum menulis master.

### 3.3 Historical weather forecast archive — sedang berjalan

Script aktif secara desain: `pipeline/fetch/fetch_aeris_historical_weather_forecasts.py`.

Desain V3 yang sudah ada:

- Source Open-Meteo Single Runs API, model default `ecmwf_ifs`.
- Kandidat run 00/06/12/18 UTC mulai 14 Maret 2024.
- Menyimpan full trajectory lead 0–72 jam untuk lima target station.
- Memisahkan run initialization, assumed availability, valid time, dan model lead untuk mencegah look-ahead leakage pada tahap as-of join berikutnya.
- Satu Parquet chunk per model run.
- Atomic chunk write, state JSON, resume, recovery state dari chunk, retry 5xx, unavailable-run tracking, dan circuit breaker untuk HTTP 429.

Progress yang diverifikasi:

| Metrik | Nilai |
|---|---:|
| Total kandidat run sampai cutoff weather master | 3.596 |
| Completed/tersedia | 2.018 |
| Unavailable dan sudah dicatat | 301 |
| Pending | 1.277 |
| Chunk Parquet | 2.018 |
| Total row di chunk | 736.570 |
| Row per chunk | 365 = 5 station × 73 lead hours |
| Run tersedia pertama | 2024-03-14 00:00 UTC |
| Run tersedia terakhir | 2025-10-14 12:00 UTC |
| Retryable failure | 0 |

Distribusi completed run berdasarkan cycle: 00 UTC = 576, 06 UTC = 431, 12 UTC = 579, dan 18 UTC = 432.

Pipeline berhenti pada kandidat `2025-10-14T18:00Z` karena HTTP 429 dengan pesan **daily API request limit exceeded** pada 4 September 2026 pukul 21:02 WIB. State sudah tersimpan; ini bukan kehilangan progress.

Resume aman setelah kuota API pulih:

```bash
backend/.venv/bin/python \
  pipeline/fetch/fetch_aeris_historical_weather_forecasts.py \
  --max-pending-runs 400 \
  --no-finalize
```

Jalankan dari root repository. Script akan membaca state/chunk yang ada dan hanya melanjutkan pending run. Jangan memakai `--overwrite`.

Setelah pending mencapai nol, finalisasi dengan menjalankan script tanpa `--no-finalize`. Target final V3 adalah:

```text
data/processed/weather_forecast_historical_raw.parquet
```

File target V3 tersebut **belum ada** saat snapshot.

Ada artefak lama `data/processed/weather_forecast_historical.parquet` dan `data/interim/weather_forecast_historical.partial.csv`, masing-masing hanya 100 baris dari 4 run uji (15–16 Maret 2024). Jangan menganggap keduanya sebagai archive final dan jangan memakai state lama `weather_forecast_historical.state.json` untuk pipeline V3.

Caveat tambahan:

- `row_counts` di state V3 memiliki 2.006 entry, sedangkan completed run/chunk berjumlah 2.018. Resume tetap aman karena completed state direkonsiliasi dari filename chunk, tetapi metadata count perlu direbuild/divalidasi saat final QA.
- Availability delay 6 jam masih merupakan asumsi konservatif, bukan metadata publikasi aktual yang sudah divalidasi.
- As-of join dari archive raw ke origin/horizon AERIS belum diimplementasikan.

## 4. Kondisi dependency dan reproducibility

`backend/requirements.txt` mem-pin dependency API, tetapi environment lokal sudah drift; contoh yang terpasang adalah FastAPI 0.141.1 dan Uvicorn 0.52.4, sementara requirements meminta FastAPI 0.115.6 dan Uvicorn 0.32.1.

Dependency pipeline juga belum lengkap:

- `pipeline/fetch/requirements.txt` hanya mencatat `requests` dan `python-dotenv`.
- Script aktual membutuhkan antara lain pandas, Polars, PyArrow, BeautifulSoup, dan library geospatial.
- Requirements clean terpisah dengan nama `requirements (1).txt` dan belum dipin.

Enam file Python utama berhasil melewati `py_compile` pada snapshot: backend main, merge all, historical forecast, dua collector ISPU, dan weather enrichment. Ini hanya syntax/import compilation check, bukan behavioural test.

Belum ditemukan pytest suite, Ruff configuration, GitHub Actions, DVC, Prefect, MLflow, atau monitoring.

## 5. Kondisi Git dan risiko kehilangan progress

Commit tracked terakhir masih hanya fondasi awal. Seluruh progres utama berikut masih untracked:

- `docs/`
- `reports/`
- `pipeline/clean/merge_aeris_all_by_station.py`
- `pipeline/fetch/fetch_aeris_historical_weather_forecasts.py`
- `pipeline/fetch/ispu.py`
- `pipeline/fetch/ispu_resume.py`
- `pipeline/fetch/weather_enrichment.py`
- `data/` yang mencakup state dan ribuan forecast chunks
- `ispu.zip`

`.gitignore` mengabaikan `data/raw/*` dan `data/processed/*`, tetapi belum mengabaikan `data/interim/*`. Jangan menjalankan `git add .` sebelum menetapkan strategi artefak data, karena ada ribuan chunk forecast di bawah `data/interim`.

## 6. Prioritas pekerjaan berikutnya

### P0 — lanjutkan dan tutup archive forecast

1. Setelah kuota harian pulih, resume V3 secara batch dengan `--max-pending-runs` dan `--no-finalize`.
2. Ulangi sampai pending nol; jangan mengulang unavailable run kecuali ada alasan sumber data berubah.
3. Finalisasi seluruh chunk ke `weather_forecast_historical_raw.parquet`.
4. QA schema, jumlah run/row, uniqueness, null rate per variable, cycle coverage, lead 0–72, dan konsistensi state/chunk.
5. Implementasikan as-of join: untuk origin `t`, pilih forecast terbaru dengan `forecast_available_utc <= t`, lalu valid time `t + horizon`.

### P0 — repair ISPU dan freeze canonical observation table

1. Perbaiki glob master pada kedua collector atau konsolidasikan ke satu entry point.
2. Regenerate `ispu_master.parquet` menjadi 117.240 valid rows untuk cutoff saat ini.
3. Tambahkan assertions dan automated tests untuk malformed/null/duplicate keys.
4. Tetapkan semantik/unit ISPU, treatment HC, dan threshold availability.
5. Jalankan ulang merge ke nama/version baru, simpan Parquet + quality report, dan jangan overwrite legacy input sebelum validasi lolos.

### P1 — bangun vertical slice backend produk

1. Tambahkan settings, DB session, SQLAlchemy models, dan Alembic migrations untuk region/station/air-quality/weather.
2. Load canonical historical observation data ke PostgreSQL/PostGIS.
3. Implementasikan minimal:
   - `GET /api/stations`
   - `GET /api/air-quality/latest`
   - `GET /api/air-quality/historical`
4. Tambahkan response schemas, pagination/time-range validation, integration tests, dan health check database/Redis.
5. Hubungkan frontend minimal setelah endpoint stabil.

### P1 — rapikan engineering foundation

- Ubah absolute paths menjadi path relatif/CLI/config.
- Satukan dan pin dependency backend + pipeline.
- Tambahkan pytest, Ruff, dan CI.
- Tetapkan versioning untuk dataset besar; jangan commit ribuan chunk langsung tanpa keputusan eksplisit.
- Konsolidasikan `ispu.py` dan `ispu_resume.py`.

## 7. Batasan untuk chat berikutnya

- Jangan memulai training ML dari `dataset_aeris_final_merged_all.csv` sebelum bug ISPU dan quality gate diselesaikan.
- Jangan menggabungkan historical forecast berdasarkan valid time saja; wajib gunakan availability-aware as-of join.
- Jangan menganggap container database/Redis berarti integrasi aplikasi sudah selesai.
- Jangan menganggap output forecast 100 baris sebagai archive final.
- Jangan menghapus atau mereset file untracked.
- Jangan memakai `--overwrite` pada collector forecast yang sedang resumable.

## Prompt siap ditempel ke chat baru

> Saya melanjutkan backend proyek AERIS. Baca `docs/BACKEND_PROGRESS_HANDOFF.md`, lalu verifikasi `git status --short` dan state terbaru `data/interim/weather_forecast_historical_raw.state.json`. Pertahankan semua file untracked dan jangan gunakan `--overwrite`. Fokus operasional pertama adalah melanjutkan historical ECMWF forecast archive secara resumable setelah kuota Open-Meteo pulih, kemudian finalisasi dan QA archive. Secara paralel urutan kualitas terpenting adalah memperbaiki glob ISPU master yang memasukkan 5 malformed rows, menambah assertions/tests, lalu membangun ulang canonical merged dataset. Jangan memulai ML atau melakukan naive forecast join. Setelah data gate lolos, lanjutkan first backend vertical slice PostgreSQL/PostGIS + endpoint stations/latest/historical. Laporkan bukti sebelum dan sesudah setiap perubahan serta perbarui dokumen handoff.
