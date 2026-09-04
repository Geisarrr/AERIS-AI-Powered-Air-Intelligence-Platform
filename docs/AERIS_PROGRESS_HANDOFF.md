# AERIS Project Progress & Chat Handoff

> Snapshot: 4 September 2026  
> Repository: `/Users/geisarrampan/Project/AERIS-AI-Powered-Air-Intelligence-Platform`  
> Product: **AERIS — AI-Powered Air Intelligence Platform**  
> Tagline: **Know Your Air. Protect Your Day.**

## Instruksi untuk ChatGPT pada chat baru

Gunakan dokumen ini sebagai konteks awal dan source of truth sementara untuk melanjutkan pengembangan AERIS.

Sebelum mengubah kode:

1. Baca repository, PRD, dokumen ini, dan `git status` terbaru.
2. Jangan menganggap fitur selesai hanya karena disebut dalam README, PRD, atau nama file.
3. Pertahankan semua perubahan pengguna. Banyak file progres terbaru masih untracked.
4. Fokus pada next action di bagian akhir; jangan lompat ke ML atau dashboard besar.
5. Bedakan “kode sudah ditulis”, “output sudah dibuat”, dan “output sudah tervalidasi”.
6. Setelah pekerjaan selesai dan diverifikasi, perbarui status, risiko, keputusan, dan next action dalam dokumen ini.

## 1. Visi produk

AERIS adalah platform kualitas udara Jakarta yang mengubah data lingkungan menjadi actionable insights melalui alur:

```text
Monitor → Predict → Explain → Personalize → Act
```

Target awal adalah Jakarta Pusat, Selatan, Barat, Timur, dan Utara.

Kapabilitas akhir dalam PRD:

- Live spatio-temporal map dan AQI monitoring
- Forecast PM2.5/PM10 selama 24–48 jam
- Pollution anomaly/event detection
- Pollution source attribution dengan SHAP
- Smart alert dan health recommendations
- AI Air Quality Brief
- Geospatial intelligence
- Pipeline dan MLOps yang reproducible

## 2. Posisi proyek saat ini

**Kesimpulan:** proyek masih berada di **Phase 1 yang belum selesai**, dengan **data preparation Phase 2 yang semakin matang**.

Fondasi full-stack, historical ingestion, incremental ISPU collection, dan enriched weather collection sudah tersedia. Namun canonical dataset belum melewati quality gate dan belum ada alur produk end-to-end dari database ke API dan frontend.

Dataset yang ada masih **working data**, belum ground truth siap training.

## 3. Progres sejak snapshot 3 September 2026

### Incremental ISPU collector

`pipeline/fetch/ispu.py` sekarang memiliki:

- UTC dan Asia/Jakarta timestamps.
- Pemisahan target station dan source station identity.
- Kolom source, source timezone, quality flag, dan source URL.
- Retry/backoff dan maksimum tiga worker.
- Resume berdasarkan tanggal yang sudah tersedia.
- Refresh tiga hari terbaru secara default.
- Atomic writes, status log, dan master Parquet builder.

`pipeline/fetch/ispu_resume.py` menambahkan:

- Import archive ZIP yang berisi raw yearly CSV.
- Fetch plan untuk tanggal yang belum tersedia.
- Checkpoint setiap batch.
- Dry-run dan progress reporting.

Kedua script masih tumpang tindih dan perlu dikonsolidasikan menjadi satu entry point.

### Historical ISPU berhasil dikumpulkan

- Periode lokal: 1 Januari 2024–3 September 2026.
- Lima wilayah × 977 tanggal = 4.885 station-days.
- Status log: 3.755 `ok`, 1.130 `empty`, 0 `error`.
- Raw yearly CSV: 117.240 hourly rows valid.
- Setiap wilayah memiliki 23.448 hourly rows.
- Master: `data/processed/ispu_master.parquet`.

Collection berhasil, tetapi kelengkapan nilai masih rendah:

- 65.190 rows (55,6%) memiliki minimal satu nilai polutan.
- 52.050 rows (44,4%) kehilangan seluruh nilai polutan.
- 56.867 rows (48,5%) lengkap untuk enam polutan non-HC.
- HC kosong pada seluruh dataset.

### Weather enrichment berhasil dikumpulkan

`pipeline/fetch/weather_enrichment.py` menambahkan sembilan fitur:

- Boundary layer height
- Dew point
- Vapour pressure deficit
- Cloud cover
- Low cloud cover
- Shortwave radiation
- Wind gusts 10 m
- Wind speed 100 m
- Wind direction 100 m

Output `data/processed/weather_enriched_master.parquet`:

- 116.640 rows.
- 22 kolom.
- 23.328 rows per wilayah.
- Periode 1 Januari 2024–29 Agustus 2026 UTC.
- `boundary_layer_height` kosong pada 21.840 rows atau 18,72%, seluruhnya Januari–Juni 2024.

### Utility merge ISPU sudah ditulis

`pipeline/clean/merge_ispu_to_aeris_v2.py` menyediakan:

- Normalisasi region ID.
- Canonical UTC-hour join key.
- Duplicate-key guard.
- Left join yang mempertahankan jumlah row base.
- Availability count, quality label, dan missing indicator per polutan.
- CSV dan Parquet output.

Output `data/processed/merge/` belum tersedia, sehingga utility ini berstatus **implemented but not executed**. Enriched weather juga belum digabung oleh script tersebut.

Read-only audit terhadap prospective base + ISPU join menunjukkan:

- 62.103 dari 62.103 base rows memiliki pasangan region-hour ISPU.
- 35.129 rows memiliki minimal satu nilai ISPU.
- 32.321 rows lengkap untuk enam polutan non-HC.
- 26.974 rows memiliki key match tetapi semua nilai ISPU kosong.

## 4. Arsitektur aktual

```text
OpenAQ ───────────────┐
                      ├── legacy merge ── dataset_aeris_final.csv
Open-Meteo base ──────┘

ISPU Jakarta ── incremental fetch ── yearly CSV.gz ── ispu_master.parquet

Open-Meteo enriched ── yearly CSV.gz ── weather_enriched_master.parquet

dataset_aeris_final + ISPU ── merge utility ditulis, output belum dibuat

OpenStreetMap ── extractor fitur spasial, belum terintegrasi

Docker Compose
├── PostgreSQL + PostGIS   [provisioned, belum dipakai aplikasi]
├── Redis                  [provisioned, belum dipakai aplikasi]
├── FastAPI                [hanya GET / dan GET /health]
└── Next.js                [default scaffold]
```

## 5. Inventaris data lokal

| Dataset | Rows | Periode | Kondisi |
|---|---:|---|---|
| OpenAQ raw, 5 wilayah | 126.163 | 2023-12-31–2026-08-31 UTC | PM2.5/PM10; coverage wilayah tidak seimbang; 15.341 value kosong |
| Open-Meteo base, 5 wilayah | 116.640 | 2024-01-01–2026-08-29 UTC | Enam fitur cuaca |
| `dataset_aeris_final.csv` | 62.103 | 2024-01-01–2026-08-29 | Legacy merged dataset, 12 kolom |
| ISPU raw valid hourly grid | 117.240 | 2023-12-31 17:00–2026-09-03 16:00 UTC | 23.448 rows per wilayah |
| `ispu_master.parquet` | 117.245 | Rentang valid sama | Mengandung lima malformed rows |
| `weather_enriched_master.parquet` | 116.640 | 2024-01-01–2026-08-29 UTC | 22 kolom; boundary layer memiliki gap historis |
| Prospective base + ISPU join | 62.103 | 2024-01-01–2026-08-29 | Hanya hasil audit memory; belum disimpan |

Semua file dalam `data/raw` dan `data/processed` diabaikan Git. Belum ada DVC atau dataset versioning lain.

## 6. Bug penting yang baru ditemukan

`build_master_dataset()` pada `ispu.py` dan `ispu_resume.py` memakai:

```python
raw_dir.glob("*/ispu_*.csv.gz")
```

Pattern tersebut tidak hanya mengambil yearly region files, tetapi juga:

```text
data/raw/ispu/status/ispu_fetch_status.csv.gz
```

Saat status log dikonkatenasi secara diagonal dengan data ISPU, terbentuk lima rows tanpa `observed_at_utc`—satu untuk setiap wilayah. Akibatnya:

- Expected valid rows: 117.240.
- Current master rows: 117.245.
- Malformed rows: 5.

Master harus dibuat ulang setelah glob dibatasi dan non-null/uniqueness assertions ditambahkan.

## 7. Status capability terhadap PRD

| Area | Status | Kondisi aktual |
|---|---|---|
| Monorepo & service structure | Implemented | Struktur utama tersedia |
| Docker Compose | Implemented | Empat service tersedia |
| Frontend product | Partial | Stack tersedia, UI masih scaffold |
| Backend API | Partial | Hanya root dan health endpoint |
| PostgreSQL/PostGIS | Partial | Container ada, belum ada schema/model/migration/integration |
| Redis | Partial | Container ada, belum digunakan |
| Historical ingestion | Partial | OpenAQ, weather, dan ISPU tersedia; belum orchestrated |
| Canonical source/time metadata | Partial | Sudah mulai diterapkan pada ISPU/weather; belum terpusat lintas sumber |
| Data validation & cleaning | Partial | Quality flags dan guards mulai ada; belum menjadi quality gate terpadu |
| Dataset generation | Partial | Tiga master ada; canonical merged output belum ada |
| Geospatial features | Partial | Extractor tersedia, output/integrasi belum ada |
| Real-time monitoring & map | Not started | Tidak ada live ingestion/map/AQI service |
| Forecast/anomaly/SHAP | Not started | Tidak ada training/evaluation/inference |
| Alert/recommendation/AI brief | Not started | Belum ada implementation layer |
| Testing & CI/CD | Not started | Tidak ada pytest suite, Ruff config, atau GitHub Actions |
| Prefect/MLflow/DVC/monitoring | Not started | Belum ada implementasi |

## 8. Risiko dan technical debt

### P0 — blocker canonical dataset dan ML

1. **ISPU master terkontaminasi status log.**
   - Lima rows tidak memiliki timestamp atau observation metadata.
   - Fix glob, regenerate master, lalu assert semua join key non-null dan unik.

2. **ISPU sangat sparse dan HC sepenuhnya kosong.**
   - 44,4% hourly rows kehilangan semua polutan.
   - Tetapkan availability threshold, missing indicator, dan apakah HC dikeluarkan dari schema training.

3. **Canonical merged dataset belum terbentuk.**
   - ISPU dan enriched weather master tersedia secara terpisah.
   - Output merge belum dibuat.
   - Base dataset masih memakai imputasi lama yang belum tervalidasi.

4. **Semantik ISPU belum dipastikan.**
   - Jangan mengasumsikan nilainya setara concentration OpenAQ.
   - Dokumentasikan apakah nilai merupakan concentration, ISPU sub-index, AQI, label, atau contextual feature.

5. **Belum ada vertical slice produk.**
   - Database belum menyimpan observations/stations.
   - API produk dan dashboard belum tersedia.

### P1 — reliability dan reproducibility

1. `ispu.py` dan `ispu_resume.py` tumpang tindih.
2. `merge_ispu_to_aeris_v2.py` menggunakan absolute default paths.
3. Dependency pipeline belum lengkap; `polars`, `beautifulsoup4`, `pyarrow`, dan `pyproj` belum tercatat konsisten.
4. Belum ada automated tests untuk parser, resume, master build, dan merge.
5. OpenAQ memetakan sensor tanpa maximum-distance threshold.
6. Legacy merge melakukan fill sebelum chronological ordering dijamin.
7. `boundary_layer_height` kehilangan seluruh Januari–Juni 2024.
8. README belum mencerminkan progres terbaru.

## 9. Kondisi Git saat snapshot

Branch dan commit tracked masih:

```text
main @ 2e6b25e — initial file for calling osm data
```

Artefak berikut masih untracked:

- `docs/`
- `reports/`
- `ispu.zip`
- `pipeline/fetch/ispu.py`
- `pipeline/fetch/ispu_resume.py`
- `pipeline/fetch/weather_enrichment.py`
- `pipeline/clean/merge_ispu_to_aeris_v2.py`

Data raw/processed tidak muncul di Git karena `.gitignore`.

Jangan menghapus, overwrite, atau melakukan reset terhadap file tersebut tanpa instruksi eksplisit pengguna.

## 10. Urutan kerja yang direkomendasikan

### Milestone A — Repair dan freeze ISPU master

1. Batasi glob master ke folder region yang dikenal.
2. Exclude folder `status` secara eksplisit.
3. Tambahkan assertion:
   - `observed_at_utc` non-null.
   - `target_region` valid.
   - Satu row per `target_region + observed_at_utc`.
4. Regenerate `ispu_master.parquet`.
5. Verifikasi expected 117.240 rows untuk cutoff 3 September 2026 dan 23.448 rows per wilayah.
6. Tambahkan unit tests untuk master builder agar regresi tidak kembali.

### Milestone B — Canonical data contract dan quality gate

- Finalisasi station registry, source/target identity, timezone, unit, dan pollutant semantics.
- Putuskan perlakuan HC dan missing ISPU hours.
- Definisikan quality rules per source, region, pollutant, dan time range.
- Buat quality report yang gagal eksplisit pada malformed/duplicate key.
- Dokumentasikan lineage dan cutoff setiap dataset build.

### Milestone C — Canonical merged dataset

- Gabungkan base air-quality observations, ISPU context, dan enriched weather.
- Jangan overwrite legacy dataset sampai output baru tervalidasi.
- Gunakan missing indicators dan hindari blind forward/backward fill.
- Pastikan join mempertahankan grain region-hour.
- Simpan CSV/Parquet beserta quality report.

### Milestone D — Reproducibility

- Pilih satu ISPU entry point dan arsipkan/hapus duplikasi secara aman.
- Ubah semua default path menjadi relative/configurable.
- Satukan dan lock dependency.
- Tambahkan tests serta Ruff.
- Tambahkan dataset versioning setelah canonical output stabil.

### Milestone E — First end-to-end product slice

- Buat schema dan Alembic migrations untuk regions, stations, air-quality observations, dan weather observations.
- Load canonical historical dataset ke PostgreSQL/PostGIS.
- Implementasikan minimal:
  - `GET /api/stations`
  - `GET /api/air-quality/latest`
  - `GET /api/historical`
- Bangun dashboard minimal dengan region selector, latest reading, dan historical chart.

### Milestone F — Baseline ML dan intelligence

- Time-based train/validation/test split.
- Persistence dan seasonal baseline.
- XGBoost PM2.5/PM10 dengan MAE/RMSE per wilayah dan horizon.
- Setelah stabil: Isolation Forest, SHAP, alerts, recommendations, AI brief, dan MLOps.

## 11. Definition of done milestone terdekat

Milestone terdekat selesai ketika:

- `ispu_master.parquet` tidak mengandung malformed rows.
- Jumlah row dan coverage sesuai cutoff yang dinyatakan.
- Semua join key non-null dan unik pada grain region-hour.
- Canonical source/time/station/pollutant contract terdokumentasi.
- Missingness report tersedia per wilayah dan polutan.
- HC dan 44,4% missing-all ISPU hours memiliki keputusan treatment eksplisit.
- Unit tests master build dan join guard lolos.
- Dokumen handoff dan README diperbarui.

## 12. Keputusan yang belum ditetapkan

Jangan mengasumsikan keputusan berikut tanpa konfirmasi atau dokumentasi:

- Ground truth utama: OpenAQ, ISPU, atau kombinasi.
- Semantik/unit nilai ISPU.
- Apakah ISPU menjadi target, feature, label, atau validation source.
- Treatment untuk HC yang sepenuhnya kosong.
- Minimum availability untuk memasukkan sebuah hour ke training.
- Treatment `boundary_layer_height` sebelum Juli 2024.
- Strategi sensor OpenAQ dan maximum-distance threshold.
- Granularitas produk: station, region, atau grid/heatmap.
- Target acceptance metric forecast per horizon dan wilayah.

## 13. Next action

**Next action:** perbaiki glob `build_master_dataset()` pada `ispu.py` dan `ispu_resume.py`, tambahkan non-null/uniqueness quality assertions, regenerate `ispu_master.parquet`, lalu buat laporan missingness resmi. Jangan menjalankan merge production atau memulai ML sebelum gate ini lolos.

Prompt untuk chat baru:

> Baca `docs/AERIS_PROGRESS_HANDOFF.md`, PRD, dan repository terbaru. Jalankan `git status` dan verifikasi bukti progres. Fokus pertama: repair ISPU master sesuai Milestone A, tambahkan tests, dan tunjukkan quality report sebelum melanjutkan canonical merge. Jangan memulai ML atau dashboard besar. Setelah implementasi dan verifikasi, perbarui dokumen handoff ini.
