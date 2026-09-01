"""
AERIS - Final Data Processing & Merging Pipeline
================================================
Lokasi file: pipeline/process/merge_data.py
Fungsi: Membersihkan, mem-pivot OpenAQ, dan menggabungkannya dengan Open-Meteo.
Output: data/processed/dataset_aeris_final.csv
"""

import polars as pl
from pathlib import Path

REGIONS = [
    "jakarta_pusat",
    "jakarta_selatan",
    "jakarta_barat",
    "jakarta_timur",
    "jakarta_utara"
]

DIR_OPENAQ = Path("data/raw/openaq")
DIR_METEO = Path("data/raw/openmeteo")
DIR_OUTPUT = Path("data/processed")

def build_final_dataset():
    all_dfs = []

    print("=== Memulai Pipeline Final AERIS (Udara + Cuaca) ===")

    for region in REGIONS:
        file_openaq = DIR_OPENAQ / f"{region}.csv"
        file_meteo = DIR_METEO / f"{region}.csv"

        if not file_openaq.exists() or not file_meteo.exists():
            print(f"  [SKIP] Data {region} tidak lengkap (Cuaca atau Udara hilang).")
            continue

        print(f"Memproses & Menggabungkan: {region} ...")

        # ---------------------------------------------------------
        # 1. PROSES OPENAQ (Polusi)
        # ---------------------------------------------------------
        # Potong huruf 'Z' dan ubah ke tipe datetime resmi
        df_aq = pl.read_csv(file_openaq).with_columns(
            pl.col("datetime_utc")
            .str.slice(0, 19)
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S", strict=False)
            .dt.truncate("1h")           
        ).drop_nulls(subset=["datetime_utc"])

        # Agregasi & Pivot (Jadikan pm25 & pm10 sebagai kolom)
        df_aq_pivot = (
            df_aq
            .group_by(["datetime_utc", "target_region", "parameter"])
            .agg(pl.col("value").mean().alias("avg_value"))
            .pivot(
                values="avg_value",
                index=["datetime_utc", "target_region"],
                on="parameter"
            )
            .fill_null(strategy="forward")
            .fill_null(strategy="backward") 
        )

        # ---------------------------------------------------------
        # 2. PROSES OPEN-METEO (Cuaca)
        # ---------------------------------------------------------
        # Format waktu Meteo: "2024-01-01T00:00" (Tanpa detik)
        df_weather = pl.read_csv(file_meteo).with_columns(
            pl.col("datetime_utc")
            .str.to_datetime(format="%Y-%m-%dT%H:%M", strict=False)
            .dt.truncate("1h")
        ).drop_nulls(subset=["datetime_utc"])

        # ---------------------------------------------------------
        # 3. JOIN (Gabung Berdasarkan Waktu dan Wilayah)
        # ---------------------------------------------------------
        # Inner join: Hanya simpan baris yang punya data polusi DAN cuaca di jam tersebut
        df_joined = df_aq_pivot.join(
            df_weather,
            on=["datetime_utc", "target_region"],
            how="inner"
        )

        all_dfs.append(df_joined)

    # ---------------------------------------------------------
    # 4. SATUKAN SEMUA WILAYAH
    # ---------------------------------------------------------
    if all_dfs:
        print("\nMenyatukan 5 wilayah menjadi 1 Dataset ML utuh...")
        
        # Concat secara diagonal agar kolom pm10 yang bolong di wilayah tertentu tidak error
        final_df = pl.concat(all_dfs, how="diagonal")

        # Urutkan secara kronologis untuk kebutuhan Time-Series Model
        final_df = final_df.sort(["datetime_utc", "target_region"])
        
        # Simpan ke folder processed
        DIR_OUTPUT.mkdir(parents=True, exist_ok=True)
        output_path = DIR_OUTPUT / "dataset_aeris_final.csv"
        final_df.write_csv(output_path)
        
        print(f"✅ Selesai! Dataset berhasil dibentuk.")
        print(f"📂 Disimpan di : {output_path}")
        print(f"📊 Dimensi Data: {final_df.shape[0]} Baris x {final_df.shape[1]} Kolom\n")
        
        print("Kolom yang tersedia:")
        print(", ".join(final_df.columns))
    else:
        print("❌ Gagal membentuk dataset final.")

if __name__ == "__main__":
    build_final_dataset()