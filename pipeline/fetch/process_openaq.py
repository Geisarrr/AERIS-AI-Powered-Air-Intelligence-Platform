"""
AERIS - Data Processing & Merging Pipeline
==========================================
Lokasi file: pipeline/process/merge_data.py
Fungsi: Menyamakan format waktu, menggabungkan data OpenAQ dan Open-Meteo, 
        serta menyatukan 5 wilayah menjadi 1 Master Dataset untuk XGBoost.

Output: 
1. data/processed/merged_{region}.csv (Data per wilayah)
2. data/processed/aeris_master_dataset.csv (Data gabungan seluruh Jakarta)
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

def process_and_merge_region(region: str) -> pl.DataFrame | None:
    file_openaq = DIR_OPENAQ / f"{region}.csv"
    file_meteo = DIR_METEO / f"{region}.csv"

    if not file_openaq.exists() or not file_meteo.exists():
        print(f"  [SKIP] Data mentah untuk {region} belum lengkap.")
        return None

    print(f"  Memproses dan menyelaraskan: {region} ...")

    # =========================================================================
    # 1. BACA & SAMAKAN FORMAT WAKTU (TRICK POLARS)
    # =========================================================================
    # OpenAQ format aslinya: "2024-01-01T00:00:00+00:00"
    # Kita slice 16 karakter pertama menjadi "2024-01-01T00:00" lalu ubah ke Datetime
    df_aq = pl.read_csv(file_openaq).with_columns(
        pl.col("datetime_utc").str.slice(0, 16).str.to_datetime(format="%Y-%m-%dT%H:%M")
    )
    
    # Open-Meteo format aslinya: "2024-01-01T00:00" (Sudah pas 16 karakter)
    df_weather = pl.read_csv(file_meteo).with_columns(
        pl.col("datetime_utc").str.to_datetime(format="%Y-%m-%dT%H:%M")
    )

    # =========================================================================
    # 2. RATA-RATA & PIVOT OPENAQ
    # =========================================================================
    df_aq_agg = (
        df_aq
        .group_by(["datetime_utc", "target_region", "parameter"])
        .agg(pl.col("value").mean().alias("avg_value"))
    )

    # Mengubah baris (pm25, pm10) menjadi kolom fitur untuk XGBoost
    df_aq_pivot = (
        df_aq_agg
        .pivot(
            values="avg_value",
            index=["datetime_utc", "target_region"],
            columns="parameter"
        )
        # Mengisi jam kosong dengan nilai terbaca dari jam sebelumnya
        .fill_null(strategy="forward")
    )

    # =========================================================================
    # 3. PENGGABUNGAN (JOIN) DATA UDARA & CUACA
    # =========================================================================
    # Inner join memastikan hanya baris waktu yang udaranya ada & cuacanya ada
    # yang akan dimasukkan ke dataset ML.
    df_merged = df_aq_pivot.join(
        df_weather,
        on=["datetime_utc", "target_region"],
        how="inner"
    )

    # Urutkan berdasarkan waktu
    df_merged = df_merged.sort("datetime_utc")
    
    # Simpan versi per wilayah (untuk cadangan atau analisis spesifik)
    output_path = DIR_OUTPUT / f"merged_{region}.csv"
    df_merged.write_csv(output_path)
    
    return df_merged


def main():
    print("=== Memulai Pipeline Penggabungan Data AERIS ===")
    DIR_OUTPUT.mkdir(parents=True, exist_ok=True)
    
    all_dfs = []
    
    # Lakukan merge per wilayah
    for region in REGIONS:
        df = process_and_merge_region(region)
        if df is not None:
            all_dfs.append(df)
            
    # =========================================================================
    # 4. GABUNGKAN SEMUA WILAYAH MENJADI 1 DATASET MASTER
    # =========================================================================
    if all_dfs:
        print("\nMenggabungkan seluruh wilayah menjadi 1 Master Dataset...")
        df_master = pl.concat(all_dfs)
        
        # Urutkan secara rapi berdasarkan Wilayah lalu Waktu
        df_master = df_master.sort(["target_region", "datetime_utc"])
        
        master_path = DIR_OUTPUT / "aeris_master_dataset.csv"
        df_master.write_csv(master_path)
        
        print(f"=== SELESAI! ===")
        print(f"File Master siap digunakan: {master_path}")
        print(f"Total Data: {df_master.shape[0]} baris x {df_master.shape[1]} kolom")
    else:
        print("Tidak ada data yang diproses. Pastikan file raw sudah diunduh.")

if __name__ == "__main__":
    main()