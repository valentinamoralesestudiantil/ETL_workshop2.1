import json
from pathlib import Path

from extract import extract_spotify_data, extract_grammy_data
from cleaning import clean_input
from transform import transform_input
from load import subir_csv_a_drive, load_to_dw
from dimensional_model import build_dimensional_model


def main():
    # ---------------------------------------------------------
    # PATHS
    # ---------------------------------------------------------
    SPOTIFY_CSV_PATH = Path("/Users/valemoravale/Documents/UNIVERSIDAD /Semestre 5/ETL/workshop2/raw/spotify_dataset.csv")

    MYSQL_URL = "mysql+pymysql://root:valemoravale@localhost:3306/grammy_db"
    GRAMMY_TABLE_NAME = "the_grammy_awards"

    CLEAN_OUTPUT_DIR = Path("/Users/valemoravale/Documents/UNIVERSIDAD /Semestre 5/ETL/workshop2/output/clean")
    TRANSFORM_OUTPUT_DIR = Path("/Users/valemoravale/Documents/UNIVERSIDAD /Semestre 5/ETL/workshop2/output/transform")
    LOGS_DIR = Path("/Users/valemoravale/Documents/UNIVERSIDAD /Semestre 5/ETL/workshop2/logs")

    # Credenciales Google Drive
    CREDENTIALS_PATH = Path("/Users/valemoravale/Documents/UNIVERSIDAD /Semestre 5/ETL/workshop2/raw/credentials.json")
    TOKEN_PATH = Path("/Users/valemoravale/Documents/UNIVERSIDAD /Semestre 5/ETL/workshop2/token.json")

    # Si no quieres subirlo dentro de una carpeta específica de Drive, déjalo en None
    GOOGLE_DRIVE_FOLDER_ID = None

    CLEAN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TRANSFORM_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------
    # 1. EXTRACTION
    # ---------------------------------------------------------
    df_spotify = extract_spotify_data(SPOTIFY_CSV_PATH)
    df_grammy = extract_grammy_data(MYSQL_URL, GRAMMY_TABLE_NAME)

    # ---------------------------------------------------------
    # 2. CLEANING
    # ---------------------------------------------------------
    df_spotify_clean, df_grammy_clean, cleaning_log = clean_input(df_spotify, df_grammy)

    print("\n=== CLEANING LOG ===")
    print(json.dumps(cleaning_log, indent=4, ensure_ascii=False))

    with open(LOGS_DIR / "cleaning_log.json", "w", encoding="utf-8") as f:
        json.dump(cleaning_log, f, indent=4, ensure_ascii=False)

    df_spotify_clean.to_csv(CLEAN_OUTPUT_DIR / "spotify_clean.csv", index=False)
    df_grammy_clean.to_csv(CLEAN_OUTPUT_DIR / "grammy_clean.csv", index=False)

    # ---------------------------------------------------------
    # 3. TRANSFORMATION + MERGE
    # ---------------------------------------------------------
    df_spotify_transformed, df_grammy_transformed, spotify_enriched, transformation_log = transform_input(
        df_spotify_clean,
        df_grammy_clean
    )

    print("\n=== TRANSFORMATION LOG ===")
    print(json.dumps(transformation_log, indent=4, ensure_ascii=False))

    with open(LOGS_DIR / "transformation_log.json", "w", encoding="utf-8") as f:
        json.dump(transformation_log, f, indent=4, ensure_ascii=False)

    df_spotify_transformed.to_csv(TRANSFORM_OUTPUT_DIR / "spotify_transformed.csv", index=False)
    df_grammy_transformed.to_csv(TRANSFORM_OUTPUT_DIR / "grammy_transformed.csv", index=False)

    spotify_enriched_path = TRANSFORM_OUTPUT_DIR / "spotify_enriched.csv"
    spotify_enriched.to_csv(spotify_enriched_path, index=False)

    print("\n=== SPOTIFY ENRICHED PREVIEW ===")
    print(spotify_enriched.head())
    print(spotify_enriched.shape)

    # ---------------------------------------------------------
    # 4. LOAD
    # ---------------------------------------------------------

    # Subir spotify_enriched.csv a Google Drive
    subir_csv_a_drive(
        ruta_archivo_local=TRANSFORM_OUTPUT_DIR / "spotify_enriched.csv",
        nombre_en_drive="spotify_enriched.csv",
        folder_id=None,
        credentials_path=Path("/Users/valemoravale/Documents/UNIVERSIDAD /Semestre 5/ETL/workshop2/raw/credentials.json"),
        token_path=Path("/Users/valemoravale/Documents/UNIVERSIDAD /Semestre 5/ETL/workshop2/token.json")
    )

    # Construir tablas dimensionales desde spotify_enriched
    dataframes_dw = build_dimensional_model(spotify_enriched)

    # Cargar tablas al Data Warehouse
    load_to_dw(dataframes_dw)

    print("\nProceso completo finalizado correctamente.")


if __name__ == "__main__":
    main()