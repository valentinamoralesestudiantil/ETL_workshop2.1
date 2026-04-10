from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import json
from sqlalchemy import create_engine, text
import unicodedata
import re
from dimensional_model import build_dimensional_model
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ---------------------------------------
# DATA SOURCES
# ---------------------------------------
CSV_SPOTIFY_PATH = "/opt/airflow/data/spotify_dataset.csv"
MYSQL_URL = "mysql+pymysql://root:valemoravale@host.docker.internal:3306/grammy_db"

# ---------------------------------------
# WORKDIRS
# ---------------------------------------
RAW_DIR = Path("/opt/airflow/data")
INTERMEDIATE_DIR = Path("/opt/airflow/data/intermediate")
CLEAN_DIR = Path("/opt/airflow/data/output/clean")
TRANSFORM_DIR = Path("/opt/airflow/data/output/transform")
DM_DIR = Path("/opt/airflow/data/output/dimensional")
LOG_DIR = Path("/opt/airflow/data/output/logs")

for folder in [INTERMEDIATE_DIR, CLEAN_DIR, TRANSFORM_DIR, DM_DIR, LOG_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


@dag(
    dag_id="pipeline_music_etl_mysql",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "spotify", "grammy", "mysql"],
    max_active_runs=1
)
def pipeline_music_etl_mysql():

    # =========================================================
    # EXTRACT
    # =========================================================
    @task
    def extract_spotify_data():
        df_spotify = pd.read_csv(CSV_SPOTIFY_PATH)

        output_path = INTERMEDIATE_DIR / "spotify_raw.csv"
        df_spotify.to_csv(output_path, index=False)

        print("Spotify dataset extracted successfully")
        print(df_spotify.shape)

        return str(output_path)

    @task
    def extract_grammy_data():
        engine = create_engine(MYSQL_URL)

        query = "SELECT * FROM the_grammy_awards"
        df_grammy = pd.read_sql(query, con=engine)

        output_path = INTERMEDIATE_DIR / "grammy_raw.csv"
        df_grammy.to_csv(output_path, index=False)

        print("Grammy dataset extracted successfully from MySQL")
        print(df_grammy.shape)

        return str(output_path)

    # =========================================================
    # VALIDATE INPUT
    # =========================================================
    @task
    def validate_input(spotify_path: str, grammy_path: str):
        df_spotify = pd.read_csv(spotify_path)
        df_grammy = pd.read_csv(grammy_path)

        errors = []

        required_spotify_cols = {
            "track_id", "artists", "album_name", "track_name", "popularity",
            "duration_ms", "explicit", "danceability", "energy", "key",
            "loudness", "mode", "speechiness", "acousticness",
            "instrumentalness", "liveness", "valence", "tempo",
            "time_signature", "track_genre"
        }

        required_grammy_cols = {
            "year", "title", "published_at", "updated_at", "category",
            "nominee", "artist", "workers", "img"
        }

        missing_spotify_cols = sorted(required_spotify_cols - set(df_spotify.columns))
        missing_grammy_cols = sorted(required_grammy_cols - set(df_grammy.columns))

        if missing_spotify_cols:
            errors.append(f"spotify: missing required columns {missing_spotify_cols}")

        if missing_grammy_cols:
            errors.append(f"grammy: missing required columns {missing_grammy_cols}")

        if df_spotify.empty:
            errors.append("spotify: dataset is empty")

        if df_grammy.empty:
            errors.append("grammy: dataset is empty")

        if errors:
            raise ValueError("Input validation failed:\n- " + "\n- ".join(errors))

        return {
            "spotify_path": spotify_path,
            "grammy_path": grammy_path
        }

    # =========================================================
    # CLEAN
    # =========================================================
    @task
    def clean_input(data_paths):
        df_spotify = pd.read_csv(data_paths["spotify_path"])
        df_grammy = pd.read_csv(data_paths["grammy_path"])

        cleaning_log = {
            "spotify": {},
            "grammy": {}
        }

        def normalize_text(text):
            if pd.isna(text):
                return ""
            text = str(text).strip().lower()
            text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
            text = re.sub(r"\s+", " ", text).strip()
            return text

        # ---------- CLEAN SPOTIFY ----------
        spotify_cols_to_drop = [col for col in ["Unnamed: 0"] if col in df_spotify.columns]
        if spotify_cols_to_drop:
            df_spotify = df_spotify.drop(columns=spotify_cols_to_drop)
        cleaning_log["spotify"]["dropped_columns"] = spotify_cols_to_drop

        for col in ["artists", "album_name", "track_name"]:
            if col in df_spotify.columns:
                before = len(df_spotify)
                df_spotify = df_spotify.dropna(subset=[col])
                df_spotify[col] = df_spotify[col].astype(str).str.strip()
                df_spotify = df_spotify[df_spotify[col] != ""]
                after = len(df_spotify)
                cleaning_log["spotify"][f"rows_removed_{col}"] = before - after

        if "loudness" in df_spotify.columns:
            before = len(df_spotify)
            df_spotify["loudness"] = pd.to_numeric(df_spotify["loudness"], errors="coerce")
            df_spotify = df_spotify[df_spotify["loudness"].notna()]
            df_spotify = df_spotify[df_spotify["loudness"] < -1]
            cleaning_log["spotify"]["rows_removed_invalid_loudness"] = before - len(df_spotify)

        if "time_signature" in df_spotify.columns:
            before = len(df_spotify)
            df_spotify["time_signature"] = pd.to_numeric(df_spotify["time_signature"], errors="coerce")
            df_spotify = df_spotify[df_spotify["time_signature"].notna()]
            df_spotify = df_spotify[df_spotify["time_signature"] > 1]
            cleaning_log["spotify"]["rows_removed_invalid_time_signature"] = before - len(df_spotify)

        if "track_id" in df_spotify.columns:
            import random
            import string

            def generate_synthetic_track_id(existing_ids, length=22):
                alphabet = string.ascii_letters + string.digits
                while True:
                    new_id = "".join(random.choices(alphabet, k=length))
                    if new_id not in existing_ids:
                        return new_id

            df_spotify["track_id"] = df_spotify["track_id"].astype(str).str.strip()
            df_spotify.loc[df_spotify["track_id"] == "", "track_id"] = pd.NA

            existing_ids = set(df_spotify["track_id"].dropna().tolist())
            duplicated_mask = df_spotify["track_id"].duplicated(keep="first")

            duplicate_count = 0
            missing_count = 0

            for idx in df_spotify.index:
                current_id = df_spotify.at[idx, "track_id"]

                if pd.isna(current_id):
                    new_id = generate_synthetic_track_id(existing_ids)
                    df_spotify.at[idx, "track_id"] = new_id
                    existing_ids.add(new_id)
                    missing_count += 1
                elif duplicated_mask.loc[idx]:
                    new_id = generate_synthetic_track_id(existing_ids)
                    df_spotify.at[idx, "track_id"] = new_id
                    existing_ids.add(new_id)
                    duplicate_count += 1

            cleaning_log["spotify"]["duplicated_track_id_reassigned"] = duplicate_count
            cleaning_log["spotify"]["missing_track_id_reassigned"] = missing_count

        # ---------- CLEAN GRAMMY ----------
        grammy_cols_to_drop = [col for col in ["winner"] if col in df_grammy.columns]
        if grammy_cols_to_drop:
            df_grammy = df_grammy.drop(columns=grammy_cols_to_drop)
        cleaning_log["grammy"]["dropped_columns"] = grammy_cols_to_drop

        for col in ["nominee", "artist", "workers", "img"]:
            if col in df_grammy.columns:
                before = len(df_grammy)
                df_grammy = df_grammy.dropna(subset=[col])
                df_grammy[col] = df_grammy[col].astype(str).str.strip()
                df_grammy = df_grammy[df_grammy[col] != ""]
                after = len(df_grammy)
                cleaning_log["grammy"][f"rows_removed_{col}"] = before - after

        confirmed_img_artist_duplicates = [
            {
                "img": "https://www.grammy.com/sites/com/files/styles/artist_circle/public/muzooka/Donald%2BLawrence/Donald%2520Lawrence_1_1_1594739006.jpg?itok=sLgm8I99",
                "artist_to_remove": "The Clark Sisters"
            },
            {
                "img": "https://www.grammy.com/sites/com/files/styles/artist_circle/public/muzooka/Jack%2BWhite/Jack%2520White_1_1_1578384853.jpg?itok=cFBueuG3",
                "artist_to_remove": "The White Stripes"
            },
            {
                "img": "https://www.grammy.com/sites/com/files/styles/artist_circle/public/muzooka/Jose%2BLugo/Jose%2520Lugo_1_1_1597180907.jpg?itok=wFL5AI88",
                "artist_to_remove": "Jose Lugo & Guasábara Combo"
            },
            {
                "img": "https://www.grammy.com/sites/com/files/styles/artist_circle/public/muzooka/Paul%2BSimon/Paul%2520Simon_1_1_1578385319.jpg?itok=H2xb3gsd",
                "artist_to_remove": "Paul Simon"
            },
            {
                "img": "https://www.grammy.com/sites/com/files/styles/artist_circle/public/muzooka/Will%2BSmith/Will%2520Smith_1_1_1581552180.jpg?itok=ZsgRG6eK",
                "artist_to_remove": "Will Smith"
            }
        ]

        if {"img", "artist"}.issubset(df_grammy.columns):
            df_grammy["img"] = df_grammy["img"].astype(str).str.strip()
            df_grammy["artist"] = df_grammy["artist"].astype(str).str.strip()

            rows_to_remove_mask = pd.Series(False, index=df_grammy.index)

            for item in confirmed_img_artist_duplicates:
                img_norm = normalize_text(item["img"])
                artist_norm = normalize_text(item["artist_to_remove"])

                current_mask = (
                    df_grammy["img"].apply(normalize_text).eq(img_norm) &
                    df_grammy["artist"].apply(normalize_text).eq(artist_norm)
                )
                rows_to_remove_mask = rows_to_remove_mask | current_mask

            df_grammy = df_grammy[~rows_to_remove_mask].copy()

        spotify_clean_path = CLEAN_DIR / "spotify_clean.csv"
        grammy_clean_path = CLEAN_DIR / "grammy_clean.csv"
        log_path = LOG_DIR / "cleaning_log.json"

        df_spotify.to_csv(spotify_clean_path, index=False)
        df_grammy.to_csv(grammy_clean_path, index=False)

        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(cleaning_log, f, indent=4, ensure_ascii=False)

        return {
            "spotify_clean_path": str(spotify_clean_path),
            "grammy_clean_path": str(grammy_clean_path),
            "cleaning_log_path": str(log_path)
        }

    # =========================================================
    # TRANSFORM
    # =========================================================
    @task
    def transform_input(clean_paths):
        df_spotify = pd.read_csv(clean_paths["spotify_clean_path"])
        df_grammy = pd.read_csv(clean_paths["grammy_clean_path"])

        transformation_log = {
            "spotify": {},
            "grammy": {},
            "merge": {}
        }

        def normalize_text(text):
            if pd.isna(text):
                return ""
            text = str(text).strip().lower()
            text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
            text = text.replace("&", " and ")
            text = re.sub(r"\s+", " ", text).strip()
            return text

        def split_spotify_artists(text):
            if pd.isna(text):
                return []
            text = str(text).strip()
            text = text.replace("[", "").replace("]", "")
            text = text.replace("'", "").replace('"', "")
            text = re.sub(r"\b(feat\.?|featuring|ft\.?|with|x)\b", ",", text, flags=re.IGNORECASE)
            parts = re.split(r"[,;/]+", text)

            artists = []
            for part in parts:
                part_norm = normalize_text(part)
                if part_norm:
                    artists.append(part_norm)

            return list(dict.fromkeys(artists))

        def concat_unique(series):
            values = [str(v).strip() for v in series.dropna().unique() if str(v).strip() != ""]
            return " | ".join(values) if values else None

        if "mode" in df_spotify.columns:
            df_spotify["mode"] = pd.to_numeric(df_spotify["mode"], errors="coerce").map({1: True, 0: False})

        if "explicit" in df_spotify.columns:
            df_spotify["explicit"] = pd.to_numeric(df_spotify["explicit"], errors="coerce").map({1: True, 0: False}).fillna(df_spotify["explicit"])

        spotify_text_cols = ["artists", "album_name", "track_name", "track_genre"]
        for col in spotify_text_cols:
            if col in df_spotify.columns:
                df_spotify[col] = df_spotify[col].apply(normalize_text)

        if "track_name" in df_spotify.columns:
            df_spotify["spotify_track_key"] = df_spotify["track_name"].apply(normalize_text)

        if "artists" in df_spotify.columns:
            df_spotify["spotify_artist_list"] = df_spotify["artists"].apply(split_spotify_artists)

        df_spotify_exploded = df_spotify.copy()
        if "spotify_artist_list" in df_spotify_exploded.columns:
            df_spotify_exploded = df_spotify_exploded.explode("spotify_artist_list")
            df_spotify_exploded["spotify_artist_key"] = df_spotify_exploded["spotify_artist_list"].fillna("")
        else:
            df_spotify_exploded["spotify_artist_key"] = ""

        if "year" in df_grammy.columns:
            df_grammy["year"] = pd.to_numeric(df_grammy["year"], errors="coerce").astype("Int64").astype("object")

        grammy_text_cols = ["category", "nominee", "artist", "workers", "img"]
        for col in grammy_text_cols:
            if col in df_grammy.columns:
                df_grammy[col] = df_grammy[col].apply(normalize_text)

        if "nominee" in df_grammy.columns:
            df_grammy["grammy_nominee_key"] = df_grammy["nominee"].apply(normalize_text)

        if "artist" in df_grammy.columns:
            df_grammy["grammy_artist_key"] = df_grammy["artist"].apply(normalize_text)

        df_grammy_merge = df_grammy.rename(columns={
            "year": "grammy_year",
            "title": "grammy_title",
            "category": "grammy_category",
            "nominee": "grammy_nominee",
            "artist": "grammy_artist",
            "workers": "grammy_workers",
            "img": "grammy_img",
            "published_at": "grammy_published_at",
            "updated_at": "grammy_updated_at"
        })

        spotify_enriched = df_spotify_exploded.merge(
            df_grammy_merge[
                [
                    "grammy_year", "grammy_title", "grammy_category", "grammy_nominee", "grammy_artist",
                    "grammy_workers", "grammy_img", "grammy_published_at", "grammy_updated_at",
                    "grammy_nominee_key", "grammy_artist_key"
                ]
            ],
            left_on=["spotify_track_key", "spotify_artist_key"],
            right_on=["grammy_nominee_key", "grammy_artist_key"],
            how="left"
        )

        spotify_base_cols = [col for col in df_spotify.columns if col != "spotify_artist_list"]

        agg_dict = {}
        for col in spotify_base_cols:
            if col == "track_id":
                continue
            agg_dict[col] = "first"

        agg_dict.update({
            "grammy_year": concat_unique,
            "grammy_title": concat_unique,
            "grammy_category": concat_unique,
            "grammy_nominee": concat_unique,
            "grammy_artist": concat_unique,
            "grammy_workers": concat_unique,
            "grammy_img": concat_unique,
            "grammy_published_at": concat_unique,
            "grammy_updated_at": concat_unique
        })

        if "track_id" in spotify_enriched.columns:
            enriched_dataset = spotify_enriched.groupby("track_id", as_index=False).agg(agg_dict)
        else:
            enriched_dataset = spotify_enriched.copy()

        enriched_dataset["has_grammy_match"] = enriched_dataset["grammy_category"].notna()

        transformation_log["merge"]["final_enriched_rows"] = len(enriched_dataset)
        transformation_log["merge"]["matched_rows"] = int(enriched_dataset["has_grammy_match"].sum())

        enriched_path = TRANSFORM_DIR / "spotify_enriched.csv"
        log_path = LOG_DIR / "transformation_log.json"

        enriched_dataset.to_csv(enriched_path, index=False)

        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(transformation_log, f, indent=4, ensure_ascii=False)

        del df_spotify_exploded
        del df_grammy_merge
        del spotify_enriched

        return {
            "enriched_path": str(enriched_path),
            "transformation_log_path": str(log_path)
        }

    # =========================================================
    # GOOGLE DRIVE HELPERS
    # =========================================================
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]

    def obtener_servicio_drive(credentials_path: str | Path, token_path: str | Path):
        credentials_path = Path(credentials_path)
        token_path = Path(token_path)
        creds = None

        if token_path.exists():
            creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                raise FileNotFoundError(
                    "No hay token.json válido para Google Drive en Airflow."
                )

        return build("drive", "v3", credentials=creds)

    def subir_csv_a_drive(
        ruta_archivo_local: str | Path,
        nombre_en_drive: str | None,
        folder_id: str | None,
        credentials_path: str | Path,
        token_path: str | Path
    ):
        ruta_archivo_local = Path(ruta_archivo_local)

        if not ruta_archivo_local.exists():
            raise FileNotFoundError(f"No existe el archivo a subir: {ruta_archivo_local}")

        servicio = obtener_servicio_drive(credentials_path, token_path)

        if nombre_en_drive is None:
            nombre_en_drive = ruta_archivo_local.name

        metadatos = {"name": nombre_en_drive}
        if folder_id:
            metadatos["parents"] = [folder_id]

        media = MediaFileUpload(str(ruta_archivo_local), mimetype="text/csv", resumable=True)

        archivo = servicio.files().create(
            body=metadatos,
            media_body=media,
            fields="id, name"
        ).execute()

        return archivo

    # =========================================================
    # DIMENSIONAL MODEL
    # =========================================================
    @task
    def build_dimensional_model_task(transform_dict):
        df_enriched = pd.read_csv(transform_dict["enriched_path"])
        dataframes_dw = build_dimensional_model(df_enriched)

        dim_time_path = DM_DIR / "dim_time.csv"
        dim_music_path = DM_DIR / "dim_music.csv"
        dim_grammy_path = DM_DIR / "dim_grammy.csv"
        fact_path = DM_DIR / "fact_music_grammy.csv"

        dataframes_dw["dim_time"].to_csv(dim_time_path, index=False)
        dataframes_dw["dim_music"].to_csv(dim_music_path, index=False)
        dataframes_dw["dim_grammy"].to_csv(dim_grammy_path, index=False)
        dataframes_dw["fact_music_grammy"].to_csv(fact_path, index=False)

        return {
            "dim_time_path": str(dim_time_path),
            "dim_music_path": str(dim_music_path),
            "dim_grammy_path": str(dim_grammy_path),
            "fact_music_grammy_path": str(fact_path)
        }

    # =========================================================
    # UPLOAD CSV TO DRIVE
    # =========================================================
    @task
    def upload_merged_dataset_to_drive(transform_dict):
        credentials_path = "/opt/airflow/data/credentials.json"
        token_path = "/opt/airflow/data/token.json"
        folder_id = None

        archivo = subir_csv_a_drive(
            ruta_archivo_local=transform_dict["enriched_path"],
            nombre_en_drive="spotify_enriched.csv",
            folder_id=folder_id,
            credentials_path=credentials_path,
            token_path=token_path
        )

        return {
            "drive_file_id": archivo.get("id"),
            "drive_file_name": archivo.get("name")
        }

    # =========================================================
    # LOAD TO DW
    # =========================================================
    def insert_ignore(df, table_name, engine):
        if df.empty:
            print(f"No data to insert into {table_name}")
            return

        temp_table = f"tmp_{table_name}"
        df.to_sql(temp_table, engine, if_exists="replace", index=False)

        cols = ", ".join(df.columns)

        insert_sql = f"""
            INSERT IGNORE INTO {table_name} ({cols})
            SELECT {cols} FROM {temp_table};
        """

        with engine.begin() as conn:
            conn.execute(text(insert_sql))
            conn.execute(text(f"DROP TABLE {temp_table}"))

    @task
    def load_to_dw_task(dimensional_paths):
        dim_time = pd.read_csv(dimensional_paths["dim_time_path"])
        dim_music = pd.read_csv(dimensional_paths["dim_music_path"])
        dim_grammy = pd.read_csv(dimensional_paths["dim_grammy_path"])
        fact_music_grammy = pd.read_csv(dimensional_paths["fact_music_grammy_path"])

        engine = create_engine(
            "mysql+pymysql://root:valemoravale@host.docker.internal:3306/workshop2_dw"
        )

        insert_ignore(dim_time, "dim_time", engine)
        insert_ignore(dim_music, "dim_music", engine)
        insert_ignore(dim_grammy, "dim_grammy", engine)

        key_cols = ["time_key", "music_key", "grammy_key"]

        existing_keys_sql = f"""
            SELECT {", ".join(key_cols)}
            FROM fact_music_grammy
        """

        try:
            existing_keys_df = pd.read_sql(existing_keys_sql, engine)
        except Exception:
            existing_keys_df = pd.DataFrame(columns=key_cols)

        for col in key_cols:
            fact_music_grammy[col] = fact_music_grammy[col].astype(str)
            existing_keys_df[col] = existing_keys_df[col].astype(str)

        if not existing_keys_df.empty:
            merged = fact_music_grammy.merge(
                existing_keys_df.drop_duplicates(),
                on=key_cols,
                how="left",
                indicator=True
            )
            fact_new = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        else:
            fact_new = fact_music_grammy.copy()

        if not fact_new.empty:
            insert_ignore(fact_new, "fact_music_grammy", engine)

        return {
            "dim_time_rows": len(dim_time),
            "dim_music_rows": len(dim_music),
            "dim_grammy_rows": len(dim_grammy),
            "fact_music_grammy_rows": len(fact_music_grammy),
            "fact_new_rows_loaded": len(fact_new)
        }

    # =========================================================
    # VALIDATE OUTPUT
    # =========================================================
    @task
    def validate_output(transform_dict, dimensional_paths, load_result):
        errors = []

        df_merged = pd.read_csv(transform_dict["enriched_path"])
        dim_time = pd.read_csv(dimensional_paths["dim_time_path"])
        dim_music = pd.read_csv(dimensional_paths["dim_music_path"])
        dim_grammy = pd.read_csv(dimensional_paths["dim_grammy_path"])
        fact_music_grammy = pd.read_csv(dimensional_paths["fact_music_grammy_path"])

        if df_merged.empty:
            errors.append("enriched dataset is empty.")

        for df_name, df_obj, key_col in [
            ("dim_time", dim_time, "time_key"),
            ("dim_music", dim_music, "music_key"),
            ("dim_grammy", dim_grammy, "grammy_key"),
        ]:
            if key_col not in df_obj.columns:
                errors.append(f"{df_name} is missing key column: {key_col}")
            elif df_obj[key_col].isna().any():
                errors.append(f"{df_name} contains nulls in {key_col}")

        expected_fact_keys = ["time_key", "music_key", "grammy_key"]
        for col in expected_fact_keys:
            if col not in fact_music_grammy.columns:
                errors.append(f"fact_music_grammy missing column: {col}")

        if set(expected_fact_keys).issubset(fact_music_grammy.columns):
            if fact_music_grammy.duplicated(subset=expected_fact_keys).any():
                errors.append("fact_music_grammy contains duplicate composite keys.")

        if not isinstance(load_result, dict):
            errors.append("load_result is not valid.")

        if errors:
            raise ValueError("Output validation failed: " + " | ".join(errors))

        return {
            "status": "success",
            "merged_rows": len(df_merged),
            "fact_rows": len(fact_music_grammy)
        }

    # =========================================================
    # ORCHESTRATION
    # =========================================================
    spotify_path = extract_spotify_data()
    grammy_path = extract_grammy_data()

    validated_paths = validate_input(spotify_path, grammy_path)
    clean_paths = clean_input(validated_paths)
    transform_paths = transform_input(clean_paths)

    dimensional_paths = build_dimensional_model_task(transform_paths)
    drive_result = upload_merged_dataset_to_drive(transform_paths)
    load_result = load_to_dw_task(dimensional_paths)

    validated_output = validate_output(
        transform_paths,
        dimensional_paths,
        load_result
    )

pipeline_music_etl_mysql()