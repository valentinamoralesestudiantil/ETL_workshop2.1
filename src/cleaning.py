import pandas as pd
import unicodedata
import re
import random
import string


def normalize_text(text: str) -> str:
    if pd.isna(text):
        return ""

    text = str(text).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def count_nulls(df: pd.DataFrame, columns: list[str]) -> dict:
    result = {}
    for col in columns:
        if col in df.columns:
            result[col] = int(df[col].isna().sum())
        else:
            result[col] = None
    return result

def normalize_missing_like(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Convert empty strings, spaces and text-like null values to pd.NA.
    """
    df = df.copy()

    for col in columns:
        if col in df.columns:
            # espacios o vacíos -> NA
            df[col] = df[col].replace(r"^\s*$", pd.NA, regex=True)

            # textos que significan nulo -> NA
            df[col] = df[col].replace(
                ["null", "NULL", "None", "none", "nan", "NaN"],
                pd.NA
            )

    return df


def count_blanks(df: pd.DataFrame, columns: list[str]) -> dict:
    result = {}
    for col in columns:
        if col in df.columns:
            result[col] = int(df[col].astype(str).str.strip().eq("").sum())
        else:
            result[col] = None
    return result


def clean_spotify_data(df_spotify: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df_spotify = df_spotify.copy()

    key_text_cols = ["artists", "album_name", "track_name"]

    cleaning_log = {
        "initial_rows": len(df_spotify),
        "null_counts_before": count_nulls(df_spotify, key_text_cols),
        "blank_counts_before": count_blanks(df_spotify, key_text_cols),
        "dropped_columns": [],
        "removal_details": {}
    }

    # 1. Drop unnecessary columns
    spotify_cols_to_drop = [col for col in ["Unnamed: 0"] if col in df_spotify.columns]
    if spotify_cols_to_drop:
        df_spotify = df_spotify.drop(columns=spotify_cols_to_drop)
    cleaning_log["dropped_columns"] = spotify_cols_to_drop

    # 2. Remove rows with null key text fields
    for col in key_text_cols:
        if col in df_spotify.columns:
            before = len(df_spotify)
            df_spotify = df_spotify.dropna(subset=[col])
            removed = before - len(df_spotify)
            cleaning_log["removal_details"][f"rows_removed_null_{col}"] = removed
        else:
            cleaning_log["removal_details"][f"rows_removed_null_{col}"] = 0

    # 3. Remove blank strings in key text fields
    for col in key_text_cols:
        if col in df_spotify.columns:
            before = len(df_spotify)
            df_spotify[col] = df_spotify[col].astype(str).str.strip()
            df_spotify = df_spotify[df_spotify[col] != ""]
            removed = before - len(df_spotify)
            cleaning_log["removal_details"][f"rows_removed_blank_{col}"] = removed
        else:
            cleaning_log["removal_details"][f"rows_removed_blank_{col}"] = 0

    # 4. Remove invalid loudness values (must be < -1)
    if "loudness" in df_spotify.columns:
        before = len(df_spotify)
        df_spotify["loudness"] = pd.to_numeric(df_spotify["loudness"], errors="coerce")
        df_spotify = df_spotify[df_spotify["loudness"].notna()]
        df_spotify = df_spotify[df_spotify["loudness"] < -1]
        removed = before - len(df_spotify)
        cleaning_log["removal_details"]["rows_removed_invalid_loudness"] = removed
    else:
        cleaning_log["removal_details"]["rows_removed_invalid_loudness"] = 0

    # 5. Remove invalid time_signature values (must be > 1)
    if "time_signature" in df_spotify.columns:
        before = len(df_spotify)
        df_spotify["time_signature"] = pd.to_numeric(df_spotify["time_signature"], errors="coerce")
        df_spotify = df_spotify[df_spotify["time_signature"].notna()]
        df_spotify = df_spotify[df_spotify["time_signature"] > 1]
        removed = before - len(df_spotify)
        cleaning_log["removal_details"]["rows_removed_invalid_time_signature"] = removed
    else:
        cleaning_log["removal_details"]["rows_removed_invalid_time_signature"] = 0

    # 6. Reassign duplicated or missing track_id instead of removing rows
    if "track_id" in df_spotify.columns:

        def generate_synthetic_track_id(existing_ids, length=22):
            alphabet = string.ascii_letters + string.digits
            while True:
                new_id = "".join(random.choices(alphabet, k=length))
                if new_id not in existing_ids:
                    return new_id

        df_spotify["track_id"] = df_spotify["track_id"].astype(str).str.strip()
        df_spotify.loc[df_spotify["track_id"] == "", "track_id"] = pd.NA

        existing_ids = set(df_spotify["track_id"].dropna().tolist())
        duplicate_count = 0
        missing_count = 0

        duplicated_mask = df_spotify["track_id"].duplicated(keep="first")

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

        cleaning_log["removal_details"]["rows_removed_duplicate_track_id"] = 0
        cleaning_log["removal_details"]["duplicated_track_id_reassigned"] = duplicate_count
        cleaning_log["removal_details"]["missing_track_id_reassigned"] = missing_count
    else:
        cleaning_log["removal_details"]["rows_removed_duplicate_track_id"] = 0
        cleaning_log["removal_details"]["duplicated_track_id_reassigned"] = 0
        cleaning_log["removal_details"]["missing_track_id_reassigned"] = 0

    cleaning_log["null_counts_after"] = count_nulls(df_spotify, key_text_cols)
    cleaning_log["blank_counts_after"] = count_blanks(df_spotify, key_text_cols)
    cleaning_log["final_rows"] = len(df_spotify)
    cleaning_log["total_rows_removed"] = cleaning_log["initial_rows"] - cleaning_log["final_rows"]

    return df_spotify, cleaning_log

def clean_grammy_data(df_grammy: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df_grammy = df_grammy.copy()

    key_text_cols = ["nominee", "artist", "workers", "img"]

    # Primero contar cómo vienen los vacíos en crudo
    raw_null_counts_before = count_nulls(df_grammy, key_text_cols)
    raw_blank_counts_before = count_blanks(df_grammy, key_text_cols)

    # Luego normalizar faltantes falsos a pd.NA
    df_grammy = normalize_missing_like(df_grammy, key_text_cols)

    cleaning_log = {
        "initial_rows": len(df_grammy),
        "raw_null_counts_before": raw_null_counts_before,
        "raw_blank_counts_before": raw_blank_counts_before,
        "null_counts_before": count_nulls(df_grammy, key_text_cols),
        "blank_counts_before": count_blanks(df_grammy, key_text_cols),
        "dropped_columns": [],
        "removal_details": {}
    }

    # 1. Drop unnecessary columns
    grammy_cols_to_drop = [col for col in ["winner"] if col in df_grammy.columns]
    if grammy_cols_to_drop:
        df_grammy = df_grammy.drop(columns=grammy_cols_to_drop)
    cleaning_log["dropped_columns"] = grammy_cols_to_drop

    # 2. Remove rows with null fields
    for col in key_text_cols:
        if col in df_grammy.columns:
            before = len(df_grammy)
            df_grammy = df_grammy.dropna(subset=[col])
            removed = before - len(df_grammy)
            cleaning_log["removal_details"][f"rows_removed_null_{col}"] = removed

    # 3. Remove blank strings
    for col in key_text_cols:
        if col in df_grammy.columns:
            before = len(df_grammy)
            df_grammy[col] = df_grammy[col].astype(str).str.strip()
            df_grammy = df_grammy[df_grammy[col] != ""]
            removed = before - len(df_grammy)
            cleaning_log["removal_details"][f"rows_removed_blank_{col}"] = removed

    # 4. Remove manually confirmed duplicated img-artist rows
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
        before = len(df_grammy)

        df_grammy["img"] = df_grammy["img"].astype(str).str.strip()
        df_grammy["artist"] = df_grammy["artist"].astype(str).str.strip()

        rows_to_remove_mask = pd.Series(False, index=df_grammy.index)
        removed_pairs = []

        for item in confirmed_img_artist_duplicates:
            img_norm = normalize_text(item["img"])
            artist_norm = normalize_text(item["artist_to_remove"])

            current_mask = (
                df_grammy["img"].apply(normalize_text).eq(img_norm)
                & df_grammy["artist"].apply(normalize_text).eq(artist_norm)
            )

            if current_mask.any():
                removed_pairs.append({
                    "img": item["img"],
                    "artist_removed": item["artist_to_remove"],
                    "rows_removed": int(current_mask.sum())
                })

            rows_to_remove_mask = rows_to_remove_mask | current_mask

        df_grammy = df_grammy[~rows_to_remove_mask].copy()
        removed = before - len(df_grammy)

        cleaning_log["removal_details"]["rows_removed_confirmed_duplicate_img_artist"] = removed
        cleaning_log["confirmed_duplicate_img_artist_details"] = removed_pairs
    else:
        cleaning_log["removal_details"]["rows_removed_confirmed_duplicate_img_artist"] = 0
        cleaning_log["confirmed_duplicate_img_artist_details"] = []

    cleaning_log["null_counts_after"] = count_nulls(df_grammy, key_text_cols)
    cleaning_log["blank_counts_after"] = count_blanks(df_grammy, key_text_cols)
    cleaning_log["final_rows"] = len(df_grammy)
    cleaning_log["total_rows_removed"] = cleaning_log["initial_rows"] - cleaning_log["final_rows"]

    return df_grammy, cleaning_log


def clean_input(df_spotify: pd.DataFrame, df_grammy: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    df_spotify_clean, spotify_log = clean_spotify_data(df_spotify)
    df_grammy_clean, grammy_log = clean_grammy_data(df_grammy)

    cleaning_log = {
        "spotify": spotify_log,
        "grammy": grammy_log
    }

    return df_spotify_clean, df_grammy_clean, cleaning_log