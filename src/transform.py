import pandas as pd
import unicodedata
import re


def normalize_text(text: str) -> str:
    if pd.isna(text):
        return ""

    text = str(text).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
    text = text.replace("&", " and ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def split_spotify_artists(text: str) -> list[str]:
    """
    Split Spotify artists into separate artist names
    to improve matching with Grammy artist column.
    """
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


def concat_unique(series: pd.Series):
    values = [str(v).strip() for v in series.dropna().unique() if str(v).strip() != ""]
    return " | ".join(values) if values else None


def transform_spotify_data(df_spotify: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Transform Spotify dataset and return:
    - transformed dataframe
    - exploded dataframe for merge
    - transformation log
    """
    df_spotify = df_spotify.copy()
    transformation_log = {}

    # 1. mode -> bool
    if "mode" in df_spotify.columns:
        df_spotify["mode"] = pd.to_numeric(df_spotify["mode"], errors="coerce").map({1: True, 0: False})
        transformation_log["mode_transformed_to_bool"] = True
    else:
        transformation_log["mode_transformed_to_bool"] = False

    # 2. explicit -> bool
    if "explicit" in df_spotify.columns:
        df_spotify["explicit"] = (
            pd.to_numeric(df_spotify["explicit"], errors="coerce")
            .map({1: True, 0: False})
            .fillna(df_spotify["explicit"])
        )
        transformation_log["explicit_standardized"] = True
    else:
        transformation_log["explicit_standardized"] = False

    # 3. standardize text columns
    spotify_text_cols = ["artists", "album_name", "track_name", "track_genre"]
    for col in spotify_text_cols:
        if col in df_spotify.columns:
            df_spotify[col] = df_spotify[col].apply(normalize_text)

    transformation_log["standardized_text_columns"] = [
        col for col in spotify_text_cols if col in df_spotify.columns
    ]

    # 4. keys for merge
    if "track_name" in df_spotify.columns:
        df_spotify["spotify_track_key"] = df_spotify["track_name"].apply(normalize_text)

    if "artists" in df_spotify.columns:
        df_spotify["spotify_artist_list"] = df_spotify["artists"].apply(split_spotify_artists)

    # explode artists
    df_spotify_exploded = df_spotify.copy()
    if "spotify_artist_list" in df_spotify_exploded.columns:
        df_spotify_exploded = df_spotify_exploded.explode("spotify_artist_list")
        df_spotify_exploded["spotify_artist_key"] = df_spotify_exploded["spotify_artist_list"].fillna("")
    else:
        df_spotify_exploded["spotify_artist_key"] = ""

    transformation_log["rows_after_artist_explode"] = len(df_spotify_exploded)

    return df_spotify, df_spotify_exploded, transformation_log


def transform_grammy_data(df_grammy: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Transform Grammy dataset and return:
    - transformed dataframe
    - renamed dataframe for merge
    - transformation log
    """
    df_grammy = df_grammy.copy()
    transformation_log = {}

    # 1. year -> object
    if "year" in df_grammy.columns:
        df_grammy["year"] = pd.to_numeric(df_grammy["year"], errors="coerce").astype("Int64").astype("object")
        transformation_log["year_transformed_to_object"] = True
    else:
        transformation_log["year_transformed_to_object"] = False

    # 2. standardize text columns
    grammy_text_cols = ["category", "nominee", "artist", "workers", "img"]
    for col in grammy_text_cols:
        if col in df_grammy.columns:
            df_grammy[col] = df_grammy[col].apply(normalize_text)

    transformation_log["standardized_text_columns"] = [
        col for col in grammy_text_cols if col in df_grammy.columns
    ]

    # 3. keys for merge
    if "nominee" in df_grammy.columns:
        df_grammy["grammy_nominee_key"] = df_grammy["nominee"].apply(normalize_text)

    if "artist" in df_grammy.columns:
        df_grammy["grammy_artist_key"] = df_grammy["artist"].apply(normalize_text)

    # 4. rename columns for merge
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

    return df_grammy, df_grammy_merge, transformation_log


def merge_spotify_grammy(
    df_spotify: pd.DataFrame,
    df_spotify_exploded: pd.DataFrame,
    df_grammy_merge: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Merge Spotify and Grammy datasets into a single enriched dataset.
    """
    transformation_log = {}

    # =========================================================
    # SPOTIFY_ENRICHED
    # =========================================================
    spotify_merge_temp = df_spotify_exploded.merge(
        df_grammy_merge[
            [
                "grammy_year", "grammy_title", "grammy_category", "grammy_nominee",
                "grammy_artist", "grammy_workers", "grammy_img",
                "grammy_published_at", "grammy_updated_at",
                "grammy_nominee_key", "grammy_artist_key"
            ]
        ],
        left_on=["spotify_track_key", "spotify_artist_key"],
        right_on=["grammy_nominee_key", "grammy_artist_key"],
        how="left"
    )

    transformation_log["spotify_rows_after_merge"] = len(spotify_merge_temp)

    # Excluir columnas auxiliares del resultado final
    spotify_base_cols = [
        col for col in df_spotify.columns
        if col not in ["spotify_artist_list", "spotify_track_key"]
    ]

    agg_dict_spotify = {}
    for col in spotify_base_cols:
        if col == "track_id":
            continue
        agg_dict_spotify[col] = "first"

    agg_dict_spotify.update({
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

    if "track_id" in spotify_merge_temp.columns:
        spotify_enriched = spotify_merge_temp.groupby("track_id", as_index=False).agg(agg_dict_spotify)
    else:
        spotify_enriched = spotify_merge_temp.copy()

    spotify_enriched["has_grammy_match"] = spotify_enriched["grammy_category"].notna()
    transformation_log["final_enriched_rows"] = len(spotify_enriched) 
    transformation_log["matched_rows"] = int(spotify_enriched["has_grammy_match"].sum()) 

    return spotify_enriched, transformation_log


def transform_input(
    df_spotify: pd.DataFrame,
    df_grammy: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Transform both datasets and merge them.
    """
    df_spotify_transformed, df_spotify_exploded, spotify_log = transform_spotify_data(df_spotify)
    df_grammy_transformed, df_grammy_merge, grammy_log = transform_grammy_data(df_grammy)

    spotify_enriched, merge_log = merge_spotify_grammy(
        df_spotify_transformed,
        df_spotify_exploded,
        df_grammy_merge
    )

    transformation_log = {
        "spotify": spotify_log,
        "grammy": grammy_log,
        "merge": merge_log
    }

    return (
        df_spotify_transformed,
        df_grammy_transformed,
        spotify_enriched,
        transformation_log
    )