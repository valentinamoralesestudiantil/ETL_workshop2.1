import pandas as pd


# --------------------------------------------------------
# DIM TIME
# --------------------------------------------------------
def create_dim_time(df: pd.DataFrame) -> pd.DataFrame:
    dim_time_cols = ["grammy_year", "grammy_published_at", "grammy_updated_at"]

    dim_time = df[dim_time_cols].drop_duplicates().reset_index(drop=True)

    # opcional: quitar filas donde todo venga vacío
    dim_time = dim_time[
        ~(
            dim_time["grammy_year"].isna()
            & dim_time["grammy_published_at"].isna()
            & dim_time["grammy_updated_at"].isna()
        )
    ].reset_index(drop=True)

    dim_time["time_key"] = dim_time.index + 1

    dim_time = dim_time[
        ["time_key", "grammy_year", "grammy_published_at", "grammy_updated_at"]
    ]

    return dim_time


# --------------------------------------------------------
# DIM MUSIC
# --------------------------------------------------------
def create_dim_music(df: pd.DataFrame) -> pd.DataFrame:
    dim_music_cols = [
        "track_id",
        "track_name",
        "album_name",
        "artists",
        "track_genre",
        "duration_ms",
        "explicit",
        "danceability",
        "energy",
        "key",
        "loudness",
        "mode",
        "speechiness",
        "acousticness",
        "instrumentalness",
        "liveness",
        "valence",
        "tempo",
        "time_signature"
    ]

    dim_music = df[dim_music_cols].drop_duplicates(subset=["track_id"]).reset_index(drop=True)

    dim_music["music_key"] = dim_music.index + 1

    # si en MySQL usaste musical_key en vez de key
    dim_music = dim_music.rename(columns={"key": "musical_key"})

    dim_music = dim_music[
        [
            "music_key",
            "track_id",
            "track_name",
            "album_name",
            "artists",
            "track_genre",
            "duration_ms",
            "explicit",
            "danceability",
            "energy",
            "musical_key",
            "loudness",
            "mode",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
            "tempo",
            "time_signature"
        ]
    ]

    return dim_music


# --------------------------------------------------------
# DIM GRAMMY
# --------------------------------------------------------
def create_dim_grammy(df: pd.DataFrame) -> pd.DataFrame:
    dim_grammy_cols = [
        "grammy_title",
        "grammy_category",
        "grammy_nominee",
        "grammy_artist",
        "grammy_workers",
        "grammy_img"
    ]

    dim_grammy = df[dim_grammy_cols].drop_duplicates().reset_index(drop=True)

    # opcional: quitar filas completamente vacías
    dim_grammy = dim_grammy[
        ~(
            dim_grammy["grammy_title"].isna()
            & dim_grammy["grammy_category"].isna()
            & dim_grammy["grammy_nominee"].isna()
            & dim_grammy["grammy_artist"].isna()
            & dim_grammy["grammy_workers"].isna()
            & dim_grammy["grammy_img"].isna()
        )
    ].reset_index(drop=True)

    dim_grammy["grammy_key"] = dim_grammy.index + 1

    dim_grammy = dim_grammy[
        [
            "grammy_key",
            "grammy_title",
            "grammy_category",
            "grammy_nominee",
            "grammy_artist",
            "grammy_workers",
            "grammy_img"
        ]
    ]

    return dim_grammy


# --------------------------------------------------------
# FACT TABLE
# --------------------------------------------------------
def create_fact_music_grammy(
    df: pd.DataFrame,
    dim_time: pd.DataFrame,
    dim_music: pd.DataFrame,
    dim_grammy: pd.DataFrame
) -> pd.DataFrame:

    fact_df = df.copy()

    # --- merge con dim_music para traer music_key
    fact_df = fact_df.merge(
        dim_music[["music_key", "track_id"]],
        on="track_id",
        how="left"
    )

    # --- merge con dim_time para traer time_key
    fact_df = fact_df.merge(
        dim_time,
        on=["grammy_year", "grammy_published_at", "grammy_updated_at"],
        how="left"
    )

    # --- merge con dim_grammy para traer grammy_key
    fact_df = fact_df.merge(
        dim_grammy,
        on=[
            "grammy_title",
            "grammy_category",
            "grammy_nominee",
            "grammy_artist",
            "grammy_workers",
            "grammy_img"
        ],
        how="left"
    )

    fact_music_grammy = fact_df[
        [
            "time_key",
            "music_key",
            "grammy_key",
            "popularity",
            "has_grammy_match"
        ]
    ].copy()

    fact_music_grammy = fact_music_grammy.drop_duplicates().reset_index(drop=True)

    return fact_music_grammy


# --------------------------------------------------------
# TRANSFORM TO DIMENSIONAL MODEL
# --------------------------------------------------------
def build_dimensional_model(df: pd.DataFrame) -> dict:
    dim_time = create_dim_time(df)
    dim_music = create_dim_music(df)
    dim_grammy = create_dim_grammy(df)

    fact_music_grammy = create_fact_music_grammy(
        df,
        dim_time,
        dim_music,
        dim_grammy
    )

    return {
        "dim_time": dim_time,
        "dim_music": dim_music,
        "dim_grammy": dim_grammy,
        "fact_music_grammy": fact_music_grammy
    }