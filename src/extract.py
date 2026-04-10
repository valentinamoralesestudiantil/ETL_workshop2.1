from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine


def extract_spotify_data(csv_path: str | Path) -> pd.DataFrame:
    """
    Extract Spotify data from CSV located in the raw folder.
    """
    csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"Spotify file not found: {csv_path}")

    df_spotify = pd.read_csv(csv_path)

    print("Spotify dataset extracted successfully")
    print(f"Path: {csv_path}")
    print(df_spotify.head())
    print(df_spotify.shape)

    return df_spotify


def extract_grammy_data(mysql_url: str, table_name: str = "the_grammy_awards") -> pd.DataFrame:
    """
    Extract Grammy data from MySQL.
    """
    engine = create_engine(mysql_url)
    query = f"SELECT * FROM {table_name}"
    df_grammy = pd.read_sql(query, con=engine)

    print("Grammy dataset extracted successfully from MySQL")
    print(f"Table: {table_name}")
    print(df_grammy.head())
    print(df_grammy.shape)

    return df_grammy