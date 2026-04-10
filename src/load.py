from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload



# SAVE GOOGLE DRIVE API

SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def obtener_servicio_drive(
    credentials_path: str | Path,
    token_path: str | Path
):
    """
    Authenticate and return Google Drive service.
    """
    credentials_path = Path(credentials_path)
    token_path = Path(token_path)

    creds = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, "w", encoding="utf-8") as token_file:
            token_file.write(creds.to_json())

    servicio = build("drive", "v3", credentials=creds)
    return servicio


def subir_csv_a_drive(
    ruta_archivo_local: str | Path,
    nombre_en_drive: str | None,
    folder_id: str | None,
    credentials_path: str | Path,
    token_path: str | Path
):
    """
    Upload a CSV file to Google Drive.
    """
    ruta_archivo_local = Path(ruta_archivo_local)

    if not ruta_archivo_local.exists():
        raise FileNotFoundError(f"No existe el archivo a subir: {ruta_archivo_local}")

    servicio = obtener_servicio_drive(credentials_path, token_path)

    if nombre_en_drive is None:
        nombre_en_drive = ruta_archivo_local.name

    metadatos = {"name": nombre_en_drive}

    if folder_id:
        metadatos["parents"] = [folder_id]

    media = MediaFileUpload(
        str(ruta_archivo_local),
        mimetype="text/csv",
        resumable=True
    )

    archivo = servicio.files().create(
        body=metadatos,
        media_body=media,
        fields="id, name"
    ).execute()

    print(f"Archivo subido a Drive correctamente: {archivo.get('name')}")
    print(f"ID en Drive: {archivo.get('id')}")

    return archivo


# Save to DW
def insert_ignore(df, table_name, engine):

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


# Load to DW
def load_to_dw(dataframes):

    dim_time   = dataframes["dim_time"]
    dim_music  = dataframes["dim_music"]
    dim_grammy = dataframes["dim_grammy"]
    fact_music_grammy   = dataframes["fact_music_grammy"]

    # Conection
    engine = create_engine(
    "mysql+pymysql://root:valemoravale@localhost:3306/workshop2_dw"
    )

    # Load dimensions
    insert_ignore(dim_time, "dim_time", engine)
    insert_ignore(dim_music, "dim_music", engine)
    insert_ignore(dim_grammy, "dim_grammy", engine)

    # Avoid duplicates
    key_cols = [
        "time_key",
        "music_key",
        "grammy_key",
    ]

    existing_keys_sql = f"""
        SELECT {', '.join(key_cols)}
        FROM fact_music_grammy
    """

    try:
        existing_keys_df = pd.read_sql(existing_keys_sql, engine)
    except Exception:
        existing_keys_df = pd.DataFrame(columns=key_cols)

    for col in key_cols:
        fact_music_grammy[col] = fact_music_grammy[col].astype(str)
        existing_keys_df[col] = existing_keys_df[col].astype(str)

    # Anti-join
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

    print(
        f"Fact total={len(fact_music_grammy)} "
        f"new rows={len(fact_new)} "
        f"duplicates omitted={len(fact_music_grammy) - len(fact_new)}"
    )

    # INSERT FACT
    if not fact_new.empty:
        insert_ignore(fact_new, "fact_music_grammy", engine)
    else:
        print("No new rows for fact_music_grammy")

    print("Load to Data Warehouse completed successfully")


