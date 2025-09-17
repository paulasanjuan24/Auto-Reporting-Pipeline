# src/to_google_sheets.py

'''Crear y escribir en Google Sheets
- Creamos (o reusamos) una Google Sheet.
- Aseguramos que la pestaña existe.
- Limpiamos y escribimos valores nuevos.

*Google Sheets API* trabaja con rangos (A1 notation).'''

from __future__ import annotations
import pandas as pd
from googleapiclient.errors import HttpError
from .auth_gmail import sheets_service

# Funciones para crear hoja y escribir datos
def create_or_get_sheet(spreadsheet_title: str) -> str:
    """
    Crea una Sheet vacía con ese título y devuelve su ID.
    (Si ya tienes una y conoces el ID, puedes saltarte esto y pegar el ID directo en main.py.)
    """
    svc = sheets_service().spreadsheets()
    body = {"properties": {"title": spreadsheet_title}}
    try:
        res = svc.create(body=body).execute()
        return res["spreadsheetId"]
    except HttpError as e:
        raise

# Función para limpiar y escribir un DataFrame en una pestaña
def clear_and_write(spreadsheet_id: str, tab_name: str, df: pd.DataFrame):
    """
    Asegura que existe la pestaña 'tab_name', limpia su contenido
    y escribe el DataFrame (encabezados + filas).
    """
    ss = sheets_service().spreadsheets()
    vals = sheets_service().spreadsheets().values()

    # 1) Asegurar que la pestaña existe
    meta = ss.get(spreadsheetId=spreadsheet_id).execute()
    tab_exists = any(s["properties"]["title"] == tab_name for s in meta["sheets"])
    if not tab_exists:
        add_req = {"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
        ss.batchUpdate(spreadsheetId=spreadsheet_id, body=add_req).execute()

    # 2) Limpiar
    vals.clear(spreadsheetId=spreadsheet_id, range=f"{tab_name}!A:ZZ").execute()

    # 3) Preparar datos a escribir (todo como string por simplicidad)
    if df.empty:
        return
    values = [list(df.columns)] + df.astype(str).values.tolist()

    # 4) Escribir
    vals.update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()
