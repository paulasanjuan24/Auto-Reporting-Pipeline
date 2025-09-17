# src/main.py
'''Script principal: orquesta todo'''

from __future__ import annotations
import sys
from .fetch_gmail import download_attachments
from .process_reports import combine_files, basic_summary, export_outputs
from .to_google_sheets import create_or_get_sheet, clear_and_write
from .config import settings
from .logger import get_logger
from .process_reports import clean_and_validate
from .notify import notify_info, notify_warn, notify_error

log = get_logger()

# Función principal
def run(gmail_query: str | None = None) -> int:
    try:
        log.info("Inicio de ejecución")
        files = download_attachments(gmail_query)
        if not files:
            msg = "No se descargaron adjuntos con la query dada."
            log.warning(msg)
            notify_warn(msg)
            return 0  # No es error operativo, solo aviso

        log.info(f"Procesando {len(files)} archivo(s)...")
        combined = combine_files(files)
        if combined.empty:
            msg = "No se pudieron leer datos de los adjuntos."
            log.error(msg)
            notify_error(msg)
            return 2

        clean_df, invalid_df = clean_and_validate(combined)
        if clean_df.empty and (invalid_df is not None and not invalid_df.empty):
            # Todo inválido
            msg = "Todos los archivos fallaron validación. No se subieron datos."
            log.error(msg)
            notify_error(msg)
            return 3

        csv_path, xlsx_path = export_outputs(clean_df, invalid_df)
        log.info(f"Exportado: {csv_path} y {xlsx_path}")

        ssid = settings.spreadsheet_id or create_or_get_sheet(settings.spreadsheet_title)
        clear_and_write(ssid, "raw_clean", clean_df)
        clear_and_write(ssid, "summary", basic_summary(clean_df))
        if invalid_df is not None and not invalid_df.empty:
            # En vez de subir invalid a Sheets (ruidoso), lo dejamos solo en el Excel y lo mencionamos
            invalid_rows = len(invalid_df)
            notify_warn(f"Cargado con advertencias. Filas inválidas: {invalid_rows}. Revisar pestaña 'invalid' en combined.xlsx")
        notify_info(f"Pipeline OK. Archivos procesados: {len(files)}. Filas válidas: {len(clean_df)}. Sheet ID: {ssid}")
        return 0
    except Exception as e:
        log.exception(f"Fallo inesperado: {e}")
        notify_error(f"Fallo inesperado: {e}")
        return 99

if __name__ == "__main__":
    q = None
    if len(sys.argv) > 1:
        q = " ".join(sys.argv[1:])
    code = run(q)
    sys.exit(code)