# src/config.py
'''
Configuración de la aplicación (todo configurable por .env sin tocar código)
Permite cambiar comportamiento sin tocar Python.
Sigue el principio 12-factor app: configuración via entorno.
'''
from __future__ import annotations
import pathlib
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = pathlib.Path(__file__).resolve().parents[1]

# Configuración de la app
class Settings(BaseSettings):
    # Gmail query por defecto
    gmail_query: str = 'has:attachment newer_than:7d filename:(csv OR xlsx)'

    # Google Sheets
    spreadsheet_title: str = "Reporte Correos (Pro)"
    spreadsheet_id: str | None = None  # si la dejas None, crea una nueva

    # Rutas
    data_dir: pathlib.Path = ROOT / "data"
    inbox_dir: pathlib.Path = data_dir / "inbox"
    processed_dir: pathlib.Path = data_dir / "processed"
    logs_dir: pathlib.Path = data_dir / "logs"
    state_db: pathlib.Path = data_dir / "state.sqlite"

    # Notificaciones (fase 3)
    slack_webhook: str | None = None

    # Configuraciones de Gmail API
    model_config = SettingsConfigDict(env_file=str(ROOT / ".env"), env_file_encoding="utf-8")

settings = Settings()
settings.logs_dir.mkdir(parents=True, exist_ok=True)
settings.inbox_dir.mkdir(parents=True, exist_ok=True)
settings.processed_dir.mkdir(parents=True, exist_ok=True)
