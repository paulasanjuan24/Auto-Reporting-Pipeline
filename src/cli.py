# src/cli.py
'''CLI para ejecutar el pipeline o ver configuración'''

from __future__ import annotations
import typer
from typing import Optional
from .main import run
from .config import settings
from .logger import get_logger

app = typer.Typer(help="Email → Sheets ETL Pipeline CLI")
log = get_logger()

@app.command()
def run_now(query: Optional[str] = typer.Option(None, help="Query de Gmail (override)")):
    """Ejecuta el pipeline ahora mismo."""
    code = run(query)
    raise typer.Exit(code)

@app.command()
def show_config():
    """Muestra la configuración efectiva."""
    log.info(f"Spreadsheet Title: {settings.spreadsheet_title}")
    log.info(f"Spreadsheet ID: {settings.spreadsheet_id}")
    log.info(f"Gmail Query: {settings.gmail_query}")
    log.info(f"Data dir: {settings.data_dir}")
    log.info(f"Slack webhook: {'set' if settings.slack_webhook else 'not set'}")

if __name__ == "__main__":
    app()



'''
# Uso:
python -m src.cli run-now  # usa la query por defecto en config.py
python -m src.cli run-now --query 'from:yo@gmail.com has:attachment filename:(csv OR xlsx)' # usa query personalizada
python -m src.cli show-config  # muestra la configuración actual
'''