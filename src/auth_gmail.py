# src/auth_gmail.py
'''Autenticación y clientes de API
- Hacemos el login una sola vez (crea token.json) y reutilizarlo.
- Construimos objetos “service” de Gmail y Sheets para el resto del código.

*OAuth 2.0*: das permiso a tu script para acceder a Gmail/Sheets en tu nombre.
*Scopes*: “qué permisos pides” (leer Gmail, editar Sheets…).
credentials.json (lo bajas de Google Cloud) y token.json (se genera la primera vez).
'''

from __future__ import annotations
import pathlib
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# 1) Scopes = permisos. Aqui pedimos leer Gmail y editar Sheets.
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

# 2) Rutas a credenciales (raíz del proyecto)
ROOT = pathlib.Path(__file__).resolve().parents[1]
CREDENTIALS_FILE = ROOT / "credentials.json"
TOKEN_FILE = ROOT / "token.json"

# Función para obtener credenciales (login si no hay token o ha caducado)
def _get_credentials() -> Credentials:
    """
    Carga token si existe; si no, abre el navegador para iniciar sesión
    y guarda token.json para la próxima.
    """
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    # Si no hay token o no es válido, refrescamos o lanzamos login
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            creds.refresh(Request())  # intenta refrescar silenciosamente
        else:
            # Primer login interactivo
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)  # abre navegador
        TOKEN_FILE.write_text(creds.to_json())  # persistimos el token
    return creds

# Funciones para crear clientes de API (service objects)
def gmail_service():
    """Cliente para Gmail API listo para usar."""
    creds = _get_credentials()
    return build("gmail", "v1", credentials=creds)

def sheets_service():
    """Cliente para Google Sheets API listo para usar."""
    creds = _get_credentials()
    return build("sheets", "v4", credentials=creds)
