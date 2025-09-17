# src/fetch_gmail.py
'''Buscar correos y descargar adjuntos
- Navegamos por tu Gmail con una query (filtro) tipo: “últimos 7 días, con adjuntos, .csv o .xlsx”.
- Entendemos la estructura de un mensaje MIME (un correo puede tener varias “partes”).
- Descargamos adjuntos a una carpeta con fecha del día.

*Query de Gmail* (como en la barra de búsqueda de Gmail):has:attachment newer_than:7d filename:(csv OR xlsx)
*API*: primero listamos mensajes (ids), luego pedimos cada mensaje y buscamos sus parts con filename y attachmentId.'''

from __future__ import annotations
import base64, os, pathlib, datetime as dt
from typing import List, Dict
from .auth_gmail import gmail_service
from .config import settings
from .state import StateDB, sha256_bytes
from .logger import get_logger

# Directorios para guardar adjuntos
log = get_logger()
DATA_DIR = settings.data_dir
INBOX_DIR = settings.inbox_dir
state = StateDB()


# Funciones internas para listar mensajes, obtener mensaje y guardar adjunto
def _gmail_list_messages(query: str) -> List[Dict]:
    """
    Devuelve una lista de diccionarios con {id: ...} de cada mensaje que cumple la query.
    Maneja la paginación (puede haber muchas páginas).
    """
    svc = gmail_service()
    user = "me"  # 'me' = la cuenta autenticada
    msgs = []
    req = svc.users().messages().list(userId=user, q=query)
    while req is not None:
        res = req.execute()
        msgs.extend(res.get("messages", []))
        req = svc.users().messages().list_next(req, res)  # pagina siguiente
    log.info(f"Mensajes encontrados: {len(msgs)}")
    return msgs

def _gmail_get_message(msg_id: str) -> Dict:
    """Recupera el mensaje completo (cabeceras, partes, etc.)."""
    svc = gmail_service()
    return svc.users().messages().get(userId="me", id=msg_id).execute()

def _save_attachment_data(filename: str, data: bytes, subdir: pathlib.Path) -> pathlib.Path:
    '''Guarda los bytes en un archivo dentro de subdir, evitando sobrescribir.'''
    path = subdir / filename
    counter = 1
    stem, ext = os.path.splitext(filename)
    # Evitar sobrescribir archivos
    while path.exists():
        path = subdir / f"{stem}_{counter}{ext}"
        counter += 1
    path.write_bytes(data)
    return path

# Función principal para buscar correos y descargar adjuntos
def download_attachments(gmail_query: str | None = None) -> list[pathlib.Path]:
    """
    Busca correos que cumplan 'gmail_query' y descarga sus adjuntos CSV/XLSX.
    Devuelve la lista de rutas de archivos guardados.
    """
    # Crear carpeta para hoy (si no existe)
    query = gmail_query or settings.gmail_query
    today_folder = INBOX_DIR / dt.date.today().isoformat()
    today_folder.mkdir(parents=True, exist_ok=True)
    saved: list[pathlib.Path] = []

    # Buscar mensajes que cumplan la query
    for msg in _gmail_list_messages(query):
        # Obtener el mensaje completo por su ID
        full = _gmail_get_message(msg["id"])
        # Extraer asunto y partes del mensaje
        payload = full.get("payload", {})
        headers = {h["name"].lower(): h["value"] for h in payload.get("headers", [])}
        subject = headers.get("subject", "(sin asunto)")

        # Un correo puede tener varias 'parts' (texto, html, adjuntos, etc.)
        parts = payload.get("parts", [])
        for part in parts:
            filename = part.get("filename")
            if not filename:
                continue
            # Filtramos por extensión
            if not (filename.lower().endswith(".csv") or filename.lower().endswith(".xlsx")):
                continue
            body = part.get("body", {})
            att_id = body.get("attachmentId")
            if att_id:
                continue

            # Obtener bytes del adjunto
            svc = gmail_service()
            att = svc.users().messages().attachments().get(
                userId="me", messageId=full["id"], id=att_id
            ).execute()
            data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
            digest = sha256_bytes(data)

            # Saltar si ya lo vimos
            if state.seen(digest):
                log.info(f"Duplicado detectado, se omite: {filename} (asunto: {subject})")
                continue

            path = _save_attachment_data(filename, data, today_folder)
            state.add(digest, filename, str(path))
            saved.append(path)
            log.info(f"Descargado: {filename} (asunto: {subject}) → {path}")

    if not saved:
        log.warning("No se ha descargado ningún adjunto.")
    return saved
