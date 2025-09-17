# src/state.py
'''Manejo de estado: qué adjuntos ya hemos visto y guardado
cada adjunto se convierte en una huella digital (SHA-256 del contenido). Guardamos esas huellas en una SQLite (state.sqlite). Si ya vimos ese archivo, lo saltamos.'''

from __future__ import annotations
import hashlib, sqlite3, pathlib, datetime as dt
from .config import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS attachments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sha256 TEXT UNIQUE,
  filename TEXT,
  saved_path TEXT,
  received_at TEXT
);
"""

class StateDB:
    def __init__(self, path: pathlib.Path = settings.state_db):
        self.path = path
        self._init()

    def _init(self):
        conn = sqlite3.connect(self.path)
        with conn:
            conn.executescript(SCHEMA)
        conn.close()

    def seen(self, sha256: str) -> bool:
        conn = sqlite3.connect(self.path)
        cur = conn.execute("SELECT 1 FROM attachments WHERE sha256 = ?", (sha256,))
        row = cur.fetchone()
        conn.close()
        return row is not None

    def add(self, sha256: str, filename: str, saved_path: str, received_at: str | None = None):
        if not received_at:
            received_at = dt.datetime.utcnow().isoformat()
        conn = sqlite3.connect(self.path)
        with conn:
            conn.execute(
                "INSERT OR IGNORE INTO attachments (sha256, filename, saved_path, received_at) VALUES (?, ?, ?, ?)",
                (sha256, filename, saved_path, received_at),
            )
        conn.close()

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
