# src/server.py
from __future__ import annotations
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import json
import traceback

from .main import run
from .logger import get_logger

log = get_logger()

def _json(self: BaseHTTPRequestHandler, status: int, payload: dict):
    body = json.dumps(payload).encode("utf-8")
    self.send_response(status)
    self.send_header("Content-Type", "application/json; charset=utf-8")
    self.send_header("Content-Length", str(len(body)))
    # (Opcional) CORS si lo necesitas
    self.send_header("Access-Control-Allow-Origin", "*")
    self.end_headers()
    self.wfile.write(body)

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # silencia log por consola del http.server
        log.info("HTTP %s - %s", self.address_string(), format % args)

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                return _json(self, 200, {"ok": True})

            if parsed.path == "/run":
                qs = parse_qs(parsed.query)
                query = qs.get("q", [None])[0]
                log.info(f"/run called with q={query!r}")
                code = run(query)  # <- tu pipeline
                status = 200 if code == 0 else 500
                return _json(self, status, {"code": code})

            return _json(self, 404, {"error": "Not found", "path": parsed.path})

        except Exception as e:
            # Log detallado para diagnosticar el 500
            log.exception("Unhandled error in HTTP handler: %s", e)
            tb = traceback.format_exc()
            return _json(self, 500, {"error": "internal", "detail": str(e), "trace": tb})

def serve(host="0.0.0.0", port=8080):
    log.info(f"HTTP server on {host}:{port}")
    HTTPServer((host, port), Handler).serve_forever()

if __name__ == "__main__":
    serve()
