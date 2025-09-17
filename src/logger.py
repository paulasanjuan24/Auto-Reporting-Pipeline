# src/logger.py
'''
Logging configurado (consola + archivo rotativo)
'''

import logging, logging.handlers
from .config import settings

def get_logger(name: str = "autoreport"):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # evitar duplicados

    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    # Consola
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Archivo rotativo (5 archivos de 2MB)
    fh = logging.handlers.RotatingFileHandler(
        settings.logs_dir / "run.log", maxBytes=2_000_000, backupCount=5, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger
