# src/notify.py
'''Notificaciones (Slack) para eventos importantes: inicio, fin, errores'''

from __future__ import annotations
import json
import urllib.request
from .config import settings
from .logger import get_logger

log = get_logger()

def _post_slack(text: str):
    if not settings.slack_webhook:
        log.info(f"[NO SLACK CONFIG] {text}")
        return
    req = urllib.request.Request(
        settings.slack_webhook,
        data=json.dumps({"text": text}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                log.warning(f"Slack webhook status: {resp.status}")
    except Exception as e:
        log.error(f"Slack webhook error: {e}")

def notify_info(text: str):
    _post_slack(f"✅ {text}")

def notify_warn(text: str):
    _post_slack(f"⚠️ {text}")

def notify_error(text: str):
    _post_slack(f"🚨 {text}")
