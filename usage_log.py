# ---------------------------------------------------------------------------
# Radial Flow by DM - optional usage log (Supabase add-on).
# Does NOTHING until a [supabase] block exists in Streamlit Secrets, so the app runs unchanged without it.
# Stores only: timestamp (HKT), the location text the user typed, and the requested start/end time. No results.
# ---------------------------------------------------------------------------
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests

# Hong Kong time (UTC+8) for readable timestamps.
HKT = timezone(timedelta(hours=8))

# Rows older than this are deleted automatically (data minimisation).
RETENTION_DAYS = 30


# Read Supabase settings from Streamlit Secrets; return None when the add-on is not configured.
def _settings() -> dict[str, str] | None:
    try:
        import streamlit as st

        block = st.secrets.get("supabase")
        if block and block.get("url") and block.get("key"):
            return {"url": str(block["url"]).rstrip("/"), "key": str(block["key"]),
                    "table": str(block.get("table", "usage_log"))}
    except Exception:
        pass
    return None


# Common REST headers for the Supabase PostgREST API.
def _headers(key: str) -> dict[str, str]:
    return {"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"}


# Save one usage row, then purge rows older than RETENTION_DAYS. Never raises: logging must not break the app.
def log_usage(location_input: str, start_time: datetime, end_time: datetime) -> None:
    settings = _settings()
    if settings is None:
        return
    endpoint = f"{settings['url']}/rest/v1/{settings['table']}"
    row: dict[str, Any] = {
        "logged_at": datetime.now(HKT).isoformat(),
        "location_input": location_input[:300],
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
    }
    try:
        requests.post(endpoint, json=row, headers={**_headers(settings["key"]), "Prefer": "return=minimal"},
                      timeout=8)
        cutoff = (datetime.now(HKT) - timedelta(days=RETENTION_DAYS)).isoformat()
        requests.delete(endpoint, params={"logged_at": f"lt.{cutoff}"},
                        headers=_headers(settings["key"]), timeout=8)
    except Exception:
        pass
