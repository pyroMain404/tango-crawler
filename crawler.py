import os
import sqlite3
import time
import logging
from datetime import datetime

import requests

# --- Config (all overridable via env vars) ---
FETCH_URL      = os.getenv("FETCH_URL",       "https://play5.newradio.it/stream/onairtxt/3881")
NORMAL_INTERVAL = int(os.getenv("NORMAL_INTERVAL", "90"))   # seconds between normal fetches
RETRY_INTERVAL  = int(os.getenv("RETRY_INTERVAL",  "30"))   # seconds when title is unchanged
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
DB_PATH        = os.getenv("DB_PATH",         "/data/tracks.db")
LOG_LEVEL      = os.getenv("LOG_LEVEL",       "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


def init_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tracks (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            title      TEXT    NOT NULL,
            fetched_at TEXT    NOT NULL  -- ISO-8601, e.g. 2026-03-31T14:05:00
        )
    """)
    # Index to make time-range queries fast
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_fetched_at ON tracks (fetched_at)
    """)
    conn.commit()
    return conn


def get_last_title(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT title FROM tracks ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def insert_track(conn: sqlite3.Connection, title: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    conn.execute("INSERT INTO tracks (title, fetched_at) VALUES (?, ?)", (title, ts))
    conn.commit()


def fetch_title() -> str | None:
    resp = requests.get(FETCH_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    for part in resp.text.strip().split("&"):
        if part.startswith("title="):
            return part[6:].strip()
    log.warning("Risposta inattesa: %s", resp.text[:200])
    return None


def main() -> None:
    log.info("Avvio crawler — url=%s normal=%ds retry=%ds db=%s",
             FETCH_URL, NORMAL_INTERVAL, RETRY_INTERVAL, DB_PATH)
    conn = init_db()

    while True:
        try:
            title = fetch_title()
            if not title:
                log.warning("Titolo non trovato nella risposta, attendo %ds", RETRY_INTERVAL)
                time.sleep(RETRY_INTERVAL)
                continue

            last = get_last_title(conn)
            if title == last:
                log.info("Invariato: '%s' — retry tra %ds", title, RETRY_INTERVAL)
                time.sleep(RETRY_INTERVAL)
                continue

            insert_track(conn, title)
            log.info("Salvato: '%s'", title)

        except requests.RequestException as exc:
            log.error("Errore HTTP: %s — retry tra %ds", exc, RETRY_INTERVAL)
            time.sleep(RETRY_INTERVAL)
            continue
        except Exception as exc:
            log.exception("Errore imprevisto: %s", exc)

        time.sleep(NORMAL_INTERVAL)


if __name__ == "__main__":
    main()
