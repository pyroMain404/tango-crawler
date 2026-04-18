import os
import sqlite3
import time
import logging
from datetime import datetime

import requests

from common import get_program, parse_track, JINGLE_ORCHESTRAS

# --- Config (all overridable via env vars) ---
FETCH_URL       = os.getenv("FETCH_URL",       "https://play5.newradio.it/stream/onairtxt/3881")
NORMAL_INTERVAL = int(os.getenv("NORMAL_INTERVAL", "90"))
RETRY_INTERVAL  = int(os.getenv("RETRY_INTERVAL",  "30"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
DB_PATH         = os.getenv("DB_PATH",         "/data/tracks.db")
LOG_LEVEL       = os.getenv("LOG_LEVEL",       "INFO")

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
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_title   TEXT    NOT NULL,
            orchestra   TEXT,
            singer      TEXT,
            track_title TEXT,
            year        INTEGER,
            author      TEXT,
            dancers     TEXT,
            program     TEXT,
            fetched_at  TEXT    NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fetched_at ON tracks (fetched_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orchestra  ON tracks (orchestra)")
    conn.commit()
    return conn


def get_last_raw_title(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT raw_title FROM tracks ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def insert_track(conn: sqlite3.Connection, raw_title: str, dt: datetime, parsed: dict) -> None:
    conn.execute("""
        INSERT INTO tracks
            (raw_title, orchestra, singer, track_title, year, author, dancers, program, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        raw_title,
        parsed['orchestra'],
        parsed['singer'],
        parsed['track_title'],
        parsed['year'],
        parsed['author'],
        parsed['dancers'],
        get_program(dt.hour),
        dt.strftime("%Y-%m-%dT%H:%M:%S"),
    ))
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
    current_program = get_program(datetime.now().hour)
    log.info("Fascia corrente: %s", current_program)

    while True:
        try:
            new_program = get_program(datetime.now().hour)
            if new_program != current_program:
                log.info("Cambio fascia: %s → %s", current_program, new_program)
                current_program = new_program

            raw_title = fetch_title()
            if not raw_title:
                log.warning("Titolo non trovato nella risposta, attendo %ds", RETRY_INTERVAL)
                time.sleep(RETRY_INTERVAL)
                continue

            if raw_title.startswith("|"):
                log.debug("Ignorato (cortina/metadati): '%s'", raw_title)
                time.sleep(NORMAL_INTERVAL)
                continue

            last = get_last_raw_title(conn)
            if raw_title == last:
                log.info("Invariato: '%s' — retry tra %ds", raw_title, RETRY_INTERVAL)
                time.sleep(RETRY_INTERVAL)
                continue

            now    = datetime.now()
            parsed = parse_track(raw_title)
            log.info("Raw: '%s'", raw_title)
            if not parsed.get('orchestra') or not parsed.get('track_title'):
                log.warning("Parsing degradato: raw='%s' parsed=%s", raw_title, parsed)
            if (parsed.get('orchestra') or '').upper() in JINGLE_ORCHESTRAS:
                log.debug("Ignorato (jingle): '%s'", raw_title)
                time.sleep(NORMAL_INTERVAL)
                continue
            insert_track(conn, raw_title, now, parsed)
            log.info("Salvato [%s] %s / %s",
                     get_program(now.hour),
                     parsed.get('orchestra', '?'),
                     parsed.get('track_title', '?'))

        except requests.RequestException as exc:
            log.error("Errore HTTP: %s — retry tra %ds", exc, RETRY_INTERVAL)
            time.sleep(RETRY_INTERVAL)
            continue
        except Exception as exc:
            log.exception("Errore imprevisto: %s", exc)

        time.sleep(NORMAL_INTERVAL)


if __name__ == "__main__":
    main()
