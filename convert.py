#!/usr/bin/env python3
"""
Manutenzione di tracks.db — operazioni in-place senza ricreare lo schema.

Uso:
  python convert.py [--db /path/to/tracks.db] [--fix-tz] [--reparse] [--all]

  (senza flag) → rimuove cortine dal DB già migrato, oppure migra il vecchio schema
  --fix-tz     → corregge i timestamp +2h (record pre-TZ=Europe/Rome)
  --reparse    → ricalcola orchestra/singer/track_title/year/author/dancers/program
                 da raw_title usando il parser attuale
  --all        → esegue tutto in sequenza: fix-tz, reparse, pulizia cortine

Sicuro: crea una copia di backup (.bak) prima di qualsiasi operazione distruttiva.
"""
import argparse
import os
import shutil
import sqlite3
from datetime import datetime, timedelta

from common import get_program, parse_track

DB_PATH = os.getenv("DB_PATH", os.path.expanduser("~/.local/share/tango-crawler/tracks.db"))


def has_new_schema(conn: sqlite3.Connection) -> bool:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(tracks)")}
    return 'raw_title' in cols


def backup(db_path: str) -> None:
    bak = db_path + ".bak"
    shutil.copy2(db_path, bak)
    print(f"Backup creato: {bak}")


# ── Operazioni in-place ───────────────────────────────────────────────────────

def remove_cortine(conn: sqlite3.Connection) -> None:
    """Rimuove le righe con raw_title che inizia per '|'."""
    cur = conn.execute("DELETE FROM tracks WHERE raw_title LIKE '|%'")
    conn.commit()
    print(f"Cortine rimosse: {cur.rowcount}")


def fix_timezone(conn: sqlite3.Connection) -> None:
    """Aggiunge +2h ai timestamp registrati in UTC (pre TZ=Europe/Rome)."""
    rows = conn.execute("SELECT id, fetched_at FROM tracks ORDER BY id").fetchall()
    updated = 0
    for row_id, fetched_at in rows:
        try:
            dt = datetime.fromisoformat(fetched_at)
        except ValueError:
            continue
        # Euristia: se l'ora suggerisce UTC (es. programmi notturni alle 22-24
        # che appaiono alle 20-22), non c'è modo di saperlo con certezza.
        # L'operazione va eseguita UNA SOLA VOLTA su tutti i record pre-fix.
        new_ts = (dt + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")
        conn.execute("UPDATE tracks SET fetched_at = ? WHERE id = ?", (new_ts, row_id))
        updated += 1
    conn.commit()
    print(f"Timestamp corretti: {updated}")


def reparse(conn: sqlite3.Connection) -> None:
    """Ricalcola tutti i campi parsati da raw_title usando il parser attuale."""
    rows = conn.execute("SELECT id, raw_title, fetched_at FROM tracks ORDER BY id").fetchall()
    for row_id, raw_title, fetched_at in rows:
        p = parse_track(raw_title)
        try:
            hour = datetime.fromisoformat(fetched_at).hour
        except ValueError:
            hour = 0
        conn.execute("""
            UPDATE tracks SET
                orchestra   = ?,
                singer      = ?,
                track_title = ?,
                year        = ?,
                author      = ?,
                dancers     = ?,
                program     = ?
            WHERE id = ?
        """, (p['orchestra'], p['singer'], p['track_title'],
              p['year'], p['author'], p['dancers'],
              get_program(hour), row_id))
    conn.commit()
    print(f"Record ri-parsati: {len(rows)}")


# ── Migrazione vecchio schema ─────────────────────────────────────────────────

def migrate_schema(conn: sqlite3.Connection, db_path: str) -> None:
    backup(db_path)
    rows = conn.execute(
        "SELECT id, title, fetched_at FROM tracks ORDER BY id"
    ).fetchall()
    print(f"Record da migrare: {len(rows)}")

    conn.execute("""
        CREATE TABLE tracks_new (
            id          INTEGER PRIMARY KEY,
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

    skipped = 0
    for row_id, title, fetched_at in rows:
        if title.startswith("|"):
            skipped += 1
            continue
        p = parse_track(title)
        try:
            hour = datetime.fromisoformat(fetched_at).hour
        except ValueError:
            hour = 0
        conn.execute("""
            INSERT INTO tracks_new
                (id, raw_title, orchestra, singer, track_title, year, author, dancers, program, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (row_id, title, p['orchestra'], p['singer'], p['track_title'],
              p['year'], p['author'], p['dancers'], get_program(hour), fetched_at))

    conn.execute("DROP TABLE tracks")
    conn.execute("ALTER TABLE tracks_new RENAME TO tracks")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fetched_at ON tracks (fetched_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orchestra  ON tracks (orchestra)")
    conn.commit()

    migrated = len(rows) - skipped
    print(f"Migrazione completata: {migrated} record, {skipped} cortine scartate.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Manutenzione di tracks.db")
    parser.add_argument("--db",      default=DB_PATH, help="Percorso al database (default: tracks.db)")
    parser.add_argument("--fix-tz",  action="store_true", help="Correggi timestamp +2h (eseguire UNA SOLA VOLTA)")
    parser.add_argument("--reparse", action="store_true", help="Ri-parsa raw_title con il parser attuale")
    parser.add_argument("--all",     action="store_true", help="fix-tz + reparse + pulizia cortine")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"Errore: file non trovato: {args.db}")
        return

    conn = sqlite3.connect(args.db)

    if not has_new_schema(conn):
        migrate_schema(conn, args.db)
        conn.close()
        return

    # DB già al nuovo schema: operazioni in-place
    needs_backup = args.fix_tz or args.all
    if needs_backup:
        backup(args.db)

    if args.all or args.fix_tz:
        fix_timezone(conn)

    if args.all or args.reparse:
        reparse(conn)

    # Pulizia cortine sempre (incluso il caso senza flag)
    remove_cortine(conn)

    conn.close()


if __name__ == "__main__":
    main()
