#!/usr/bin/env python3
"""
Migrazione del vecchio tracks.db (schema: id, title, fetched_at)
al nuovo schema con campi parsati.

Uso:
  python convert.py [--db /path/to/tracks.db]

Sicuro: crea una copia di backup (.bak) prima di procedere.
"""
import argparse
import os
import shutil
import sqlite3
from datetime import datetime

from common import get_program, parse_track

DB_PATH = os.getenv("DB_PATH", "data/tracks.db")


def has_new_schema(conn: sqlite3.Connection) -> bool:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(tracks)")}
    return 'raw_title' in cols


def migrate(db_path: str) -> None:
    if not os.path.exists(db_path):
        print(f"Errore: file non trovato: {db_path}")
        return

    conn = sqlite3.connect(db_path)

    if has_new_schema(conn):
        print("Il database ha già il nuovo schema. Nulla da fare.")
        conn.close()
        return

    bak = db_path + ".bak"
    shutil.copy2(db_path, bak)
    print(f"Backup creato: {bak}")

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
    conn.close()

    migrated = len(rows) - skipped
    print(f"Migrazione completata: {migrated} record convertiti, {skipped} cortine/metadati scartati.")
    print(f"Backup disponibile in: {bak}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migra tracks.db al nuovo schema parsato")
    parser.add_argument("--db", default=DB_PATH, help="Percorso al database")
    args = parser.parse_args()
    migrate(args.db)


if __name__ == "__main__":
    main()
