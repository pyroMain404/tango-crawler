#!/usr/bin/env python3
"""
Normalizza tracks.db → tango.db con schema relazionale completo.
Svuota tracks.db SOLO se tutti i record sono stati scritti correttamente.

Da eseguire una volta al giorno (es. alle 06:00).

Uso:
  python normalize.py [--source /data/tracks.db] [--dest /data/tango.db]

Schema tango.db:
  orchestras   (id, name)
  singers      (id, name)
  titles       (id, name)
  programs     (id, name, start_hour, end_hour)
  plays        (id, orchestra_id, title_id, year, author, dancers, program_id, fetched_at)
  play_singers (play_id, singer_id)   ← N:M per cantanti multipli

Query utili:
  -- Orchestre più suonate
  SELECT o.name, COUNT(*) n FROM plays p JOIN orchestras o ON o.id=p.orchestra_id
  GROUP BY o.id ORDER BY n DESC LIMIT 20;

  -- Tutti i brani di Di Sarli
  SELECT t.name, p.year, p.fetched_at FROM plays p
  JOIN orchestras o ON o.id=p.orchestra_id
  JOIN titles t ON t.id=p.title_id
  WHERE o.name LIKE '%DI SARLI%' ORDER BY p.fetched_at;

  -- Cantanti di Troilo
  SELECT DISTINCT s.name FROM plays p
  JOIN orchestras o ON o.id=p.orchestra_id
  JOIN play_singers ps ON ps.play_id=p.id
  JOIN singers s ON s.id=ps.singer_id
  WHERE o.name LIKE '%TROILO%';

  -- Passaggi per fascia di palinsesto
  SELECT pr.name, COUNT(*) FROM plays p JOIN programs pr ON pr.id=p.program_id
  GROUP BY pr.id ORDER BY 2 DESC;
"""
import argparse
import os
import re
import sqlite3
import sys

from common import DEFAULT_PROGRAM, PROGRAMS

SOURCE_DB  = os.getenv("DB_PATH",       "/data/tracks.db")
DEST_DB    = os.getenv("NORMALIZED_DB", "/data/tango.db")

# Seed: PROGRAMS from common + the default/filler slot
PROGRAMS_SEED = [(name, start, end) for start, end, name in PROGRAMS] + [(DEFAULT_PROGRAM, 0, 0)]

DDL = """
CREATE TABLE IF NOT EXISTS orchestras (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS singers (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS titles (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS programs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL UNIQUE,
    start_hour INTEGER,
    end_hour   INTEGER
);
CREATE TABLE IF NOT EXISTS plays (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    orchestra_id INTEGER REFERENCES orchestras(id),
    title_id     INTEGER REFERENCES titles(id),
    year         INTEGER,
    author       TEXT,
    dancers      TEXT,
    program_id   INTEGER REFERENCES programs(id),
    fetched_at   TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS play_singers (
    play_id   INTEGER NOT NULL REFERENCES plays(id) ON DELETE CASCADE,
    singer_id INTEGER NOT NULL REFERENCES singers(id),
    PRIMARY KEY (play_id, singer_id)
);
CREATE TABLE IF NOT EXISTS playlists (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now', 'localtime'))
);
CREATE TABLE IF NOT EXISTS playlist_items (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    playlist_id  INTEGER NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
    orchestra_id INTEGER REFERENCES orchestras(id),
    title_id     INTEGER REFERENCES titles(id),
    position     INTEGER NOT NULL DEFAULT 0,
    note         TEXT
);
CREATE INDEX IF NOT EXISTS idx_playlist_items_playlist ON playlist_items(playlist_id);
CREATE INDEX IF NOT EXISTS idx_plays_orchestra ON plays(orchestra_id);
CREATE INDEX IF NOT EXISTS idx_plays_title     ON plays(title_id);
CREATE INDEX IF NOT EXISTS idx_plays_year      ON plays(year);
CREATE INDEX IF NOT EXISTS idx_plays_program   ON plays(program_id);
CREATE INDEX IF NOT EXISTS idx_plays_fetched   ON plays(fetched_at);
CREATE VIEW IF NOT EXISTS repertoire AS
    SELECT
        o.name                      AS orchestra,
        t.name                      AS title,
        MIN(p.year)                 AS year,
        p.author                    AS author,
        COUNT(*)                    AS times_played,
        MAX(p.fetched_at)           AS last_seen
    FROM plays p
    JOIN orchestras o ON o.id = p.orchestra_id
    JOIN titles     t ON t.id = p.title_id
    GROUP BY p.orchestra_id, p.title_id;
"""


_id_cache: dict[tuple[str, str], int] = {}


def get_or_create(conn: sqlite3.Connection, table: str, col: str, value: str | None) -> int | None:
    if not value:
        return None
    key = (table, value)
    if key in _id_cache:
        return _id_cache[key]
    row = conn.execute(f"SELECT id FROM {table} WHERE {col} = ?", (value,)).fetchone()
    if row:
        _id_cache[key] = row[0]
        return row[0]
    rowid = conn.execute(f"INSERT INTO {table} ({col}) VALUES (?)", (value,)).lastrowid
    _id_cache[key] = rowid
    return rowid


def get_program_id(conn: sqlite3.Connection, name: str | None) -> int | None:
    if not name:
        return None
    row = conn.execute("SELECT id FROM programs WHERE name = ?", (name,)).fetchone()
    return row[0] if row else None


def normalize(source_path: str, dest_path: str) -> None:
    src  = sqlite3.connect(source_path)
    dest = sqlite3.connect(dest_path)
    dest.execute("PRAGMA foreign_keys = ON")

    dest.executescript(DDL)
    for name, start, end in PROGRAMS_SEED:
        dest.execute(
            "INSERT OR IGNORE INTO programs (name, start_hour, end_hour) VALUES (?, ?, ?)",
            (name, start, end),
        )
    dest.commit()

    rows = src.execute("""
        SELECT orchestra, singer, track_title, year, author, dancers, program, fetched_at
        FROM   tracks
        ORDER  BY id
    """).fetchall()

    total    = len(rows)
    inserted = 0
    skipped  = 0

    if total == 0:
        print("Nessun record da normalizzare.")
        src.close()
        dest.close()
        return

    print(f"Record da normalizzare: {total}")

    try:
        for (orchestra, singer, track_title, year, author,
             dancers, program, fetched_at) in rows:

            orchestra_id = get_or_create(dest, "orchestras", "name", orchestra)
            title_id     = get_or_create(dest, "titles",     "name", track_title)
            program_id   = get_program_id(dest, program)

            cur = dest.execute("""
                INSERT OR IGNORE INTO plays
                    (orchestra_id, title_id, year, author, dancers, program_id, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (orchestra_id, title_id, year, author, dancers, program_id, fetched_at))

            play_id = cur.lastrowid
            if not play_id:
                skipped += 1
                continue

            if singer:
                for s in re.split(r',\s*', singer):
                    s = s.strip()
                    if s:
                        singer_id = get_or_create(dest, "singers", "name", s)
                        dest.execute(
                            "INSERT OR IGNORE INTO play_singers (play_id, singer_id) VALUES (?, ?)",
                            (play_id, singer_id),
                        )
            inserted += 1

        dest.commit()

    except Exception as exc:
        dest.rollback()
        src.close()
        dest.close()
        print(f"ERRORE durante la normalizzazione: {exc}", file=sys.stderr)
        print("tracks.db NON è stato svuotato — i dati sono al sicuro.", file=sys.stderr)
        sys.exit(1)

    # Verifica: tutti i record processati (inseriti + già presenti) == totale atteso
    processed = inserted + skipped
    if processed != total:
        src.close()
        dest.close()
        print(
            f"ATTENZIONE: processati {processed}/{total} record. "
            "tracks.db NON è stato svuotato.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Solo ora è sicuro svuotare tracks.db
    src.execute("DELETE FROM tracks")
    src.commit()
    src.close()

    # VACUUM richiede una connessione senza transazioni aperte
    vac = sqlite3.connect(source_path, isolation_level=None)
    vac.execute("VACUUM")
    vac.close()
    dest.close()
    print(f"OK: {inserted} inseriti, {skipped} già presenti. tracks.db svuotato.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalizza tracks.db → tango.db")
    parser.add_argument("--source", default=SOURCE_DB)
    parser.add_argument("--dest",   default=DEST_DB)
    args = parser.parse_args()
    normalize(args.source, args.dest)


if __name__ == "__main__":
    main()
