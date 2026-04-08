#!/usr/bin/env python3
"""
Gestione tango.db — normalizzazione, analisi titoli, confini palinsesto.

Comandi:
  python normalize.py                                  # ingest (default)
  python normalize.py ingest [--source X] [--dest Y]   # normalizza tracks.db → tango.db
  python normalize.py similar-titles [--threshold 0.8] [--limit N]
  python normalize.py boundary [--minutes 5] [--limit N]
"""
import argparse
import difflib
import os
import re
import sqlite3
import sys
from datetime import datetime

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


def similar_titles(dest_path: str, threshold: float, limit: int) -> None:
    conn = sqlite3.connect(dest_path)
    rows = conn.execute("SELECT id, name FROM titles ORDER BY name").fetchall()
    conn.close()

    if not rows:
        print("Nessun titolo nel database.")
        return

    pairs = []
    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            ratio = difflib.SequenceMatcher(None, rows[i][1], rows[j][1]).ratio()
            if ratio >= threshold:
                pairs.append((rows[i][1], rows[j][1], ratio))

    pairs.sort(key=lambda x: x[2], reverse=True)
    if limit:
        pairs = pairs[:limit]

    if not pairs:
        print(f"Nessun titolo simile con soglia {threshold}.")
        return

    for a, b, ratio in pairs:
        print(f'  "{a}" ~ "{b}"  ({ratio:.2f})')
    print(f"\n{len(pairs)} coppie trovate.")


def boundary_tracks(dest_path: str, minutes: int, limit: int) -> None:
    # Calcola le ore di confine dalle fasce di palinsesto
    boundaries = set()
    for start, end, _ in PROGRAMS:
        boundaries.add(start)
        boundaries.add(end)

    conn = sqlite3.connect(dest_path)

    # Per ogni confine, cerca brani entro N minuti prima/dopo
    conditions = []
    for h in sorted(boundaries):
        # Ultimi N minuti prima del confine (h-1):MM >= 60-minutes
        prev_h = (h - 1) % 24
        conditions.append(
            f"(CAST(strftime('%H', p.fetched_at) AS INT) = {prev_h} "
            f"AND CAST(strftime('%M', p.fetched_at) AS INT) >= {60 - minutes})"
        )
        # Primi N minuti dopo il confine (h):MM < minutes
        conditions.append(
            f"(CAST(strftime('%H', p.fetched_at) AS INT) = {h} "
            f"AND CAST(strftime('%M', p.fetched_at) AS INT) < {minutes})"
        )

    where = " OR ".join(conditions)
    limit_clause = f" LIMIT {limit}" if limit else ""
    rows = conn.execute(f"""
        SELECT p.fetched_at, o.name, t.name, p.year, pr.name,
               CAST(strftime('%H', p.fetched_at) AS INT) AS hour,
               CAST(strftime('%M', p.fetched_at) AS INT) AS min
        FROM plays p
        LEFT JOIN orchestras o  ON o.id = p.orchestra_id
        LEFT JOIN titles t      ON t.id = p.title_id
        LEFT JOIN programs pr   ON pr.id = p.program_id
        WHERE {where}
        ORDER BY p.fetched_at{limit_clause}
    """).fetchall()
    conn.close()

    if not rows:
        print(f"Nessun brano a cavallo delle fasce (±{minutes} min).")
        return

    for row in rows:
        fetched_at, orchestra, title, year, program, h, m = row
        year_str = f" ({year})" if year else ""
        slot = f"[{program}]  " if program else ""
        print(f"  {fetched_at}  {slot}{orchestra or '?'} — {title or '?'}{year_str}")
    print(f"\n{len(rows)} brani a cavallo delle fasce.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Gestione tango.db")
    sub = parser.add_subparsers(dest="command")

    # ingest (default)
    p_ingest = sub.add_parser("ingest", help="Normalizza tracks.db → tango.db")
    p_ingest.add_argument("--source", default=SOURCE_DB)
    p_ingest.add_argument("--dest",   default=DEST_DB)

    # similar-titles
    p_similar = sub.add_parser("similar-titles", help="Trova titoli simili")
    p_similar.add_argument("--dest",      default=DEST_DB)
    p_similar.add_argument("--threshold", type=float, default=0.8,
                           help="Soglia di similarità 0.0-1.0 (default: 0.8)")
    p_similar.add_argument("--limit", type=int, default=0,
                           help="Limita il numero di risultati")

    # boundary
    p_boundary = sub.add_parser("boundary", help="Brani a cavallo delle fasce di palinsesto")
    p_boundary.add_argument("--dest",    default=DEST_DB)
    p_boundary.add_argument("--minutes", type=int, default=5,
                            help="Minuti di margine dal confine (default: 5)")
    p_boundary.add_argument("--limit", type=int, default=0,
                            help="Limita il numero di risultati")

    args = parser.parse_args()

    if args.command is None or args.command == "ingest":
        source = getattr(args, "source", SOURCE_DB)
        dest   = getattr(args, "dest",   DEST_DB)
        normalize(source, dest)
    elif args.command == "similar-titles":
        similar_titles(args.dest, args.threshold, args.limit)
    elif args.command == "boundary":
        boundary_tracks(args.dest, args.minutes, args.limit)


if __name__ == "__main__":
    main()
