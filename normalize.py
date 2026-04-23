#!/usr/bin/env python3
"""
Gestione tango.db — normalizzazione, analisi titoli, confini palinsesto.

Interfaccia principale:
  tango db normalize      → ingest tracks.db → tango.db
  tango db purge          → elimina jingle ed errori da entrambi i DB
  tango analyze similar   → trova titoli simili
  tango analyze boundary  → brani a cavallo delle fasce di palinsesto

Comandi diretti (avanzato):
  python normalize.py                                  # ingest (default)
  python normalize.py ingest [--source X] [--dest Y]
  python normalize.py similar-titles [--threshold 0.8] [--limit N]
  python normalize.py boundary [--minutes 5] [--limit N]
  python normalize.py purge [--source X] [--dest Y]
"""
import argparse
from collections import defaultdict
import difflib
import os
import re
import sqlite3
import sys

from common import DEFAULT_PROGRAM, JINGLE_ORCHESTRAS, PROGRAMS, canonicalize_title

SOURCE_DB  = os.getenv("DB_PATH",       "/data/tracks.db")
DEST_DB    = os.getenv("NORMALIZED_DB", "/data/tango.db")

_DEDUP_PUNCT_RE = re.compile(r"[`',;:_\-]+")  # no punto: le abbreviazioni (C.DANTE) restano distinte
_MULTI_SPACE_RE = re.compile(r" {2,}")


def normalize_for_dedup(name: str) -> str:
    """Forma canonica per confronto dedup.

    Applica prima canonicalize_title (strip trailing punct incluso '.'),
    poi rimuove backtick, virgole e caratteri speciali interni, infine
    collassa spazi multipli. Il punto interno NON viene toccato: così
    'C. DANTE' e 'C.DANTE' rimangono distinti e non vengono mai uniti.
    """
    name = canonicalize_title(name)      # strip trailing punct (incluso '.')
    name = _DEDUP_PUNCT_RE.sub(" ", name)
    name = _MULTI_SPACE_RE.sub(" ", name)
    return name.strip()

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
CREATE INDEX IF NOT EXISTS idx_play_singers_singer     ON play_singers(singer_id);
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

            if not orchestra or not track_title:
                skipped += 1
                continue
            if orchestra.upper() in JINGLE_ORCHESTRAS:
                skipped += 1
                continue

            orchestra_id = get_or_create(dest, "orchestras", "name", orchestra)
            title_id     = get_or_create(dest, "titles",     "name", canonicalize_title(track_title))
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

    max_len_diff = 1 - threshold
    sm = difflib.SequenceMatcher(autojunk=False)
    pairs = []
    for i in range(len(rows)):
        sm.set_seq1(rows[i][1])
        len_a = len(rows[i][1])
        for j in range(i + 1, len(rows)):
            len_b = len(rows[j][1])
            longer = max(len_a, len_b)
            if longer and abs(len_a - len_b) / longer > max_len_diff:
                continue
            sm.set_seq2(rows[j][1])
            ratio = sm.ratio()
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


# ── Dedup helpers ─────────────────────────────────────────────────────────────

def _find_dedup_groups(items: list[tuple]) -> list[tuple]:
    """Raggruppa gli item per forma normalizzata (punteggiatura ignorata).

    items: (id, name, count, ...).
    Restituisce (canon_id, canon_name, canon_clean, dup_id, dup_name) per ogni dup.
    Due nomi che differiscono solo per punteggiatura/spazi sono uniti;
    parole effettivamente diverse non vengono mai toccate.
    """
    by_norm: dict[str, list[tuple]] = defaultdict(list)
    for item in items:
        by_norm[normalize_for_dedup(item[1])].append(item)
    merges: list[tuple] = []
    for group in by_norm.values():
        if len(group) < 2:
            continue
        canon_idx  = max(range(len(group)), key=lambda i: group[i][2])
        canon_id   = group[canon_idx][0]
        canon_name = group[canon_idx][1]
        canon_clean = canonicalize_title(canon_name)
        for i, item in enumerate(group):
            if i == canon_idx:
                continue
            merges.append((canon_id, canon_name, canon_clean, item[0], item[1]))
    return merges


def _apply_merges(
    conn: sqlite3.Connection,
    table: str,
    fk_updates: list[tuple[str, str]],
    merges: list[tuple],        # (canon_id, canon_name, canon_clean, dup_id, dup_name)
    prefix_fn=None,             # callable(canon_id) → str prefix per display
) -> int:
    merged = 0
    canon_redirect: dict[int, int] = {}
    already_merged: set[int] = set()
    for canon_id, canon_name, canon_clean, dup_id, dup_name in merges:
        if dup_id in already_merged:
            continue
        effective = canon_redirect.get(canon_id, canon_id)
        prefix = prefix_fn(canon_id) if prefix_fn else "  "
        if canon_clean != canon_name and canon_id not in canon_redirect:
            existing = conn.execute(
                f"SELECT id FROM {table} WHERE name = ? AND id != ?",
                (canon_clean, effective),
            ).fetchone()
            if existing:
                real_id = existing[0]
                for ft, fc in fk_updates:
                    conn.execute(f"UPDATE {ft} SET {fc} = ? WHERE {fc} = ?", (real_id, effective))
                conn.execute(f"DELETE FROM {table} WHERE id = ?", (effective,))
                already_merged.add(effective)
                canon_redirect[canon_id] = real_id
                effective = real_id
                print(f"{prefix}{canon_name!r} → {canon_clean!r}  (unito a esistente)")
            else:
                conn.execute(f"UPDATE {table} SET name = ? WHERE id = ?", (canon_clean, effective))
                canon_redirect[canon_id] = effective
        if dup_id == effective or dup_id in already_merged:
            continue
        for ft, fc in fk_updates:
            conn.execute(f"UPDATE {ft} SET {fc} = ? WHERE {fc} = ?", (effective, dup_id))
        conn.execute(f"DELETE FROM {table} WHERE id = ?", (dup_id,))
        already_merged.add(dup_id)
        merged += 1
        label = canon_clean if canon_clean != canon_name else canon_name
        print(f"{prefix}{dup_name!r} → {label!r}")
    return merged


# ── Dedup per entità ───────────────────────────────────────────────────────────

_DEDUP_CONFIG: dict[str, dict] = {
    "orchestras": {
        "table":    "orchestras",
        "label":    "orchestre",
        "count_sql": """
            SELECT o.id, o.name, COUNT(p.id)
            FROM orchestras o LEFT JOIN plays p ON p.orchestra_id = o.id
            GROUP BY o.id ORDER BY o.name
        """,
        "fk_updates": [("plays", "orchestra_id"), ("playlist_items", "orchestra_id")],
    },
    "singers": {
        "table":    "singers",
        "label":    "cantanti",
        "count_sql": """
            SELECT s.id, s.name, COUNT(ps.play_id)
            FROM singers s LEFT JOIN play_singers ps ON ps.singer_id = s.id
            GROUP BY s.id ORDER BY s.name
        """,
        "fk_updates": [("play_singers", "singer_id")],
    },
    "programs": {
        "table":    "programs",
        "label":    "programmi",
        "count_sql": """
            SELECT p.id, p.name, COUNT(pl.id)
            FROM programs p LEFT JOIN plays pl ON pl.program_id = p.id
            GROUP BY p.id ORDER BY p.name
        """,
        "fk_updates": [("plays", "program_id")],
    },
}


def dedup_global(dest_path: str, entity: str, apply: bool) -> list[tuple]:
    cfg = _DEDUP_CONFIG[entity]
    conn = sqlite3.connect(dest_path)
    conn.execute("PRAGMA foreign_keys = ON")
    rows   = conn.execute(cfg["count_sql"]).fetchall()
    merges = _find_dedup_groups(list(rows))
    if not merges:
        if not apply:
            print(f"Nessun duplicato trovato per {cfg['label']}.")
        conn.close()
        return []
    if not apply:
        print(f"{'Canonico':<50} {'Duplicato':<50}")
        print("-" * 105)
        for _, cn, cc, _, dn in merges:
            label = cc if cc != cn else cn
            print(f"  {label!r:<48} ← {dn!r:<48}")
        print(f"\n{len(merges)} duplicati trovati. Usa --apply per eseguire la merge.")
        conn.close()
        return [(cn, cc, dn) for _, cn, cc, _, dn in merges]
    try:
        n = _apply_merges(conn, cfg["table"], cfg["fk_updates"], merges)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        conn.close()
        print(f"ERRORE durante la deduplicazione ({entity}): {exc}", file=sys.stderr)
        sys.exit(1)
    conn.close()
    print(f"\n{n} {cfg['label']} duplicate unite.")
    return [(cn, cc, dn) for _, cn, cc, _, dn in merges]


def dedup_titles(dest_path: str, apply: bool) -> list[tuple]:
    conn = sqlite3.connect(dest_path)
    conn.execute("PRAGMA foreign_keys = ON")
    rows = conn.execute("""
        SELECT o.id, o.name, t.id, t.name, COUNT(p.id)
        FROM plays p
        JOIN orchestras o ON o.id = p.orchestra_id
        JOIN titles t     ON t.id = p.title_id
        GROUP BY o.id, t.id ORDER BY o.name, t.name
    """).fetchall()
    by_orch: dict[int, tuple[str, dict]] = {}
    for orch_id, orch_name, tid, tname, cnt in rows:
        if orch_id not in by_orch:
            by_orch[orch_id] = (orch_name, {})
        by_orch[orch_id][1].setdefault(tname, (tid, tname, cnt))
    # (orch_name, canon_id, canon_name, canon_clean, dup_id, dup_name)
    all_merges: list[tuple] = []
    for orch_id, (orch_name, seen) in by_orch.items():
        for cm in _find_dedup_groups(list(seen.values())):
            all_merges.append((orch_name, *cm))
    if not all_merges:
        if not apply:
            print("Nessun titolo duplicato trovato.")
        conn.close()
        return []
    if not apply:
        print(f"{'Orchestra':<35} {'Canonico':<40} {'Duplicato':<40}")
        print("-" * 120)
        for orch_name, _, cn, cc, _, dn in all_merges:
            label = cc if cc != cn else cn
            print(f"  [{orch_name:<33}] {label!r:<40} ← {dn!r:<40}")
        print(f"\n{len(all_merges)} duplicati trovati. Usa --apply per eseguire la merge.")
        conn.close()
        return [(cn, cc, dn, o) for o, _, cn, cc, _, dn in all_merges]
    orch_of: dict[int, str] = {m[1]: m[0] for m in all_merges}
    plain = [m[1:] for m in all_merges]
    try:
        n = _apply_merges(
            conn, "titles",
            [("plays", "title_id"), ("playlist_items", "title_id")],
            plain,
            prefix_fn=lambda cid: f"  [{orch_of.get(cid, ''):<33}] ",
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        conn.close()
        print(f"ERRORE durante la deduplicazione dei titoli: {exc}", file=sys.stderr)
        sys.exit(1)
    conn.close()
    print(f"\n{n} titoli duplicati uniti.")
    return [(cn, cc, dn, o) for o, _, cn, cc, _, dn in all_merges]


def dedup(dest_path: str, target: str, apply: bool) -> None:
    targets = ["orchestras", "titles", "singers"] if target == "all" else [target]
    for t in targets:
        if len(targets) > 1:
            print(f"\n── {t} ──")
        if t == "titles":
            dedup_titles(dest_path, apply)
        else:
            dedup_global(dest_path, t, apply)


# ── fix-abbrev ────────────────────────────────────────────────────────────────

_ABBREV_RE = re.compile(r'\.(?=[A-ZÁÉÍÓÚÀÈÌÒÙÑÜ])')

_ABBREV_TABLES = [
    ("orchestras", "name", [("plays", "orchestra_id"), ("playlist_items", "orchestra_id")]),
    ("titles",     "name", [("plays", "title_id"),     ("playlist_items", "title_id")]),
    ("singers",    "name", [("play_singers", "singer_id")]),
]


def _fix_abbrev_name(name: str) -> str:
    """'C.DANTE' → 'C. DANTE': aggiunge spazio dopo punto in abbreviazioni."""
    return _ABBREV_RE.sub('. ', name)


def fix_abbrev_spaces(dest_path: str, target: str, apply: bool) -> None:
    """Normalizza 'X.LETTERA' → 'X. LETTERA' in orchestras, titles, singers.

    Gestisce conflitti UNIQUE: se la forma corretta esiste già, redirige i
    riferimenti e cancella la voce errata (come il dedup).
    """
    conn = sqlite3.connect(dest_path)
    conn.execute("PRAGMA foreign_keys = ON")

    total_changes = 0
    tables = _ABBREV_TABLES if target == "all" else [t for t in _ABBREV_TABLES if t[0] == target]

    for table, col, fk_updates in tables:
        rows = conn.execute(f"SELECT id, {col} FROM {table} ORDER BY {col}").fetchall()
        candidates = [(rid, name, _fix_abbrev_name(name)) for rid, name in rows
                      if _fix_abbrev_name(name) != name]
        if not candidates:
            continue

        if not apply:
            print(f"\n── {table} ──")
            for _, name, fixed in candidates:
                print(f"  {name!r}  →  {fixed!r}")
            total_changes += len(candidates)
            continue

        print(f"\n── {table} ──")
        try:
            for rid, name, fixed in candidates:
                existing = conn.execute(
                    f"SELECT id FROM {table} WHERE {col} = ? AND id != ?",
                    (fixed, rid),
                ).fetchone()
                if existing:
                    real_id = existing[0]
                    for ft, fc in fk_updates:
                        conn.execute(f"UPDATE {ft} SET {fc} = ? WHERE {fc} = ?", (real_id, rid))
                    conn.execute(f"DELETE FROM {table} WHERE id = ?", (rid,))
                    print(f"  {name!r} → {fixed!r}  (unito a esistente)")
                else:
                    conn.execute(f"UPDATE {table} SET {col} = ? WHERE id = ?", (fixed, rid))
                    print(f"  {name!r} → {fixed!r}")
                total_changes += 1
            conn.commit()
        except Exception as exc:
            conn.rollback()
            conn.close()
            print(f"ERRORE ({table}): {exc}", file=sys.stderr)
            sys.exit(1)

    if not apply:
        if total_changes:
            print(f"\n{total_changes} nomi da correggere. Usa --apply per applicare.")
        else:
            print("Nessuna abbreviazione da correggere.")
    else:
        print(f"\n{total_changes} nomi corretti.")

    conn.close()


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
        SELECT p.fetched_at, o.name, t.name, p.year, pr.name
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
        fetched_at, orchestra, title, year, program = row
        year_str = f" ({year})" if year else ""
        slot = f"[{program}]  " if program else ""
        print(f"  {fetched_at}  {slot}{orchestra or '?'} — {title or '?'}{year_str}")
    print(f"\n{len(rows)} brani a cavallo delle fasce.")


def purge(source_path: str, dest_path: str) -> None:
    jingles = tuple(JINGLE_ORCHESTRAS)
    placeholders = ",".join("?" * len(jingles))

    # --- tracks.db ---
    src = sqlite3.connect(source_path)
    cur = src.execute(
        f"DELETE FROM tracks WHERE UPPER(orchestra) IN ({placeholders}) "
        "OR orchestra IS NULL OR track_title IS NULL",
        jingles,
    )
    n_tracks = cur.rowcount
    src.commit()
    src.close()

    # --- tango.db ---
    dest = sqlite3.connect(dest_path)
    dest.execute("PRAGMA foreign_keys = ON")

    dest.execute(
        f"DELETE FROM plays WHERE orchestra_id IN ("
        f"  SELECT id FROM orchestras WHERE UPPER(name) IN ({placeholders})"
        f")",
        jingles,
    )
    dest.execute("DELETE FROM plays WHERE title_id IS NULL OR orchestra_id IS NULL")

    dest.execute(
        "DELETE FROM orchestras WHERE id NOT IN ("
        "  SELECT orchestra_id FROM plays        WHERE orchestra_id IS NOT NULL"
        "  UNION"
        "  SELECT orchestra_id FROM playlist_items WHERE orchestra_id IS NOT NULL"
        ")"
    )
    dest.execute(
        "DELETE FROM titles WHERE id NOT IN ("
        "  SELECT title_id FROM plays          WHERE title_id IS NOT NULL"
        "  UNION"
        "  SELECT title_id FROM playlist_items WHERE title_id IS NOT NULL"
        ")"
    )
    dest.execute(
        "DELETE FROM singers WHERE id NOT IN (SELECT singer_id FROM play_singers)"
    )
    dest.commit()
    dest.close()

    print(f"tracks.db: {n_tracks} righe eliminate.")
    print("tango.db: jingle e brani incompleti eliminati, entità orfane ripulite.")


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

    # purge
    p_purge = sub.add_parser("purge", help="Elimina jingle ed errori di parsing da entrambi i DB")
    p_purge.add_argument("--source", default=SOURCE_DB)
    p_purge.add_argument("--dest",   default=DEST_DB)

    # dedup
    p_dedup = sub.add_parser("dedup", help="Unisce entità quasi-duplicate")
    p_dedup.add_argument("target",
                         choices=["orchestras", "titles", "singers", "programs", "all"],
                         help="Entità da deduplicare")
    p_dedup.add_argument("--dest",      default=DEST_DB)
    p_dedup.add_argument("--apply",     action="store_true",
                         help="Esegui la merge (default: dry-run)")

    # fix-abbrev
    p_abbrev = sub.add_parser("fix-abbrev",
                              help="Normalizza 'X.LETTERA' → 'X. LETTERA' in orchestre/titoli/cantanti")
    p_abbrev.add_argument("target",
                          choices=["orchestras", "titles", "singers", "all"],
                          help="Tabella da correggere")
    p_abbrev.add_argument("--dest",  default=DEST_DB)
    p_abbrev.add_argument("--apply", action="store_true",
                          help="Applica le correzioni (default: dry-run)")

    args = parser.parse_args()

    if args.command is None or args.command == "ingest":
        source = getattr(args, "source", SOURCE_DB)
        dest   = getattr(args, "dest",   DEST_DB)
        normalize(source, dest)
    elif args.command == "similar-titles":
        similar_titles(args.dest, args.threshold, args.limit)
    elif args.command == "boundary":
        boundary_tracks(args.dest, args.minutes, args.limit)
    elif args.command == "purge":
        purge(args.source, args.dest)
    elif args.command == "dedup":
        dedup(args.dest, args.target, args.apply)
    elif args.command == "fix-abbrev":
        fix_abbrev_spaces(args.dest, args.target, args.apply)


if __name__ == "__main__":
    main()
