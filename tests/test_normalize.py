import sqlite3
import pytest
from normalize import normalize, _id_cache


@pytest.fixture()
def db_pair(tmp_path):
    src = tmp_path / "tracks.db"
    dst = tmp_path / "tango.db"
    conn = sqlite3.connect(src)
    conn.execute("""
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_title TEXT NOT NULL,
            orchestra TEXT,
            singer TEXT,
            track_title TEXT,
            year INTEGER,
            author TEXT,
            dancers TEXT,
            program TEXT,
            fetched_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    _id_cache.clear()
    return str(src), str(dst)


def insert_track(src_path, orchestra, title, fetched_at, singer=None):
    conn = sqlite3.connect(src_path)
    conn.execute(
        "INSERT INTO tracks (raw_title, orchestra, track_title, fetched_at, singer) VALUES (?, ?, ?, ?, ?)",
        (f"{orchestra} * {title}", orchestra, title, fetched_at, singer),
    )
    conn.commit()
    conn.close()


def test_normalize_canonicalizes_trailing_punct(db_pair):
    """'BAHIA BLANCA.' e 'BAHIA BLANCA' devono risultare nello stesso title_id."""
    src, dst = db_pair
    insert_track(src, "CARLOS DI SARLI", "BAHIA BLANCA.",  "2026-01-01T10:00:00")
    insert_track(src, "CARLOS DI SARLI", "BAHIA BLANCA",   "2026-01-01T10:01:00")
    normalize(src, dst)

    conn = sqlite3.connect(dst)
    titles = conn.execute("SELECT name FROM titles").fetchall()
    conn.close()
    assert len(titles) == 1, f"Atteso 1 titolo, trovati {len(titles)}: {titles}"
    assert titles[0][0] == "BAHIA BLANCA"


def test_normalize_canonicalizes_trailing_underscore(db_pair):
    src, dst = db_pair
    insert_track(src, "CARLOS DI SARLI", "EL INGENIERO_", "2026-01-01T10:00:00")
    normalize(src, dst)

    conn = sqlite3.connect(dst)
    name = conn.execute("SELECT name FROM titles").fetchone()[0]
    conn.close()
    assert name == "EL INGENIERO"


from normalize import dedup_titles


def _make_dst_with_duplicates(tmp_path):
    """Crea tango.db con duplicati noti inseriti manualmente."""
    src = tmp_path / "tracks2.db"
    dst = tmp_path / "tango2.db"
    conn = sqlite3.connect(src)
    conn.execute("""
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_title TEXT NOT NULL,
            orchestra TEXT,
            singer TEXT,
            track_title TEXT,
            year INTEGER,
            author TEXT,
            dancers TEXT,
            program TEXT,
            fetched_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

    # Normalizza brani puliti per avere orchestras, titles, plays di base
    tracks = [
        ("CARLOS DI SARLI", "BAHIA BLANCA",  "2026-01-01T10:00:00"),
        ("CARLOS DI SARLI", "TORMENTA",      "2026-01-01T10:02:00"),
        ("OSVALDO PUGLIESE", "LA YUMBA",     "2026-01-01T11:00:00"),
    ]
    for orch, title, ts in tracks:
        insert_track(str(src), orch, title, ts)
    _id_cache.clear()
    normalize(str(src), str(dst))
    _id_cache.clear()

    # Inserisci manualmente titoli "sporchi" e plays che puntano ad essi
    conn = sqlite3.connect(str(dst))
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("INSERT INTO titles (name) VALUES ('BAHIA BLANCA.')")
    conn.execute("INSERT INTO titles (name) VALUES ('TORMENTA.')")
    dup_bahia_id  = conn.execute("SELECT id FROM titles WHERE name = 'BAHIA BLANCA.'").fetchone()[0]
    dup_torm_id   = conn.execute("SELECT id FROM titles WHERE name = 'TORMENTA.'").fetchone()[0]
    orch_id       = conn.execute("SELECT id FROM orchestras WHERE name = 'CARLOS DI SARLI'").fetchone()[0]
    conn.execute(
        "INSERT INTO plays (orchestra_id, title_id, fetched_at) VALUES (?, ?, ?)",
        (orch_id, dup_bahia_id, "2026-01-01T10:99:00"),
    )
    conn.execute(
        "INSERT INTO plays (orchestra_id, title_id, fetched_at) VALUES (?, ?, ?)",
        (orch_id, dup_torm_id, "2026-01-01T10:98:00"),
    )
    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")
    conn.close()
    return str(dst), dup_bahia_id, dup_torm_id


def test_dedup_dry_run_finds_pairs(tmp_path):
    dst, dup_bahia_id, _ = _make_dst_with_duplicates(tmp_path)
    pairs = dedup_titles(dst, threshold=0.9, apply=False)
    assert len(pairs) >= 1
    names = {(a, b) for a, b, *_ in pairs}
    assert ("BAHIA BLANCA", "BAHIA BLANCA.") in names or \
           ("BAHIA BLANCA.", "BAHIA BLANCA") in names


def test_dedup_apply_merges_plays(tmp_path):
    dst, dup_bahia_id, _ = _make_dst_with_duplicates(tmp_path)
    dedup_titles(dst, threshold=0.9, apply=True)

    conn = sqlite3.connect(dst)
    row = conn.execute("SELECT id FROM titles WHERE name = 'BAHIA BLANCA.'").fetchone()
    assert row is None, "Titolo duplicato dovrebbe essere stato cancellato"
    bad = conn.execute(
        "SELECT COUNT(*) FROM plays WHERE title_id = ?", (dup_bahia_id,)
    ).fetchone()[0]
    assert bad == 0, f"{bad} plays ancora puntano al titolo duplicato"
    conn.close()


def test_dedup_apply_idempotent(tmp_path):
    dst, _, _ = _make_dst_with_duplicates(tmp_path)
    dedup_titles(dst, threshold=0.9, apply=True)
    pairs_second = dedup_titles(dst, threshold=0.9, apply=True)
    assert pairs_second == [], "Seconda esecuzione non dovrebbe trovare ulteriori duplicati"
