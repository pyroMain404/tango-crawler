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
