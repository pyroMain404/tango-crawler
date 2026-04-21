import sqlite3

def make_tango_db():
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE orchestras (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);
        CREATE TABLE titles     (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);
        CREATE TABLE programs   (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
                                 start_hour INTEGER, end_hour INTEGER);
        CREATE TABLE singers (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);
        CREATE TABLE plays (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            orchestra_id INTEGER REFERENCES orchestras(id),
            title_id     INTEGER REFERENCES titles(id),
            year         INTEGER,
            author       TEXT,
            dancers      TEXT,
            program_id   INTEGER REFERENCES programs(id),
            fetched_at   TEXT NOT NULL UNIQUE
        );
        CREATE TABLE play_singers (
            play_id   INTEGER NOT NULL REFERENCES plays(id) ON DELETE CASCADE,
            singer_id INTEGER NOT NULL REFERENCES singers(id),
            PRIMARY KEY (play_id, singer_id)
        );
    """)
    return conn

def make_tracks_db():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE tracks (
            id          INTEGER PRIMARY KEY,
            raw_title   TEXT NOT NULL,
            orchestra   TEXT,
            singer      TEXT,
            track_title TEXT,
            year        INTEGER,
            author      TEXT,
            dancers     TEXT,
            program     TEXT,
            fetched_at  TEXT NOT NULL
        );
    """)
    return conn

def test_check_gaps_no_gap():
    from audit import check_gaps
    conn = make_tracks_db()
    conn.executemany("INSERT INTO tracks (raw_title, fetched_at) VALUES (?, ?)", [
        ("A * B", "2026-04-01T10:00:00"),
        ("A * B", "2026-04-01T10:05:00"),
        ("A * B", "2026-04-01T10:10:00"),
    ])
    result = check_gaps(conn, gap_minutes=90)
    assert result == []

def test_check_gaps_detects_gap():
    from audit import check_gaps
    conn = make_tracks_db()
    conn.executemany("INSERT INTO tracks (raw_title, fetched_at) VALUES (?, ?)", [
        ("A * B", "2026-04-01T10:00:00"),
        ("A * B", "2026-04-01T12:00:00"),  # 120 min gap
    ])
    result = check_gaps(conn, gap_minutes=90)
    assert len(result) == 1
    assert "120" in result[0]

def test_import():
    import audit
    assert hasattr(audit, 'main')
