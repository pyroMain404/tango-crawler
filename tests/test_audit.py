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

def test_check_fascia_names_clean():
    from audit import check_fascia_names_tracks
    conn = make_tracks_db()
    conn.executemany("INSERT INTO tracks (raw_title, orchestra, fetched_at) VALUES (?, ?, ?)", [
        ("A * B", "CARLOS DI SARLI", "2026-04-01T10:00:00"),
        ("A * B", "OSVALDO PUGLIESE", "2026-04-01T10:05:00"),
    ])
    result = check_fascia_names_tracks(conn)
    assert result == []

def test_check_fascia_names_detects():
    from audit import check_fascia_names_tracks
    conn = make_tracks_db()
    conn.executemany("INSERT INTO tracks (raw_title, orchestra, fetched_at) VALUES (?, ?, ?)", [
        ("A * B", "CREMA DI TANGO",   "2026-04-01T10:00:00"),
        ("A * B", "MILONGA12",         "2026-04-01T10:05:00"),
        ("A * B", "1915*1985",         "2026-04-01T10:10:00"),
    ])
    result = check_fascia_names_tracks(conn)
    assert len(result) == 3

def test_check_duplicate_timestamps_clean():
    from audit import check_duplicate_timestamps_tracks
    conn = make_tracks_db()
    conn.executemany("INSERT INTO tracks (raw_title, fetched_at) VALUES (?, ?)", [
        ("A * B", "2026-04-01T10:00:00"),
        ("A * B", "2026-04-01T10:05:00"),
    ])
    result = check_duplicate_timestamps_tracks(conn)
    assert result == []

def test_check_duplicate_timestamps_detects():
    from audit import check_duplicate_timestamps_tracks
    conn = make_tracks_db()
    conn.execute("INSERT INTO tracks (raw_title, fetched_at) VALUES ('A * B', '2026-04-01T10:00:00')")
    conn.execute("INSERT INTO tracks (raw_title, fetched_at) VALUES ('C * D', '2026-04-01T10:00:00')")
    result = check_duplicate_timestamps_tracks(conn)
    assert len(result) == 1
    assert "2026-04-01T10:00:00" in result[0]

def test_import():
    import audit
    assert hasattr(audit, 'main')


def _insert_orchestra(conn, name):
    return conn.execute("INSERT OR IGNORE INTO orchestras (name) VALUES (?)", (name,)).lastrowid or \
           conn.execute("SELECT id FROM orchestras WHERE name=?", (name,)).fetchone()[0]

def _insert_title(conn, name):
    return conn.execute("INSERT OR IGNORE INTO titles (name) VALUES (?)", (name,)).lastrowid or \
           conn.execute("SELECT id FROM titles WHERE name=?", (name,)).fetchone()[0]

def _insert_play(conn, orch_id, title_id, fetched_at, year=None):
    conn.execute(
        "INSERT INTO plays (orchestra_id, title_id, year, fetched_at) VALUES (?, ?, ?, ?)",
        (orch_id, title_id, year, fetched_at)
    )

def test_check_rare_orchestras_none():
    from audit import check_rare_orchestras
    conn = make_tango_db()
    oid = _insert_orchestra(conn, "CARLOS DI SARLI")
    tid = _insert_title(conn, "BAHIA BLANCA")
    for i in range(5):
        _insert_play(conn, oid, tid, f"2026-04-01T10:0{i}:00")
    result = check_rare_orchestras(conn, min_plays=3)
    assert result == []

def test_check_rare_orchestras_detects():
    from audit import check_rare_orchestras
    conn = make_tango_db()
    oid = _insert_orchestra(conn, "BANDA SCONOSCIUTA")
    tid = _insert_title(conn, "BRANO MISTERIOSO")
    _insert_play(conn, oid, tid, "2026-04-01T10:00:00")
    result = check_rare_orchestras(conn, min_plays=3)
    assert len(result) == 1
    assert "BANDA SCONOSCIUTA" in result[0]

def test_check_unusual_chars_clean():
    from audit import check_unusual_chars
    conn = make_tango_db()
    _insert_orchestra(conn, "CARLOS DI SARLI")
    _insert_title(conn, "BAHIA BLANCA")
    result = check_unusual_chars(conn)
    assert result == []

def test_check_unusual_chars_detects():
    from audit import check_unusual_chars
    conn = make_tango_db()
    _insert_orchestra(conn, "ORQUESTA`TIPICA")
    _insert_title(conn, "TANGO|BEAT")
    result = check_unusual_chars(conn)
    assert len(result) == 2

def test_check_similar_titles_no_match():
    from audit import check_similar_titles
    conn = make_tango_db()
    oid = _insert_orchestra(conn, "CARLOS DI SARLI")
    for title, ts in [("BAHIA BLANCA", "2026-04-01T10:00:00"),
                      ("LA CUMPARSITA", "2026-04-01T10:05:00")]:
        tid = _insert_title(conn, title)
        _insert_play(conn, oid, tid, ts)
    result = check_similar_titles(conn, threshold=0.85)
    assert result == []

def test_check_similar_titles_detects():
    from audit import check_similar_titles
    conn = make_tango_db()
    oid = _insert_orchestra(conn, "CARLOS DI SARLI")
    for title, ts in [("BAHIA BLANCA",  "2026-04-01T10:00:00"),
                      ("BAHIA BLANCA2", "2026-04-01T10:05:00")]:
        tid = _insert_title(conn, title)
        _insert_play(conn, oid, tid, ts)
    result = check_similar_titles(conn, threshold=0.85)
    assert len(result) == 1
    assert "BAHIA BLANCA" in result[0]

def test_check_year_inconsistency_clean():
    from audit import check_year_inconsistency
    conn = make_tango_db()
    oid = _insert_orchestra(conn, "CARLOS DI SARLI")
    tid = _insert_title(conn, "BAHIA BLANCA")
    _insert_play(conn, oid, tid, "2026-04-01T10:00:00", year=1941)
    _insert_play(conn, oid, tid, "2026-04-01T10:05:00", year=1943)
    result = check_year_inconsistency(conn)
    assert result == []

def test_check_year_inconsistency_detects():
    from audit import check_year_inconsistency
    conn = make_tango_db()
    oid = _insert_orchestra(conn, "CARLOS DI SARLI")
    tid = _insert_title(conn, "BAHIA BLANCA")
    _insert_play(conn, oid, tid, "2026-04-01T10:00:00", year=1930)
    _insert_play(conn, oid, tid, "2026-04-01T10:05:00", year=1960)
    result = check_year_inconsistency(conn)
    assert len(result) == 1
    assert "1930" in result[0]
    assert "1960" in result[0]

def test_check_program_mismatch_clean():
    from audit import check_program_mismatch
    conn = make_tango_db()
    # EPOCA D'ORO 1935-1955: ore 11-12
    conn.execute("INSERT INTO programs (name, start_hour, end_hour) VALUES ('EPOCA D''ORO 1935-1955', 11, 12)")
    prog_id = conn.execute("SELECT id FROM programs WHERE name='EPOCA D''ORO 1935-1955'").fetchone()[0]
    oid = _insert_orchestra(conn, "CARLOS DI SARLI")
    tid = _insert_title(conn, "BAHIA BLANCA")
    conn.execute(
        "INSERT INTO plays (orchestra_id, title_id, program_id, fetched_at) VALUES (?, ?, ?, ?)",
        (oid, tid, prog_id, "2026-04-01T11:30:00")
    )
    result = check_program_mismatch(conn)
    assert result == []

def test_check_program_mismatch_detects():
    from audit import check_program_mismatch
    conn = make_tango_db()
    conn.execute("INSERT INTO programs (name, start_hour, end_hour) VALUES ('EPOCA D''ORO 1935-1955', 11, 12)")
    prog_id = conn.execute("SELECT id FROM programs WHERE name='EPOCA D''ORO 1935-1955'").fetchone()[0]
    oid = _insert_orchestra(conn, "CARLOS DI SARLI")
    tid = _insert_title(conn, "BAHIA BLANCA")
    # Brano alle 15:00 ma assegnato a fascia 11-12
    conn.execute(
        "INSERT INTO plays (orchestra_id, title_id, program_id, fetched_at) VALUES (?, ?, ?, ?)",
        (oid, tid, prog_id, "2026-04-01T15:30:00")
    )
    result = check_program_mismatch(conn)
    assert len(result) == 1
    assert "15" in result[0]
