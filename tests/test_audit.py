import sqlite3
import pytest

def make_tango_db():
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE orchestras (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);
        CREATE TABLE titles     (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);
        CREATE TABLE programs   (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
                                 start_hour INTEGER, end_hour INTEGER);
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
        CREATE TABLE singers (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE);
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

def test_import():
    import audit
    assert hasattr(audit, 'main')
