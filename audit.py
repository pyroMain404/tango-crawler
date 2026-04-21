#!/usr/bin/env python3
"""
Analisi anomalie su tracks.db e tango.db.

Uso:
  python audit.py
  python audit.py --tracks /path/tracks.db --tango /path/tango.db
  python audit.py --threshold 0.85 --min-plays 3 --gap 90
"""
import argparse
import os
import sqlite3
from datetime import datetime

from common import get_program

_BASE = os.path.expanduser("~/.local/share/tango-crawler")
DEFAULT_TRACKS = os.getenv("DB_PATH",       os.path.join(_BASE, "tracks.db"))
DEFAULT_TANGO  = os.getenv("NORMALIZED_DB", os.path.join(_BASE, "tango.db"))


def sep(title: str) -> None:
    print(f"\n── {title} ──")


def ok(label: str) -> None:
    print(f"  OK  {label}")


def anomaly(line: str) -> None:
    print(f"  *** {line}")


def section_header(title: str) -> None:
    print()
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f" {title}")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analisi anomalie tango-crawler")
    parser.add_argument("--tracks",    default=DEFAULT_TRACKS,
                        help="Percorso tracks.db (default: $DB_PATH)")
    parser.add_argument("--tango",     default=DEFAULT_TANGO,
                        help="Percorso tango.db (default: $NORMALIZED_DB)")
    parser.add_argument("--threshold", type=float, default=0.85,
                        help="Soglia similarità titoli (default: 0.85)")
    parser.add_argument("--min-plays", type=int,   default=3,
                        help="Min passaggi perché un'orchestra non sia 'rara' (default: 3)")
    parser.add_argument("--gap",       type=int,   default=90,
                        help="Minuti di silenzio considerati gap del crawler (default: 90)")
    args = parser.parse_args()
    print("audit.py — tango-crawler")


if __name__ == "__main__":
    main()
