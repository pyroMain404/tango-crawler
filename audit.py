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
import re
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


def check_gaps(conn: sqlite3.Connection, gap_minutes: int) -> list[str]:
    rows = conn.execute(
        "SELECT fetched_at FROM tracks ORDER BY fetched_at"
    ).fetchall()
    if len(rows) < 2:
        return []
    findings = []
    for i in range(1, len(rows)):
        try:
            t0 = datetime.fromisoformat(rows[i - 1][0])
            t1 = datetime.fromisoformat(rows[i][0])
        except ValueError:
            continue
        diff = int((t1 - t0).total_seconds() / 60)
        if diff > gap_minutes:
            findings.append(
                f"{rows[i-1][0]} → {rows[i][0]}  ({diff} min)"
            )
    return findings


_FASCIA_RE = re.compile(
    r'^(MILONGA\d+|LE VIE DEL TANGO|CREMA DI TANGO|ORCHESTRE TIPICHE ATTUALI'
    r'|EPOCA D.ORO|\d{4}[\*\-]?\d{4}.*|OTA|MILONGA\d*)$',
    re.IGNORECASE,
)


def check_fascia_names_tracks(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT orchestra, COUNT(*) FROM tracks "
        "WHERE orchestra IS NOT NULL GROUP BY orchestra"
    ).fetchall()
    findings = []
    for name, cnt in rows:
        if _FASCIA_RE.match(name.strip()):
            findings.append(f"{name!r}  ({cnt} record)")
    return findings


def check_duplicate_timestamps_tracks(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT fetched_at, COUNT(*) n FROM tracks "
        "GROUP BY fetched_at HAVING n > 1"
    ).fetchall()
    return [f"{ts}  ({n} volte)" for ts, n in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Analisi anomalie tango-crawler")
    parser.add_argument("--tracks",    default=DEFAULT_TRACKS,
                        help="Percorso tracks.db (default: $DB_PATH)")
    parser.add_argument("--tango",     default=DEFAULT_TANGO,
                        help="Percorso tango.db (default: $NORMALIZED_DB)")
    parser.add_argument("--threshold", type=float, default=0.85)
    parser.add_argument("--min-plays", type=int,   default=3)
    parser.add_argument("--gap",       type=int,   default=90)
    args = parser.parse_args()

    total_issues = 0

    # ── tracks.db ────────────────────────────────────────────────────────────
    if os.path.exists(args.tracks):
        conn_t = sqlite3.connect(args.tracks)
        n = conn_t.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
        if n > 0:
            section_header("AUDIT tracks.db")

            sep("Gap del crawler")
            findings = check_gaps(conn_t, args.gap)
            if findings:
                for f in findings:
                    anomaly(f)
                print(f"  {len(findings)} gap trovati.")
                total_issues += 1
            else:
                ok(f"nessun gap > {args.gap} min")

            sep("Orchestre-fascia in tracks.db")
            findings = check_fascia_names_tracks(conn_t)
            if findings:
                for f in findings:
                    anomaly(f)
                total_issues += 1
            else:
                ok("nessuna orchestra-fascia rilevata")

            sep("fetched_at duplicati")
            findings = check_duplicate_timestamps_tracks(conn_t)
            if findings:
                for f in findings:
                    anomaly(f)
                total_issues += 1
            else:
                ok("nessun timestamp duplicato")

        conn_t.close()

    print()
    if total_issues == 0:
        print("Tutto OK — nessuna anomalia trovata.")
    else:
        print(f"*** {total_issues} sezioni con anomalie.")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


if __name__ == "__main__":
    main()
