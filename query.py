#!/usr/bin/env python3
"""
Query rapida sul DB — esempi:

  # tutti i titoli di oggi
  python query.py

  # fascia oraria abbreviata: ore 13 (13:00-13:59)
  python query.py 13

  # fascia oraria abbreviata: dalle 13 alle 14 (13:00-14:59)
  python query.py 13-14

  # fascia oraria specifica
  python query.py --from "2026-03-31T21:00" --to "2026-03-31T23:59"

  # solo una data
  python query.py --date 2026-03-31

  # fascia oraria su data specifica
  python query.py 21-23 --date 2026-03-31

  # ultimi N inserimenti
  python query.py --last 20
"""
import argparse
import sqlite3
import os
import re
from datetime import date

DB_PATH = os.getenv("DB_PATH", "data/tracks.db")


def parse_hour_range(value: str) -> tuple[str, str]:
    """Parsa '13' o '13-14' e restituisce (from_hour, to_hour) come stringhe 'HH'."""
    m = re.fullmatch(r"(\d{1,2})(?:-(\d{1,2}))?", value)
    if not m:
        raise argparse.ArgumentTypeError(
            f"Formato ora non valido: '{value}'. Usa '13' oppure '13-14'."
        )
    h_from = int(m.group(1))
    h_to   = int(m.group(2)) if m.group(2) else h_from
    if not (0 <= h_from <= 23 and 0 <= h_to <= 23):
        raise argparse.ArgumentTypeError("Le ore devono essere tra 0 e 23.")
    return f"{h_from:02d}", f"{h_to:02d}"


def main():
    parser = argparse.ArgumentParser(description="Interroga il DB dei brani")
    parser.add_argument("hours",   nargs="?",  help="Fascia oraria: '13' o '13-14'")
    parser.add_argument("--from",  dest="from_ts", help="Da timestamp (YYYY-MM-DDTHH:MM)")
    parser.add_argument("--to",    dest="to_ts",   help="A timestamp  (YYYY-MM-DDTHH:MM)")
    parser.add_argument("--date",  help="Giorno (YYYY-MM-DD), default oggi")
    parser.add_argument("--last",  type=int, default=0, help="Ultimi N record")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    if args.last:
        rows = conn.execute(
            "SELECT fetched_at, title FROM tracks ORDER BY id DESC LIMIT ?",
            (args.last,)
        ).fetchall()
        rows.reverse()
    elif args.hours:
        day = args.date or date.today().isoformat()
        h_from, h_to = parse_hour_range(args.hours)
        from_ts = f"{day}T{h_from}:00:00"
        to_ts   = f"{day}T{h_to}:59:59"
        rows = conn.execute(
            "SELECT fetched_at, title FROM tracks WHERE fetched_at BETWEEN ? AND ? ORDER BY id",
            (from_ts, to_ts)
        ).fetchall()
    elif args.date:
        rows = conn.execute(
            "SELECT fetched_at, title FROM tracks WHERE fetched_at LIKE ? ORDER BY id",
            (f"{args.date}%",)
        ).fetchall()
    elif args.from_ts or args.to_ts:
        from_ts = args.from_ts or "0000-00-00"
        to_ts   = args.to_ts   or "9999-99-99"
        rows = conn.execute(
            "SELECT fetched_at, title FROM tracks WHERE fetched_at BETWEEN ? AND ? ORDER BY id",
            (from_ts, to_ts)
        ).fetchall()
    else:
        today = date.today().isoformat()
        rows = conn.execute(
            "SELECT fetched_at, title FROM tracks WHERE fetched_at LIKE ? ORDER BY id",
            (f"{today}%",)
        ).fetchall()

    if not rows:
        print("Nessun risultato.")
        return

    for ts, title in rows:
        print(f"{ts}  {title}")

    print(f"\n{len(rows)} brani trovati.")


if __name__ == "__main__":
    main()
