#!/usr/bin/env python3
"""
Query rapida sul DB — eseguibile direttamente sull'host senza Docker.

Default: tango.db (storico normalizzato).
Flag --raw: tracks.db (giornata in corso, non ancora normalizzata).

Esempi:
  python query.py                          # oggi da tango.db
  python query.py --raw                    # oggi da tracks.db
  python query.py 13                       # ore 13:00-13:59 di oggi
  python query.py 13-14                    # ore 13:00-14:59 di oggi
  python query.py 21-23 --date 2026-03-31
  python query.py --date 2026-03-31
  python query.py --from "2026-03-31T21:00" --to "2026-03-31T23:59"
  python query.py --last 20
"""
import argparse
import os
import re
import sqlite3
from datetime import date

_BASE = os.path.expanduser("~/.local/share/tango-crawler")
TANGO_DB  = os.getenv("NORMALIZED_DB", os.path.join(_BASE, "tango.db"))
TRACKS_DB = os.getenv("DB_PATH",       os.path.join(_BASE, "tracks.db"))


def _tango_query(where: str = "", order: str = "p.fetched_at") -> str:
    """Costruisce la query su tango.db inserendo WHERE prima di GROUP BY."""
    where_clause = f"WHERE {where}" if where else ""
    return f"""
        SELECT p.fetched_at,
               o.name,
               GROUP_CONCAT(s.name, ', '),
               t.name,
               p.year,
               pr.name
        FROM   plays p
        LEFT JOIN orchestras o    ON o.id  = p.orchestra_id
        LEFT JOIN titles t        ON t.id  = p.title_id
        LEFT JOIN programs pr     ON pr.id = p.program_id
        LEFT JOIN play_singers ps ON ps.play_id = p.id
        LEFT JOIN singers s       ON s.id  = ps.singer_id
        {where_clause}
        GROUP BY p.id
        ORDER BY {order}
    """


def _tracks_query(where: str = "", order: str = "id") -> str:
    where_clause = f"WHERE {where}" if where else ""
    return f"SELECT fetched_at, orchestra, singer, track_title, year, program FROM tracks {where_clause} ORDER BY {order}"


def fmt(row) -> str:
    fetched_at, orchestra, singer, track_title, year, program = row
    artist = f"{orchestra} / {singer}" if singer else orchestra or "?"
    title  = track_title or "?"
    suffix = f" ({year})" if year else ""
    slot   = f"[{program}]  " if program else ""
    return f"{fetched_at}  {slot}{artist} — {title}{suffix}"


def parse_hour_range(value: str) -> tuple[str, str]:
    m = re.fullmatch(r"(\d{1,2})(?:-(\d{1,2}))?", value)
    if not m:
        raise argparse.ArgumentTypeError(
            f"Formato non valido: '{value}'. Usa '13' oppure '13-14'."
        )
    h_from = int(m.group(1))
    h_to   = int(m.group(2)) if m.group(2) else h_from
    if not (0 <= h_from <= 23 and 0 <= h_to <= 23):
        raise argparse.ArgumentTypeError("Le ore devono essere tra 0 e 23.")
    return f"{h_from:02d}", f"{h_to:02d}"


def main():
    parser = argparse.ArgumentParser(description="Interroga il DB dei brani")
    parser.add_argument("hours",  nargs="?",  help="Fascia oraria: '13' o '13-14'")
    parser.add_argument("--from", dest="from_ts", help="Da timestamp (YYYY-MM-DDTHH:MM)")
    parser.add_argument("--to",   dest="to_ts",   help="A timestamp  (YYYY-MM-DDTHH:MM)")
    parser.add_argument("--date", help="Giorno (YYYY-MM-DD), default oggi")
    parser.add_argument("--last", type=int, default=0, help="Ultimi N record")
    parser.add_argument("--raw",  action="store_true",
                        help="Usa tracks.db (giornata in corso) invece di tango.db")
    args = parser.parse_args()

    build = _tracks_query if args.raw else _tango_query
    ts    = "fetched_at"  if args.raw else "p.fetched_at"
    db    = TRACKS_DB     if args.raw else TANGO_DB
    conn  = sqlite3.connect(db)

    if args.last:
        order = "id DESC" if args.raw else "p.id DESC"
        rows  = conn.execute(build(order=order) + " LIMIT ?", (args.last,)).fetchall()
        rows.reverse()
    elif args.hours:
        day = args.date or date.today().isoformat()
        h_from, h_to = parse_hour_range(args.hours)
        rows = conn.execute(
            build(where=f"{ts} BETWEEN ? AND ?"),
            (f"{day}T{h_from}:00:00", f"{day}T{h_to}:59:59"),
        ).fetchall()
    elif args.date:
        rows = conn.execute(
            build(where=f"{ts} LIKE ?"), (f"{args.date}%",)
        ).fetchall()
    elif args.from_ts or args.to_ts:
        rows = conn.execute(
            build(where=f"{ts} BETWEEN ? AND ?"),
            (args.from_ts or "0000-00-00", args.to_ts or "9999-99-99"),
        ).fetchall()
    else:
        rows = conn.execute(
            build(where=f"{ts} LIKE ?"), (f"{date.today().isoformat()}%",)
        ).fetchall()

    conn.close()

    if not rows:
        print("Nessun risultato.")
        return

    for row in rows:
        print(fmt(row))
    print(f"\n{len(rows)} brani trovati.")


if __name__ == "__main__":
    main()
