#!/usr/bin/env python3
"""
Analisi anomalie su tracks.db e tango.db.

Uso:
  python audit.py
  python audit.py --tracks /path/tracks.db --tango /path/tango.db
  python audit.py --threshold 0.85 --min-plays 3 --gap 90
"""
import argparse
import difflib
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


def check_rare_orchestras(conn: sqlite3.Connection, min_plays: int) -> list[str]:
    rows = conn.execute("""
        SELECT o.name, COUNT(*) n,
               GROUP_CONCAT(DISTINCT t.name) titles
        FROM plays p
        JOIN orchestras o ON o.id = p.orchestra_id
        JOIN titles     t ON t.id = p.title_id
        GROUP BY o.id
        HAVING n < ?
        ORDER BY n
    """, (min_plays,)).fetchall()
    findings = []
    for name, cnt, titles in rows:
        titles_preview = (titles or "")[:80]
        findings.append(f"{name!r}  ({cnt} play)  titoli: {titles_preview}")
    return findings


_UNUSUAL_RE = re.compile(r'[`|]|^\d+$')


def check_unusual_chars(conn: sqlite3.Connection) -> list[str]:
    findings = []
    for table, col in [("orchestras", "name"), ("titles", "name")]:
        rows = conn.execute(f"SELECT {col} FROM {table}").fetchall()
        for (name,) in rows:
            if _UNUSUAL_RE.search(name):
                findings.append(f"[{table}] {name!r}")
    return findings


def check_similar_titles(conn: sqlite3.Connection, threshold: float) -> list[str]:
    rows = conn.execute("""
        SELECT o.name, GROUP_CONCAT(t.name, '||') titles_concat
        FROM plays p
        JOIN orchestras o ON o.id = p.orchestra_id
        JOIN titles     t ON t.id = p.title_id
        GROUP BY o.id
        HAVING COUNT(DISTINCT t.id) > 1
    """).fetchall()

    sm = difflib.SequenceMatcher(autojunk=False)
    findings = []
    for orch_name, titles_concat in rows:
        titles = list(set(titles_concat.split("||")))
        for i in range(len(titles)):
            sm.set_seq1(titles[i])
            for j in range(i + 1, len(titles)):
                sm.set_seq2(titles[j])
                ratio = sm.ratio()
                if ratio >= threshold:
                    findings.append(
                        f"[{orch_name}] {titles[i]!r} ~ {titles[j]!r}  ({ratio:.2f})"
                    )
    return findings


def check_year_inconsistency(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("""
        SELECT o.name, t.name, MIN(p.year), MAX(p.year), COUNT(*)
        FROM plays p
        JOIN orchestras o ON o.id = p.orchestra_id
        JOIN titles     t ON t.id = p.title_id
        WHERE p.year IS NOT NULL
        GROUP BY p.orchestra_id, p.title_id
        HAVING MAX(p.year) - MIN(p.year) > 10
        ORDER BY (MAX(p.year) - MIN(p.year)) DESC
    """).fetchall()
    findings = []
    for orch, title, yr_min, yr_max, cnt in rows:
        findings.append(
            f"{orch} — {title!r}  anni: {yr_min}–{yr_max}  ({cnt} play)"
        )
    return findings


def check_program_mismatch(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("""
        SELECT p.fetched_at, pr.name, pr.start_hour, pr.end_hour,
               o.name, t.name
        FROM plays p
        JOIN programs   pr ON pr.id = p.program_id
        JOIN orchestras o  ON o.id  = p.orchestra_id
        JOIN titles     t  ON t.id  = p.title_id
        WHERE pr.start_hour IS NOT NULL AND pr.end_hour IS NOT NULL
          AND pr.start_hour != 0 AND pr.end_hour != 0
    """).fetchall()

    findings = []
    for fetched_at, prog_name, start_h, end_h, orch, title in rows:
        try:
            hour = datetime.fromisoformat(fetched_at).hour
        except ValueError:
            continue
        expected = get_program(hour)
        if expected != prog_name:
            findings.append(
                f"{fetched_at}  [{prog_name}]  ora reale={hour:02d}h → atteso [{expected}]"
                f"  ({orch} — {title})"
            )
    return findings


def check_temporal_duplicates(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("""
        SELECT a.fetched_at, b.fetched_at, o.name, t.name
        FROM plays a
        JOIN plays b ON a.orchestra_id = b.orchestra_id
                     AND a.title_id    = b.title_id
                     AND a.id < b.id
        JOIN orchestras o ON o.id = a.orchestra_id
        JOIN titles     t ON t.id = a.title_id
        WHERE (julianday(b.fetched_at) - julianday(a.fetched_at)) * 1440 < 5
        ORDER BY a.fetched_at
    """).fetchall()
    findings = []
    for ts_a, ts_b, orch, title in rows:
        diff = int(
            (datetime.fromisoformat(ts_b) - datetime.fromisoformat(ts_a)).total_seconds() / 60
        )
        findings.append(f"{ts_a} + {ts_b}  ({diff} min)  {orch} — {title}")
    return findings


def snapshot_ai(conn: sqlite3.Connection, args: argparse.Namespace) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"\n=== SNAPSHOT tango.db — {today} ===\n")

    # Statistiche generali
    total_plays   = conn.execute("SELECT COUNT(*) FROM plays").fetchone()[0]
    total_orch    = conn.execute("SELECT COUNT(*) FROM orchestras").fetchone()[0]
    total_titles  = conn.execute("SELECT COUNT(*) FROM titles").fetchone()[0]
    total_singers = conn.execute("SELECT COUNT(*) FROM singers").fetchone()[0]
    first, last   = conn.execute(
        "SELECT MIN(fetched_at), MAX(fetched_at) FROM plays"
    ).fetchone()

    print(f"Plays totali  : {total_plays}")
    print(f"Orchestre     : {total_orch} orchestre")
    print(f"Titoli unici  : {total_titles}")
    print(f"Cantanti      : {total_singers}")
    print(f"Arco temporale: {first} → {last}")

    # Top 30 orchestre
    print("\n--- Top 30 orchestre per passaggi ---")
    rows = conn.execute("""
        SELECT o.name, COUNT(*) n, MIN(p.year), MAX(p.year)
        FROM plays p
        JOIN orchestras o ON o.id = p.orchestra_id
        GROUP BY o.id
        ORDER BY n DESC
        LIMIT 30
    """).fetchall()
    for name, cnt, yr_min, yr_max in rows:
        yr_str = f"  {yr_min}–{yr_max}" if yr_min else ""
        print(f"  {cnt:>5}  {name}{yr_str}")

    # Distribuzione per fascia
    print("\n--- Distribuzione per fascia di palinsesto ---")
    rows = conn.execute("""
        SELECT pr.name, COUNT(*) n
        FROM plays p
        JOIN programs pr ON pr.id = p.program_id
        GROUP BY pr.id
        ORDER BY n DESC
    """).fetchall()
    for name, cnt in rows:
        print(f"  {cnt:>5}  {name}")

    # Distribuzione per decennio
    print("\n--- Distribuzione per decennio (anno brano) ---")
    rows = conn.execute("""
        SELECT (year / 10) * 10 decade, COUNT(*) n
        FROM plays
        WHERE year IS NOT NULL
        GROUP BY decade
        ORDER BY decade
    """).fetchall()
    for decade, cnt in rows:
        print(f"  {decade}s: {cnt}")

    # Orchestre rare con titoli
    rare = conn.execute("""
        SELECT o.name, COUNT(*) n,
               GROUP_CONCAT(DISTINCT t.name) titles
        FROM plays p
        JOIN orchestras o ON o.id = p.orchestra_id
        JOIN titles     t ON t.id = p.title_id
        GROUP BY o.id
        HAVING n < ?
        ORDER BY n
    """, (args.min_plays,)).fetchall()
    if rare:
        print(f"\n--- Orchestre rare (< {args.min_plays} passaggi) ---")
        for name, cnt, titles in rare:
            print(f"  {name}  ({cnt})  → {titles}")

    # Coppie quasi-duplicate
    pairs = check_similar_titles(conn, args.threshold)
    if pairs:
        print(f"\n--- Titoli quasi-duplicati (threshold {args.threshold}) ---")
        for p in pairs:
            print(f"  {p}")

    print(f"\n=== FINE SNAPSHOT ===")


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

    # ── tango.db ─────────────────────────────────────────────────────────────
    if os.path.exists(args.tango):
        conn_n = sqlite3.connect(args.tango)
        section_header("AUDIT tango.db")

        sep("Orchestre rare (< " + str(args.min_plays) + " passaggi)")
        findings = check_rare_orchestras(conn_n, args.min_plays)
        if findings:
            for f in findings:
                anomaly(f)
            total_issues += 1
        else:
            ok(f"nessuna orchestra con meno di {args.min_plays} passaggi")

        sep("Caratteri insoliti in orchestre/titoli")
        findings = check_unusual_chars(conn_n)
        if findings:
            for f in findings:
                anomaly(f)
            total_issues += 1
        else:
            ok("nessun carattere insolito trovato")

        sep("Titoli quasi-duplicati per stessa orchestra")
        findings = check_similar_titles(conn_n, args.threshold)
        if findings:
            for f in findings:
                anomaly(f)
            total_issues += 1
        else:
            ok(f"nessuna coppia con similarità >= {args.threshold}")

        sep("Anni inconsistenti (stesso brano, range > 10 anni)")
        findings = check_year_inconsistency(conn_n)
        if findings:
            for f in findings:
                anomaly(f)
            total_issues += 1
        else:
            ok("nessuna inconsistenza negli anni")

        sep("Disallineamento programma/orario")
        findings = check_program_mismatch(conn_n)
        if findings:
            for f in findings[:20]:
                anomaly(f)
            if len(findings) > 20:
                print(f"  ... e altri {len(findings) - 20}")
            total_issues += 1
        else:
            ok("programmi allineati agli orari reali")

        sep("Duplicati temporali (stesso brano < 5 min)")
        findings = check_temporal_duplicates(conn_n)
        if findings:
            for f in findings:
                anomaly(f)
            total_issues += 1
        else:
            ok("nessun duplicato temporale")

        section_header("SNAPSHOT AI")
        snapshot_ai(conn_n, args)

        conn_n.close()

    print()
    if total_issues == 0:
        print("Tutto OK — nessuna anomalia trovata.")
    else:
        print(f"*** {total_issues} sezioni con anomalie.")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


if __name__ == "__main__":
    main()
