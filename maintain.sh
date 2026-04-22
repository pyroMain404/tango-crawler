#!/bin/bash
# Manutenzione e verifica dei database tango-crawler.
#
# Uso:
#   ./maintain.sh           # solo verifica (non modifica nulla)
#   ./maintain.sh --fix     # verifica + pulizia completa
set -euo pipefail

DATA_DIR="$HOME/.local/share/tango-crawler"
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE="docker compose -f $REPO_DIR/docker-compose.yml"
TRACKS="$DATA_DIR/tracks.db"
TANGO="$DATA_DIR/tango.db"
FIX="${1:-}"

ERRORS=0

# ── Helper ────────────────────────────────────────────────────────────────────

q()  { sqlite3 "$1" "$2"; }
sep() { echo ""; echo "── $* ──"; }
check() {
    local label="$1" db="$2" sql="$3"
    local n; n=$(q "$db" "$sql")
    if [ "$n" -eq 0 ]; then
        printf "  %-50s OK\n" "$label"
    else
        printf "  %-50s *** %s record anomali\n" "$label" "$n"
        ERRORS=$((ERRORS + 1))
    fi
}

# ── Pulizia (solo con --fix) ──────────────────────────────────────────────────

if [ "$FIX" = "--fix" ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " PULIZIA"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    sep "1/5 purge (fascia ID + plays NULL)"
    $COMPOSE run --rm -e DB_PATH=/data/tracks.db -e NORMALIZED_DB=/data/tango.db \
        crawler python normalize.py purge

    sep "2/5 reparse tracks.db con parser aggiornato"
    $COMPOSE run --rm -e DB_PATH=/data/tracks.db \
        crawler python convert.py --reparse

    sep "3/5 fix titoli con asterisco finale in tango.db"
    q "$TANGO" "
BEGIN;

UPDATE plays
SET title_id = (
  SELECT clean.id FROM titles clean
  JOIN titles dirty ON dirty.id = plays.title_id
  WHERE clean.name = TRIM(RTRIM(dirty.name, '*'))
    AND dirty.name LIKE '%*'
)
WHERE title_id IN (
  SELECT dirty.id FROM titles dirty
  JOIN titles clean ON clean.name = TRIM(RTRIM(dirty.name, '*'))
  WHERE dirty.name LIKE '%*'
);

UPDATE playlist_items
SET title_id = (
  SELECT clean.id FROM titles clean
  JOIN titles dirty ON dirty.id = playlist_items.title_id
  WHERE clean.name = TRIM(RTRIM(dirty.name, '*'))
    AND dirty.name LIKE '%*'
)
WHERE title_id IN (
  SELECT dirty.id FROM titles dirty
  JOIN titles clean ON clean.name = TRIM(RTRIM(dirty.name, '*'))
  WHERE dirty.name LIKE '%*'
);

DELETE FROM titles
WHERE name LIKE '%*'
  AND id NOT IN (SELECT title_id FROM plays          WHERE title_id IS NOT NULL)
  AND id NOT IN (SELECT title_id FROM playlist_items WHERE title_id IS NOT NULL);

UPDATE titles SET name = TRIM(RTRIM(name, '*')) WHERE name LIKE '%*';

COMMIT;
"
    echo "  fatto."

    sep "4/5 merge OSVALDO PULIESE → OSVALDO PUGLIESE"
    if q "$TANGO" "SELECT COUNT(*) FROM orchestras WHERE name = 'OSVALDO PULIESE';" | grep -q "^1$"; then
        q "$TANGO" "
BEGIN;

UPDATE plays
SET orchestra_id = (SELECT id FROM orchestras WHERE name = 'OSVALDO PUGLIESE')
WHERE orchestra_id = (SELECT id FROM orchestras WHERE name = 'OSVALDO PULIESE');

UPDATE playlist_items
SET orchestra_id = (SELECT id FROM orchestras WHERE name = 'OSVALDO PUGLIESE')
WHERE orchestra_id = (SELECT id FROM orchestras WHERE name = 'OSVALDO PULIESE');

DELETE FROM orchestras WHERE name = 'OSVALDO PULIESE';

COMMIT;
"
        echo "  fatto."
    else
        echo "  già pulito, skip."
    fi

    sep "5/5 ingest tracks.db → tango.db"
    $COMPOSE run --rm -e DB_PATH=/data/tracks.db -e NORMALIZED_DB=/data/tango.db \
        crawler python normalize.py ingest

    echo ""
fi

# ── Verifica ──────────────────────────────────────────────────────────────────

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " VERIFICA"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

sep "tracks.db"
check "track_title NULL (fascia ID card)"  "$TRACKS" "SELECT COUNT(*) FROM tracks WHERE track_title IS NULL;"
check "orchestra NULL"                     "$TRACKS" "SELECT COUNT(*) FROM tracks WHERE orchestra IS NULL;"
check "jingle non filtrati"                "$TRACKS" "SELECT COUNT(*) FROM tracks WHERE UPPER(orchestra) = 'TANGO PASION RADIO';"
check "titoli con asterisco finale"        "$TRACKS" "SELECT COUNT(*) FROM tracks WHERE track_title LIKE '%*';"
check "orchestre con asterisco finale"     "$TRACKS" "SELECT COUNT(*) FROM tracks WHERE orchestra LIKE '%*';"

sep "tango.db"
check "plays con title_id/orchestra_id NULL"  "$TANGO" "SELECT COUNT(*) FROM plays WHERE title_id IS NULL OR orchestra_id IS NULL;"
check "titoli con asterisco finale"           "$TANGO" "SELECT COUNT(*) FROM titles WHERE name LIKE '%*';"
check "orchestre con asterisco finale"        "$TANGO" "SELECT COUNT(*) FROM orchestras WHERE name LIKE '%*';"
check "typo PULIESE"                          "$TANGO" "SELECT COUNT(*) FROM orchestras WHERE name LIKE '%PULIESE%';"
check "orchestre-fascia (MILONGA*)"           "$TANGO" "SELECT COUNT(*) FROM orchestras WHERE name GLOB 'MILONGA[0-9]*';"
check "orchestre-fascia (anno*anno)"          "$TANGO" "SELECT COUNT(*) FROM orchestras o WHERE NOT EXISTS (SELECT 1 FROM plays p WHERE p.orchestra_id = o.id) AND o.name GLOB '[0-9]*';"

sep "audit anomalie contenuto"
$COMPOSE run --rm -e DB_PATH=/data/tracks.db -e NORMALIZED_DB=/data/tango.db \
    crawler python audit.py

echo ""
if [ "$ERRORS" -eq 0 ]; then
    echo "Tutto OK — nessuna anomalia trovata."
else
    echo "*** $ERRORS controlli falliti. Esegui: ./maintain.sh --fix"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
