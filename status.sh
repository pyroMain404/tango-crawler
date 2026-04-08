#!/bin/bash
# Stato rapido di tango-crawler + normalizzazione manuale.
#
# Uso:
#   ./status.sh              # mostra stato
#   ./status.sh normalize    # normalizza subito
set -euo pipefail

DATA_DIR="$HOME/.local/share/tango-crawler"
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE="docker compose -f $REPO_DIR/docker-compose.yml"

# ── Normalizzazione manuale ───────────────────────────────────────────────────
if [ "${1:-}" = "normalize" ]; then
    echo "Normalizzo tracks.db → tango.db ..."
    $COMPOSE --profile normalize run --rm normalizer
    echo "Fatto."
    exit 0
fi

# ── Stato ─────────────────────────────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " tango-crawler — stato"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Container
printf "\n%-18s" "Crawler:"
if docker compose -f "$REPO_DIR/docker-compose.yml" ps --status running 2>/dev/null | grep -q crawler; then
    echo "attivo"
else
    echo "fermo"
fi

# Database
echo ""
for db in tracks.db tango.db; do
    path="$DATA_DIR/$db"
    if [ -f "$path" ]; then
        size=$(du -h "$path" | cut -f1)
        count=$(sqlite3 "$path" "SELECT COUNT(*) FROM $([ "$db" = "tracks.db" ] && echo tracks || echo plays);" 2>/dev/null || echo "?")
        printf "%-18s %s, %s record\n" "$db:" "$size" "$count"
    else
        printf "%-18s non trovato\n" "$db:"
    fi
done

# Catalogo
if [ -f "$DATA_DIR/tango.db" ]; then
    orchestras=$(sqlite3 "$DATA_DIR/tango.db" "SELECT COUNT(*) FROM orchestras;" 2>/dev/null || echo "?")
    titles=$(sqlite3 "$DATA_DIR/tango.db" "SELECT COUNT(*) FROM titles;" 2>/dev/null || echo "?")
    printf "\n%-18s %s orchestre, %s titoli\n" "Catalogo:" "$orchestras" "$titles"
fi

# Cron
echo ""
printf "%-18s" "Cron:"
if crontab -l 2>/dev/null | grep -q tango-crawler; then
    crontab -l 2>/dev/null | grep tango-crawler | awk '{print $2":"$1}'
else
    echo "non configurato"
fi

echo ""
echo "Comandi: ./tango help"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
