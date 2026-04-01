#!/bin/bash
# Setup iniziale di tango-crawler.
# Eseguire una sola volta dalla directory del progetto.
set -euo pipefail

# ── Configurazione ────────────────────────────────────────────────────────────
NORMALIZE_AT="06:00"      # orario giornaliero di normalizzazione (HH:MM)
DATA_DIR="$HOME/.local/share/tango-crawler"
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE="docker compose -f $REPO_DIR/docker-compose.yml"

# ── Helpers ───────────────────────────────────────────────────────────────────
info()    { echo "[INFO]  $*"; }
success() { echo "[OK]    $*"; }
warning() { echo "[WARN]  $*"; }

# ── 1. Directory dati ─────────────────────────────────────────────────────────
info "Creo directory dati: $DATA_DIR"
mkdir -p "$DATA_DIR"
success "Directory pronta."

# ── 2. sqlite3 sull'host (per query dirette) ──────────────────────────────────
if command -v sqlite3 &>/dev/null; then
    success "sqlite3 già installato: $(sqlite3 --version | cut -d' ' -f1)"
else
    info "Installo sqlite3..."
    sudo apt-get install -y --quiet sqlite3
    success "sqlite3 installato."
fi

# ── 3. Build immagine Docker ──────────────────────────────────────────────────
info "Build immagine Docker..."
$COMPOSE build --quiet
success "Immagine pronta."

# ── 4. Permessi su tracks.db (se esiste ed è di root) ────────────────────────
if [ -f "$DATA_DIR/tracks.db" ] && [ "$(stat -c '%U' "$DATA_DIR/tracks.db")" = "root" ]; then
    info "Correggo proprietario di tracks.db..."
    sudo chown "$USER" "$DATA_DIR/tracks.db"
    success "Proprietario corretto."
fi

# ── 5. Migrazione/pulizia DB ──────────────────────────────────────────────────
if [ -f "$DATA_DIR/tracks.db" ]; then
    info "Eseguo manutenzione tracks.db (migrazione schema + pulizia cortine)..."
    python3 "$REPO_DIR/convert.py" --db "$DATA_DIR/tracks.db"
    success "DB pronto."
else
    info "Nessun tracks.db esistente, verrà creato al primo avvio."
fi

# ── 6. Cron job per normalizzazione giornaliera ───────────────────────────────
CRON_HOUR=$(echo "$NORMALIZE_AT" | cut -d: -f1)
CRON_MIN=$(echo  "$NORMALIZE_AT" | cut -d: -f2)
CRON_CMD="$CRON_MIN $CRON_HOUR * * * $COMPOSE --profile normalize run --rm normalizer >> $DATA_DIR/normalize.log 2>&1"

# Rimuove eventuali voci precedenti, aggiunge quella aggiornata
( crontab -l 2>/dev/null | grep -v "tango-crawler"; echo "$CRON_CMD" ) | crontab -
success "Cron job configurato: normalizzazione ogni giorno alle $NORMALIZE_AT."

# ── 7. Avvio crawler ──────────────────────────────────────────────────────────
info "Avvio crawler..."
$COMPOSE up -d
success "Crawler avviato."

# ── Riepilogo ─────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Setup completato."
echo ""
echo " Dati:        $DATA_DIR"
echo " Normalizza:  ogni giorno alle $NORMALIZE_AT"
echo " Log cron:    $DATA_DIR/normalize.log"
echo ""
echo " Comandi utili:"
echo "   docker compose logs -f              # log crawler"
echo "   python3 query.py --last 20          # ultimi brani (tango.db)"
echo "   python3 query.py --raw --last 20    # brani di oggi (tracks.db)"
echo "   python3 convert.py --help           # manutenzione DB"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
