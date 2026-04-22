# tango CLI Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Riorganizzare `bin/tango` con struttura gerarchica a tre gruppi (`db`, `query`, `analyze`), esporre `audit` e `maintain`, aggiornare i docstring Python.

**Architecture:** Riscrittura di `bin/tango` con dispatch a due livelli (gruppo → sottocomando). I file Python non cambiano logica — solo docstring/help. I vecchi comandi flat restano con deprecation warning su stderr.

**Tech Stack:** bash, Python 3.12 (solo docstring)

---

## File Structure

| File | Azione | Responsabilità |
|---|---|---|
| `bin/tango` | Riscrittura completa | CLI principale gerarchica |
| `convert.py` | Modifica docstring + help argparse | Allineamento vocabolario |
| `normalize.py` | Modifica descrizioni subparser | Allineamento vocabolario |
| `query.py` | Modifica docstring modulo | Allineamento vocabolario |
| `audit.py` | Modifica docstring modulo | Allineamento vocabolario |

---

### Task 1: Riscrittura `bin/tango`

**Files:**
- Modifica: `bin/tango`

- [ ] **Step 1: Verifica sintassi del file attuale (baseline)**

```bash
bash -n bin/tango
```
Atteso: nessun output (nessun errore).

- [ ] **Step 2: Sostituisci `bin/tango` con la nuova versione**

Contenuto completo del file:

```bash
#!/bin/bash
# tango — CLI principale per tango-crawler.
# Wrappa tutti gli script Python e bash del progetto.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="$HOME/.local/share/tango-crawler"
COMPOSE="docker compose -f $REPO_DIR/docker-compose.yml"

# ── Help ──────────────────────────────────────────────────────────────────────

usage() {
    cat <<'EOF'
tango-crawler — CLI

Uso: tango <gruppo> <sottocomando> [opzioni]
     tango <comando>  [opzioni]

Comandi:
  status                Stato del sistema (crawler, DB, cron)
  setup [build|fresh]   Setup iniziale o rebuild
  logs                  Log del crawler (docker compose logs -f)
  help                  Mostra questo messaggio

Gruppi:
  db        Gestione database (normalize, reparse, fix-tz, purge, maintain)
  query     Interrogazione dati (query per data/ora, catalog, stats)
  analyze   Analisi qualitativa (audit, similar, boundary)

Esempi:
  tango status
  tango db normalize
  tango db maintain --fix
  tango query --last 20
  tango query catalog --orchestra "DI SARLI"
  tango query stats orchestras --limit 10
  tango analyze audit
  tango analyze similar --threshold 0.9
  tango analyze boundary --minutes 3
EOF
}

usage_db() {
    cat <<'EOF'
tango db — Gestione database

Sottocomandi:
  normalize             Normalizza tracks.db → tango.db
  reparse               Ricalcola orchestra/singer/titolo/anno da raw_title
  fix-tz                Correggi timestamp +2h (eseguire UNA SOLA VOLTA)
  purge                 Elimina jingle ed errori di parsing da entrambi i DB
  maintain [--fix]      Verifica DB (con --fix: pulizia completa)

Esempi:
  tango db normalize
  tango db reparse
  tango db maintain
  tango db maintain --fix
EOF
}

usage_query() {
    cat <<'EOF'
tango query — Interrogazione dati

Sottocomandi:
  [nessuno]             Brani di oggi (o con filtri data/ora)
  catalog               Catalogo brani unici
  stats <tipo>          Statistiche di riproduzione

Tipi stats: orchestras, titles, singers, programs

Esempi:
  tango query --last 20
  tango query 13-14 --date 2026-04-01
  tango query --from "2026-03-31T21:00" --to "2026-03-31T23:59"
  tango query catalog --orchestra "DI SARLI"
  tango query stats orchestras --limit 10
  tango query stats titles --limit 20
  tango query stats singers
  tango query stats programs
EOF
}

usage_analyze() {
    cat <<'EOF'
tango analyze — Analisi qualitativa

Sottocomandi:
  audit                 Analisi anomalie contenuto + snapshot per AI
  similar               Trova titoli simili per normalizzazione
  boundary              Brani a cavallo delle fasce di palinsesto

Esempi:
  tango analyze audit
  tango analyze audit --threshold 0.9 --min-plays 5
  tango analyze similar --threshold 0.85 --limit 20
  tango analyze boundary --minutes 3
EOF
}

# ── Deprecation warning ───────────────────────────────────────────────────────

_deprecated() {
    echo "[DEPRECATO] usa: $*" >&2
}

# ── Dispatch ──────────────────────────────────────────────────────────────────

case "${1:-help}" in

    # ── Top-level ──────────────────────────────────────────────────────────────

    status)
        "$REPO_DIR/status.sh"
        ;;

    setup)
        shift
        "$REPO_DIR/setup.sh" "$@"
        ;;

    logs)
        docker compose -f "$REPO_DIR/docker-compose.yml" logs -f
        ;;

    # ── db ─────────────────────────────────────────────────────────────────────

    db)
        shift
        case "${1:-}" in
            normalize)
                shift
                $COMPOSE run --rm \
                    -e DB_PATH=/data/tracks.db \
                    -e NORMALIZED_DB=/data/tango.db \
                    crawler python normalize.py ingest "$@"
                ;;
            reparse)
                shift
                $COMPOSE run --rm \
                    -e DB_PATH=/data/tracks.db \
                    crawler python convert.py --reparse "$@"
                ;;
            fix-tz)
                shift
                $COMPOSE run --rm \
                    -e DB_PATH=/data/tracks.db \
                    crawler python convert.py --fix-tz "$@"
                ;;
            purge)
                shift
                $COMPOSE run --rm \
                    -e DB_PATH=/data/tracks.db \
                    -e NORMALIZED_DB=/data/tango.db \
                    crawler python normalize.py purge "$@"
                ;;
            maintain)
                shift
                "$REPO_DIR/maintain.sh" "$@"
                ;;
            "")
                usage_db
                ;;
            *)
                echo "Sottocomando db sconosciuto: $1" >&2
                echo "" >&2
                usage_db
                exit 1
                ;;
        esac
        ;;

    # ── query ──────────────────────────────────────────────────────────────────

    query)
        shift
        case "${1:-}" in
            catalog)
                shift
                python3 "$REPO_DIR/query.py" --catalog "$@"
                ;;
            stats)
                shift
                sub="${1:-}"
                [ $# -gt 0 ] && shift
                case "$sub" in
                    orchestras|orchestra)
                        python3 "$REPO_DIR/query.py" --top-orchestras "$@"
                        ;;
                    titles|title)
                        python3 "$REPO_DIR/query.py" --top-titles "$@"
                        ;;
                    singers|singer)
                        python3 "$REPO_DIR/query.py" --top-singers "$@"
                        ;;
                    programs|program)
                        python3 "$REPO_DIR/query.py" --programs "$@"
                        ;;
                    "")
                        echo "Uso: tango query stats <orchestras|titles|singers|programs> [--limit N]" >&2
                        exit 1
                        ;;
                    *)
                        echo "Tipo stats sconosciuto: $sub" >&2
                        echo "Uso: tango query stats <orchestras|titles|singers|programs> [--limit N]" >&2
                        exit 1
                        ;;
                esac
                ;;
            *)
                python3 "$REPO_DIR/query.py" "$@"
                ;;
        esac
        ;;

    # ── analyze ────────────────────────────────────────────────────────────────

    analyze)
        shift
        case "${1:-}" in
            audit)
                shift
                python3 "$REPO_DIR/audit.py" "$@"
                ;;
            similar)
                shift
                python3 "$REPO_DIR/normalize.py" similar-titles \
                    --dest "$DATA_DIR/tango.db" "$@"
                ;;
            boundary)
                shift
                python3 "$REPO_DIR/normalize.py" boundary \
                    --dest "$DATA_DIR/tango.db" "$@"
                ;;
            "")
                usage_analyze
                ;;
            *)
                echo "Sottocomando analyze sconosciuto: $1" >&2
                echo "" >&2
                usage_analyze
                exit 1
                ;;
        esac
        ;;

    # ── Legacy (deprecato) ─────────────────────────────────────────────────────

    normalize)
        _deprecated "tango db normalize"
        $COMPOSE run --rm \
            -e DB_PATH=/data/tracks.db \
            -e NORMALIZED_DB=/data/tango.db \
            crawler python normalize.py ingest
        ;;

    convert)
        shift
        _deprecated "tango db reparse / tango db fix-tz"
        $COMPOSE run --rm \
            -e DB_PATH=/data/tracks.db \
            crawler python convert.py "$@"
        ;;

    catalog)
        shift
        _deprecated "tango query catalog"
        python3 "$REPO_DIR/query.py" --catalog "$@"
        ;;

    stats)
        _deprecated "tango query stats"
        sub="${2:-}"
        shift; [ $# -gt 0 ] && shift
        case "$sub" in
            orchestras|orchestra)
                python3 "$REPO_DIR/query.py" --top-orchestras "$@"
                ;;
            titles|title)
                python3 "$REPO_DIR/query.py" --top-titles "$@"
                ;;
            singers|singer)
                python3 "$REPO_DIR/query.py" --top-singers "$@"
                ;;
            programs|program)
                python3 "$REPO_DIR/query.py" --programs "$@"
                ;;
            "")
                echo "Uso: tango stats <orchestras|titles|singers|programs> [--limit N]" >&2
                exit 1
                ;;
            *)
                echo "Statistiche sconosciute: $sub" >&2
                exit 1
                ;;
        esac
        ;;

    similar-titles)
        shift
        _deprecated "tango analyze similar"
        python3 "$REPO_DIR/normalize.py" similar-titles \
            --dest "$DATA_DIR/tango.db" "$@"
        ;;

    boundary)
        shift
        _deprecated "tango analyze boundary"
        python3 "$REPO_DIR/normalize.py" boundary \
            --dest "$DATA_DIR/tango.db" "$@"
        ;;

    purge)
        shift
        _deprecated "tango db purge"
        python3 "$REPO_DIR/normalize.py" purge \
            --source "$DATA_DIR/tracks.db" \
            --dest   "$DATA_DIR/tango.db" \
            "$@"
        ;;

    # ── Help ───────────────────────────────────────────────────────────────────

    help|--help|-h)
        usage
        ;;

    *)
        echo "Comando sconosciuto: $1" >&2
        echo "" >&2
        usage
        exit 1
        ;;
esac
```

- [ ] **Step 3: Verifica sintassi del nuovo file**

```bash
bash -n bin/tango
```
Atteso: nessun output.

- [ ] **Step 4: Smoke test help**

```bash
bash bin/tango help
bash bin/tango db
bash bin/tango query --help 2>&1 || true
bash bin/tango analyze
```
Atteso: output di help per ogni gruppo, nessun errore di sintassi bash.

- [ ] **Step 5: Smoke test legacy deprecation warning**

```bash
bash bin/tango catalog 2>&1 | head -1
```
Atteso: `[DEPRECATO] usa: tango query catalog`

- [ ] **Step 6: Smoke test comando sconosciuto**

```bash
bash bin/tango db foobar 2>&1; echo "exit: $?"
```
Atteso: messaggio di errore + `exit: 1`

- [ ] **Step 7: Commit**

```bash
git add bin/tango
git commit -m "feat: bin/tango — struttura gerarchica db/query/analyze"
```

---

### Task 2: Aggiornamento docstring Python

**Files:**
- Modifica: `convert.py`
- Modifica: `normalize.py`
- Modifica: `query.py`
- Modifica: `audit.py`

- [ ] **Step 1: Aggiorna `convert.py`**

Sostituisci il docstring del modulo (righe 1-16) con:

```python
#!/usr/bin/env python3
"""
Manutenzione di tracks.db — operazioni in-place senza ricreare lo schema.

Interfaccia principale:
  tango db reparse    → ricalcola orchestra/singer/titolo/anno da raw_title
  tango db fix-tz     → correggi timestamp +2h (eseguire UNA SOLA VOLTA)

Uso diretto (Docker/avanzato):
  python convert.py [--db /path/to/tracks.db] [--fix-tz] [--reparse] [--all]

  (senza flag) → rimuove cortine dal DB già migrato, oppure migra il vecchio schema
  --fix-tz     → corregge i timestamp +2h (record pre-TZ=Europe/Rome)
  --reparse    → ricalcola orchestra/singer/track_title/year/author/dancers/program
                 da raw_title usando il parser attuale
  --all        → esegue tutto in sequenza: fix-tz, reparse, pulizia cortine

Sicuro: crea una copia di backup (.bak) prima di qualsiasi operazione distruttiva.
"""
```

Aggiorna anche gli argparse help per `--fix-tz` e `--reparse`:

```python
    parser.add_argument("--fix-tz",  action="store_true",
                        help="Correggi timestamp +2h (tango db fix-tz)")
    parser.add_argument("--reparse", action="store_true",
                        help="Ri-parsa raw_title con il parser attuale (tango db reparse)")
```

- [ ] **Step 2: Aggiorna `normalize.py`**

Sostituisci il docstring del modulo (righe 1-11) con:

```python
#!/usr/bin/env python3
"""
Gestione tango.db — normalizzazione, analisi titoli, confini palinsesto.

Interfaccia principale:
  tango db normalize      → ingest tracks.db → tango.db
  tango db purge          → elimina jingle ed errori da entrambi i DB
  tango analyze similar   → trova titoli simili
  tango analyze boundary  → brani a cavallo delle fasce di palinsesto

Comandi diretti (avanzato):
  python normalize.py                                  # ingest (default)
  python normalize.py ingest [--source X] [--dest Y]
  python normalize.py similar-titles [--threshold 0.8] [--limit N]
  python normalize.py boundary [--minutes 5] [--limit N]
  python normalize.py purge [--source X] [--dest Y]
"""
```

- [ ] **Step 3: Aggiorna `query.py`**

Sostituisci il docstring del modulo (righe 1-24) con:

```python
#!/usr/bin/env python3
"""
Query rapida sul DB — eseguibile direttamente sull'host senza Docker.

Interfaccia principale:
  tango query [opzioni]              → brani per data/ora
  tango query catalog [opzioni]      → catalogo brani unici
  tango query stats <tipo>           → statistiche

Default: tango.db (storico normalizzato).
Flag --raw: tracks.db (giornata in corso, non ancora normalizzata).

Esempi diretti:
  python query.py                          # oggi da tango.db
  python query.py --raw                    # oggi da tracks.db
  python query.py 13                       # ore 13:00-13:59 di oggi
  python query.py 13-14                    # ore 13:00-14:59 di oggi
  python query.py 21-23 --date 2026-03-31
  python query.py --date 2026-03-31
  python query.py --from "2026-03-31T21:00" --to "2026-03-31T23:59"
  python query.py --last 20
  python query.py --catalog                        # tutti i brani unici
  python query.py --catalog --orchestra "DI SARLI" # catalogo di un'orchestra
  python query.py --catalog --title "cumparsita"   # cerca un titolo
  python query.py --top-orchestras                 # orchestre più riprodotte
  python query.py --top-titles --limit 20          # titoli più riprodotti (top 20)
  python query.py --top-singers --limit 10         # cantanti più riprodotti
  python query.py --programs                       # passaggi per fascia oraria
"""
```

- [ ] **Step 4: Aggiorna `audit.py`**

Sostituisci il docstring del modulo (righe 1-8) con:

```python
#!/usr/bin/env python3
"""
Analisi anomalie su tracks.db e tango.db.

Interfaccia principale:
  tango analyze audit [opzioni]

Uso diretto:
  python audit.py
  python audit.py --tracks /path/tracks.db --tango /path/tango.db
  python audit.py --threshold 0.85 --min-plays 3 --gap 90
"""
```

- [ ] **Step 5: Verifica che i test Python passino ancora**

```bash
python3 -m pytest tests/ -q
```
Atteso: `27 passed`

- [ ] **Step 6: Verifica sintassi Python**

```bash
python3 -m py_compile convert.py normalize.py query.py audit.py
```
Atteso: nessun output (nessun errore).

- [ ] **Step 7: Commit**

```bash
git add convert.py normalize.py query.py audit.py
git commit -m "docs: allinea docstring Python al nuovo vocabolario tango CLI"
```
