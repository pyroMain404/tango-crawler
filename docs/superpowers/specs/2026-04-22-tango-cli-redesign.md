# tango CLI — Redesign Gerarchico
*Data: 2026-04-22*

## Obiettivo

Riorganizzare `bin/tango` con struttura gerarchica a tre gruppi, esporre i comandi finora non accessibili (`audit`, `maintain`), e allineare i docstring/help dei file Python al nuovo vocabolario. Zero cambiamenti alla logica Python.

## Approccio

**B — `bin/tango` + aggiornamento help Python:**
- `bin/tango` viene riscritto con dispatch gerarchico
- I file Python mantengono le loro CLI attuali (compatibilità Docker/standalone)
- Solo docstring e testi `help=` vengono aggiornati per riflettere i nuovi nomi

## Struttura Comandi

### Top-level (invariati)

| Comando | Descrizione |
|---|---|
| `tango status` | Stato sistema (crawler, DB, cron) |
| `tango setup [build\|fresh]` | Setup iniziale o rebuild |
| `tango logs` | Log del crawler in streaming |
| `tango help` | Help generale |

### `tango db <sottocomando>`

| Comando | Invocazione | Precedente |
|---|---|---|
| `tango db normalize` | `normalize.py ingest` | `tango normalize` |
| `tango db reparse` | `convert.py --reparse` | `tango convert --reparse` |
| `tango db fix-tz` | `convert.py --fix-tz` | `tango convert --fix-tz` |
| `tango db purge` | `normalize.py purge` | `tango purge` |
| `tango db maintain [--fix]` | `maintain.sh [--fix]` | *(non esposto)* |

`tango db` senza sottocomando → help del gruppo.

### `tango query <sottocomando>`

| Comando | Invocazione | Precedente |
|---|---|---|
| `tango query [time/date opts]` | `query.py [opts]` | `tango query` |
| `tango query catalog [opts]` | `query.py --catalog [opts]` | `tango catalog` |
| `tango query stats <type> [--limit N]` | `query.py --top-<type>` / `--programs` | `tango stats` |

Tipi per `stats`: `orchestras`, `titles`, `singers`, `programs`.

`tango query` senza sottocomando → esegue query del giorno (compatibile con il comportamento attuale).

### `tango analyze <sottocomando>`

| Comando | Invocazione | Precedente |
|---|---|---|
| `tango analyze audit [opts]` | `audit.py [opts]` | *(non esposto)* |
| `tango analyze similar [opts]` | `normalize.py similar-titles [opts]` | `tango similar-titles` |
| `tango analyze boundary [opts]` | `normalize.py boundary [opts]` | `tango boundary` |

`tango analyze` senza sottocomando → help del gruppo.

## Compatibilità Legacy

I vecchi comandi flat (`normalize`, `convert`, `catalog`, `stats`, `similar-titles`, `boundary`, `purge`) restano funzionanti con un avviso di deprecazione su stderr:

```
[DEPRECATO] usa: tango db normalize
```

L'avviso non blocca l'esecuzione. Questo garantisce che cron job e script esistenti continuino a funzionare senza modifiche.

## Struttura Interna di `bin/tango`

```bash
case "${1:-help}" in
    status|setup|logs|help)
        # dispatch diretto (invariato)
        ;;
    db)
        shift
        case "${1:-}" in
            normalize|reparse|fix-tz|purge|maintain) ... ;;
            *) usage_db ;;
        esac
        ;;
    query)
        shift
        case "${1:-}" in
            catalog|stats|"") ... ;;   # "" → query del giorno
            *) usage_query ;;
        esac
        ;;
    analyze)
        shift
        case "${1:-}" in
            audit|similar|boundary) ... ;;
            *) usage_analyze ;;
        esac
        ;;
    # Legacy con deprecation warning:
    normalize|convert|catalog|stats|similar-titles|boundary|purge)
        echo "[DEPRECATO] ..." >&2
        # esegui come prima
        ;;
    *)
        echo "Comando sconosciuto: $1" >&2; usage; exit 1 ;;
esac
```

## Modifiche ai File Python

Solo testi `help=` e docstring — zero cambiamenti alla logica.

| File | Cosa cambia |
|---|---|
| `convert.py` | Docstring modulo + help `--fix-tz`, `--reparse` |
| `normalize.py` | Descrizioni subparser `ingest`, `similar-titles`, `boundary`, `purge` |
| `query.py` | Docstring modulo (esempi con nuovi comandi) |
| `audit.py` | Docstring modulo (aggiunge nota `tango analyze audit`) |

## File Modificati

| File | Azione |
|---|---|
| `bin/tango` | Riscrittura completa |
| `convert.py` | Modifica docstring/help |
| `normalize.py` | Modifica docstring/help |
| `query.py` | Modifica docstring |
| `audit.py` | Modifica docstring |

## Test

Nessun test unitario da aggiungere (i test Python testano funzioni, non il wrapper bash). Verifica manuale con `bash -n bin/tango` per la sintassi e smoke test dei comandi principali.
