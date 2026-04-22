# Fix Log Degradations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminare i quattro problemi di qualità dati identificati nel log: fascia ID card salvate nel DB, asterischi finali nei titoli, typo nei nomi orchestra, e normalizzazione delle orchestre durante il parsing.

**Architecture:** Tutte le fix stanno in `common.py` (parsing) e `crawler.py` (decisione di salvataggio). I test vengono aggiunti in `tests/test_common.py` come primo file di test del progetto. Nessuna dipendenza nuova oltre a pytest.

**Tech Stack:** Python 3.11+, pytest, sqlite3 (built-in)

---

### Task 1: Setup pytest

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/test_common.py`

- [ ] **Step 1: Installa pytest come dev dependency**

```bash
pip install pytest
```

Expected output: `Successfully installed pytest-...`

- [ ] **Step 2: Crea `tests/__init__.py` vuoto**

```python
```

- [ ] **Step 3: Crea `tests/test_common.py` con smoke test**

```python
from common import parse_track


def test_smoke():
    result = parse_track("CARLOS DI SARLI * SENTIMIENTO CRIOLLO * 1941 *")
    assert result['orchestra'] == "CARLOS DI SARLI"
    assert result['track_title'] == "SENTIMIENTO CRIOLLO"
    assert result['year'] == 1941
```

- [ ] **Step 4: Verifica che il test passi**

```bash
pytest tests/test_common.py -v
```

Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add tests/__init__.py tests/test_common.py
git commit -m "chore: setup pytest con smoke test"
```

---

### Task 2: Fix — non salvare le fascia ID card (`track_title=None`)

**Problema:** `crawler.py:120–126` logga il warning ma poi esegue `insert_track` lo stesso. Le fascia ID card (raw `* * NOME_FASCIA * * * *`) producono 127 record spuri al giorno in `tracks.db`.

**Files:**
- Modify: `crawler.py:120-126`
- Modify: `tests/test_common.py`

- [ ] **Step 1: Scrivi il test che descrive il comportamento atteso**

Aggiungi a `tests/test_common.py`:

```python
def test_fascia_card_has_no_track_title():
    """Una fascia ID card non deve avere track_title."""
    result = parse_track("* * 1915*1985 *  *  *")
    assert result['track_title'] is None

def test_fascia_card_three_asterisks():
    """Variante con tre asterischi iniziali."""
    result = parse_track("* * * RANA FELICE *  *  *")
    assert result['track_title'] is None
```

- [ ] **Step 2: Esegui per verificare che i test passino (il parser già si comporta così)**

```bash
pytest tests/test_common.py -v
```

Expected: `3 passed`

- [ ] **Step 3: Modifica `crawler.py` per skippare i record con `track_title=None`**

Sostituisci le righe 120–126 di `crawler.py`:

```python
            now    = datetime.now()
            parsed = parse_track(raw_title)
            log.info("Raw: '%s'", raw_title)
            if not parsed.get('orchestra') or not parsed.get('track_title'):
                log.warning("Parsing degradato: raw='%s' parsed=%s", raw_title, parsed)
            if (parsed.get('orchestra') or '').upper() in JINGLE_ORCHESTRAS:
                log.debug("Ignorato (jingle): '%s'", raw_title)
                time.sleep(NORMAL_INTERVAL)
                continue
            insert_track(conn, raw_title, now, parsed)
```

Con:

```python
            now    = datetime.now()
            parsed = parse_track(raw_title)
            log.info("Raw: '%s'", raw_title)
            if not parsed.get('track_title'):
                log.warning("Parsing degradato (skippato): raw='%s' parsed=%s", raw_title, parsed)
                time.sleep(NORMAL_INTERVAL)
                continue
            if (parsed.get('orchestra') or '').upper() in JINGLE_ORCHESTRAS:
                log.debug("Ignorato (jingle): '%s'", raw_title)
                time.sleep(NORMAL_INTERVAL)
                continue
            insert_track(conn, raw_title, now, parsed)
```

- [ ] **Step 4: Verifica che i test passino ancora**

```bash
pytest tests/test_common.py -v
```

Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add crawler.py tests/test_common.py
git commit -m "fix: non salvare record con track_title=None (fascia ID card)"
```

---

### Task 3: Strip asterischi finali nei campi parsati

**Problema:** Titoli come `FLOR DE LINO*` e `LA LOCA DE AMOR*` conservano l'asterisco finale. Questo crea titoli duplicati se la stessa traccia viene ricevuta con e senza asterisco finale.

**Files:**
- Modify: `common.py:parse_track`
- Modify: `tests/test_common.py`

- [ ] **Step 1: Scrivi i test che falliscono**

Aggiungi a `tests/test_common.py`:

```python
def test_trailing_asterisk_stripped_from_title():
    result = parse_track("CARLOS DI SARLI * FLOR DE LINO* * 1941 *")
    assert result['track_title'] == "FLOR DE LINO"

def test_trailing_asterisk_stripped_from_orchestra():
    result = parse_track("CARLOS DI SARLI* * FLOR DE LINO * 1941 *")
    assert result['orchestra'] == "CARLOS DI SARLI"
```

- [ ] **Step 2: Esegui per verificare che i test falliscano**

```bash
pytest tests/test_common.py::test_trailing_asterisk_stripped_from_title tests/test_common.py::test_trailing_asterisk_stripped_from_orchestra -v
```

Expected: `2 failed`

- [ ] **Step 3: Modifica `parse_track` in `common.py` per strippare `*` finali**

Alla fine di `parse_track`, prima del `return`, aggiungi il clean-up:

```python
    def _clean(s: str | None) -> str | None:
        return s.rstrip('*').strip() if s else s

    return {
        'orchestra':   _clean(orchestra) or None,
        'singer':      _clean(singer),
        'track_title': _clean(track_title),
        'year':        year,
        'author':      author,
        'dancers':     dancers,
    }
```

Sostituisce il blocco `return` esistente (righe 65–72 di `common.py`):

```python
    return {
        'orchestra':   orchestra   or None,
        'singer':      singer,
        'track_title': track_title,
        'year':        year,
        'author':      author,
        'dancers':     dancers,
    }
```

- [ ] **Step 4: Verifica che tutti i test passino**

```bash
pytest tests/test_common.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add common.py tests/test_common.py
git commit -m "fix: strip asterischi finali da orchestra/singer/track_title"
```

---

### Task 4: Normalizzazione typo nomi orchestra

**Problema:** `OSVALDO PULIESE` (typo) viene salvato come entità separata da `OSVALDO PUGLIESE`, creando duplicati nel DB. Il meccanismo più semplice è una mappa di alias noti applicata al termine del parsing.

**Files:**
- Modify: `common.py`
- Modify: `tests/test_common.py`

- [ ] **Step 1: Scrivi il test che fallisce**

Aggiungi a `tests/test_common.py`:

```python
def test_orchestra_typo_normalized():
    result = parse_track("OSVALDO PULIESE * LA YUMBA * 1946 *")
    assert result['orchestra'] == "OSVALDO PUGLIESE"
```

- [ ] **Step 2: Esegui per verificare che fallisca**

```bash
pytest tests/test_common.py::test_orchestra_typo_normalized -v
```

Expected: `1 failed`

- [ ] **Step 3: Aggiungi `ORCHESTRA_ALIASES` a `common.py`**

Dopo le costanti `JINGLE_ORCHESTRAS` (riga ~21) aggiungi:

```python
# Mappa typo → nome canonico (uppercase). Aggiungere qui nuovi alias noti.
ORCHESTRA_ALIASES: dict[str, str] = {
    "OSVALDO PULIESE": "OSVALDO PUGLIESE",
}
```

- [ ] **Step 4: Applica la normalizzazione alla fine di `parse_track`**

Dentro la funzione `_clean` che hai aggiunto nel Task 3, espandi il blocco finale di `parse_track`:

```python
    def _clean(s: str | None) -> str | None:
        return s.rstrip('*').strip() if s else s

    norm_orchestra = _clean(orchestra) or None
    if norm_orchestra:
        norm_orchestra = ORCHESTRA_ALIASES.get(norm_orchestra.upper(), norm_orchestra)

    return {
        'orchestra':   norm_orchestra,
        'singer':      _clean(singer),
        'track_title': _clean(track_title),
        'year':        year,
        'author':      author,
        'dancers':     dancers,
    }
```

- [ ] **Step 5: Verifica che tutti i test passino**

```bash
pytest tests/test_common.py -v
```

Expected: `6 passed`

- [ ] **Step 6: Commit**

```bash
git add common.py tests/test_common.py
git commit -m "fix: normalizzazione typo nomi orchestra (PULIESE → PUGLIESE)"
```

---

### Task 5: Reparse dei record esistenti in tracks.db

**Problema:** I record già in `tracks.db` potrebbero avere titoli con `*` finali o typo orchestra. `convert.py --reparse` ricalcola tutti i campi da `raw_title` usando il parser aggiornato.

**Files:**
- Nessun file da modificare — usa `convert.py --reparse` esistente.

- [ ] **Step 1: Esegui reparse su tracks.db (dentro il container Docker)**

```bash
docker compose exec crawler python convert.py --reparse
```

Expected output:
```
Record ri-parsati: N
```

- [ ] **Step 2: Esegui ingest per portare le fix anche in tango.db**

```bash
docker compose exec crawler python normalize.py ingest
```

Expected output:
```
Record da normalizzare: N
OK: N inseriti, N già presenti. tracks.db svuotato.
```

- [ ] **Step 3: Verifica che non ci siano più record con titoli terminanti in `*` in tango.db**

```bash
docker compose exec crawler python query.py --catalog | grep '\*'
```

Expected: nessun output (zero risultati con asterisco).

---

## Self-Review

**Spec coverage:**
| Problema | Task |
|----------|------|
| Fascia ID card salvate (127/giorno) | Task 2 |
| Asterischi finali in titoli | Task 3 |
| Typo PULIESE/PUGLIESE | Task 4 |
| Variante tre asterischi (`* * * RANA FELICE`) | Task 2 (già coperta dalla guard `track_title=None`) |
| Reparse dati esistenti | Task 5 |

**Non in scope (rimandati):**
- Field-mapping con year vuoto + author non-parentesizzato (es. RADIOHEAD): edge case raro, richiede cambio del formato del sorgente radio, non del parser.
- Deduplicazione per `(orchestra, track_title, year)`: già gestita da `plays.fetched_at UNIQUE` che previene doppi timestamp; i duplicati esistenti (stessa traccia in orari diversi) sono dati legittimi.

**Tipo check:** `ORCHESTRA_ALIASES.get(norm_orchestra.upper(), norm_orchestra)` — chiave e valore sono entrambi `str`, coerente con il tipo `str | None` del campo.
