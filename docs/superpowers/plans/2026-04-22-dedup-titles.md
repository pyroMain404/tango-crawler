# Dedup Titles Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminare i titoli quasi-duplicati nel database: (1) durante `normalize`, canonicalizzare i titoli prima dell'inserimento; (2) aggiungere `tango db dedup` per risolvere retroattivamente i duplicati già presenti in tango.db.

**Architecture:** La canonicalizzazione (strip della punteggiatura finale) viene aggiunta in `common.py` e applicata in `normalize.py` prima di ogni lookup nel DB. Il comando `dedup` carica orchestra→titoli in memoria, usa `SequenceMatcher` (come `audit.py`) per trovare coppie simili per orchestra, poi esegue la merge SQL aggiornando `plays.title_id` e `playlist_items.title_id` e cancellando i titoli orfani.

**Tech Stack:** Python 3.11+, sqlite3, difflib.SequenceMatcher, pytest

---

## File Structure

| File | Ruolo |
|---|---|
| `common.py` | Aggiunta di `canonicalize_title()` |
| `normalize.py` | Applica `canonicalize_title` nel loop; aggiunge `dedup_titles()` e subparser `dedup` |
| `bin/tango` | Aggiunge routing `tango db dedup` e aggiorna help |
| `tests/test_common.py` | Test per `canonicalize_title` |
| `tests/test_normalize.py` | Nuovo file — test per `dedup_titles` e per normalize con canonicalizzazione |

---

### Task 1: `canonicalize_title()` in common.py

**Files:**
- Modify: `common.py` (dopo riga 32, prima di `get_program`)
- Modify: `tests/test_common.py` (append)

- [ ] **Step 1: Scrivi i test che falliscono**

Aggiungi in fondo a `tests/test_common.py`:

```python
from common import canonicalize_title


def test_canonicalize_strips_trailing_period():
    assert canonicalize_title("BAHIA BLANCA.") == "BAHIA BLANCA"


def test_canonicalize_strips_trailing_underscore():
    assert canonicalize_title("EL INGENIERO_") == "EL INGENIERO"


def test_canonicalize_strips_trailing_comma():
    assert canonicalize_title("CORAZON DE ORO,") == "CORAZON DE ORO"


def test_canonicalize_strips_trailing_colon():
    assert canonicalize_title("LA PUNALADA:") == "LA PUNALADA"


def test_canonicalize_strips_multiple_trailing():
    assert canonicalize_title("TITULO..") == "TITULO"


def test_canonicalize_no_change_for_clean_title():
    assert canonicalize_title("BAHIA BLANCA") == "BAHIA BLANCA"


def test_canonicalize_preserves_internal_punct():
    assert canonicalize_title("L`HYMNE A L`AMOUR") == "L`HYMNE A L`AMOUR"


def test_canonicalize_strips_trailing_space_with_punct():
    assert canonicalize_title("TITULO . ") == "TITULO"
```

- [ ] **Step 2: Esegui i test per verificare che falliscano**

```
pytest tests/test_common.py -k "canonicalize" -v
```

Atteso: `ImportError` o `AttributeError` — `canonicalize_title` non esiste.

- [ ] **Step 3: Implementa `canonicalize_title` in common.py**

Inserisci dopo la riga 32 (dopo `_SPLIT_RE`) e prima di `def get_program`:

```python
_TRAILING_PUNCT = re.compile(r'[\s.,_:]+$')


def canonicalize_title(title: str) -> str:
    return _TRAILING_PUNCT.sub('', title)
```

- [ ] **Step 4: Esegui i test per verificare che passino**

```
pytest tests/test_common.py -k "canonicalize" -v
```

Atteso: tutti PASS.

- [ ] **Step 5: Esegui la suite completa per verificare no regressioni**

```
pytest tests/test_common.py -v
```

Atteso: tutti i test precedenti passano ancora.

- [ ] **Step 6: Commit**

```bash
git add common.py tests/test_common.py
git commit -m "feat: canonicalize_title() rimuove punteggiatura finale dai titoli"
```

---

### Task 2: Applica canonicalizzazione in normalize.py

**Files:**
- Modify: `normalize.py` (riga 25 — import; riga 171 — loop)
- Modify: `tests/test_normalize.py` (nuovo file)

- [ ] **Step 1: Crea `tests/test_normalize.py` con un test che fallisce**

```python
import sqlite3
import pytest
from normalize import normalize


@pytest.fixture()
def db_pair(tmp_path):
    src = tmp_path / "tracks.db"
    dst = tmp_path / "tango.db"
    conn = sqlite3.connect(src)
    conn.execute("""
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_title TEXT NOT NULL,
            orchestra TEXT,
            singer TEXT,
            track_title TEXT,
            year INTEGER,
            author TEXT,
            dancers TEXT,
            program TEXT,
            fetched_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    return str(src), str(dst)


def insert_track(src_path, orchestra, title, fetched_at, singer=None):
    conn = sqlite3.connect(src_path)
    conn.execute(
        "INSERT INTO tracks (raw_title, orchestra, track_title, fetched_at, singer) VALUES (?, ?, ?, ?, ?)",
        (f"{orchestra} * {title}", orchestra, title, fetched_at, singer),
    )
    conn.commit()
    conn.close()


def test_normalize_canonicalizes_trailing_punct(db_pair):
    """'BAHIA BLANCA.' e 'BAHIA BLANCA' devono risultare nello stesso title_id."""
    src, dst = db_pair
    insert_track(src, "CARLOS DI SARLI", "BAHIA BLANCA.",  "2026-01-01T10:00:00")
    insert_track(src, "CARLOS DI SARLI", "BAHIA BLANCA",   "2026-01-01T10:01:00")
    normalize(src, dst)

    conn = sqlite3.connect(dst)
    titles = conn.execute("SELECT name FROM titles").fetchall()
    conn.close()
    assert len(titles) == 1, f"Atteso 1 titolo, trovati {len(titles)}: {titles}"
    assert titles[0][0] == "BAHIA BLANCA"


def test_normalize_canonicalizes_trailing_underscore(db_pair):
    src, dst = db_pair
    insert_track(src, "CARLOS DI SARLI", "EL INGENIERO_", "2026-01-01T10:00:00")
    normalize(src, dst)

    conn = sqlite3.connect(dst)
    name = conn.execute("SELECT name FROM titles").fetchone()[0]
    conn.close()
    assert name == "EL INGENIERO"
```

- [ ] **Step 2: Esegui il test per verificare che fallisca**

```
pytest tests/test_normalize.py::test_normalize_canonicalizes_trailing_punct -v
```

Atteso: FAIL — il test trova 2 titoli distinti (`'BAHIA BLANCA.'` e `'BAHIA BLANCA'`).

- [ ] **Step 3: Aggiorna l'import in normalize.py**

Riga 25, cambia:

```python
from common import DEFAULT_PROGRAM, JINGLE_ORCHESTRAS, PROGRAMS
```

in:

```python
from common import DEFAULT_PROGRAM, JINGLE_ORCHESTRAS, PROGRAMS, canonicalize_title
```

- [ ] **Step 4: Applica canonicalizzazione nel loop in normalize.py**

Riga 171, cambia:

```python
            title_id     = get_or_create(dest, "titles",     "name", track_title)
```

in:

```python
            title_id     = get_or_create(dest, "titles",     "name", canonicalize_title(track_title))
```

- [ ] **Step 5: Esegui i test per verificare che passino**

```
pytest tests/test_normalize.py -v
```

Atteso: tutti PASS.

- [ ] **Step 6: Esegui la suite completa**

```
pytest -v
```

Atteso: tutti PASS.

- [ ] **Step 7: Commit**

```bash
git add normalize.py tests/test_normalize.py
git commit -m "feat: applica canonicalize_title durante normalize per prevenire duplicati"
```

---

### Task 3: `dedup_titles()` — comando retroattivo per il DB esistente

**Files:**
- Modify: `normalize.py` — aggiunta funzione `dedup_titles()` e subparser `dedup`
- Modify: `tests/test_normalize.py` — aggiunta test per dedup

Il comando:
- Raggruppa titoli per orchestra (come `audit.py::check_similar_titles`)
- Trova coppie con `SequenceMatcher.ratio() >= threshold`
- Determina titolo canonico: quello uguale a `canonicalize_title(name)` fra i due; se entrambi/nessuno lo è, usa quello con più plays; a parità, l'id minore
- Modalità dry-run (default): stampa il piano
- Con `--apply`: esegue la merge nel DB

- [ ] **Step 1: Aggiungi i test per dedup in tests/test_normalize.py**

Append al file esistente:

```python
from normalize import dedup_titles


def _make_dst_with_duplicates(tmp_path):
    """Crea un tango.db con duplicati noti per i test."""
    src = tmp_path / "tracks.db"
    dst = tmp_path / "tango.db"
    conn = sqlite3.connect(src)
    conn.execute("""
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_title TEXT NOT NULL,
            orchestra TEXT,
            singer TEXT,
            track_title TEXT,
            year INTEGER,
            author TEXT,
            dancers TEXT,
            program TEXT,
            fetched_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

    tracks = [
        ("CARLOS DI SARLI", "BAHIA BLANCA",  "2026-01-01T10:00:00"),
        ("CARLOS DI SARLI", "BAHIA BLANCA.", "2026-01-01T10:01:00"),
        ("CARLOS DI SARLI", "TORMENTA",      "2026-01-01T10:02:00"),
        ("CARLOS DI SARLI", "TORMENTA.",     "2026-01-01T10:03:00"),
        ("OSVALDO PUGLIESE", "LA YUMBA",     "2026-01-01T11:00:00"),
        ("OSVALDO PUGLIESE", "LA YUMBA.",    "2026-01-01T11:01:00"),
    ]
    for orch, title, ts in tracks:
        insert_track(str(src), orch, title, ts)

    normalize(str(src), str(dst))
    # Dopo normalize con canonicalizzazione i duplicati "." sarebbero già risolti.
    # Per testare dedup su dati ESISTENTI già nel DB introduciamo manualmente duplicati:
    conn = sqlite3.connect(str(dst))
    conn.execute("PRAGMA foreign_keys = OFF")
    # Inserisci titoli "sporchi" manualmente
    conn.execute("INSERT INTO titles (name) VALUES ('BAHIA BLANCA.')")
    dup_id = conn.execute("SELECT id FROM titles WHERE name = 'BAHIA BLANCA.'").fetchone()[0]
    canonical_id = conn.execute("SELECT id FROM titles WHERE name = 'BAHIA BLANCA'").fetchone()[0]
    orch_id = conn.execute("SELECT id FROM orchestras WHERE name = 'CARLOS DI SARLI'").fetchone()[0]
    # Inserisci un play che punta al titolo "sporco"
    conn.execute(
        "INSERT INTO plays (orchestra_id, title_id, fetched_at) VALUES (?, ?, ?)",
        (orch_id, dup_id, "2026-01-01T10:99:00"),
    )
    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")
    conn.close()
    return str(dst), canonical_id, dup_id, orch_id


def test_dedup_dry_run_finds_pairs(tmp_path):
    dst, canonical_id, dup_id, _ = _make_dst_with_duplicates(tmp_path)
    pairs = dedup_titles(dst, threshold=0.9, apply=False)
    assert len(pairs) >= 1
    names = {(a, b) for a, b, *_ in pairs}
    assert ("BAHIA BLANCA", "BAHIA BLANCA.") in names or \
           ("BAHIA BLANCA.", "BAHIA BLANCA") in names


def test_dedup_apply_merges_plays(tmp_path):
    dst, canonical_id, dup_id, orch_id = _make_dst_with_duplicates(tmp_path)
    dedup_titles(dst, threshold=0.9, apply=True)

    conn = sqlite3.connect(dst)
    # Il titolo duplicato non deve più esistere
    row = conn.execute("SELECT id FROM titles WHERE name = 'BAHIA BLANCA.'").fetchone()
    assert row is None, "Il titolo duplicato dovrebbe essere stato cancellato"
    # Tutti i plays devono puntare al titolo canonico
    bad = conn.execute(
        "SELECT COUNT(*) FROM plays WHERE title_id = ?", (dup_id,)
    ).fetchone()[0]
    assert bad == 0, f"{bad} plays ancora puntano al titolo duplicato"
    conn.close()


def test_dedup_apply_idempotent(tmp_path):
    dst, _, _, _ = _make_dst_with_duplicates(tmp_path)
    pairs_first  = dedup_titles(dst, threshold=0.9, apply=True)
    pairs_second = dedup_titles(dst, threshold=0.9, apply=True)
    assert pairs_second == [], "Seconda esecuzione non dovrebbe trovare ulteriori duplicati"
```

- [ ] **Step 2: Esegui i test per verificare che falliscano**

```
pytest tests/test_normalize.py -k "dedup" -v
```

Atteso: `ImportError` — `dedup_titles` non esiste ancora.

- [ ] **Step 3: Implementa `dedup_titles()` in normalize.py**

Inserisci la funzione dopo `similar_titles()` (circa riga 267):

```python
def dedup_titles(dest_path: str, threshold: float, apply: bool) -> list[tuple]:
    """
    Trova e (opzionalmente) unisce titoli quasi-duplicati per la stessa orchestra.

    Restituisce lista di tuple (canonical_name, duplicate_name, ratio, orchestra_name).
    """
    conn = sqlite3.connect(dest_path)
    conn.execute("PRAGMA foreign_keys = ON")

    # Carica (orchestra_id, orchestra_name, title_id, title_name, play_count)
    rows = conn.execute("""
        SELECT o.id, o.name, t.id, t.name, COUNT(p.id)
        FROM plays p
        JOIN orchestras o ON o.id = p.orchestra_id
        JOIN titles t     ON t.id = p.title_id
        GROUP BY o.id, t.id
        ORDER BY o.name, t.name
    """).fetchall()

    # Raggruppa per orchestra
    from collections import defaultdict
    by_orch: dict[int, list[tuple]] = defaultdict(list)
    for orch_id, orch_name, title_id, title_name, play_count in rows:
        by_orch[orch_id].append((orch_name, title_id, title_name, play_count))

    sm = difflib.SequenceMatcher(autojunk=False)
    pairs: list[tuple] = []

    for orch_id, entries in by_orch.items():
        titles = list({t[2]: t for t in entries}.values())  # dedup per titolo
        for i in range(len(titles)):
            orch_name_i, tid_i, tname_i, plays_i = titles[i]
            sm.set_seq1(tname_i)
            for j in range(i + 1, len(titles)):
                orch_name_j, tid_j, tname_j, plays_j = titles[j]
                sm.set_seq2(tname_j)
                ratio = sm.ratio()
                if ratio >= threshold:
                    # Determina canonico: quello già privo di punteggiatura finale
                    clean_i = canonicalize_title(tname_i) == tname_i
                    clean_j = canonicalize_title(tname_j) == tname_j
                    if clean_i and not clean_j:
                        canon_id, canon_name, dup_id, dup_name = tid_i, tname_i, tid_j, tname_j
                    elif clean_j and not clean_i:
                        canon_id, canon_name, dup_id, dup_name = tid_j, tname_j, tid_i, tname_i
                    elif plays_i >= plays_j:
                        canon_id, canon_name, dup_id, dup_name = tid_i, tname_i, tid_j, tname_j
                    else:
                        canon_id, canon_name, dup_id, dup_name = tid_j, tname_j, tid_i, tname_i
                    pairs.append((canon_name, dup_name, ratio, orch_name_i, canon_id, dup_id))

    if not pairs:
        if not apply:
            print(f"Nessun titolo duplicato trovato (soglia {threshold}).")
        conn.close()
        return []

    if not apply:
        print(f"{'Orchestra':<35} {'Canonico':<40} {'Duplicato':<40} {'Ratio':>5}")
        print("-" * 125)
        for canon_name, dup_name, ratio, orch_name, *_ in pairs:
            print(f"  [{orch_name:<33}] {canon_name!r:<40} ← {dup_name!r:<40} ({ratio:.2f})")
        print(f"\n{len(pairs)} coppie trovate. Usa --apply per eseguire la merge.")
        conn.close()
        return [(a, b, r, o) for a, b, r, o, *_ in pairs]

    # --- Applica merge ---
    merged = 0
    for canon_name, dup_name, ratio, orch_name, canon_id, dup_id in pairs:
        conn.execute(
            "UPDATE plays SET title_id = ? WHERE title_id = ?",
            (canon_id, dup_id),
        )
        conn.execute(
            "UPDATE playlist_items SET title_id = ? WHERE title_id = ?",
            (canon_id, dup_id),
        )
        conn.execute("DELETE FROM titles WHERE id = ?", (dup_id,))
        merged += 1
        print(f"  [{orch_name}] {dup_name!r} → {canon_name!r}  ({ratio:.2f})")

    conn.commit()
    conn.close()
    print(f"\n{merged} titoli duplicati uniti.")
    return [(a, b, r, o) for a, b, r, o, *_ in pairs]
```

- [ ] **Step 4: Aggiungi subparser `dedup` in `main()` di normalize.py**

Aggiungi dopo il blocco `p_boundary` (circa riga 392):

```python
    # dedup
    p_dedup = sub.add_parser("dedup", help="Unisce titoli quasi-duplicati per orchestra")
    p_dedup.add_argument("--dest",      default=DEST_DB)
    p_dedup.add_argument("--threshold", type=float, default=0.92,
                         help="Soglia di similarità 0.0-1.0 (default: 0.92)")
    p_dedup.add_argument("--apply",     action="store_true",
                         help="Esegui la merge (default: dry-run)")
```

E aggiungi il branch in `if/elif` di `main()` (dopo `elif args.command == "purge":`):

```python
    elif args.command == "dedup":
        dedup_titles(args.dest, args.threshold, args.apply)
```

- [ ] **Step 5: Esegui i test per verificare che passino**

```
pytest tests/test_normalize.py -k "dedup" -v
```

Atteso: tutti PASS.

- [ ] **Step 6: Esegui la suite completa**

```
pytest -v
```

Atteso: tutti PASS.

- [ ] **Step 7: Commit**

```bash
git add normalize.py tests/test_normalize.py
git commit -m "feat: dedup_titles() — merge retroattivo titoli quasi-duplicati per orchestra"
```

---

### Task 4: Wire up `bin/tango`

**Files:**
- Modify: `bin/tango`

- [ ] **Step 1: Aggiorna `usage_db()` in bin/tango**

Cambia:

```bash
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
```

in:

```bash
usage_db() {
    cat <<'EOF'
tango db — Gestione database

Sottocomandi:
  normalize             Normalizza tracks.db → tango.db
  reparse               Ricalcola orchestra/singer/titolo/anno da raw_title
  fix-tz                Correggi timestamp +2h (eseguire UNA SOLA VOLTA)
  purge                 Elimina jingle ed errori di parsing da entrambi i DB
  maintain [--fix]      Verifica DB (con --fix: pulizia completa)
  dedup [--apply]       Trova e unisce titoli quasi-duplicati (dry-run senza --apply)

Esempi:
  tango db normalize
  tango db reparse
  tango db maintain
  tango db maintain --fix
  tango db dedup
  tango db dedup --threshold 0.95 --apply
EOF
}
```

- [ ] **Step 2: Aggiungi il case `dedup` nel blocco `db)` in bin/tango**

Aggiungi prima del blocco `"")`:

```bash
            dedup)
                shift
                python3 "$REPO_DIR/normalize.py" dedup \
                    --dest "$DATA_DIR/tango.db" "$@"
                ;;
```

- [ ] **Step 3: Verifica manuale che il routing funzioni**

```bash
./bin/tango db dedup --help
```

Atteso: help di argparse con `--threshold` e `--apply`.

```bash
./bin/tango db
```

Atteso: help `usage_db` aggiornato con la riga `dedup`.

- [ ] **Step 4: Esegui la suite completa un'ultima volta**

```
pytest -v
```

Atteso: tutti PASS.

- [ ] **Step 5: Commit**

```bash
git add bin/tango
git commit -m "feat: tango db dedup — aggiunto routing CLI per merge titoli duplicati"
```

---

## Self-Review

**Spec coverage:**
- ✅ Canonicalizzazione durante `normalize` (Task 1 + 2)
- ✅ Comando retroattivo per dati esistenti (Task 3 + 4)
- ✅ Threshold configurabile (`--threshold`)
- ✅ Dry-run safe by default, `--apply` per eseguire

**Placeholder scan:** nessuno.

**Type consistency:** `dedup_titles` ritorna `list[tuple]` — usato coerentemente nei test e in `main()`.

**Edge cases coperti:**
- Idempotenza: seconda esecuzione non trova ulteriori duplicati (test `test_dedup_apply_idempotent`)
- `playlist_items.title_id` aggiornato insieme a `plays.title_id`
- Titoli orfani cancellati dopo merge
