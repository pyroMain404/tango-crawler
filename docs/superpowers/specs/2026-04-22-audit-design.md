# audit.py — Design Spec
*Data: 2026-04-22*

## Obiettivo

Script autonomo `audit.py` che analizza entrambi i database (`tracks.db` e `tango.db`) per
rilevare anomalie sconosciute nel contenuto e produrre uno snapshot leggibile da incollare
in una chat AI per analisi ulteriore.

Non modifica nulla — è read-only.

## CLI

```
python audit.py [--tracks PATH] [--tango PATH] [--threshold 0.85] [--min-plays 3] [--gap 90]
```

| Flag | Default | Descrizione |
|---|---|---|
| `--tracks` | `$DB_PATH` o `~/.local/share/tango-crawler/tracks.db` | Percorso tracks.db |
| `--tango` | `$NORMALIZED_DB` o `~/.local/share/tango-crawler/tango.db` | Percorso tango.db |
| `--threshold` | `0.85` | Soglia similarità SequenceMatcher per titoli quasi-duplicati |
| `--min-plays` | `3` | Soglia sotto cui un'orchestra è considerata "rara" |
| `--gap` | `90` | Minuti di silenzio consecutivo considerati gap del crawler |

## Output

Stesso stile di `maintain.sh`: sezioni `━━━` e `──`, anomalie indentate, conteggio finale.
Nessun file scritto — solo stdout.

Struttura generale:
```
━━━ AUDIT tracks.db ━━━
── Gap del crawler ──
── Orchestre-fascia in tracks.db ──
── fetched_at duplicati ──

━━━ AUDIT tango.db ━━━
── Orchestre rare (< N passaggi) ──
── Caratteri insoliti in orchestre/titoli ──
── Titoli quasi-duplicati per stessa orchestra ──
── Anni inconsistenti ──
── Disallineamento programma/orario ──
── Duplicati temporali (stesso brano < 5 min) ──

━━━ SNAPSHOT AI ━━━
=== SNAPSHOT tango.db — YYYY-MM-DD ===
...
```

## Sezione 1 — AUDIT tracks.db

Saltata interamente se il DB non esiste o è vuoto.

### 1.1 Gap del crawler
- Carica tutti i `fetched_at` ordinati per tempo.
- Calcola la differenza in minuti tra ogni coppia consecutiva.
- Stampa le coppie con gap > `--gap` minuti.
- **Tecnica:** SQL per fetch, Python per diff.

### 1.2 Orchestre-fascia in tracks.db
- Cerca `orchestra` che corrisponde a pattern di fasce di palinsesto:
  regex `MILONGA\d+`, `^\d{4}[\*\-]?\d{4}`, nomi in `PROGRAMS` da `common.py`.
- **Tecnica:** SQL + regex Python.

### 1.3 `fetched_at` duplicati
- `SELECT fetched_at, COUNT(*) FROM tracks GROUP BY fetched_at HAVING COUNT(*) > 1`
- **Tecnica:** SQL puro.

## Sezione 2 — AUDIT tango.db

### 2.1 Orchestre rare
- Orchestre con meno di `--min-plays` passaggi totali.
- Per ognuna stampa: nome, conteggio, lista titoli associati (per giudicare se è un artista reale o un artifact).
- **Tecnica:** SQL.

### 2.2 Caratteri insoliti
- Orchestre e titoli che contengono: backtick `` ` ``, pipe `|`, sequenze di sole cifre, caratteri di controllo.
- **Tecnica:** SQL (`GLOB`, `LIKE`) + regex Python post-fetch.

### 2.3 Titoli quasi-duplicati per stessa orchestra
- Per ogni orchestra, confronta a coppie i titoli con `SequenceMatcher`.
- Stampa coppie con ratio >= `--threshold`.
- **Tecnica:** Python (stesso algoritmo di `normalize.py similar-titles` ma filtrato per orchestra).

### 2.4 Anni inconsistenti
- Coppie (orchestra, titolo) con più di un anno registrato e range > 10 anni.
- `SELECT orchestra_id, title_id, MIN(year), MAX(year) FROM plays WHERE year IS NOT NULL GROUP BY orchestra_id, title_id HAVING MAX(year) - MIN(year) > 10`
- **Tecnica:** SQL.

### 2.5 Disallineamento programma/orario
- Per ogni play: ricava l'ora reale da `fetched_at`, calcola il programma atteso via `get_program()` da `common.py`, confronta con `program_id` salvato.
- Stampa i play dove i due non coincidono.
- **Tecnica:** SQL per fetch, Python (`get_program`) per confronto.

### 2.6 Duplicati temporali
- Plays con stessa `orchestra_id` e `title_id` a distanza < 5 minuti l'uno dall'altro.
- `SELECT a.fetched_at, b.fetched_at, o.name, t.name FROM plays a JOIN plays b ON a.orchestra_id = b.orchestra_id AND a.title_id = b.title_id AND a.id < b.id JOIN orchestras o ... WHERE (julianday(b.fetched_at) - julianday(a.fetched_at)) * 1440 < 5`
- **Tecnica:** SQL.

## Sezione 3 — SNAPSHOT AI

Blocco di testo compatto, inizia con:
```
=== SNAPSHOT tango.db — YYYY-MM-DD ===
```

Contenuto:
1. **Statistiche generali**: totale plays, orchestre uniche, titoli unici, cantanti unici, arco temporale (primo/ultimo `fetched_at`).
2. **Top 30 orchestre** per passaggi: nome, conteggio, anni min-max osservati.
3. **Distribuzione per fascia di palinsesto**: nome fascia, conteggio plays.
4. **Distribuzione per decennio**: `1930s: N`, `1940s: N`, ecc. (basato su `year`).
5. **Orchestre rare** (< `--min-plays`): nome + titoli associati.
6. **Coppie quasi-duplicate** trovate nella sezione 2.3.

## Architettura del codice

```
audit.py
├── main()                        # CLI argparse, orchestrazione
├── audit_tracks(conn, args)      # sezione 1
│   ├── check_gaps()
│   ├── check_fascia_names()
│   └── check_duplicate_timestamps()
├── audit_tango(conn, args)       # sezione 2
│   ├── check_rare_orchestras()
│   ├── check_unusual_chars()
│   ├── check_similar_titles()    # SequenceMatcher per orchestra
│   ├── check_year_inconsistency()
│   ├── check_program_mismatch()
│   └── check_temporal_duplicates()
├── snapshot_ai(conn, args)       # sezione 3
└── helpers: sep(), fmt_ok(), fmt_anomaly()
```

Ogni funzione `check_*` ritorna una lista di stringhe (vuota = OK). `main()` stampa e tiene
il conteggio globale di sezioni con anomalie.

## Dipendenze

Solo stdlib Python + `common.py` già presente nel repo (per `get_program`, `PROGRAMS`,
`JINGLE_ORCHESTRAS`). Nessuna dipendenza esterna aggiuntiva.
