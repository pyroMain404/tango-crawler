"""Shared parsing logic used by crawler, convert, and normalize."""
import os
import re

PROGRAMS = [
    (1,  2,  "LE VIE DEL TANGO"),
    (5,  6,  "QUANDO NASCE UN AMORE 1915-1934"),
    (9,  10, "ORCHESTRE TIPICHE ATTUALI"),
    (11, 12, "EPOCA D'ORO 1935-1955"),
    (13, 14, "LE VIE DEL TANGO"),
    (15, 16, "IL TANGO SI FA ASCOLTARE 1956-1985"),
    (17, 19, "CREMA DI TANGO"),
    (20, 21, "ORCHESTRE TIPICHE ATTUALI"),
    (22, 24, "LA MILONGA DI TANGO PASIÓN RADIO"),
]
DEFAULT_PROGRAM = "1915-1985"

_jingle_env = os.getenv("JINGLE_ORCHESTRAS", "TANGO PASION RADIO")
JINGLE_ORCHESTRAS: frozenset[str] = frozenset(
    n.strip().upper() for n in _jingle_env.split(",") if n.strip()
)

_YEAR_RE   = re.compile(r'^(19|20)\d{2}$')
_PAREN_EXT = re.compile(r'\(([^)]*)\)')   # capture content
_PAREN_DEL = re.compile(r'\([^)]*\)')     # delete entire group
# Split on * only when NOT flanked by word/digit chars (avoids splitting "1915*1985")
_SPLIT_RE  = re.compile(r'(?<!\w)\*(?!\w)')


def get_program(hour: int) -> str:
    for start, end, name in PROGRAMS:
        if start <= hour < end:
            return name
    return DEFAULT_PROGRAM


def parse_track(raw: str) -> dict:
    """
    Parsa ORCHESTRA * [SINGER *] TITLE * [YEAR *] [(AUTORE)] * [(COPPIA)].
    I campi parentesizzati vengono estratti prima di splittare su '*'
    perché possono contenere '*' interni.
    """
    parens  = _PAREN_EXT.findall(raw)
    author  = parens[0].strip() if parens else None
    dancers = parens[1].strip() if len(parens) > 1 else None

    parts    = [p.strip() for p in _SPLIT_RE.split(_PAREN_DEL.sub('', raw)) if p.strip()]
    if not parts:
        return {'orchestra': None, 'singer': None, 'track_title': None,
                'year': None, 'author': None, 'dancers': None}

    orchestra = parts[0]
    year_idx  = next((i for i, p in enumerate(parts[1:], 1) if _YEAR_RE.match(p)), None)
    year      = int(parts[year_idx]) if year_idx is not None else None
    pre_year  = parts[1:year_idx] if year_idx is not None else parts[1:]

    singer = track_title = None
    if len(pre_year) == 1:
        track_title = pre_year[0]
    elif len(pre_year) == 2:
        singer, track_title = pre_year
    elif len(pre_year) > 2:
        singer, track_title = ', '.join(pre_year[:-1]), pre_year[-1]

    return {
        'orchestra':   orchestra   or None,
        'singer':      singer,
        'track_title': track_title,
        'year':        year,
        'author':      author,
        'dancers':     dancers,
    }
