"""Title-based language classifier for prehraj.to uploads.

Pure-Python module shared by:
  * scripts/import-prehrajto-uploads.py
  * scripts/import-prehrajto-new-films.py
  * scripts/test_detect_lang.py

Keeping the regexes + `detect_lang()` here lets the test suite import
them without dragging in DB/HTTP runtime deps (psycopg2, requests).
The two importer scripts re-export the same names for backwards
compatibility with any existing internal/external references.

Output classes match the DB CHECK on `video_sources.lang_class`:
CZ_DUB / CZ_NATIVE / CZ_SUB / SK_DUB / SK_SUB / EN / UNKNOWN.
"""

from __future__ import annotations

import re

CZ_DIACRITICS = set("ěščřžýáíéúůťďňôäľĺŕ")
CZ_WORDS = {
    "a", "i", "do", "na", "se", "si", "ze", "za", "po", "pro", "pod", "nad",
    "v", "u", "o", "s", "k", "ke", "ku", "je", "jsou", "byl", "byla", "bylo",
    "mě", "mně", "mi", "tě", "ty", "ten", "ta", "to", "jeho", "její",
    "není", "náš", "naše", "svůj", "svá", "svou", "svém", "svému",
    "co", "kdo", "kde", "kdy", "proč", "jak", "jaký", "která",
    "jsem", "jsi", "jsme", "jste",
}

CZ_DUB_RE = re.compile(r"(?:\bcz[\s\-_]*dab(?:ing)?\b|\bczdab\w*|\bczdub\w*|\bcesk[aáyý][\s\-_]*dab(?:ing)?\b|\bc[zs][\s\-_]*dabing\b|cesky[\s\-_]*dabing|cz[\s\-_]*\.dab\b)", re.IGNORECASE)
CZ_SUB_RE = re.compile(r"(?:\bcz[\s\-_]*tit(?:ulky)?\b|\bcztit\w*|\bcz[\s\-_]*subs?\b|\bc[zs][\s\-_]*titulky\b|cesk[yé][\s\-_]*titulky)", re.IGNORECASE)
SK_DUB_RE = re.compile(r"(?:\bsk[\s\-_]*dab(?:ing)?\b|\bskdab\w*|\bskdub\w*|\bsloven(?:sk[yáé]|ina)[\s\-_]*dab(?:ing)?\b)", re.IGNORECASE)
SK_SUB_RE = re.compile(r"(?:\bsk[\s\-_]*tit(?:ulky)?\b|\bsktit\w*)", re.IGNORECASE)
EN_ONLY_RE = re.compile(r"(?:\bengsub\b|\beng\s*sub\b|\beng\s*only\b|\bengdub\b)", re.IGNORECASE)


def detect_lang(title: str) -> str:
    if not title:
        return "UNKNOWN"
    t = title.lower()
    if CZ_DUB_RE.search(t):
        return "CZ_DUB"
    if SK_DUB_RE.search(t):
        return "SK_DUB"
    if CZ_SUB_RE.search(t):
        return "CZ_SUB"
    if SK_SUB_RE.search(t):
        return "SK_SUB"
    has_cz = bool(re.search(r"\bcz\b", t)) or bool(re.search(r"\bcesk[yáyé]", t))
    if EN_ONLY_RE.search(t) and not has_cz:
        return "EN"
    dia_hits = sum(1 for c in t if c in CZ_DIACRITICS)
    tokens = re.findall(r"[a-záčďéěíňóřšťúůýž]+", t)
    cz_word_hits = sum(1 for tok in tokens if tok in CZ_WORDS)
    if dia_hits >= 1 and cz_word_hits >= 1:
        return "CZ_NATIVE"
    if dia_hits >= 2:
        return "CZ_NATIVE"
    return "UNKNOWN"
