"""Search prehraj.to for an upload matching a given film row.

Originally lived as private functions inside `scripts/backfill-prehrajto-
from-search.py`. Moved here so the daily auto-import can attach prehraj.to
sources to films right after they're added from SK Torrent — without
spawning a subprocess or duplicating the heuristic.

Public API:

    try_prehrajto_match(cur, providers, film_id, *,
                        title, original_title, year, runtime_min,
                        sess) -> dict

`cur` is a psycopg2 cursor in an open transaction; the helper writes via
`dual_write_prehrajto_upload` (and re-points cross-film collisions when
the title evidence strongly favours `film_id`).

Raises `BlockedError` when the CZ proxy / prehraj.to returns HTTP non-200
or a suspiciously short body — propagate up so the caller can abort the
run before burning more proxy quota.
"""

from __future__ import annotations

import difflib
import logging
import re
import time
import unicodedata
import urllib.parse
from dataclasses import dataclass

import psycopg2
import requests
from bs4 import BeautifulSoup

from .cz_proxy import proxy_get

# Re-exported so callers (backfill script, auto-import) can use a single
# helper module without duplicating the dual-write helpers' import path.
from video_sources_helper import dual_write_prehrajto_upload  # noqa: F401

log = logging.getLogger(__name__)

SEARCH_BASE = "https://prehraj.to/hledej/"
SEARCH_SLEEP_S = 2.5
SIM_GATE = 0.50
YEAR_TOL = 1
MIN_BODY_LEN = 5000
DUR_TOL = 0.20
DUR_HARD_REJECT = 0.50
MAX_PAGES = 5

EP_RE = re.compile(r"\b[Ss]\d{1,2}[Ee]\d{1,3}\b|\b\d{1,2}x\d{1,3}\b")
_HIT_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")

CZ_DUB_RE = re.compile(
    r"(?:\bcz\s*dab(?:ing)?\b|\bczdab\w*|\bczdub\w*|"
    r"\bcesk[aáyý]\s*dab(?:ing)?\b|\bc[zs]\s*dabing\b|cesky\s*dabing|cz\s*\.dab\b)",
    re.IGNORECASE,
)
CZ_SUB_RE = re.compile(
    r"(?:\bcz\s*tit(?:ulky)?\b|\bcztit\w*|\bcz\s*subs?\b|\bc[zs]\s*titulky\b|cesk[yé]\s*titulky)",
    re.IGNORECASE,
)
SK_DUB_RE = re.compile(
    r"(?:\bsk\s*dab(?:ing)?\b|\bskdab\w*|\bskdub\w*|\bsloven(?:sk[yáé]|ina)\s*dab(?:ing)?\b)",
    re.IGNORECASE,
)
SK_SUB_RE = re.compile(r"(?:\bsk\s*tit(?:ulky)?\b|\bsktit\w*)", re.IGNORECASE)
EN_ONLY_RE = re.compile(
    r"(?:\bengsub\b|\beng\s*sub\b|\beng\s*only\b|\bengdub\b)", re.IGNORECASE
)
_RES_RE = re.compile(
    r"(2160p|1080p|720p|480p|BDRip|BluRay|WEBRip|WEB[\s-]?DL|HDRip|DVDRip|HDTV|TVRip|CAM|TS)",
    re.IGNORECASE,
)


class BlockedError(RuntimeError):
    """Raised when prehraj.to (or the CZ proxy) appears to block us."""


@dataclass
class Hit:
    href: str
    external_id: str
    title: str
    duration_sec: int | None
    filesize_bytes: int | None


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
    return "UNKNOWN"


def extract_resolution(title: str) -> str | None:
    m = _RES_RE.search(title or "")
    return m.group(1).lower() if m else None


def ascii_fold(s: str) -> str:
    """Lowercase + strip diacritics + normalize ampersand-equivalents +
    fold release-style separators (`.`, `-`, `_`, `:`) to spaces.
    """
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    for token in (" and ", " und ", " & ", "&"):
        s = s.replace(token, " a ")
    for ch in ".,;:_-":
        s = s.replace(ch, " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _sim(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, ascii_fold(a), ascii_fold(b)).ratio()


def _parse_duration_to_sec(s: str | None) -> int | None:
    if not s:
        return None
    parts = s.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None
    if len(nums) == 3:
        h, m, sec = nums
        return h * 3600 + m * 60 + sec
    if len(nums) == 2:
        m, sec = nums
        return m * 60 + sec
    return None


def parse_search_html(html: str) -> list[Hit]:
    soup = BeautifulSoup(html, "html.parser")
    hits: list[Hit] = []
    seen_ext_ids: set[str] = set()
    for div in soup.select("div.video__picture--container"):
        a = div.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        m = re.match(r"^/(.+?)/([0-9a-f]{8,})$", href)
        if not m:
            continue
        _slug, ext_id = m.groups()
        if ext_id in seen_ext_ids:
            continue
        seen_ext_ids.add(ext_id)
        h3 = div.find("h3", class_="video__title")
        title = h3.get_text(strip=True) if h3 else _slug
        dur = div.find("div", class_="video__tag--time")
        duration_sec = _parse_duration_to_sec(
            dur.get_text(strip=True) if dur else None
        )
        sz = div.find("div", class_="video__tag--size")
        filesize_bytes = None
        if sz:
            sz_text = sz.get_text(strip=True)
            mz = re.match(r"^([\d\.]+)\s*(MB|GB)", sz_text, re.IGNORECASE)
            if mz:
                v = float(mz.group(1))
                if mz.group(2).upper() == "GB":
                    v *= 1024
                filesize_bytes = int(v * 1024 * 1024)
        hits.append(Hit(
            href=href, external_id=ext_id, title=title,
            duration_sec=duration_sec, filesize_bytes=filesize_bytes,
        ))
    return hits


def classify_match(
    hit: Hit, db_titles: list[str], year: int | None,
    runtime_min: int | None,
) -> tuple[str, float, dict]:
    """Tier-based match classification.

    Returns (tier, sim, meta) where tier ∈ strong, solid, weak,
    reject_tv, reject_duration, reject_low_sim, reject. See backfill
    script docstring for the rationale.
    """
    if EP_RE.search(hit.title):
        return ("reject_tv", 0.0, {"reason": "tv_episode"})

    best = 0.0
    contains = False
    hit_folded = ascii_fold(hit.title)
    for t in db_titles:
        s = _sim(hit.title, t)
        if s > best:
            best = s
        t_folded = ascii_fold(t)
        if len(t_folded) >= 4 and t_folded in hit_folded:
            contains = True
    if contains:
        best = max(best, 1.0)

    yr_m = _HIT_YEAR_RE.search(hit.title)
    hit_year = int(yr_m.group(1)) if yr_m else None
    year_match = bool(year and hit_year and abs(hit_year - year) <= YEAR_TOL)

    hit_dur_min = (hit.duration_sec // 60) if hit.duration_sec else None
    dur_match = None
    dur_delta = None
    if hit_dur_min and runtime_min:
        dur_delta = abs(hit_dur_min - runtime_min) / runtime_min
        dur_match = dur_delta <= DUR_TOL

    meta = {
        "contains": contains, "year_match": year_match,
        "hit_year": hit_year, "hit_dur_min": hit_dur_min,
        "dur_match": dur_match, "dur_delta": dur_delta,
    }

    if dur_delta is not None and dur_delta > DUR_HARD_REJECT:
        return ("reject_duration", best, meta)

    title_ok = best >= SIM_GATE or contains
    if not title_ok:
        return ("reject_low_sim", best, meta)

    if year_match and dur_match:
        return ("strong", best, meta)
    if year_match or dur_match:
        return ("solid", best, meta)
    if hit_year is None and hit_dur_min is None:
        return ("weak", best, meta)
    return ("reject", best, meta)


def build_query(title: str, original: str | None, year: int | None) -> str:
    base = title or original or ""
    if year:
        base = f"{base} ({year})"
    base = re.sub(r"[/?#&%]+", " ", base)
    return re.sub(r"\s+", " ", base).strip()


def _detect_max_page(html: str) -> int:
    pages = re.findall(r"visualPaginator-page=(\d+)", html)
    if not pages:
        return 1
    return min(MAX_PAGES, max(int(p) for p in pages))


def search_prehrajto(
    sess: requests.Session, query: str,
    db_titles: list[str], db_year: int | None, db_runtime_min: int | None,
) -> list[Hit]:
    all_hits: list[Hit] = []
    seen: set[str] = set()
    db_titles_full = list(db_titles)
    for t in list(db_titles):
        folded = ascii_fold(t)
        if folded != t.lower():
            db_titles_full.append(folded)
    have_strong = False
    page = 1
    max_page = MAX_PAGES
    while page <= max_page:
        if page == 1:
            url = SEARCH_BASE + urllib.parse.quote(query, safe='')
        else:
            url = (
                SEARCH_BASE + urllib.parse.quote(query, safe='')
                + f"?videoListing-visualPaginator-page={page}"
            )
        r = proxy_get(url, sess, timeout=30)
        if r.status_code != 200:
            log.error("BLOCKED: HTTP %d for query=%r url=%s",
                      r.status_code, query, url)
            raise BlockedError(f"HTTP {r.status_code}")
        body = r.text
        if len(body) < MIN_BODY_LEN:
            log.error("BLOCKED: short body len=%d for query=%r", len(body), query)
            raise BlockedError(f"body too short ({len(body)})")
        if page == 1:
            max_page = _detect_max_page(body)
        page_hits = parse_search_html(body)
        new_hits = 0
        for h in page_hits:
            if h.external_id in seen:
                continue
            seen.add(h.external_id)
            all_hits.append(h)
            new_hits += 1
            tier, _, _ = classify_match(h, db_titles_full, db_year, db_runtime_min)
            if tier == "strong":
                have_strong = True
        if page == 1 and have_strong:
            return all_hits
        if new_hits == 0:
            break
        page += 1
        if page <= max_page:
            time.sleep(SEARCH_SLEEP_S)
    return all_hits


_FPU_UPSERT_SQL = """
    INSERT INTO film_prehrajto_uploads
        (film_id, upload_id, url, title, duration_sec, view_count,
         lang_class, resolution_hint, last_seen_at, is_alive)
    VALUES
        (%(film_id)s, %(upload_id)s, %(url)s, %(title)s, %(duration_sec)s, %(view_count)s,
         %(lang_class)s, %(resolution_hint)s, NOW(), TRUE)
    ON CONFLICT (upload_id) DO UPDATE SET
        url             = EXCLUDED.url,
        title           = EXCLUDED.title,
        duration_sec    = EXCLUDED.duration_sec,
        lang_class      = EXCLUDED.lang_class,
        resolution_hint = EXCLUDED.resolution_hint,
        last_seen_at    = EXCLUDED.last_seen_at,
        is_alive        = TRUE
    WHERE film_prehrajto_uploads.film_id = EXCLUDED.film_id
"""


def _is_existing_owner_worse_match(
    cur, existing_film_id: int, hit: Hit, our_film_id: int,
    our_titles: list[str],
) -> tuple[bool, str]:
    if existing_film_id == our_film_id:
        return False, "same_film"
    cur.execute(
        "SELECT title, year, runtime_min FROM films WHERE id = %s",
        (existing_film_id,),
    )
    row = cur.fetchone()
    if not row:
        return True, "owner_missing"
    o_title, o_year, o_runtime = row

    yr_m = _HIT_YEAR_RE.search(hit.title)
    hit_year = int(yr_m.group(1)) if yr_m else None
    hit_dur_min = (hit.duration_sec // 60) if hit.duration_sec else None

    if hit_year and o_year and abs(hit_year - o_year) > YEAR_TOL:
        return True, f"year_mismatch hit={hit_year} owner={o_year}"
    if hit_dur_min and o_runtime:
        delta = abs(hit_dur_min - o_runtime) / o_runtime
        if delta > DUR_TOL:
            return True, (f"dur_mismatch hit={hit_dur_min}min "
                          f"owner={o_runtime}min Δ={delta:.0%}")

    hit_folded = ascii_fold(hit.title)
    sim_to_us = max(_sim(hit.title, t) for t in our_titles) if our_titles else 0.0
    contains_us = any(
        ascii_fold(t) in hit_folded for t in our_titles
        if t and len(ascii_fold(t)) >= 4
    )
    if contains_us:
        sim_to_us = max(sim_to_us, 1.0)
    sim_to_owner = _sim(hit.title, o_title) if o_title else 0.0
    o_folded = ascii_fold(o_title) if o_title else ""
    if o_folded and len(o_folded) >= 4 and o_folded in hit_folded:
        sim_to_owner = max(sim_to_owner, 1.0)
    if sim_to_us - sim_to_owner >= 0.20:
        return True, (f"title_evidence sim_us={sim_to_us:.2f} "
                      f"sim_owner={sim_to_owner:.2f}")

    return False, (f"owner_consistent sim_us={sim_to_us:.2f} "
                   f"sim_owner={sim_to_owner:.2f} "
                   f"yr={o_year}/{hit_year} dur={o_runtime}/{hit_dur_min}")


def write_hits(
    cur, providers: dict, film_id: int,
    hits_with_meta: list[tuple],
    our_titles: list[str] | None = None,
) -> tuple[int, int, int]:
    """Insert / re-point accepted hits.

    `hits_with_meta` items: (hit, tier).
    Returns (written, repointed, collision_skipped).
    """
    written = 0
    repointed = 0
    skipped_collision = 0
    for hit, tier in hits_with_meta:
        lang = detect_lang(hit.title)
        res = extract_resolution(hit.title)
        url = "https://prehraj.to" + hit.href
        row = {
            "film_id": film_id, "upload_id": hit.external_id, "url": url,
            "title": hit.title, "duration_sec": hit.duration_sec,
            "view_count": None, "lang_class": lang, "resolution_hint": res,
        }
        cur.execute(_FPU_UPSERT_SQL, row)
        cur.execute(
            "SELECT id, film_id FROM video_sources "
            "WHERE provider_id = %s AND external_id = %s",
            (providers["prehrajto"], hit.external_id),
        )
        existing = cur.fetchone()

        if existing is None:
            cur.execute("SAVEPOINT dw")
            try:
                dual_write_prehrajto_upload(
                    cur, providers=providers, film_id=film_id,
                    upload_row={**row, "is_direct": False, "is_alive": True},
                    primary_upload_id=None,
                )
                cur.execute("RELEASE SAVEPOINT dw")
                written += 1
            except psycopg2.errors.UniqueViolation as e:
                cur.execute("ROLLBACK TO SAVEPOINT dw")
                cur.execute("RELEASE SAVEPOINT dw")
                log.warning(
                    "dual_write race film_id=%d upload_id=%s (%s)",
                    film_id, hit.external_id,
                    getattr(getattr(e, "diag", None), "constraint_name", "unique"),
                )
            continue

        existing_id, existing_film_id = existing
        if existing_film_id == film_id:
            cur.execute("SAVEPOINT dw")
            try:
                dual_write_prehrajto_upload(
                    cur, providers=providers, film_id=film_id,
                    upload_row={**row, "is_direct": False, "is_alive": True},
                    primary_upload_id=None,
                )
                cur.execute("RELEASE SAVEPOINT dw")
                written += 1
            except psycopg2.errors.UniqueViolation:
                cur.execute("ROLLBACK TO SAVEPOINT dw")
                cur.execute("RELEASE SAVEPOINT dw")
            continue

        if tier != "strong":
            skipped_collision += 1
            log.info("  collision skipped (tier=%s): upload_id=%s on film=%d, ours=%d",
                     tier, hit.external_id, existing_film_id, film_id)
            continue

        should_repoint, reason = _is_existing_owner_worse_match(
            cur, existing_film_id, hit, film_id, our_titles or [],
        )
        if not should_repoint:
            skipped_collision += 1
            log.info("  collision keeps owner (%s): upload_id=%s on film=%d, ours=%d",
                     reason, hit.external_id, existing_film_id, film_id)
            continue

        cur.execute(
            "UPDATE video_sources "
            "   SET film_id = %s, "
            "       audio_lang = COALESCE(%s, audio_lang), "
            "       lang_class = %s, "
            "       audio_detected_by = COALESCE(audio_detected_by, 'title_regex'), "
            "       updated_at = NOW() "
            " WHERE id = %s",
            (film_id, "cs" if lang in ("CZ_DUB", "CZ_NATIVE") else None,
             lang, existing_id),
        )
        repointed += 1
        log.info("  RE-POINTED upload_id=%s row=%d: film=%d → film=%d (%s)",
                 hit.external_id, existing_id, existing_film_id,
                 film_id, reason)
        written += 1
    return written, repointed, skipped_collision


def try_prehrajto_match(
    cur, providers: dict, film_id: int, *,
    title: str, original_title: str | None,
    year: int | None, runtime_min: int | None,
    sess: requests.Session,
) -> dict:
    """Search prehraj.to for a film and write matched hits.

    Single-film entry point used by the daily auto-import. Returns a
    dict with counters: hits, accepted, written, repointed, collisions,
    tier_counts. Caller is responsible for transaction control.

    Raises `BlockedError` if the proxy / prehraj.to misbehaves.
    """
    query = build_query(title, original_title, year)
    db_titles = [t for t in (title, original_title) if t]
    db_titles_full = list(db_titles)
    for t in list(db_titles):
        folded = ascii_fold(t)
        if folded != t.lower():
            db_titles_full.append(folded)

    hits = search_prehrajto(
        sess, query, db_titles_full, year, runtime_min,
    )
    classified = [
        (h, *classify_match(h, db_titles_full, year, runtime_min))
        for h in hits
    ]
    tier_counts: dict[str, int] = {}
    for _, t, _, _ in classified:
        tier_counts[t] = tier_counts.get(t, 0) + 1
    accepted = [(h, t) for h, t, _, _ in classified
                if t in ("strong", "solid", "weak")]

    written = repointed = collisions = 0
    if accepted:
        written, repointed, collisions = write_hits(
            cur, providers, film_id, accepted, our_titles=db_titles_full,
        )
    return {
        "query": query, "hits": len(hits), "accepted": len(accepted),
        "written": written, "repointed": repointed,
        "collisions": collisions, "tier_counts": tier_counts,
    }
