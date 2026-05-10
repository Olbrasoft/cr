"""Find SK Torrent series NOT yet in our DB.

Crawls SK Torrent's two series sections (`serialy-cz-sk` dubbed +
`serialy-cz-sk-titulky` subtitled), parses every episode title to extract the
series identity, and reports which series have ZERO presence in our `series`
table on production.

Two-layer match against prod DB:
  1. Hard match by `episodes.sktorrent_video_id` — if even one episode of a
     given sktorrent series is already linked to an existing series row, the
     series IS in DB (regardless of how we titled it).
  2. Soft match by normalized title — if every episode of a sktorrent series
     is unknown to us, we still try to match the parsed (cz_title, en_title)
     against `series.title` / `series.original_title`. Only when both layers
     miss is the series classified as MISSING.

Read-only against the production DB via SSH tunnel (default 127.0.0.1:25432).
The tunnel must already be open before running this script:

    ssh -p 2222 -fN -L 25432:127.0.0.1:5432 root@46.225.101.253

Outputs:
  * Markdown summary on stdout (counts + top-50 missing by episode count)
  * Full CSV at data/movies/sktorrent-new-series.csv (all unique series with
    in-DB / MISSING flag, sorted by episode count desc)
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import threading
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

# Make `scripts.auto_import.*` importable when running as a top-level script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.auto_import.cz_proxy import proxy_get  # noqa: E402
from scripts.auto_import.sktorrent_scanner import _parse_listing_html  # noqa: E402
from scripts.auto_import.title_parser import parse_sktorrent_title  # noqa: E402

log = logging.getLogger("find-skt-new-series")

CATEGORIES = {
    "serialy-cz-sk": "https://online.sktorrent.eu/videos/serialy-cz-sk",
    "serialy-cz-sk-titulky": "https://online.sktorrent.eu/videos/serialy-cz-sk-titulky",
}

DEFAULT_DSN = "postgresql://cr:cr_secret_2026@127.0.0.1:25432/cr"

# SK Torrent appends the requester's hex-encoded IP as a tracking suffix on
# every video URL ("...-3139332e3130352e3135382e3135302d" decodes to
# "193.105.158.150-"). Strip it so the CSV stays committable — the repo's
# pre-commit hook flags `sk-<long alnum>` as a possible API key, which is a
# false positive on this exact pattern.
_URL_IP_SUFFIX_RE = re.compile(r"-[0-9a-f]{20,}d$")

# Throttle per-worker. Two workers run in parallel (one per category) so the
# real request rate against the proxy is ~2 req/s.
PAGE_SLEEP_S = 0.6


def normalize_title(s: str | None) -> str:
    """Lowercase, strip diacritics, drop punctuation/whitespace.

    Used for the soft-match layer so that "Špión" / "Spion" / "spion" all
    collapse to the same key. Aggressive on purpose — false-positive matches
    (collapsing two different shows together) are MUCH less harmful here than
    false-negative matches (reporting an existing series as missing).
    """
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    no_diacritics = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", no_diacritics.lower())


@dataclass
class RawItem:
    """One parsed listing item before series grouping."""

    cz_title: str | None
    en_title: str | None
    cz_norm: str
    en_norm: str
    year: int | None
    video_id: int
    title: str
    url: str
    category: str


@dataclass
class SeriesAgg:
    """Accumulator for one unique series across all listing pages."""

    cz_title: str | None
    en_title: str | None
    year: int | None = None
    episode_count: int = 0
    seen_video_ids: set[int] = field(default_factory=set)
    sample_video_id: int = 0
    sample_title: str = ""
    sample_url: str = ""
    categories: set[str] = field(default_factory=set)
    # All distinct normalized titles seen for this series (CZ + EN alternates).
    # Used downstream so the soft title-match step doesn't miss a series whose
    # canonical key happens to be the EN form when our DB stores only the CZ
    # translation, or vice versa.
    aliases: set[str] = field(default_factory=set)


def _fetch_with_retry(
    name: str,
    session: requests.Session,
    target: str,
    page: int,
    base_sleep: float,
) -> requests.Response | None:
    """One page fetch with backoff on 403/429/5xx.

    SK Torrent rate-limits aggressively: a sustained ~2 req/s burst trips a
    ~1-minute IP ban that returns 403 to *all* subsequent pages until it
    expires. The fix is to back off long enough for the ban to lift instead of
    hammering through it.
    """
    delays = [30, 60, 120, 240]  # cumulative ~7.5 min on the 4th retry
    for attempt, wait in enumerate([0, *delays]):
        if wait:
            log.warning("[%s] page %d backoff #%d — sleeping %ds before retry",
                        name, page, attempt, wait)
            time.sleep(wait)
        try:
            r = proxy_get(target, session, timeout=30)
        except requests.RequestException as exc:
            log.warning("[%s] page %d transport error: %s", name, page, exc)
            continue
        if r.status_code == 200:
            return r
        if r.status_code in (403, 429) or 500 <= r.status_code < 600:
            log.warning("[%s] page %d returned HTTP %d — will back off",
                        name, page, r.status_code)
            continue
        log.warning("[%s] page %d returned unexpected HTTP %d — skipping",
                    name, page, r.status_code)
        time.sleep(base_sleep)
        return None
    log.error("[%s] page %d exhausted retries — skipping", name, page)
    return None


def crawl_category(
    name: str,
    url: str,
    max_pages: int,
    sleep_s: float,
) -> tuple[list[RawItem], int]:
    """Walk one category page-by-page, return ALL parsed listing items.

    Series-grouping is deferred until both crawls finish so we can run a
    proper union-find that merges (cz_only) with (cz+en) entries of the
    same show — see `group_series()`.
    """
    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
    )
    session.headers["Accept-Encoding"] = "identity"

    items_out: list[RawItem] = []
    seen_video_ids: set[int] = set()
    pages_done = 0

    try:
        for page in range(1, max_pages + 1):
            target = url if page == 1 else f"{url}?page={page}"
            r = _fetch_with_retry(name, session, target, page, sleep_s)
            if r is None:
                time.sleep(sleep_s)
                continue
            items = _parse_listing_html(r.text)
            if not items:
                log.info("[%s] page %d empty — end of listing", name, page)
                break
            # SK Torrent caps pagination silently: requesting any page beyond
            # the real end returns the LAST page on repeat. Detect this by
            # checking whether the page contributes any video_ids we haven't
            # already seen — if every id is a duplicate, we've wrapped.
            new_on_page = sum(1 for it in items if it.video_id not in seen_video_ids)
            if new_on_page == 0:
                log.info("[%s] page %d is a duplicate of an earlier page — "
                         "real end of listing reached", name, page)
                break
            for item in items:
                if item.video_id in seen_video_ids:
                    continue
                seen_video_ids.add(item.video_id)
                parsed = parse_sktorrent_title(item.title)
                cz_norm = normalize_title(parsed.cz_title)
                en_norm = normalize_title(parsed.en_title)
                if not cz_norm and not en_norm:
                    continue
                items_out.append(RawItem(
                    cz_title=parsed.cz_title,
                    en_title=parsed.en_title,
                    cz_norm=cz_norm,
                    en_norm=en_norm,
                    year=parsed.year,
                    video_id=item.video_id,
                    title=item.title,
                    url=_URL_IP_SUFFIX_RE.sub("", item.url),
                    category=name,
                ))
            pages_done += 1
            if page % 50 == 0:
                log.info("[%s] page %d/%d done — %d items so far",
                         name, page, max_pages, len(items_out))
            time.sleep(sleep_s)
    finally:
        session.close()

    log.info("[%s] crawl done — %d pages, %d items", name, pages_done, len(items_out))
    return items_out, pages_done


def group_series(items: list[RawItem]) -> dict[str, SeriesAgg]:
    """Cluster RawItems into series in three steps.

    Step 1 — union by (cz_norm, en_norm) pairs. Same show with sometimes
    only-CZ and sometimes CZ+EN titles get merged via the EN form acting as
    a bridge node:
        cz="V mlhách", en=None         → node "vmlhach"
        cz="V mlhách", en="In the Fog" → links "vmlhach" ↔ "inthefog"
        cz=None,       en="In the Fog" → joins via "inthefog"

    Step 2 — prefix-merge for SK Torrent's "Show NN- Episode title" pattern
    (no slash, no SxxExx). Each episode of e.g. "Policajti z předměstí" looks
    like "Policajti z předměstí 14- Rychlejší než světlo" → normalized
    "policajtizpredmesti14rychlejsinesvetlo". After lex-sorting all keys, any
    two adjacent keys that share a ≥10-char common prefix and diverge into a
    digit are merged — they're enumerated episodes of the same show.

    Step 3 — pick the longest non-empty title across cluster members for the
    user-visible CZ/EN names (more informative than the first one we saw).
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # Step 1: register all nodes + bridge cz↔en for items with both titles.
    for item in items:
        for n in (item.cz_norm, item.en_norm):
            if n and n not in parent:
                parent[n] = n
        if item.cz_norm and item.en_norm and item.cz_norm != item.en_norm:
            union(item.cz_norm, item.en_norm)

    # Step 2: prefix-merge enumerated episode keys.
    keys_sorted = sorted(parent.keys())
    for k1, k2 in zip(keys_sorted, keys_sorted[1:]):
        lcp = 0
        limit = min(len(k1), len(k2))
        while lcp < limit and k1[lcp] == k2[lcp]:
            lcp += 1
        if lcp < 10:
            continue
        # Next char in both keys must be either end-of-string or a digit so we
        # only merge real episode-number suffixes — random shows that happen
        # to share an alphabetic prefix of 10+ chars stay distinct.
        ch1 = k1[lcp] if lcp < len(k1) else ""
        ch2 = k2[lcp] if lcp < len(k2) else ""
        ok1 = ch1 == "" or ch1.isdigit()
        ok2 = ch2 == "" or ch2.isdigit()
        if ok1 and ok2 and (ch1 or ch2):
            union(k1, k2)

    # Step 3: build SeriesAgg per connected component.
    out: dict[str, SeriesAgg] = {}
    for item in items:
        anchor = item.cz_norm or item.en_norm
        if not anchor:
            continue
        root = find(anchor)
        a = out.get(root)
        if a is None:
            a = SeriesAgg(
                cz_title=item.cz_title,
                en_title=item.en_title,
                year=item.year,
                sample_video_id=item.video_id,
                sample_title=item.title,
                sample_url=item.url,
            )
            out[root] = a
        else:
            if item.cz_title and (not a.cz_title or len(item.cz_title) > len(a.cz_title)):
                a.cz_title = item.cz_title
            if item.en_title and (not a.en_title or len(item.en_title) > len(a.en_title)):
                a.en_title = item.en_title
        a.episode_count += 1
        a.seen_video_ids.add(item.video_id)
        a.categories.add(item.category)
        if item.cz_norm:
            a.aliases.add(item.cz_norm)
        if item.en_norm:
            a.aliases.add(item.en_norm)
        if item.year and (a.year is None or item.year > a.year):
            a.year = item.year
    return out


def load_db_signals(dsn: str) -> tuple[set[int], set[str]]:
    """Returns (set of episode.sktorrent_video_id, set of normalized series titles)."""
    log.info("connecting to prod DB via tunnel: %s", dsn.replace("cr_secret_2026", "***"))
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sktorrent_video_id FROM episodes "
                "WHERE sktorrent_video_id IS NOT NULL"
            )
            video_ids = {row[0] for row in cur.fetchall()}
            cur.execute("SELECT title, original_title FROM series")
            titles: set[str] = set()
            for title, original_title in cur.fetchall():
                if title:
                    titles.add(normalize_title(title))
                if original_title:
                    titles.add(normalize_title(original_title))
    log.info("loaded %d known sktorrent episode IDs and %d known series titles",
             len(video_ids), len(titles))
    return video_ids, titles


def fetch_closest_matches(
    dsn: str,
    queries: list[tuple[SeriesAgg, str, str]],
    min_similarity: float = 0.4,
) -> dict[int, str]:
    """For each MISSING series, find the closest existing series by trigram
    similarity (unaccented) on title OR original_title.

    Returns a dict keyed by `id(SeriesAgg)` so callers can attach the hint
    without reordering. Empty string when no candidate clears the threshold —
    that's signal too: nothing remotely similar exists in our catalogue.
    """
    out: dict[int, str] = {}
    if not queries:
        return out
    sql = """
        WITH q(needle) AS (VALUES (%s))
        SELECT s.title, s.original_title,
               GREATEST(
                 similarity(unaccent(lower(s.title)), unaccent(lower((SELECT needle FROM q)))),
                 similarity(unaccent(lower(coalesce(s.original_title, ''))), unaccent(lower((SELECT needle FROM q))))
               ) AS sim
        FROM series s
        WHERE
          unaccent(lower(s.title)) %% unaccent(lower((SELECT needle FROM q)))
          OR unaccent(lower(coalesce(s.original_title, ''))) %% unaccent(lower((SELECT needle FROM q)))
        ORDER BY sim DESC
        LIMIT 1
    """
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            for ser, _status, _reason in queries:
                needle = (ser.en_title or ser.cz_title or "").strip()
                if len(needle) < 3:
                    continue
                cur.execute(sql, (needle,))
                row = cur.fetchone()
                if not row:
                    continue
                title, original_title, sim = row
                if sim is None or sim < min_similarity:
                    continue
                hint = title if not original_title else f"{title} / {original_title}"
                out[id(ser)] = f"{hint}  (sim={sim:.2f})"
    return out


def classify(
    series: dict[str, SeriesAgg],
    known_video_ids: set[int],
    known_titles: set[str],
) -> list[tuple[SeriesAgg, str, str]]:
    """Tag each aggregated series with (status, reason).

    status ∈ {"in_db", "missing"}; reason explains which signal matched.
    """
    out: list[tuple[SeriesAgg, str, str]] = []
    for ser in series.values():
        hard_match_count = sum(1 for v in ser.seen_video_ids if v in known_video_ids)
        if hard_match_count > 0:
            out.append((ser, "in_db",
                        f"hard:{hard_match_count}/{len(ser.seen_video_ids)} eps already linked"))
            continue
        title_hit = next((alias for alias in ser.aliases if alias in known_titles), None)
        if title_hit:
            out.append((ser, "in_db", f"soft:title match on '{title_hit}'"))
            continue
        out.append((ser, "missing", "no episode link, no title match"))
    return out


def write_csv(
    rows: list[tuple[SeriesAgg, str, str]],
    closest: dict[int, str],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "status", "cz_title", "en_title", "year",
            "episode_count", "categories", "sample_video_id",
            "sample_url", "sample_title", "reason", "closest_db_match",
        ])
        for ser, status, reason in sorted(
            rows, key=lambda t: (t[1] != "missing", -t[0].episode_count)
        ):
            w.writerow([
                status,
                ser.cz_title or "",
                ser.en_title or "",
                ser.year or "",
                ser.episode_count,
                ",".join(sorted(ser.categories)),
                ser.sample_video_id,
                ser.sample_url,
                ser.sample_title,
                reason,
                closest.get(id(ser), ""),
            ])
    log.info("wrote %d rows to %s", len(rows), path)


def print_top_missing(
    rows: list[tuple[SeriesAgg, str, str]],
    closest: dict[int, str],
    top: int,
) -> None:
    missing = sorted(rows, key=lambda t: -t[0].episode_count)
    print()
    print(f"## SK Torrent series NOT in our DB — total: {len(missing)}")
    print()
    print(f"Top {min(top, len(missing))} by episode count seen on sktorrent:")
    print()
    print("| eps | year | CZ title                              | EN title                              | closest in DB (similarity)             |")
    print("|----:|-----:|---------------------------------------|---------------------------------------|----------------------------------------|")
    for ser, _status, _reason in missing[:top]:
        cz = (ser.cz_title or "")[:37]
        en = (ser.en_title or "")[:37]
        year = ser.year or ""
        hint = closest.get(id(ser), "")[:38]
        print(f"| {ser.episode_count:3d} | {year!s:>4} | {cz:<37} | {en:<37} | {hint:<38} |")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dsn", default=os.environ.get("PROD_DSN", DEFAULT_DSN),
                    help="Prod Postgres DSN (default uses SSH tunnel on port 25432)")
    ap.add_argument("--max-pages", type=int, default=2000,
                    help="Per-category page ceiling (default 2000)")
    ap.add_argument("--sleep", type=float, default=PAGE_SLEEP_S,
                    help="Seconds to sleep between page fetches per worker")
    ap.add_argument("--out", default="data/movies/sktorrent-new-series.csv",
                    help="Output CSV path")
    ap.add_argument("--top", type=int, default=50,
                    help="How many missing series to show in stdout summary")
    ap.add_argument("--min-episodes", type=int, default=2,
                    help="Hide series whose only evidence is a single episode "
                         "(those are mostly title-parser fragments). Default 2.")
    ap.add_argument("--limit-pages", type=int, default=None,
                    help="DEBUG: cap pages per category (for quick smoke runs)")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    max_pages = args.limit_pages or args.max_pages
    log.info("crawling %d categories × max %d pages each (sleep=%.2fs/worker)",
             len(CATEGORIES), max_pages, args.sleep)
    t0 = time.time()
    all_items: list[RawItem] = []
    pages_done_total = 0
    with ThreadPoolExecutor(max_workers=len(CATEGORIES)) as ex:
        futures = {
            ex.submit(crawl_category, name, url, max_pages, args.sleep): name
            for name, url in CATEGORIES.items()
        }
        for fut in futures:
            items, pages = fut.result()
            all_items.extend(items)
            pages_done_total += pages
    merged = group_series(all_items)
    log.info("crawl finished in %.0fs — %d pages, %d items, %d unique series",
             time.time() - t0, pages_done_total, len(all_items), len(merged))

    known_video_ids, known_titles = load_db_signals(args.dsn)

    rows = classify(merged, known_video_ids, known_titles)
    in_db = sum(1 for _, s, _ in rows if s == "in_db")
    missing = sum(1 for _, s, _ in rows if s == "missing")
    multi = [r for r in rows
             if r[1] == "missing" and r[0].episode_count >= args.min_episodes]
    print(f"\nTotal unique series on SK Torrent: {len(rows)}")
    print(f"  in our DB:        {in_db}")
    print(f"  MISSING (any):    {missing}")
    print(f"  MISSING (≥{args.min_episodes} eps): {len(multi)}  ← strong signal")

    missing_rows = [r for r in rows if r[1] == "missing"]
    log.info("looking up closest existing-series match for %d missing rows…",
             len(missing_rows))
    closest = fetch_closest_matches(args.dsn, missing_rows)
    log.info("found a similar existing series for %d/%d missing rows",
             len(closest), len(missing_rows))

    write_csv(rows, closest, Path(args.out))
    print_top_missing(multi, closest, args.top)
    return 0


if __name__ == "__main__":
    sys.exit(main())
