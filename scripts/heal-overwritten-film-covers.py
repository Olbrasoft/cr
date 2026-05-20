#!/usr/bin/env python3
"""One-shot heal: re-download film covers overwritten by the sledujteto /
prehrajto series importer cover-prefix bug.

Background: `import-sledujteto-series.py` and `import-prehrajto-series.py`
defaulted to `--covers-dir data/movies/series-covers`. The
`cover_downloader._push_cover_to_r2` derives the R2 prefix from path
parts and looks for the literal segment `series`; `series-covers` is
NOT that. The detection fell through to the `films/` default, so every
series cover was uploaded to `cr-r2:cr-images/films/{series.id}/...` —
which silently overwrote any real film whose `films.id` collided with
the just-created `series.id`. 5897 films are affected.

This script reads the list of series IDs that have a local-disk cover
under `/opt/cr/data/movies/series-covers/` (= the set of bad uploads),
finds films whose id is in that set, and re-downloads each from TMDB to
restore `cr-r2:cr-images/films/{id}/cover.webp` + `cover-large.webp`.

Usage on prod:
    DATABASE_URL=postgres://... TMDB_API_KEY=... \
        python3 scripts/heal-overwritten-film-covers.py \
            --series-ids /tmp/series_ids.txt \
            [--limit N] [--start-from FILM_ID]

Idempotent: pass `--start-from` to resume after an interrupted run.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

try:
    import psycopg2
except ImportError as e:
    print(f"ERROR: missing dependency ({e}). pip install psycopg2-binary",
          file=sys.stderr)
    sys.exit(2)

from scripts.auto_import.cover_downloader import download_cover

log = logging.getLogger("heal-film-covers")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--series-ids", type=Path, required=True,
                    help="File with one series id per line (the set of bad uploads)")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N films (0 = no limit)")
    ap.add_argument("--start-from", type=int, default=0,
                    help="Resume from this film id (inclusive)")
    ap.add_argument("--out-dir", type=Path,
                    default=_PROJECT_ROOT / "data" / "movies" / "covers-webp",
                    help="Local cache dir; MUST not contain 'series' or "
                         "'tv-shows' segments so cover_downloader uses the "
                         "films/ R2 prefix")
    ap.add_argument("--delay-ms", type=int, default=100,
                    help="Inter-request delay (default 100 ms)")
    ns = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL not set")
        return 2

    if not os.environ.get("TMDB_API_KEY"):
        log.warning("TMDB_API_KEY not set — cover_downloader will hit "
                    "unauthenticated TMDB (works for image CDN, not API)")

    # Read the candidate series-ids list
    series_ids = []
    for line in ns.series_ids.read_text().splitlines():
        line = line.strip()
        if line.isdigit():
            series_ids.append(int(line))
    log.info("loaded %d series ids from %s", len(series_ids), ns.series_ids)

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, title, tmdb_poster_path "
        "  FROM films "
        " WHERE id = ANY(%s) "
        "   AND tmdb_poster_path IS NOT NULL "
        "   AND tmdb_poster_path != '' "
        "   AND id >= %s "
        " ORDER BY id",
        (series_ids, ns.start_from),
    )
    films = cur.fetchall()
    conn.close()

    if ns.limit:
        films = films[: ns.limit]

    log.info("healing %d films (overwriting cover.webp + cover-large.webp)",
             len(films))

    ns.out_dir.mkdir(parents=True, exist_ok=True)

    n_ok = 0
    n_fail = 0
    n_noop = 0
    for i, (film_id, title, poster_path) in enumerate(films, 1):
        try:
            result = download_cover(
                poster_path=poster_path,
                entity_id=film_id,
                out_dir=ns.out_dir,
                overwrite=True,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("  film %d (%s): exception %s", film_id, title, e)
            n_fail += 1
            continue

        if result == "written":
            n_ok += 1
        elif result == "already_present":
            n_noop += 1
        else:
            n_fail += 1
            log.warning("  film %d (%s): %s", film_id, title, result)

        if i % 50 == 0 or i == len(films):
            log.info("  [%d/%d] written=%d already=%d failed=%d",
                     i, len(films), n_ok, n_noop, n_fail)
        time.sleep(ns.delay_ms / 1000.0)

    print("\n=== summary ===", file=sys.stderr)
    print(f"  films targeted: {len(films)}", file=sys.stderr)
    print(f"  written:        {n_ok}", file=sys.stderr)
    print(f"  already:        {n_noop}", file=sys.stderr)
    print(f"  failed:         {n_fail}", file=sys.stderr)
    # Non-zero exit on partial failure so automation / shell wrappers can
    # detect that the recovery wasn't fully clean and re-run with
    # --start-from to cover the gaps.
    return 1 if n_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
