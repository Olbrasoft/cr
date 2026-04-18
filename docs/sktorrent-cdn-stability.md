# SK Torrent CDN edge node stability test

Empirical check of whether `films.sktorrent_cdn` (historical import-time CDN
assignment) still matches where SK Torrent currently hosts each film. If
stable enough, we can add a DB-cached single-node hint before the full scan
in `scan_sktorrent_cdns` (`cr-web/src/handlers/films.rs`).

## Background

- Playback endpoint `/api/films/sktorrent-resolve` brute-forces 30 nodes × 4
  qualities = 120 HEAD requests per cache miss (in-memory cache has 6h TTL).
- URL pattern is deterministic on the file side:
  `https://online{N}.sktorrent.eu/media/videos//h264/{video_id}_{quality}.mp4`
- Historical data in `films.sktorrent_cdn` spans 13 162 films across 25 nodes
  (`online1` .. `online25`). The code still scans up to `online30` — the extra
  5 slots are currently empty but cheap insurance against future expansion.
- Deterministic hash-sharding hypothesis was tested against the 13 162 pairs
  (`MOD(vid, 24)+1`, `MOD(vid, 25)+1`, `MOD(vid/100, 25)+1`, etc.). Best hit
  rate 4.3 % — indistinguishable from random (1/25 = 4 %). **No formula
  recoverable from `video_id` alone.**
- The labels `HD` and `SD` are rare but not unused. The 20-film baseline
  missed them, but re-checking video_id 36411 ("Ukradená bitva") during the
  first prod verification showed it lives **only** under `HD` on its node.
  Dropping those labels would silently break playback for that minority —
  so the fallback scan keeps all four (25 × 4 = 100 HEAD).

## Strategy (shipped 2026-04-18)

If the CDN assignment SK Torrent made at upload time is stable (film stays
on the same node for years), we can optimise playback resolve to:

1. First try DB-cached `sktorrent_cdn` via `probe_sktorrent_cdn`:
   4 HEAD requests on that node (720p / 480p / HD / SD).
2. On success — cache, return.
3. On miss — fall back to full scan (25 × 4 = 100 HEAD), then write the
   discovered node back to `films|series_episodes|tv_episodes.sktorrent_cdn`
   so the next play goes through the 4-HEAD fast path (self-healing).

Expected average load given day 1 results (80 % hit / 20 % miss):
`0.8 × 4 + 0.2 × (4 + 100) ≈ 24 HEAD per cache miss` — down from 120. −80 %.

Plus the 6-hour in-memory `sktorrent_cache` already amortises repeat plays.

## Day 1 — 2026-04-18 (baseline)

20 random films from `films WHERE sktorrent_cdn IS NOT NULL`. For each,
`curl -I` (720p + 480p) against the DB-stored CDN node only. No full scan
issued — we're testing whether the stored hint is enough.

| video_id | db_cdn | 720p | 480p | status |
|---------:|-------:|:-----|:-----|:-------|
|    52101 |     23 | 200  | 200  | both_ok |
|     5409 |      5 | 200  | 200  | both_ok |
|    33240 |      2 | 200  | 200  | both_ok |
|    36411 |     11 | 404  | 404  | MOVED |
|     2491 |     10 | 404  | 200  | 480p_only |
|    12891 |      7 | 200  | 200  | both_ok |
|    27643 |      9 | 200  | 200  | both_ok |
|     5757 |      3 | 200  | 200  | both_ok |
|     3582 |     21 | 404  | 404  | MOVED |
|    46755 |     20 | 200  | 200  | both_ok |
|    33833 |     13 | 200  | 200  | both_ok |
|     3495 |     21 | 404  | 200  | 480p_only |
|     2562 |      7 | 404  | 200  | 480p_only |
|     3841 |     21 | 200  | 200  | both_ok |
|    15421 |      3 | 404  | 404  | MOVED |
|    31754 |      9 | 404  | 404  | MOVED |
|    36858 |     19 | 404  | 200  | 480p_only |
|     6412 |      2 | 200  | 200  | both_ok |
|    18921 |     10 | 404  | 404  | MOVED |
|    20484 |      9 | 200  | 200  | both_ok |

**Summary:**
- 12 / 20 (60 %) — both qualities still on DB cdn
- 4 / 20 (20 %) — only 480p survived (720p dropped by SK Torrent)
- 4 / 20 (20 %) — MOVED: neither quality present; film is now on a different node

## Day 2 — 2026-04-19 (re-test, open)

_Same 20 video_ids, same check. Do NOT drop this task — day 2 data decides
whether the strategy stays as-is or needs re-tuning._

Goal: distinguish "CDN assignment is long-term stable, the 4 MOVEDs are a
permanent shuffle" from "CDN rotates daily, every film migrates
unpredictably".

If day 2 shows the 12 previously-stable films are still stable and only new
shuffles appear, the shipped strategy is correct: self-heal on miss, no
further change.

If day 2 shows many new shuffles among previously-stable films, the DB hint
is weaker than expected and we should consider shortening `sktorrent_cache`
TTL or writing to the DB more aggressively.

## Decision log

- **2026-04-18**: Shipped strategy "DB hint probe (4 HEAD) → full scan
  fallback (100 HEAD) → self-heal DB". Known-stable 80 % of films resolve
  in 4 HEAD. See PR for the perf(player) commit.
