//! Public open-data feed of every row in cr that carries a populated
//! `csfd_id` (#730 / #733). The shape is intentionally minimal so the
//! consumer side ([Olbrasoft/csfd-data-hub]) can build its daily
//! ČSFD rating scraping watchlist without holding a copy of cr's
//! schema — just `(csfd_id, imdb_id, tmdb_id, title, year)` plus a
//! `kind` discriminator so films/series/tv_shows can be told apart.
//!
//! Volume: ~27.5 k rows ≈ 3 MB JSON on 2026-05-14 (measured at deploy
//! time). Served with a 1-hour `Cache-Control` so Cloudflare's edge
//! handles the bulk of fetches; the origin only sees one DB hit per
//! cache window per PoP.
//!
//! Data-quality status: the #740 reconcile pass (2026-05-14) cross-
//! referenced every pre-existing `csfd_id` against Wikidata P345→P2529
//! and auto-rewrote 725 confirmed disagreements where Wikidata's Czech
//! label matched cr.title verbatim. A 200-row Playwright re-check now
//! shows 89.5 % raw match against ČSFD's `og:title`; roughly half of
//! the remaining 21 mismatches are translation artefacts (cr stores
//! the original/EN title, ČSFD shows the Czech translation — same
//! film, different label), so the effective accuracy is ≥ 95 %.
//! 623 rows where Wikidata returned a disagreeing P2529 but no Czech
//! label remain in `csfd_id_reconcile_review` for manual triage.
//!
//! [Olbrasoft/csfd-data-hub]: https://github.com/Olbrasoft/csfd-data-hub

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::error::WebResult;
use crate::state::AppState;

#[derive(sqlx::FromRow)]
struct WatchlistRow {
    csfd_id: i32,
    imdb_id: Option<String>,
    tmdb_id: Option<i32>,
    title: Option<String>,
    year: Option<i16>,
    kind: String,
}

pub async fn csfd_watchlist(State(state): State<AppState>) -> WebResult<Response> {
    // UNION of the three source tables flattened into a single shape.
    // `kind` lets consumers route films vs. TV without inspecting the
    // ID space, which doesn't carry that information. The ORDER BY
    // makes the JSON output stable across runs — useful for diffing
    // snapshots in csfd-data-hub. `first_air_year` for series /
    // tv_shows is cast to SMALLINT so it lines up with `films.year`'s
    // type and the row struct stays homogeneous.
    let rows = sqlx::query_as::<_, WatchlistRow>(
        "SELECT csfd_id, imdb_id, tmdb_id, title, year::SMALLINT AS year, \
                'film'::TEXT AS kind \
         FROM films WHERE csfd_id IS NOT NULL \
         UNION ALL \
         SELECT csfd_id, imdb_id, tmdb_id, title, first_air_year AS year, \
                'series'::TEXT AS kind \
         FROM series WHERE csfd_id IS NOT NULL \
         UNION ALL \
         SELECT csfd_id, imdb_id, tmdb_id, title, first_air_year AS year, \
                'tv_show'::TEXT AS kind \
         FROM tv_shows WHERE csfd_id IS NOT NULL \
         ORDER BY csfd_id",
    )
    .fetch_all(&state.db)
    .await?;

    // Pull the current size of the manual-review queue at request
    // time. Hardcoding the production snapshot (623 at deploy) would
    // go stale as maintainers clear rows and as new reconcile passes
    // queue more (PR #741, Copilot review). The query is a single
    // partial-index scan, cheap enough to run on every uncached
    // request — the 1-hour Cloudflare cache absorbs most reads anyway.
    let pending_review: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM csfd_id_reconcile_review \
         WHERE action_taken = 'pending_review'",
    )
    .fetch_one(&state.db)
    .await?;

    let items: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "csfd_id": r.csfd_id,
                "imdb_id": r.imdb_id,
                "tmdb_id": r.tmdb_id,
                "title": r.title,
                "year": r.year,
                "kind": r.kind,
            })
        })
        .collect();

    let body = serde_json::to_string(&serde_json::json!({
        // `generated_at` lets the consumer detect stale snapshots if
        // Cloudflare returns a long-cached response after origin
        // changes. `count` is a sanity gauge — if it drops by orders
        // of magnitude the consumer can refuse to ingest the file.
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "count": items.len(),
        // Surface accuracy stats inline so the consumer can decide
        // whether to trust each row. The raw rate counts ČSFD's Czech
        // translation of the title as a mismatch even when the csfd_id
        // is correct — manual review of the 200-row sample suggests
        // the effective accuracy after discarding translation noise
        // is roughly 95–97 %. `pending_manual_review` is the size of
        // the csfd_id_reconcile_review queue still flagged for a
        // human (Wikidata disagrees but no Czech label to gate auto-
        // rewrite on, so a maintainer has to make the call).
        "data_quality": {
            "sample_match_rate": 0.895,
            "sample_size": 200,
            "measured_at": "2026-05-14",
            "auto_rewrites_applied": 725,
            "pending_manual_review": pending_review,
            "issue": "https://github.com/Olbrasoft/cr/issues/740",
            "note": "Raw rate compares cr.title to ČSFD og:title via \
                     normalised string match; CZ/EN/original-language \
                     translation differences are counted as mismatches \
                     even when the csfd_id is correct, so effective \
                     accuracy is higher."
        },
        "items": items,
    }))?;

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json; charset=utf-8"),
            // 1-hour public cache. csfd-data-hub polls daily so an hour
            // is plenty fresh; Cloudflare's edge absorbs the request
            // pressure even if the consumer accidentally polls more.
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        body,
    )
        .into_response())
}
