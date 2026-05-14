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
//! Data-quality caveat: a Playwright-based spot-check (#733) flagged
//! ~16 % of pre-existing `csfd_id` values in cr as disagreeing with
//! Wikidata's authoritative P345→P2529 mapping. Those wrong values
//! pre-date the #732 bulk resolver (which only fills NULLs) and the
//! reconcile pass that will fix them is tracked in #740. The response
//! body carries an explicit `data_quality.sample_match_rate` field so
//! consumers (csfd-data-hub) can decide what to do with the noise
//! instead of trusting the feed silently.
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
        // Surface the known data-quality issue (#740) inline so the
        // consumer sees it on every fetch instead of having to read
        // README. `sample_match_rate` is the most recent measurement
        // from the #733 verification pass; update both this number
        // and the issue link whenever a fresh pass runs.
        "data_quality": {
            "sample_match_rate": 0.84,
            "sample_size": 50,
            "measured_at": "2026-05-14",
            "issue": "https://github.com/Olbrasoft/cr/issues/740",
            "note": "~16% of pre-existing csfd_id values disagree with \
                     Wikidata's P345->P2529 mapping; reconcile pass in #740."
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
