use std::collections::HashSet;
use std::sync::LazyLock;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

use super::prehrajto::is_valid_upload_id;
use super::subtitles::{SubtitleTrack, extract_subtitles_from_html};
use super::thumbnail::is_prehrajto_url;

// --- Request / response types ---

#[derive(Deserialize)]
pub struct SearchQuery {
    q: String,
    /// When set, the search is being run for the "Další zdroje" panel of a
    /// specific `episodes` row. Triggers two extra filters that prevent
    /// wrong-series uploads from showing on the episode page:
    ///   1. Pack/range rejection — drops titles whose `SxxExx` is followed
    ///      by `-N` (e.g. `S01E02-13`, a Winchesterovi multi-episode pack
    ///      that bleeds into the Supernatural single-episode search).
    ///   2. DB cross-check — drops uploads whose `external_id` is already
    ///      claimed in `video_sources` by a different parent (another
    ///      series' episode, a tv-shows episode, or a film).
    ///
    /// When neither id is set, the endpoint stays a pass-through search.
    #[serde(default)]
    episode_id: Option<i32>,
    /// Same as `episode_id` but identifies the row in `tv_episodes` (the
    /// `/tv-porady/...` template).
    #[serde(default)]
    tv_episode_id: Option<i32>,
}

/// Multi-episode pack range marker, e.g. "S01E02-13" — an `SxxExx` token
/// immediately followed by an optional-whitespace `- N`, which is uploader
/// shorthand for "this file covers episodes E..N". Single-episode uploads
/// (no trailing range) don't match and pass through. The compact `SxxExx`
/// half is intentional: every spaced-out variant we've seen in prehraj.to
/// search results has lacked one of the two tokens, so the loose form
/// would over-match (e.g. "S 01" alone, common in season packs without a
/// range).
static PACK_RANGE_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?i)\bS\d{1,2}E\d{1,2}\s*-\s*\d{1,2}\b")
        .expect("const regex literal compiles")
});

/// Extract the 13- or 16-hex `upload_id` from a prehraj.to URL.
/// Prehraj.to URLs end in `/<slug>/<upload_id>` (the slug being a
/// human-readable title) or sometimes `/<upload_id>` directly. Verifies
/// the host is prehraj.to and the last segment matches
/// [`is_valid_upload_id`] before returning — anything else yields `None`
/// so a query-string token or an unrelated host whose URL happens to end
/// in a hex-looking segment can't accidentally collide with a real
/// `video_sources.external_id`.
fn extract_upload_id_from_url(url: &str) -> Option<String> {
    if !is_prehrajto_url(url) {
        return None;
    }
    let trimmed = url.trim().trim_end_matches('/');
    let last = trimmed.rsplit('/').next()?;
    let id = last.split(['?', '#']).next()?;
    if is_valid_upload_id(id) {
        Some(id.to_string())
    } else {
        None
    }
}

#[derive(Deserialize)]
pub struct VideoUrlQuery {
    url: String,
}

#[derive(Serialize)]
pub struct MovieResult {
    url: String,
    title: String,
    thumbnail: String,
    year: String,
}

#[derive(Serialize)]
pub struct SearchResponse {
    success: bool,
    query: String,
    count: usize,
    movies: Vec<MovieResult>,
}

#[derive(Serialize)]
pub struct VideoUrlResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    video_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subtitles: Option<Vec<SubtitleTrack>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// --- Proxy response types (deserialization only) ---

#[derive(Deserialize)]
struct ProxySearchResponse {
    success: Option<bool>,
    movies: Option<Vec<ProxyMovie>>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct ProxyMovie {
    url: Option<String>,
    title: Option<String>,
    thumbnail: Option<String>,
    year: Option<String>,
}

#[derive(Deserialize)]
struct ProxySubtitleTrack {
    url: Option<String>,
    lang: Option<String>,
    label: Option<String>,
}

#[derive(Deserialize)]
struct ProxyVideoResponse {
    success: Option<bool>,
    #[serde(rename = "videoUrl")]
    video_url: Option<String>,
    subtitles: Option<Vec<ProxySubtitleTrack>>,
    error: Option<String>,
}

// --- CzProxy config helper ---

/// Extract CzProxy (url, key) from AppConfig if both are configured.
/// Reads from the central `AppConfig` instead of `std::env` so tests can
/// instantiate an empty config and per-env overrides live in one place.
pub(crate) fn cz_proxy_config(config: &crate::config::AppConfig) -> Option<(String, String)> {
    config
        .cz_proxy
        .as_ref()
        .map(|p| (p.url.clone(), p.key.clone()))
}

// --- Handlers ---

/// Search movies via CzProxy -> prehraj.to
pub async fn movies_search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> crate::error::WebResult<Json<SearchResponse>> {
    use crate::error::WebError;

    // Issue #521 moved the detail-page "Další zdroje" flow off this
    // endpoint. Rare traffic here while the flag is on usually means a
    // stale cached template or a manual caller — worth an info log for
    // the cutover window. Kept fully functional for rollback.
    if state.config.prehrajto_sources_from_db {
        tracing::info!(
            q = %params.q.trim(),
            "movies_search hit while PREHRAJTO_SOURCES_FROM_DB is on (deprecated path)"
        );
    }

    let query = params.q.trim().to_string();
    if query.is_empty() {
        return Err(WebError::bad_request("Missing search query"));
    }

    // The two id params are mutually exclusive — an upload belongs to
    // exactly one parent kind, and `filter_extra_episode_sources` runs
    // the `episodes`-branch query when `episode_id` is set and the
    // `tv_episodes`-branch when only `tv_episode_id` is set. Accepting
    // both would silently prefer one and is almost certainly a caller bug.
    if params.episode_id.is_some() && params.tv_episode_id.is_some() {
        return Err(WebError::bad_request(
            "episode_id and tv_episode_id are mutually exclusive",
        ));
    }

    let (proxy_url, proxy_key) = cz_proxy_config(&state.config).ok_or_else(|| {
        WebError::status(StatusCode::INTERNAL_SERVER_ERROR, "Proxy not configured")
    })?;

    let url = format!(
        "{}?action=search&q={}&key={}",
        proxy_url,
        urlencoding::encode(&query),
        proxy_key
    );

    let resp = state
        .http_client
        .get(&url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| {
            tracing::error!("CzProxy search failed: {e}");
            WebError::bad_gateway(format!("Proxy error: {e}"))
        })?;

    let data: ProxySearchResponse = resp.json().await.map_err(|e| {
        tracing::error!("CzProxy search parse failed: {e}");
        WebError::bad_gateway("Invalid proxy response")
    })?;

    if data.success != Some(true) {
        return Err(WebError::bad_gateway(
            data.error.unwrap_or_else(|| "Search failed".to_string()),
        ));
    }

    let mut movies: Vec<MovieResult> = data
        .movies
        .unwrap_or_default()
        .into_iter()
        .filter_map(|m| {
            let url = m.url.filter(|s| !s.trim().is_empty())?;
            let title = m.title.filter(|s| !s.trim().is_empty())?;
            Some(MovieResult {
                url,
                title,
                thumbnail: m.thumbnail.unwrap_or_default(),
                year: m.year.unwrap_or_default(),
            })
        })
        .collect();

    if params.episode_id.is_some() || params.tv_episode_id.is_some() {
        filter_extra_episode_sources(&state, &mut movies, params.episode_id, params.tv_episode_id)
            .await;
    }

    let count = movies.len();

    Ok(Json(SearchResponse {
        success: true,
        query,
        count,
        movies,
    }))
}

/// In-place filter for the "Další zdroje" panel: drops multi-episode packs
/// (range marker like `S01E02-13`) and drops uploads whose `external_id` is
/// already linked in `video_sources` to a different parent (different
/// series, a tv-shows episode, or a film).
///
/// The DB cross-check assumes the import pipelines (e.g.
/// `scripts/import-prehrajto-series.py`) have claimed each upload against
/// its rightful parent — uploads we haven't seen yet pass through.
async fn filter_extra_episode_sources(
    state: &AppState,
    movies: &mut Vec<MovieResult>,
    episode_id: Option<i32>,
    tv_episode_id: Option<i32>,
) {
    movies.retain(|m| !PACK_RANGE_RE.is_match(&m.title));

    let upload_ids: Vec<String> = movies
        .iter()
        .filter_map(|m| extract_upload_id_from_url(&m.url))
        .collect();
    if upload_ids.is_empty() {
        return;
    }

    // Sibling-aware exclusion list — populated by the branch that matches
    // the caller's parent kind. Uploads claimed by the *current* parent
    // (same series / same tv_show) are NOT in this set, so primary-source
    // duplicates render normally if they happen to appear in the search.
    let claimed_elsewhere: HashSet<String> = if let Some(eid) = episode_id {
        let series_id: Option<i32> =
            match sqlx::query_scalar::<_, i32>("SELECT series_id FROM episodes WHERE id = $1")
                .bind(eid)
                .fetch_optional(&state.db)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        episode_id = eid,
                        error = ?e,
                        "movies_search: failed to load series_id; skipping DB cross-check"
                    );
                    return;
                }
            };
        let Some(series_id) = series_id else {
            return;
        };
        match sqlx::query_scalar::<_, String>(
            "SELECT vs.external_id \
               FROM video_sources vs \
               JOIN video_providers p ON p.id = vs.provider_id \
               LEFT JOIN episodes e ON e.id = vs.episode_id \
              WHERE p.slug = 'prehrajto' \
                AND vs.external_id = ANY($1) \
                AND (vs.film_id IS NOT NULL \
                     OR vs.tv_episode_id IS NOT NULL \
                     OR (e.series_id IS NOT NULL AND e.series_id <> $2))",
        )
        .bind(&upload_ids)
        .bind(series_id)
        .fetch_all(&state.db)
        .await
        {
            Ok(rows) => rows.into_iter().collect(),
            Err(e) => {
                tracing::warn!(error = ?e, "movies_search: cross-check query failed; skipping");
                return;
            }
        }
    } else if let Some(teid) = tv_episode_id {
        let tv_show_id: Option<i32> =
            match sqlx::query_scalar::<_, i32>("SELECT tv_show_id FROM tv_episodes WHERE id = $1")
                .bind(teid)
                .fetch_optional(&state.db)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        tv_episode_id = teid,
                        error = ?e,
                        "movies_search: failed to load tv_show_id; skipping DB cross-check"
                    );
                    return;
                }
            };
        let Some(tv_show_id) = tv_show_id else {
            return;
        };
        match sqlx::query_scalar::<_, String>(
            "SELECT vs.external_id \
               FROM video_sources vs \
               JOIN video_providers p ON p.id = vs.provider_id \
               LEFT JOIN tv_episodes te ON te.id = vs.tv_episode_id \
              WHERE p.slug = 'prehrajto' \
                AND vs.external_id = ANY($1) \
                AND (vs.film_id IS NOT NULL \
                     OR vs.episode_id IS NOT NULL \
                     OR (te.tv_show_id IS NOT NULL AND te.tv_show_id <> $2))",
        )
        .bind(&upload_ids)
        .bind(tv_show_id)
        .fetch_all(&state.db)
        .await
        {
            Ok(rows) => rows.into_iter().collect(),
            Err(e) => {
                tracing::warn!(error = ?e, "movies_search: cross-check query failed; skipping");
                return;
            }
        }
    } else {
        return;
    };

    if claimed_elsewhere.is_empty() {
        return;
    }
    movies.retain(|m| match extract_upload_id_from_url(&m.url) {
        Some(id) => !claimed_elsewhere.contains(&id),
        None => true,
    });
}

/// Get video CDN URL via CzProxy -> prehraj.to page
pub async fn movies_video_url(
    State(state): State<AppState>,
    Query(params): Query<VideoUrlQuery>,
) -> crate::error::WebResult<Json<VideoUrlResponse>> {
    use crate::error::WebError;

    let video_url = params.url.trim().to_string();
    if !is_prehrajto_url(&video_url) {
        return Err(WebError::bad_request("Invalid prehraj.to URL"));
    }

    let (proxy_url, proxy_key) = cz_proxy_config(&state.config).ok_or_else(|| {
        WebError::status(StatusCode::INTERNAL_SERVER_ERROR, "Proxy not configured")
    })?;

    // Fetch video URL and page HTML concurrently via CZ proxy
    let video_api_url = format!(
        "{}?action=video&url={}&key={}",
        proxy_url,
        urlencoding::encode(&video_url),
        proxy_key
    );
    let page_api_url = format!(
        "{}?action=proxy&url={}&key={}",
        proxy_url,
        urlencoding::encode(&video_url),
        proxy_key
    );

    let (video_resp, page_resp) = tokio::join!(
        state
            .http_client
            .get(&video_api_url)
            .timeout(std::time::Duration::from_secs(30))
            .send(),
        state
            .http_client
            .get(&page_api_url)
            .timeout(std::time::Duration::from_secs(30))
            .send()
    );

    let resp = video_resp.map_err(|e| {
        tracing::error!("CzProxy video failed: {e}");
        WebError::bad_gateway(format!("Proxy error: {e}"))
    })?;

    let data: ProxyVideoResponse = resp.json().await.map_err(|e| {
        tracing::error!("CzProxy video parse failed: {e}");
        WebError::bad_gateway("Invalid proxy response")
    })?;

    // Extract subtitles: prefer CZ proxy response, fallback to parsing page HTML
    let mut subtitles = data.subtitles.map(|tracks| {
        tracks
            .into_iter()
            .filter_map(|t| {
                Some(SubtitleTrack {
                    url: t.url?,
                    lang: t.lang.unwrap_or_default(),
                    label: t.label.unwrap_or_default(),
                })
            })
            .collect::<Vec<_>>()
    });

    // If CZ proxy didn't return subtitles, extract from page HTML
    if subtitles.as_ref().is_none_or(|s| s.is_empty())
        && let Ok(page_r) = page_resp
        && let Ok(html) = page_r.text().await
    {
        let extracted = extract_subtitles_from_html(&html);
        if !extracted.is_empty() {
            subtitles = Some(extracted);
        }
    }

    Ok(Json(VideoUrlResponse {
        success: data.success.unwrap_or(false),
        video_url: data.video_url,
        subtitles,
        error: data.error,
    }))
}
