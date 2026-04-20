use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

use super::subtitles::{SubtitleTrack, extract_subtitles_from_html};
use super::thumbnail::is_prehrajto_url;

// --- Request / response types ---

#[derive(Deserialize)]
pub struct SearchQuery {
    q: String,
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

    let movies: Vec<MovieResult> = data
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

    let count = movies.len();

    Ok(Json(SearchResponse {
        success: true,
        query,
        count,
        movies,
    }))
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
