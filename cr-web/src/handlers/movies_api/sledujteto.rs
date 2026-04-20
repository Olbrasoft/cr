//! API endpoints for sledujteto.cz source on /filmy-a-serialy-1/.
//!
//! Architecture mirror of `cz_proxy.rs` (prehraj.to), but sledujteto's
//! application layer (`www.sledujteto.cz`) is NOT geo-blocked from Hetzner —
//! we call it directly without the CZ proxy. Only the stream CDN
//! (`data{N}.sledujteto.cz`) is geo-blocked, and the client browser streams
//! from it directly, so the proxy is not our concern here.
//!
//! Routes:
//!   GET /api/sledujteto/search?q=<query>     — search upstream API
//!   GET /api/sledujteto/resolve?id=<filesId> — turn files_id into stream URL

use axum::{
    Json,
    extract::{Query, State},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct SearchQuery {
    q: String,
}

#[derive(Serialize)]
pub struct SearchResultItem {
    /// sledujteto `files_id` (stable DB key) — pass back to `/resolve`.
    files_id: i64,
    name: String,
    preview: Option<String>,
    filesize: Option<String>,
    duration: Option<String>,
    resolution: Option<String>,
    is_hd: bool,
    views: Option<i64>,
    /// Full page URL on sledujteto.cz (for external link / reference).
    full_url: Option<String>,
}

#[derive(Serialize)]
pub struct SearchResponse {
    success: bool,
    movies: Vec<SearchResultItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

pub async fn sledujteto_search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> Json<SearchResponse> {
    let q = params.q.trim();
    if q.is_empty() {
        return Json(SearchResponse {
            success: false,
            movies: vec![],
            error: Some("empty query".into()),
        });
    }

    let url = format!(
        "https://www.sledujteto.cz/api/web/videos?query={}&page=1&limit=30&collection=suggestions&sort=relevance&me=0",
        urlencoding::encode(q)
    );

    let resp = state
        .http_client
        .get(&url)
        .header("Accept", "application/json")
        .header("X-Requested-With", "XMLHttpRequest")
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        )
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await;

    let resp = match resp {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("sledujteto search upstream failed: {e}");
            return Json(SearchResponse {
                success: false,
                movies: vec![],
                error: Some(format!("upstream: {e}")),
            });
        }
    };

    #[derive(Deserialize)]
    struct UpstreamResp {
        data: Option<UpstreamData>,
    }
    #[derive(Deserialize)]
    struct UpstreamData {
        files: Option<Vec<UpstreamFile>>,
    }
    #[derive(Deserialize)]
    struct UpstreamFile {
        id: i64,
        name: Option<String>,
        preview: Option<String>,
        filesize: Option<String>,
        duration: Option<String>,
        resolution: Option<String>,
        is_hd: Option<bool>,
        views: Option<i64>,
        full_url: Option<String>,
    }

    let data: UpstreamResp = match resp.json().await {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!("sledujteto search parse failed: {e}");
            return Json(SearchResponse {
                success: false,
                movies: vec![],
                error: Some(format!("parse: {e}")),
            });
        }
    };

    let files = data.data.and_then(|d| d.files).unwrap_or_default();
    let movies: Vec<SearchResultItem> = files
        .into_iter()
        .map(|f| SearchResultItem {
            files_id: f.id,
            name: f.name.unwrap_or_default(),
            preview: f.preview,
            filesize: f.filesize,
            duration: f.duration,
            resolution: f.resolution,
            is_hd: f.is_hd.unwrap_or(false),
            views: f.views,
            full_url: f.full_url,
        })
        .collect();

    Json(SearchResponse {
        success: true,
        movies,
        error: None,
    })
}

#[derive(Deserialize)]
pub struct ResolveQuery {
    id: i64,
}

#[derive(Serialize)]
pub struct ResolveResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    video_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    download_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

pub async fn sledujteto_resolve(
    State(state): State<AppState>,
    Query(params): Query<ResolveQuery>,
) -> Json<ResolveResponse> {
    let body = json!({ "params": { "id": params.id } });

    let resp = state
        .http_client
        .post("https://www.sledujteto.cz/services/add-file-link")
        .header("Content-Type", "application/json;charset=UTF-8")
        .header("Accept", "application/json, text/plain, */*")
        .header("Requested-With-AngularJS", "true")
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        )
        .json(&body)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await;

    let resp = match resp {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("sledujteto resolve upstream failed: {e}");
            return Json(ResolveResponse {
                success: false,
                video_url: None,
                download_url: None,
                error: Some(format!("upstream: {e}")),
            });
        }
    };

    #[derive(Deserialize)]
    struct UpstreamResp {
        error: Option<bool>,
        msg: Option<String>,
        video_url: Option<String>,
        download_url: Option<String>,
    }

    let data: UpstreamResp = match resp.json().await {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!("sledujteto resolve parse failed: {e}");
            return Json(ResolveResponse {
                success: false,
                video_url: None,
                download_url: None,
                error: Some(format!("parse: {e}")),
            });
        }
    };

    if data.error.unwrap_or(false) {
        return Json(ResolveResponse {
            success: false,
            video_url: None,
            download_url: None,
            error: data.msg.or(Some("upstream error".into())),
        });
    }

    Json(ResolveResponse {
        success: true,
        video_url: data.video_url,
        download_url: data.download_url,
        error: None,
    })
}
