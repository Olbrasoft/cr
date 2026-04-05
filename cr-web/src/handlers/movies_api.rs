use axum::Json;
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

#[derive(Deserialize)]
pub struct SearchQuery {
    q: String,
}

#[derive(Deserialize)]
pub struct VideoUrlQuery {
    url: String,
}

#[derive(Deserialize)]
pub struct StreamQuery {
    url: String,
}

#[derive(Deserialize)]
pub struct ValidateQuery {
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
    error: Option<String>,
}

#[derive(Serialize)]
pub struct ValidateResponse {
    valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

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
struct ProxyVideoResponse {
    success: Option<bool>,
    #[serde(rename = "videoUrl")]
    video_url: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct ProxyValidateResponse {
    valid: Option<bool>,
    status: Option<u16>,
}

/// Validate URL is actually from prehraj.to domain.
fn is_prehrajto_url(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()))
        .map(|h| h == "prehraj.to" || h.ends_with(".prehraj.to"))
        .unwrap_or(false)
}

/// Allowed CDN domains for video streaming proxy.
const STREAM_ALLOWED_DOMAINS: &[&str] = &["premiumcdn.net"];

/// Validate URL is from allowed CDN domain.
fn is_allowed_stream_url(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()))
        .map(|h| {
            STREAM_ALLOWED_DOMAINS
                .iter()
                .any(|d| h == *d || h.ends_with(&format!(".{d}")))
        })
        .unwrap_or(false)
}

/// Get CzProxy base URL and key from env vars.
fn cz_proxy_config() -> Option<(String, String)> {
    let url = std::env::var("CZ_PROXY_URL").ok()?;
    let key = std::env::var("CZ_PROXY_KEY").ok()?;
    if url.is_empty() || key.is_empty() {
        return None;
    }
    Some((url, key))
}

/// Search movies via CzProxy → prehraj.to
pub async fn movies_search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, (StatusCode, String)> {
    let query = params.q.trim().to_string();
    if query.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Missing search query".to_string()));
    }

    let (proxy_url, proxy_key) = cz_proxy_config().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "Proxy not configured".to_string(),
    ))?;

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
            (StatusCode::BAD_GATEWAY, format!("Proxy error: {e}"))
        })?;

    let data: ProxySearchResponse = resp.json().await.map_err(|e| {
        tracing::error!("CzProxy search parse failed: {e}");
        (
            StatusCode::BAD_GATEWAY,
            "Invalid proxy response".to_string(),
        )
    })?;

    if data.success != Some(true) {
        return Err((
            StatusCode::BAD_GATEWAY,
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

/// Get video CDN URL via CzProxy → prehraj.to page
pub async fn movies_video_url(
    State(state): State<AppState>,
    Query(params): Query<VideoUrlQuery>,
) -> Result<Json<VideoUrlResponse>, (StatusCode, String)> {
    let video_url = params.url.trim().to_string();
    if !is_prehrajto_url(&video_url) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid prehraj.to URL".to_string(),
        ));
    }

    let (proxy_url, proxy_key) = cz_proxy_config().ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "Proxy not configured".to_string(),
    ))?;

    let url = format!(
        "{}?action=video&url={}&key={}",
        proxy_url,
        urlencoding::encode(&video_url),
        proxy_key
    );

    let resp = state
        .http_client
        .get(&url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| {
            tracing::error!("CzProxy video failed: {e}");
            (StatusCode::BAD_GATEWAY, format!("Proxy error: {e}"))
        })?;

    let data: ProxyVideoResponse = resp.json().await.map_err(|e| {
        tracing::error!("CzProxy video parse failed: {e}");
        (
            StatusCode::BAD_GATEWAY,
            "Invalid proxy response".to_string(),
        )
    })?;

    Ok(Json(VideoUrlResponse {
        success: data.success.unwrap_or(false),
        video_url: data.video_url,
        error: data.error,
    }))
}

/// PHP proxy URL for prehraj.to validation (ASP.NET proxy returns 401, PHP works).
const PREHRAJTO_PHP_PROXY: &str = "http://tumarsrobot.unas.cz/index.php";

/// Validate video availability via PHP proxy (tumarsrobot.unas.cz)
pub async fn movies_validate(
    State(state): State<AppState>,
    Query(params): Query<ValidateQuery>,
) -> Json<ValidateResponse> {
    let url = params.url.trim().to_string();
    if url.is_empty() {
        return Json(ValidateResponse {
            valid: false,
            status: None,
            error: Some("Missing url".to_string()),
        });
    }
    // Use PHP proxy for validation — ASP.NET proxy returns 401 for valid CDN URLs
    let req_url = format!(
        "{}?action=validate&url={}",
        PREHRAJTO_PHP_PROXY,
        urlencoding::encode(&url),
    );

    match state
        .http_client
        .get(&req_url)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
    {
        Ok(resp) => match resp.json::<ProxyValidateResponse>().await {
            Ok(data) => Json(ValidateResponse {
                valid: data.valid.unwrap_or(false),
                status: data.status,
                error: None,
            }),
            Err(_) => Json(ValidateResponse {
                valid: false,
                status: None,
                error: Some("Invalid response".to_string()),
            }),
        },
        Err(e) => Json(ValidateResponse {
            valid: false,
            status: None,
            error: Some(format!("Request failed: {e}")),
        }),
    }
}

/// Stream video via CzProxy (for geo-blocked content)
pub async fn movies_stream(
    State(state): State<AppState>,
    Query(params): Query<StreamQuery>,
    req: axum::http::Request<axum::body::Body>,
) -> impl IntoResponse {
    let video_url = params.url.trim().to_string();
    if video_url.is_empty() {
        return (StatusCode::BAD_REQUEST, "Missing url").into_response();
    }
    if !is_allowed_stream_url(&video_url) {
        return (StatusCode::BAD_REQUEST, "URL not allowed").into_response();
    }

    let config = cz_proxy_config();
    if config.is_none() {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Proxy not configured").into_response();
    }
    let (proxy_url, proxy_key) = config.unwrap();

    let stream_url = format!(
        "{}?action=stream&url={}&key={}",
        proxy_url,
        urlencoding::encode(&video_url),
        proxy_key
    );

    let mut proxy_req = state.http_client.get(&stream_url);

    // Forward Range header for seeking
    if let Some(range) = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
    {
        proxy_req = proxy_req.header("Range", range);
    }

    match proxy_req
        .timeout(std::time::Duration::from_secs(300))
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            let content_type = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("video/mp4")
                .to_string();
            let content_length = resp
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            let content_range = resp
                .headers()
                .get("content-range")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            let axum_status =
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);

            let bytes = resp.bytes().await.unwrap_or_default();

            let mut builder = axum::http::Response::builder()
                .status(axum_status)
                .header(header::CONTENT_TYPE, &content_type)
                .header("Accept-Ranges", "bytes")
                .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*");

            if let Some(cl) = content_length {
                builder = builder.header(header::CONTENT_LENGTH, cl);
            }
            if let Some(cr) = content_range {
                builder = builder.header("Content-Range", cr);
            }

            builder
                .body(axum::body::Body::from(bytes))
                .unwrap()
                .into_response()
        }
        Err(e) => {
            tracing::error!("Stream proxy failed: {e}");
            (StatusCode::BAD_GATEWAY, "Stream failed").into_response()
        }
    }
}
