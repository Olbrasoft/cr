use axum::Json;
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

use super::cz_proxy::cz_proxy_config;

// --- Types ---

#[derive(Deserialize)]
pub struct ValidateQuery {
    url: String,
}

#[derive(Serialize)]
pub struct ValidateResponse {
    valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_sec: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Deserialize)]
struct ProxyValidateResponse {
    valid: Option<bool>,
    status: Option<u16>,
}

#[derive(Deserialize)]
pub struct ThumbQuery {
    pub url: String,
}

// --- URL validation helpers ---

/// Validate URL is actually from prehraj.to domain.
pub(crate) fn is_prehrajto_url(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()))
        .map(|h| h == "prehraj.to" || h.ends_with(".prehraj.to"))
        .unwrap_or(false)
}

/// Allowed CDN domains for video streaming proxy.
const STREAM_ALLOWED_DOMAINS: &[&str] = &["premiumcdn.net"];

/// Validate URL is from allowed CDN domain.
pub(crate) fn is_allowed_stream_url(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()))
        .map(|h| {
            STREAM_ALLOWED_DOMAINS
                .iter()
                .any(|d| h == *d || h.strip_suffix(d).is_some_and(|p| p.ends_with('.')))
        })
        .unwrap_or(false)
}

// --- HTML extraction helpers ---

/// Extract `'videoLength': 772` from prehraj.to page HTML (seconds).
fn extract_video_length(html: &str) -> Option<u32> {
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"'videoLength'\s*:\s*(\d+)").expect("const regex literal compiles")
    });
    RE.captures(html)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok())
}

/// Extract real video width/height from prehraj.to microdata.
/// Names in the candidate title are unreliable ("1080p" uploads can be 480p);
/// the page's `<meta itemprop="height">` reports the actual stream dimensions.
fn extract_dimensions(html: &str) -> (Option<u32>, Option<u32>) {
    static RE_W: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"itemprop="width"\s+content="(\d+)""#)
            .expect("const regex literal compiles")
    });
    static RE_H: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"itemprop="height"\s+content="(\d+)""#)
            .expect("const regex literal compiles")
    });
    let w = RE_W
        .captures(html)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok());
    let h = RE_H
        .captures(html)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<u32>().ok());
    (w, h)
}

/// Extract `contentUrl` from prehraj.to page (mp4 on CDN). Return true if the
/// CDN is one of the direct-playable hosts (premiumcdn.net). A non-direct URL
/// would require proxying -- not what we want for scale.
fn extract_is_direct(html: &str) -> bool {
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"itemprop="contentUrl"\s+content="([^"]+)""#)
            .expect("const regex literal compiles")
    });
    RE.captures(html)
        .and_then(|c| c.get(1))
        .map(|m| {
            let url = m.as_str();
            url.contains(".premiumcdn.net/") || url.contains("premiumcdn.net:")
        })
        .unwrap_or(false)
}

// --- Constants ---

/// PHP proxy URL for prehraj.to validation (ASP.NET proxy returns 401, PHP works).
const PREHRAJTO_PHP_PROXY: &str = "http://tumarsrobot.unas.cz/index.php";

/// Only thumb.prehrajto.cz is allowed to prevent SSRF/open-proxy abuse.
const MOVIE_THUMB_ALLOWED_HOST: &str = "thumb.prehrajto.cz";

// --- Handlers ---

/// Validate video availability and fetch duration in one request by parsing
/// the prehraj.to page HTML (via CZ proxy). Returns both `valid` (DIRECT on
/// premiumcdn) and `duration_sec`.
pub async fn movies_validate(
    State(state): State<AppState>,
    Query(params): Query<ValidateQuery>,
) -> Json<ValidateResponse> {
    let url = params.url.trim().to_string();
    if url.is_empty() || !is_prehrajto_url(&url) {
        return Json(ValidateResponse {
            valid: false,
            status: None,
            duration_sec: None,
            width: None,
            height: None,
            error: Some("Invalid or missing prehraj.to URL".to_string()),
        });
    }

    // Prefer CZ proxy (action=proxy) for HTML fetch -- same infra used by
    // movies_video_url. Fall back to PHP proxy HEAD-check if CZ proxy is
    // unavailable (loses duration info but preserves legacy valid flag).
    if let Some((proxy_url, proxy_key)) = cz_proxy_config(&state.config) {
        let req_url = format!(
            "{}?action=proxy&url={}&key={}",
            proxy_url,
            urlencoding::encode(&url),
            proxy_key
        );
        match state
            .http_client
            .get(&req_url)
            .timeout(std::time::Duration::from_secs(25))
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status().as_u16();
                if !resp.status().is_success() {
                    return Json(ValidateResponse {
                        valid: false,
                        status: Some(status),
                        duration_sec: None,
                        width: None,
                        height: None,
                        error: None,
                    });
                }
                match resp.text().await {
                    Ok(html) => {
                        let valid = extract_is_direct(&html);
                        let duration_sec = extract_video_length(&html);
                        let (width, height) = extract_dimensions(&html);
                        return Json(ValidateResponse {
                            valid,
                            status: Some(status),
                            duration_sec,
                            width,
                            height,
                            error: None,
                        });
                    }
                    Err(e) => {
                        tracing::warn!("validate: read body failed: {e}");
                    }
                }
            }
            Err(e) => {
                tracing::warn!("validate: CZ proxy request failed: {e}");
            }
        }
    }

    // Fallback: legacy PHP proxy HEAD check (no duration_sec)
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
                duration_sec: None,
                width: None,
                height: None,
                error: None,
            }),
            Err(_) => Json(ValidateResponse {
                valid: false,
                status: None,
                duration_sec: None,
                width: None,
                height: None,
                error: Some("Invalid response".to_string()),
            }),
        },
        Err(e) => Json(ValidateResponse {
            valid: false,
            status: None,
            duration_sec: None,
            width: None,
            height: None,
            error: Some(format!("Request failed: {e}")),
        }),
    }
}

/// Proxy movie thumbnails through our domain so the browser never hits
/// thumb.prehrajto.cz directly. Keeps the source domain out of the user's
/// network traffic and lets us add caching headers.
pub async fn movies_thumb(
    State(state): State<AppState>,
    Query(query): Query<ThumbQuery>,
) -> impl IntoResponse {
    // Strict host check: URL must parse, use HTTPS, and host must exactly match.
    let parsed = match reqwest::Url::parse(&query.url) {
        Ok(u) => u,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid URL").into_response(),
    };
    if parsed.scheme() != "https" || parsed.host_str() != Some(MOVIE_THUMB_ALLOWED_HOST) {
        return (StatusCode::FORBIDDEN, "URL not allowed").into_response();
    }

    let resp = state
        .http_client
        .get(parsed.as_str())
        .header("User-Agent", "Mozilla/5.0")
        .header("Referer", "https://prehraj.to/")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => {
            let ct = r
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("image/jpeg")
                .to_string();
            match r.bytes().await {
                Ok(bytes) => (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, ct),
                        // Cache for 1 day in browser, 7 days on any CDN in front of us
                        (
                            header::CACHE_CONTROL,
                            "public, max-age=86400, s-maxage=604800, immutable".to_string(),
                        ),
                    ],
                    bytes,
                )
                    .into_response(),
                Err(_) => (StatusCode::BAD_GATEWAY, "Failed to read upstream body").into_response(),
            }
        }
        _ => (StatusCode::NOT_FOUND, "Thumbnail not available").into_response(),
    }
}
