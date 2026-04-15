use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::state::AppState;

/// In-memory cache for resolved filemoon m3u8 URLs.
/// Key: filemoon_code, Value: (url, resolved_at).
static FILEMOON_CACHE: std::sync::LazyLock<
    Mutex<std::collections::HashMap<String, (String, Instant)>>,
> = std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

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

#[derive(Serialize, Clone)]
pub struct SubtitleTrack {
    url: String,
    lang: String,
    label: String,
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
                .any(|d| h == *d || h.strip_suffix(d).is_some_and(|p| p.ends_with('.')))
        })
        .unwrap_or(false)
}

/// Extract CzProxy (url, key) from AppConfig if both are configured.
/// Reads from the central `AppConfig` instead of `std::env` so tests can
/// instantiate an empty config and per-env overrides live in one place.
fn cz_proxy_config(config: &crate::config::AppConfig) -> Option<(String, String)> {
    config
        .cz_proxy
        .as_ref()
        .map(|p| (p.url.clone(), p.key.clone()))
}

/// Search movies via CzProxy → prehraj.to
pub async fn movies_search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> crate::error::WebResult<Json<SearchResponse>> {
    use crate::error::WebError;

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

/// Get video CDN URL via CzProxy → prehraj.to page
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

/// PHP proxy URL for prehraj.to validation (ASP.NET proxy returns 401, PHP works).
const PREHRAJTO_PHP_PROXY: &str = "http://tumarsrobot.unas.cz/index.php";

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
/// would require proxying — not what we want for scale.
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

    // Prefer CZ proxy (action=proxy) for HTML fetch — same infra used by
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

    let Some((proxy_url, proxy_key)) = cz_proxy_config(&state.config) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Proxy not configured").into_response();
    };

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

            // builder.body can only fail on invalid header pairs we set
            // above; fall back to a plain OK(bytes) so we never panic on a
            // broken upstream response.
            builder
                .body(axum::body::Body::from(bytes.clone()))
                .unwrap_or_else(|_| axum::http::Response::new(axum::body::Body::from(bytes)))
                .into_response()
        }
        Err(e) => {
            tracing::error!("Stream proxy failed: {e}");
            (StatusCode::BAD_GATEWAY, "Stream failed").into_response()
        }
    }
}

// --- Subtitle (VTT) proxy ---

#[derive(Deserialize)]
pub struct SubtitleQuery {
    pub url: String,
}

/// Proxy VTT subtitle files from premiumcdn.net through our domain.
/// HTML5 <track> elements require CORS headers that CDN doesn't provide.
pub async fn movies_subtitle(
    State(state): State<AppState>,
    Query(query): Query<SubtitleQuery>,
) -> impl IntoResponse {
    let parsed = match reqwest::Url::parse(&query.url) {
        Ok(u) => u,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid URL").into_response(),
    };
    // Only allow premiumcdn.net VTT files
    let host = parsed.host_str().unwrap_or("");
    if !host.ends_with("premiumcdn.net") || !parsed.path().ends_with(".vtt") {
        return (StatusCode::FORBIDDEN, "URL not allowed").into_response();
    }

    let resp = state
        .http_client
        .get(parsed.as_str())
        .header("User-Agent", "Mozilla/5.0")
        .header("Referer", "https://prehraj.to/")
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => match r.bytes().await {
            Ok(bytes) => (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "text/vtt; charset=utf-8".to_string()),
                    (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".to_string()),
                    (header::CACHE_CONTROL, "public, max-age=3600".to_string()),
                ],
                bytes,
            )
                .into_response(),
            Err(_) => (StatusCode::BAD_GATEWAY, "Failed to read subtitle").into_response(),
        },
        _ => (StatusCode::NOT_FOUND, "Subtitle not available").into_response(),
    }
}

// --- Thumbnail proxy ---

#[derive(Deserialize)]
pub struct ThumbQuery {
    pub url: String,
}

/// Only thumb.prehrajto.cz is allowed to prevent SSRF/open-proxy abuse.
const MOVIE_THUMB_ALLOWED_HOST: &str = "thumb.prehrajto.cz";

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

// --- Filemoon stream resolver ---

#[derive(Deserialize)]
pub struct StreamResolveQuery {
    /// Provider: filemoon, streamtape, mixdrop, vidlink
    provider: String,
    /// Stable code/ID for the provider
    code: String,
}

#[derive(Serialize)]
pub struct StreamResolveResponse {
    provider: String,
    code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_url: Option<String>,
    /// "hls" or "mp4"
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    cached: bool,
}

const ALLOWED_PROVIDERS: &[&str] = &["filemoon", "streamtape", "mixdrop", "vidlink"];

/// `GET /api/movies/stream-resolve?provider={provider}&code={code}`
///
/// Resolves a stable code into a fresh stream URL via headless browser.
/// Supported providers: filemoon (HLS), streamtape (MP4), mixdrop (MP4), vidlink (HLS).
/// Results are cached per provider+code with TTL based on token expiry.
pub async fn stream_resolve(
    Query(params): Query<StreamResolveQuery>,
) -> Json<StreamResolveResponse> {
    let provider = params.provider.trim().to_lowercase();
    let code = params.code.trim().to_string();

    if !ALLOWED_PROVIDERS.contains(&provider.as_str()) {
        return Json(StreamResolveResponse {
            provider,
            code,
            stream_url: None,
            format: None,
            error: Some(format!(
                "Unknown provider. Use: {}",
                ALLOWED_PROVIDERS.join(", ")
            )),
            cached: false,
        });
    }

    if code.len() < 4 || code.len() > 20 {
        return Json(StreamResolveResponse {
            provider,
            code,
            stream_url: None,
            format: None,
            error: Some("Invalid code format".to_string()),
            cached: false,
        });
    }

    let cache_key = format!("{provider}:{code}");

    // Check cache (2h TTL — conservative, tokens last 3-4h)
    let cache_ttl = Duration::from_secs(2 * 3600);
    {
        let cache = FILEMOON_CACHE.lock().await;
        if let Some((url, resolved_at)) = cache.get(&cache_key)
            && resolved_at.elapsed() < cache_ttl
        {
            let fmt = if url.contains(".m3u8") { "hls" } else { "mp4" };
            return Json(StreamResolveResponse {
                provider,
                code,
                stream_url: Some(url.clone()),
                format: Some(fmt.to_string()),
                error: None,
                cached: true,
            });
        }
    }

    // Use Playwright (Python script) for all providers — it handles browser
    // session, cookies, and JS execution needed for CDN token generation.
    // Pure-HTTP resolvers (resolve_streamtape, resolve_mixdrop) extract tokens
    // that are session-bound and don't work for streaming.
    let result = resolve_via_playwright(&provider, &code).await;

    match result {
        Ok(pr) => {
            {
                let mut cache = FILEMOON_CACHE.lock().await;
                // Store URL + cookies in cache (cookies separated by \n)
                let cache_val = if let Some(ref cookies) = pr.cookies {
                    format!("{}\n{cookies}", pr.url)
                } else {
                    pr.url.clone()
                };
                cache.insert(cache_key, (cache_val, Instant::now()));
            }
            Json(StreamResolveResponse {
                provider,
                code,
                stream_url: Some(pr.url),
                format: Some(pr.format),
                error: None,
                cached: false,
            })
        }
        Err(error) => Json(StreamResolveResponse {
            provider,
            code,
            stream_url: None,
            format: None,
            error: Some(error),
            cached: false,
        }),
    }
}

/// Proxy-stream: resolve + proxy video bytes to the client.
/// For providers where the CDN URL is IP-bound to the server.
pub async fn movies_proxy_stream(
    State(state): State<AppState>,
    Query(params): Query<StreamResolveQuery>,
    req: axum::http::Request<axum::body::Body>,
) -> impl IntoResponse {
    let provider = params.provider.trim().to_lowercase();
    let code = params.code.trim().to_string();

    // Resolve stream URL + cookies via Playwright
    let cache_key = format!("{provider}:{code}");
    let cache_ttl = Duration::from_secs(2 * 3600);

    // Check cache first (stores "url\ncookies" or just "url")
    let (stream_url, cookies) = {
        let cache = FILEMOON_CACHE.lock().await;
        if let Some((cached_val, resolved_at)) = cache.get(&cache_key)
            && resolved_at.elapsed() < cache_ttl
        {
            let parts: Vec<&str> = cached_val.splitn(2, '\n').collect();
            let url = parts[0].to_string();
            let cookies = parts.get(1).map(|s| s.to_string());
            (url, cookies)
        } else {
            drop(cache); // Release lock before calling resolve
            // Resolve fresh
            let result = resolve_via_playwright(&provider, &code).await;
            match result {
                Ok(pr) => {
                    let mut cache = FILEMOON_CACHE.lock().await;
                    let cache_val = if let Some(ref c) = pr.cookies {
                        format!("{}\n{c}", pr.url)
                    } else {
                        pr.url.clone()
                    };
                    cache.insert(cache_key, (cache_val, Instant::now()));
                    (pr.url, pr.cookies)
                }
                Err(e) => {
                    return (StatusCode::BAD_GATEWAY, e).into_response();
                }
            }
        }
    };

    // Proxy the video bytes to client (with cookies from Playwright session)
    let mut proxy_req = state
        .http_client
        .get(&stream_url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/145",
        )
        .timeout(Duration::from_secs(300));

    if let Some(ref cookie_str) = cookies {
        proxy_req = proxy_req.header("Cookie", cookie_str.as_str());
    }

    // Forward Range header for seeking
    if let Some(range) = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
    {
        proxy_req = proxy_req.header("Range", range);
    }

    match proxy_req.send().await {
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

            let mut headers = axum::http::HeaderMap::new();
            // All header-value parses that COULD fail on malformed upstream
            // strings are skipped silently on error instead of panicking the
            // process — the proxied byte stream still works without these
            // optional cache/CORS hints.
            if let Ok(v) = content_type.parse() {
                headers.insert(header::CONTENT_TYPE, v);
            }
            if let Ok(v) = "*".parse() {
                headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
            }
            if let Ok(v) = "bytes".parse() {
                headers.insert(header::HeaderName::from_static("accept-ranges"), v);
            }
            if let Some(cl) = content_length
                && let Ok(v) = cl.parse()
            {
                headers.insert(header::CONTENT_LENGTH, v);
            }
            if let Some(cr) = content_range
                && let Ok(v) = cr.parse()
            {
                headers.insert(header::HeaderName::from_static("content-range"), v);
            }

            let body = axum::body::Body::from_stream(resp.bytes_stream());
            (status, headers, body).into_response()
        }
        Err(e) => (StatusCode::BAD_GATEWAY, format!("Proxy error: {e}")).into_response(),
    }
}

/// Backwards-compatible wrapper — calls stream_resolve with provider=filemoon.
pub async fn filemoon_resolve(
    Query(params): Query<StreamResolveQuery>,
) -> Json<StreamResolveResponse> {
    stream_resolve(Query(params)).await
}

// ── Pure-HTTP stream resolvers (no Playwright) ───────────────────

/// Resolve streamtape embed → direct MP4 URL via regex on inline JS.
#[allow(dead_code)]
async fn resolve_streamtape(
    client: &reqwest::Client,
    code: &str,
) -> Result<(String, String), String> {
    let url = format!("https://streamtape.com/e/{code}");
    let html = client
        .get(&url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/145",
        )
        .send()
        .await
        .map_err(|e| format!("Fetch failed: {e}"))?
        .text()
        .await
        .map_err(|e| format!("Read failed: {e}"))?;

    // Check for "not found"
    if html.contains("Video not found") {
        return Err("Video not found on Streamtape".to_string());
    }

    // Fallback first: robotlink div is pre-rendered with the actual URL
    let re_div = regex::Regex::new(r#"<div[^>]*id="robotlink"[^>]*>([^<]*get_video[^<]*)</div>"#)
        .expect("const regex literal compiles");
    if let Some(cap) = re_div.captures(&html) {
        let raw = cap[1].trim();
        let get_video_url = if raw.starts_with("//") {
            format!("https:{raw}")
        } else if raw.starts_with('/') {
            format!("https:/{raw}")
        } else {
            format!("https://{raw}")
        };

        // get_video does a 302 redirect to tapecontent.net CDN — follow it to get final URL
        let no_redirect = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_default();
        if let Ok(resp) = no_redirect
            .get(&get_video_url)
            .header("User-Agent", "Mozilla/5.0")
            .send()
            .await
            && resp.status().is_redirection()
            && let Some(location) = resp.headers().get("location").and_then(|v| v.to_str().ok())
        {
            return Ok((location.to_string(), "mp4".to_string()));
        }

        // If redirect fails, return get_video URL anyway
        return Ok((get_video_url, "mp4".to_string()));
    }

    // JS pattern: getElementById('robotlink').innerHTML = 'PREFIX' + ... ('INNER').substring(N).substring(M)
    // Streamtape uses multiple fake targets (ideoolink, botlink) — only 'robotlink' is real
    let re = regex::Regex::new(
        r#"getElementById\('robotlink'\)\.innerHTML\s*=\s*'([^']+)'\s*\+\s*[^(]*\('([^']+)'\)((?:\.substring\(\d+\))+)"#,
    )
    .expect("const regex literal compiles");

    if let Some(cap) = re.captures(&html) {
        let prefix = &cap[1];
        let mut inner = cap[2].to_string();

        // Apply chained .substring(N) calls
        let sub_re =
            regex::Regex::new(r"\.substring\((\d+)\)").expect("const regex literal compiles");
        for sub_cap in sub_re.captures_iter(&cap[3]) {
            let skip: usize = sub_cap[1].parse().unwrap_or(0);
            if skip <= inner.len() {
                inner = inner[skip..].to_string();
            }
        }

        let mp4_url = format!("https:{prefix}{inner}");
        return Ok((mp4_url, "mp4".to_string()));
    }

    Err("robotlink pattern not found in Streamtape page".to_string())
}

/// Resolve mixdrop embed → direct MP4 URL by unpacking p,a,c,k,e,d JS.
#[allow(dead_code)]
async fn resolve_mixdrop(client: &reqwest::Client, code: &str) -> Result<(String, String), String> {
    let url = format!("https://mixdrop.ag/e/{code}");
    let html = client
        .get(&url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/145",
        )
        .send()
        .await
        .map_err(|e| format!("Fetch failed: {e}"))?
        .text()
        .await
        .map_err(|e| format!("Read failed: {e}"))?;

    if html.contains("can't find") || html.is_empty() {
        return Err("Video not found on Mixdrop".to_string());
    }

    // Extract p,a,c,k,e,d packed JS
    let re = regex::Regex::new(
        r#"eval\(function\(p,a,c,k,e,d\)\{.*?\}\('([^']+)',(\d+),(\d+),'([^']+)'"#,
    )
    .expect("const regex literal compiles");

    let cap = re
        .captures(&html)
        .ok_or("p,a,c,k,e,d packed JS not found")?;

    let p = &cap[1];
    let a: u32 = cap[2].parse().unwrap_or(36);
    let c: usize = cap[3].parse().unwrap_or(0);
    let k_str = &cap[4];
    let keywords: Vec<&str> = k_str.split('|').collect();

    // Unpack: replace base-N tokens in p with keywords
    let unpacked = unpack_js(p, a, c, &keywords);

    // Extract MDCore.wurl
    let wurl_re =
        regex::Regex::new(r#"MDCore\.wurl="([^"]+)""#).expect("const regex literal compiles");
    if let Some(m) = wurl_re.captures(&unpacked) {
        let video_url = if m[1].starts_with("//") {
            format!("https:{}", &m[1])
        } else {
            m[1].to_string()
        };
        return Ok((video_url, "mp4".to_string()));
    }

    Err("MDCore.wurl not found in unpacked JS".to_string())
}

/// Simple p,a,c,k,e,d JS unpacker.
#[allow(dead_code)]
fn unpack_js(packed: &str, base: u32, count: usize, keywords: &[&str]) -> String {
    let word_re = regex::Regex::new(r"\b\w+\b").expect("const regex literal compiles");
    word_re
        .replace_all(packed, |caps: &regex::Captures| {
            let word = &caps[0];
            if let Some(n) = decode_base_n(word, base)
                && (n as usize) < count
                && (n as usize) < keywords.len()
            {
                let kw = keywords[n as usize];
                if !kw.is_empty() {
                    return kw.to_string();
                }
            }
            word.to_string()
        })
        .to_string()
}

/// Decode a base-N string (supports up to base 62: 0-9, a-z, A-Z).
#[allow(dead_code)]
fn decode_base_n(s: &str, base: u32) -> Option<u32> {
    let mut result: u32 = 0;
    for ch in s.chars() {
        let digit = match ch {
            '0'..='9' => ch as u32 - '0' as u32,
            'a'..='z' => ch as u32 - 'a' as u32 + 10,
            'A'..='Z' => ch as u32 - 'A' as u32 + 36,
            _ => return None,
        };
        if digit >= base {
            return None;
        }
        result = result.checked_mul(base)?.checked_add(digit)?;
    }
    Some(result)
}

/// Extract VTT subtitle tracks from Přehraj.to page HTML.
/// Matches JWPlayer track config: { file: "...vtt", label: "CZE - 123 - cze", kind: "captions" }
fn extract_subtitles_from_html(html: &str) -> Vec<SubtitleTrack> {
    let re = regex::Regex::new(
        r#"\{\s*file\s*:\s*"([^"]+\.vtt[^"]*)"\s*,\s*(?:"default"\s*:\s*true\s*,\s*)?label\s*:\s*"([^"]+)"\s*,\s*kind\s*:\s*"captions"\s*\}"#,
    )
    .expect("const regex literal compiles");

    let lang_re = regex::Regex::new(r"(\w{2,3})\s*$").expect("const regex literal compiles");

    re.captures_iter(html)
        .map(|cap| {
            let vtt_url = cap[1].replace("\\u0026", "&").replace("&amp;", "&");
            let label_raw = &cap[2];

            // Extract language code from label like "CZE - 8929014 - cze"
            let lang = lang_re
                .captures(label_raw)
                .map(|m| m[1].to_lowercase())
                .unwrap_or_default();

            // Clean label: "CZE - 8929014 - cze" → "CZE"
            let label = regex::Regex::new(r"\s*-\s*\d+\s*-\s*\w+$")
                .expect("const regex literal compiles")
                .replace(label_raw, "")
                .trim()
                .to_string();

            SubtitleTrack {
                url: vtt_url,
                lang,
                label,
            }
        })
        .collect()
}

/// Result from Playwright resolve — URL + optional cookies for CDN access.
struct PlaywrightResult {
    url: String,
    format: String,
    cookies: Option<String>,
}

/// Resolve via Playwright (Python extract-stream.py script).
async fn resolve_via_playwright(provider: &str, code: &str) -> Result<PlaywrightResult, String> {
    let script_path = std::env::current_dir()
        .map(|p| p.join("scripts/extract-stream.py"))
        .unwrap_or_else(|_| std::path::PathBuf::from("scripts/extract-stream.py"));

    if !script_path.exists() {
        return Err(format!(
            "extract-stream.py not found at {}",
            script_path.display()
        ));
    }

    let output = tokio::process::Command::new("python3")
        .arg(&script_path)
        .arg(provider)
        .arg(code)
        .output()
        .await
        .map_err(|e| format!("Script execution failed: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let val: serde_json::Value =
        serde_json::from_str(&stdout).map_err(|e| format!("Invalid script output: {e}"))?;

    if let Some(url) = val.get("stream_url").and_then(|v| v.as_str()) {
        let fmt = val.get("format").and_then(|v| v.as_str()).unwrap_or("mp4");
        let cookies = val
            .get("cookies")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok(PlaywrightResult {
            url: url.to_string(),
            format: fmt.to_string(),
            cookies,
        })
    } else {
        Err(val
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown error")
            .to_string())
    }
}

/// Resolve via CZ proxy (chobotnice.aspfree.cz) — for providers that need CZ IP or browser.
#[allow(dead_code)]
async fn resolve_via_cz_proxy(
    config: &crate::config::AppConfig,
    _client: &reqwest::Client,
    provider: &str,
    code: &str,
) -> Result<(String, String), String> {
    let (_proxy_url, _proxy_key) =
        cz_proxy_config(config).ok_or("CZ proxy not configured (CZ_PROXY_URL/CZ_PROXY_KEY)")?;

    // Try the Python script as fallback (if available locally)
    let script_path = std::env::current_dir()
        .map(|p| p.join("scripts/extract-stream.py"))
        .unwrap_or_else(|_| std::path::PathBuf::from("scripts/extract-stream.py"));

    if script_path.exists()
        && let Ok(output) = tokio::process::Command::new("python3")
            .arg(&script_path)
            .arg(provider)
            .arg(code)
            .output()
            .await
    {
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&stdout) {
            if let Some(url) = val.get("stream_url").and_then(|v| v.as_str()) {
                let fmt = val.get("format").and_then(|v| v.as_str()).unwrap_or("mp4");
                return Ok((url.to_string(), fmt.to_string()));
            }
            if let Some(err) = val.get("error").and_then(|v| v.as_str()) {
                return Err(err.to_string());
            }
        }
    }

    Err(format!(
        "{provider} resolution not available on this server"
    ))
}
