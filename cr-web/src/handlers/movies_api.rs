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
                .any(|d| h == *d || h.strip_suffix(d).is_some_and(|p| p.ends_with('.')))
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
    if url.is_empty() || !is_prehrajto_url(&url) {
        return Json(ValidateResponse {
            valid: false,
            status: None,
            error: Some("Invalid or missing prehraj.to URL".to_string()),
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

    // Resolve stream URL — pure HTTP + regex (no Playwright needed for streamtape/mixdrop)
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(5))
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap_or_default();

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
            headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
            headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
            headers.insert(
                header::HeaderName::from_static("accept-ranges"),
                "bytes".parse().unwrap(),
            );
            if let Some(cl) = content_length {
                headers.insert(header::CONTENT_LENGTH, cl.parse().unwrap());
            }
            if let Some(cr) = content_range {
                headers.insert(
                    header::HeaderName::from_static("content-range"),
                    cr.parse().unwrap(),
                );
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
    let re_div =
        regex::Regex::new(r#"<div[^>]*id="robotlink"[^>]*>([^<]*get_video[^<]*)</div>"#).unwrap();
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
    .unwrap();

    if let Some(cap) = re.captures(&html) {
        let prefix = &cap[1];
        let mut inner = cap[2].to_string();

        // Apply chained .substring(N) calls
        let sub_re = regex::Regex::new(r"\.substring\((\d+)\)").unwrap();
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
    .unwrap();

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
    let wurl_re = regex::Regex::new(r#"MDCore\.wurl="([^"]+)""#).unwrap();
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
fn unpack_js(packed: &str, base: u32, count: usize, keywords: &[&str]) -> String {
    let word_re = regex::Regex::new(r"\b\w+\b").unwrap();
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
async fn resolve_via_cz_proxy(
    _client: &reqwest::Client,
    provider: &str,
    code: &str,
) -> Result<(String, String), String> {
    let (_proxy_url, _proxy_key) =
        cz_proxy_config().ok_or("CZ proxy not configured (CZ_PROXY_URL/CZ_PROXY_KEY)")?;

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
