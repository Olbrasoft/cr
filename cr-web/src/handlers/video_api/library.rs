//! Streamtape/R2 hosted video library CRUD + streaming proxy.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;

use crate::state::AppState;

use super::{VideoErrorResponse, sanitize_filename_ascii, sanitize_filename_unicode};

// --- Response types ---

#[derive(Serialize)]
pub struct LibraryEntry {
    id: i32,
    title: String,
    duration_sec: Option<i32>,
    quality: String,
    format_ext: String,
    /// Human-readable resolution from yt-dlp (e.g. `"1080p"`). Drives
    /// the card's top-right badge — #366. `None` on legacy rows
    /// where the backfill regex failed, in which case the JS hides
    /// the badge entirely.
    resolution: Option<String>,
    file_size_mb: f64,
    thumbnail_url: Option<String>,
    streamtape_url: String,
    created_at: String,
}

impl From<cr_domain::repository::VideoRecord> for LibraryEntry {
    fn from(r: cr_domain::repository::VideoRecord) -> Self {
        let mb = r.file_size_bytes as f64 / (1024.0 * 1024.0);
        Self {
            id: r.id,
            title: r.title,
            duration_sec: r.duration_sec,
            quality: r.quality,
            format_ext: r.format_ext,
            resolution: r.resolution,
            file_size_mb: (mb * 10.0).round() / 10.0,
            thumbnail_url: r.thumbnail_url,
            streamtape_url: r.streamtape_url,
            created_at: r.created_at,
        }
    }
}

#[derive(Serialize)]
pub struct LibraryPlayResponse {
    stream_url: String,
}

// --- Handlers ---

/// `GET /api/video/library` — list the most recent 50 hosted videos.
pub async fn library_list(
    State(state): State<AppState>,
) -> Result<Json<Vec<LibraryEntry>>, (StatusCode, Json<VideoErrorResponse>)> {
    use cr_domain::repository::VideoRepository;
    let rows = state.video_repo.list_recent(50).await.map_err(|e| {
        tracing::error!("library_list query failed: {e:?}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(VideoErrorResponse {
                error: "Knihovnu se nepodařilo načíst.".to_string(),
            }),
        )
    })?;
    Ok(Json(rows.into_iter().map(LibraryEntry::from).collect()))
}

/// `GET /api/video/library/{id}/play` — resolve a fresh `tapecontent.net`
/// MP4 URL for inline playback. Costs ~5s on the first call (Streamtape's
/// dlticket → wait → dl) but is then served instantly until the upstream
/// token expires; the frontend treats the URL as ephemeral and refetches
/// when it expires.
pub async fn library_play(
    State(state): State<AppState>,
    Path(id): Path<i32>,
) -> Result<Json<LibraryPlayResponse>, (StatusCode, Json<VideoErrorResponse>)> {
    use cr_domain::repository::VideoRepository;
    let pipeline = state.video_library.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(VideoErrorResponse {
                error: "Knihovna není nakonfigurována.".to_string(),
            }),
        )
    })?;
    let record = state
        .video_repo
        .find_by_id(id)
        .await
        .map_err(|e| {
            tracing::error!("library find_by_id failed: {e:?}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(VideoErrorResponse {
                    error: "Chyba databáze.".to_string(),
                }),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(VideoErrorResponse {
                    error: "Video nenalezeno.".to_string(),
                }),
            )
        })?;

    let stream_url = pipeline
        .streamtape_client()
        .get_stream_url(&record.streamtape_file_id)
        .await
        .map_err(|e| {
            tracing::error!(
                "get_stream_url failed for {}: {e}",
                record.streamtape_file_id
            );
            (
                StatusCode::BAD_GATEWAY,
                Json(VideoErrorResponse {
                    error: "Streamtape URL se nepodařilo získat.".to_string(),
                }),
            )
        })?;

    Ok(Json(LibraryPlayResponse { stream_url }))
}

/// `GET /api/video/library/{id}/stream` — proxy the video bytes from
/// Streamtape to the browser so the inline `<video>` element can play
/// without hitting the Streamtape session/IP-bound URL directly.
///
/// We forward the Range header so seeking works, and copy the upstream
/// content type. The upstream URL is resolved via `get_stream_url` (a
/// dlticket roundtrip) which is bound to *our* server's IP — exactly
/// what we want, since this server then makes the request.
///
/// The resolved URL is cached per file_id (~50 min) so a single video
/// playback session — which makes dozens of Range requests — does not
/// pay the 5 s dlticket wait on every chunk.
pub async fn library_stream(
    State(state): State<AppState>,
    Path(id): Path<i32>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    use cr_domain::repository::VideoRepository;
    let pipeline = match state.video_library.as_ref() {
        Some(p) => p,
        None => return (StatusCode::SERVICE_UNAVAILABLE, "library disabled").into_response(),
    };
    let record = match state.video_repo.find_by_id(id).await {
        Ok(Some(r)) => r,
        Ok(None) => return (StatusCode::NOT_FOUND, "video not found").into_response(),
        Err(e) => {
            tracing::error!("library_stream find: {e:?}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response();
        }
    };
    let upstream_url =
        match resolve_cached_stream_url(&state, pipeline, &record.streamtape_file_id).await {
            Ok(u) => u,
            Err(e) => {
                tracing::error!(
                    "library_stream get_stream_url failed for {}: {e}",
                    record.streamtape_file_id
                );
                return (StatusCode::BAD_GATEWAY, "stream resolve failed").into_response();
            }
        };
    // Inline disposition + filename: the browser still plays the response
    // inside the <video> element, but the in-player ⋮ menu's "Save video as…"
    // (and any other Save As path) pre-fills with the proper title instead
    // of "stream.mp4". This means the duplicate Stáhnout button is no
    // longer needed — see #337.
    let download_name = library_download_filename(&record);
    proxy_streamtape(
        state.http_client.clone(),
        &upstream_url,
        &headers,
        Some(ContentDisposition::Inline(&download_name)),
    )
    .await
}

/// `GET /api/video/library/{id}/file` — same upstream proxy as `/stream`
/// but with a `Content-Disposition: attachment` header derived from the
/// stored title so the browser triggers a download instead of trying to
/// play it inline. Used by the Stáhnout button on the library card.
pub async fn library_file(
    State(state): State<AppState>,
    Path(id): Path<i32>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    use cr_domain::repository::VideoRepository;
    let pipeline = match state.video_library.as_ref() {
        Some(p) => p,
        None => return (StatusCode::SERVICE_UNAVAILABLE, "library disabled").into_response(),
    };
    let record = match state.video_repo.find_by_id(id).await {
        Ok(Some(r)) => r,
        Ok(None) => return (StatusCode::NOT_FOUND, "video not found").into_response(),
        Err(e) => {
            tracing::error!("library_file find: {e:?}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response();
        }
    };
    let upstream_url =
        match resolve_cached_stream_url(&state, pipeline, &record.streamtape_file_id).await {
            Ok(u) => u,
            Err(e) => {
                tracing::error!("library_file get_stream_url failed: {e}");
                return (StatusCode::BAD_GATEWAY, "stream resolve failed").into_response();
            }
        };
    let download_name = library_download_filename(&record);
    proxy_streamtape(
        state.http_client.clone(),
        &upstream_url,
        &headers,
        Some(ContentDisposition::Attachment(&download_name)),
    )
    .await
}

/// `DELETE /api/video/library/{id}` — remove a hosted video from
/// Streamtape, R2 and the DB. Order matters: external services first
/// so a partial failure leaves the DB row in place to retry, then DB.
pub async fn library_delete(
    State(state): State<AppState>,
    Path(id): Path<i32>,
) -> Result<StatusCode, (StatusCode, Json<VideoErrorResponse>)> {
    use cr_domain::repository::VideoRepository;
    let record = state
        .video_repo
        .find_by_id(id)
        .await
        .map_err(|e| {
            tracing::error!("library_delete find: {e:?}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(VideoErrorResponse {
                    error: "Chyba databáze.".to_string(),
                }),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(VideoErrorResponse {
                    error: "Video nenalezeno.".to_string(),
                }),
            )
        })?;

    if let Some(pipeline) = state.video_library.as_ref() {
        if let Err(e) = pipeline
            .streamtape_client()
            .delete(&record.streamtape_file_id)
            .await
        {
            tracing::warn!(
                "streamtape delete failed for {}: {e} — continuing with DB delete",
                record.streamtape_file_id
            );
        }
        if let Some(key) = record.thumbnail_r2_key.as_deref()
            && let Err(e) = pipeline.r2_client().delete_object(key).await
        {
            tracing::warn!("r2 delete failed for {key}: {e} — continuing");
        }
    }

    state.video_repo.delete(id).await.map_err(|e| {
        tracing::error!("library_delete db: {e:?}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(VideoErrorResponse {
                error: "Chyba databáze.".to_string(),
            }),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

// --- Private helpers ---

/// Returns the cached Streamtape CDN URL for `file_id`, resolving a fresh
/// one (5 s dlticket wait) only when the cache is empty or the entry is
/// older than 45 minutes (Streamtape tokens are valid ~50 min).
async fn resolve_cached_stream_url(
    state: &AppState,
    pipeline: &cr_infra::video_library::VideoLibraryPipeline,
    file_id: &str,
) -> Result<String, cr_infra::streamtape::StreamtapeError> {
    const TTL: std::time::Duration = std::time::Duration::from_secs(45 * 60);
    {
        let cache = state.streamtape_url_cache.lock().await;
        if let Some((url, ts)) = cache.get(file_id)
            && ts.elapsed() < TTL
        {
            return Ok(url.clone());
        }
    }
    let url = pipeline.streamtape_client().get_stream_url(file_id).await?;
    state.streamtape_url_cache.lock().await.insert(
        file_id.to_string(),
        (url.clone(), std::time::Instant::now()),
    );
    Ok(url)
}

/// Two filenames for a library entry — an ASCII fallback (always
/// header-safe) and a Unicode form that keeps Czech / Cyrillic / CJK
/// letters intact. They feed `Content-Disposition` as `filename="…"`
/// (the ASCII one) and `filename*=UTF-8''…` (the percent-encoded
/// Unicode one); browsers prefer `filename*` when both are present.
struct DownloadFilename {
    ascii: String,
    unicode: String,
}

fn library_download_filename(record: &cr_domain::repository::VideoRecord) -> DownloadFilename {
    let mut ascii = sanitize_filename_ascii(&record.title, 80);
    if ascii == "video" {
        ascii = format!("video-{}", record.id);
    }
    let mut unicode = sanitize_filename_unicode(&record.title, 80);
    if unicode == "video" {
        unicode = format!("video-{}", record.id);
    }
    DownloadFilename {
        ascii: format!("{ascii}.{}", record.format_ext),
        unicode: format!("{unicode}.{}", record.format_ext),
    }
}

/// Shared upstream proxy for `/stream` and `/file`.
///
/// Streams the upstream body chunk-by-chunk via `bytes_stream()` instead
/// of buffering the whole file in memory. A 100 MB video used to wait for
/// the full download before sending a single byte to the browser; now the
/// browser starts receiving the first chunk within ~50 ms of the upstream
/// response headers landing.
/// Disposition mode for the proxied response.
///
/// `Inline` keeps the response playable inside `<video>` while still
/// telling the browser which filename to use when the user picks Save As
/// from the in-player ⋮ menu. `Attachment` triggers an immediate
/// download dialog (used by the explicit `/file` endpoint).
#[derive(Clone, Copy)]
enum ContentDisposition<'a> {
    Inline(&'a DownloadFilename),
    Attachment(&'a DownloadFilename),
}

async fn proxy_streamtape(
    http: reqwest::Client,
    upstream_url: &str,
    incoming: &axum::http::HeaderMap,
    disposition: Option<ContentDisposition<'_>>,
) -> axum::response::Response {
    let mut req = http.get(upstream_url);
    // Forward Range so seeking + partial content works.
    if let Some(range) = incoming.get(axum::http::header::RANGE) {
        req = req.header(axum::http::header::RANGE, range);
    }
    let upstream = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("proxy_streamtape upstream send failed: {e}");
            return (StatusCode::BAD_GATEWAY, "upstream error").into_response();
        }
    };
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();

    let mut response_headers = axum::http::HeaderMap::new();
    for h in [
        axum::http::header::CONTENT_TYPE,
        axum::http::header::CONTENT_LENGTH,
        axum::http::header::CONTENT_RANGE,
        axum::http::header::ACCEPT_RANGES,
        axum::http::header::CACHE_CONTROL,
    ] {
        if let Some(v) = upstream_headers.get(&h) {
            response_headers.insert(h, v.clone());
        }
    }
    // Make sure the browser knows it can seek even if upstream omits it.
    response_headers
        .entry(axum::http::header::ACCEPT_RANGES)
        .or_insert_with(|| axum::http::HeaderValue::from_static("bytes"));
    if let Some(disp) = disposition {
        let (kind, names) = match disp {
            ContentDisposition::Inline(n) => ("inline", n),
            ContentDisposition::Attachment(n) => ("attachment", n),
        };
        // RFC 6266 / RFC 5987: send both `filename` (ASCII fallback for
        // ancient clients) and `filename*=UTF-8''…` so modern browsers
        // get the original Czech / Unicode title intact. The percent-
        // encoding is intentionally aggressive — over-encoding is safe.
        let encoded = urlencoding::encode(&names.unicode);
        let header_str = format!(
            "{kind}; filename=\"{}\"; filename*=UTF-8''{}",
            names.ascii, encoded
        );
        let header_value = header_str.parse::<axum::http::HeaderValue>().or_else(|_| {
            axum::http::HeaderValue::from_str(&format!("{kind}; filename=\"video.bin\""))
        });
        if let Ok(value) = header_value {
            response_headers.insert(axum::http::header::CONTENT_DISPOSITION, value);
        }
    }

    let body = axum::body::Body::from_stream(upstream.bytes_stream());
    (status, response_headers, body).into_response()
}
