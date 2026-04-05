use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::{Json, extract::Path};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};

/// Maximum concurrent video downloads.
pub static VIDEO_DOWNLOAD_SEMAPHORE: Semaphore = Semaphore::const_new(3);

use crate::state::AppState;

/// Shared state for tracking video download tasks (async).
pub type VideoDownloads = Arc<Mutex<HashMap<String, VideoTask>>>;

pub struct VideoTask {
    pub status: DownloadStatus,
    pub progress: Arc<AtomicU8>,
    pub file_path: std::path::PathBuf,
    pub filename: String,
    pub parts: Vec<PartInfo>,
    #[allow(dead_code)]
    pub created_at: std::time::Instant,
}

#[derive(Clone, Serialize)]
pub struct PartInfo {
    pub index: usize,
    pub filename: String,
    pub size_mb: f64,
    pub file_path: std::path::PathBuf,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum DownloadStatus {
    Downloading { progress_percent: u8 },
    Converting { progress_percent: u8 },
    Ready { size_mb: f64, filename: String },
    ReadyParts { parts: Vec<PartResponse> },
    Failed { error: String },
}

#[derive(Clone, Serialize)]
pub struct PartResponse {
    pub index: usize,
    pub filename: String,
    pub size_mb: f64,
}

// --- Request/Response types ---

#[derive(Deserialize)]
pub struct VideoInfoRequest {
    url: String,
}

#[derive(Serialize)]
pub struct VideoInfoResponse {
    title: String,
    thumbnail: Option<String>,
    duration: Option<f64>,
    uploader: Option<String>,
    formats: Vec<FormatResponse>,
    whatsapp_parts: u32,
}

#[derive(Serialize)]
pub struct FormatResponse {
    format_id: String,
    resolution: String,
    ext: String,
}

#[derive(Deserialize)]
pub struct VideoPrepareRequest {
    url: String,
    #[serde(default = "default_format")]
    #[allow(dead_code)]
    format: String,
    #[serde(default = "default_quality")]
    quality: String,
}

fn default_format() -> String {
    "video".to_string()
}
fn default_quality() -> String {
    "480p".to_string()
}

#[derive(Serialize)]
pub struct VideoPrepareResponse {
    token: String,
}

#[derive(Serialize)]
pub struct VideoErrorResponse {
    error: String,
}

// --- Handlers ---

pub async fn video_info(
    State(state): State<AppState>,
    Json(req): Json<VideoInfoRequest>,
) -> Result<Json<VideoInfoResponse>, (StatusCode, Json<VideoErrorResponse>)> {
    let url = req.url.trim().to_string();

    if url.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(VideoErrorResponse {
                error: "URL is required".to_string(),
            }),
        ));
    }

    let info = cr_infra::video::extract_video_info(&state.http_client, &url)
        .await
        .map_err(|e| {
            tracing::error!("Video extraction failed for {url}: {e}");
            let msg = sanitize_error(&e.to_string());
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(VideoErrorResponse { error: msg }),
            )
        })?;

    let thumbnail = info.thumbnail.map(|t| {
        if t.contains("cdninstagram.com") || t.contains("fbcdn.net") {
            format!("/api/video/thumb?url={}", urlencoding::encode(&t))
        } else {
            t
        }
    });

    let whatsapp_parts = info
        .duration
        .map(cr_infra::video::estimate_whatsapp_parts)
        .unwrap_or(1);

    Ok(Json(VideoInfoResponse {
        title: info.title,
        thumbnail,
        duration: info.duration,
        uploader: info.uploader,
        formats: info
            .formats
            .iter()
            .map(|f| FormatResponse {
                format_id: f.format_id.clone(),
                resolution: f.resolution.clone(),
                ext: f.ext.clone(),
            })
            .collect(),
        whatsapp_parts,
    }))
}

pub async fn video_prepare(
    State(state): State<AppState>,
    Json(req): Json<VideoPrepareRequest>,
) -> Result<Json<VideoPrepareResponse>, (StatusCode, Json<VideoErrorResponse>)> {
    let url = req.url.trim().to_string();

    // Extract video info to get download URL
    let info = cr_infra::video::extract_video_info(&state.http_client, &url)
        .await
        .map_err(|e| {
            tracing::error!("Video extraction failed for {url}: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(VideoErrorResponse {
                    error: format!("Nepodařilo se získat info o videu: {e}"),
                }),
            )
        })?;

    let is_whatsapp = req.quality == "whatsapp";

    // Find the requested quality
    let quality = &req.quality;
    let format = if is_whatsapp {
        // For WhatsApp, pick 480p or lower to keep file small and conversion fast
        info.formats
            .iter()
            .filter(|f| {
                f.resolution
                    .trim_end_matches('p')
                    .parse::<u32>()
                    .unwrap_or(0)
                    <= 480
            })
            .max_by_key(|f| {
                f.resolution
                    .trim_end_matches('p')
                    .parse::<u32>()
                    .unwrap_or(0)
            })
            .or(info.formats.last())
    } else {
        info.formats
            .iter()
            .find(|f| &f.format_id == quality)
            .or_else(|| info.formats.iter().find(|f| f.format_id == "480p"))
            .or(info.formats.last())
    }
    .ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(VideoErrorResponse {
                error: "Žádné formáty k dispozici".to_string(),
            }),
        )
    })?;

    // Create temp directory if it doesn't exist
    let tmp_dir = std::path::PathBuf::from("/tmp/cr-videos");
    tokio::fs::create_dir_all(&tmp_dir).await.map_err(|e| {
        tracing::error!("Failed to create tmp dir: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(VideoErrorResponse {
                error: "Server error".to_string(),
            }),
        )
    })?;

    // Generate token and file path
    let token = uuid::Uuid::new_v4().to_string();
    let decoded_title = info
        .title
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'");
    let safe_title: String = decoded_title
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '-')
        .take(60)
        .collect();
    let filename = format!("{safe_title}.{}", format.ext);
    let file_path = tmp_dir.join(format!("{token}.{}", format.ext));

    // Store task as "downloading" with shared progress counter
    let progress = Arc::new(AtomicU8::new(0));
    state.video_downloads.lock().await.insert(
        token.clone(),
        VideoTask {
            status: DownloadStatus::Downloading {
                progress_percent: 0,
            },
            progress: progress.clone(),
            file_path: file_path.clone(),
            filename,
            parts: Vec::new(),
            created_at: std::time::Instant::now(),
        },
    );

    // Spawn download in background — returns immediately
    let dl_token = token.clone();
    let dl_url = url.clone();
    let dl_format_id = format.format_id.clone();
    let dl_resolution = format.resolution.clone();
    let dl_state = state.clone();
    let dl_client = state.http_client.clone();

    // Check concurrency limit before spawning
    let permit = VIDEO_DOWNLOAD_SEMAPHORE.try_acquire();
    if permit.is_err() {
        if let Some(t) = state.video_downloads.lock().await.get_mut(&token) {
            t.status = DownloadStatus::Failed {
                error: "Příliš mnoho souběžných stahování. Zkuste to za chvíli.".to_string(),
            };
        }
        return Ok(Json(VideoPrepareResponse { token }));
    }

    tokio::spawn(async move {
        let _permit = permit.unwrap();

        let result = cr_infra::video::download_video_with_progress(
            &dl_client,
            &dl_url,
            &dl_format_id,
            &dl_resolution,
            &file_path,
            Some(progress.clone()),
        )
        .await;

        let mut downloads = dl_state.video_downloads.lock().await;
        if let Some(task) = downloads.get_mut(&dl_token) {
            match result {
                Ok(_size) if is_whatsapp => {
                    // WhatsApp: convert with ffmpeg
                    let safe: String = task
                        .filename
                        .trim_end_matches(".mp4")
                        .trim_end_matches(".webm")
                        .chars()
                        .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '-')
                        .take(50)
                        .collect();
                    task.status = DownloadStatus::Converting {
                        progress_percent: 0,
                    };
                    drop(downloads); // release lock during conversion

                    let wa_result = cr_infra::video::convert_for_whatsapp(
                        &file_path,
                        &std::path::PathBuf::from("/tmp/cr-videos"),
                        &dl_token,
                        Some(progress),
                    )
                    .await;

                    // Clean up source file
                    let _ = tokio::fs::remove_file(&file_path).await;

                    let mut downloads = dl_state.video_downloads.lock().await;
                    if let Some(task) = downloads.get_mut(&dl_token) {
                        match wa_result {
                            Ok(cr_infra::video::WhatsAppResult::Single { path, size }) => {
                                let size_mb = size as f64 / (1024.0 * 1024.0);
                                let fname = format!("{safe} (WhatsApp).mp4");
                                task.file_path = path;
                                task.filename = fname.clone();
                                task.status = DownloadStatus::Ready {
                                    size_mb: (size_mb * 10.0).round() / 10.0,
                                    filename: fname,
                                };
                                tracing::info!(
                                    "WhatsApp video ready: {dl_token} ({size_mb:.1} MB)"
                                );
                            }
                            Ok(cr_infra::video::WhatsAppResult::Parts(parts)) => {
                                let part_infos: Vec<PartInfo> = parts
                                    .iter()
                                    .map(|p| {
                                        let size_mb = p.size as f64 / (1024.0 * 1024.0);
                                        PartInfo {
                                            index: p.index,
                                            filename: format!(
                                                "{safe} (WhatsApp část {}).mp4",
                                                p.index + 1
                                            ),
                                            size_mb: (size_mb * 10.0).round() / 10.0,
                                            file_path: p.path.clone(),
                                        }
                                    })
                                    .collect();
                                let part_responses: Vec<PartResponse> = part_infos
                                    .iter()
                                    .map(|p| PartResponse {
                                        index: p.index,
                                        filename: p.filename.clone(),
                                        size_mb: p.size_mb,
                                    })
                                    .collect();
                                task.parts = part_infos;
                                task.status = DownloadStatus::ReadyParts {
                                    parts: part_responses,
                                };
                                tracing::info!(
                                    "WhatsApp video ready: {dl_token} ({} parts)",
                                    parts.len()
                                );
                            }
                            Err(e) => {
                                tracing::error!("WhatsApp conversion failed: {e}");
                                task.status = DownloadStatus::Failed {
                                    error: format!("Konverze pro WhatsApp selhala: {e}"),
                                };
                            }
                        }
                    }
                }
                Ok(size) => {
                    let size_mb = size as f64 / (1024.0 * 1024.0);
                    tracing::info!("Video ready: {dl_token} ({size_mb:.1} MB) for {dl_url}");
                    task.status = DownloadStatus::Ready {
                        size_mb: (size_mb * 10.0).round() / 10.0,
                        filename: task.filename.clone(),
                    };
                }
                Err(e) => {
                    tracing::error!("Video download failed for {dl_url}: {e}");
                    task.status = DownloadStatus::Failed {
                        error: format!("Stažení se nezdařilo: {e}"),
                    };
                }
            }
        }
    });

    tracing::info!("Video download started: {token} for {url}");

    Ok(Json(VideoPrepareResponse { token }))
}

pub async fn video_status(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    let downloads = state.video_downloads.lock().await;
    match downloads.get(&token) {
        Some(task) => {
            // For downloading/converting status, read live progress from atomic counter
            let status = match &task.status {
                DownloadStatus::Downloading { .. } => DownloadStatus::Downloading {
                    progress_percent: task.progress.load(Ordering::Relaxed),
                },
                DownloadStatus::Converting { .. } => DownloadStatus::Converting {
                    progress_percent: task.progress.load(Ordering::Relaxed),
                },
                other => other.clone(),
            };
            (
                StatusCode::OK,
                [(header::CACHE_CONTROL, "no-store")],
                Json(status),
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            [(header::CACHE_CONTROL, "no-store")],
            Json(DownloadStatus::Failed {
                error: "Video not found or expired".to_string(),
            }),
        )
            .into_response(),
    }
}

pub async fn video_file(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    let downloads = state.video_downloads.lock().await;
    let task = match downloads.get(&token) {
        Some(t) if matches!(t.status, DownloadStatus::Ready { .. }) => t,
        Some(_) => {
            return (StatusCode::CONFLICT, "Video is still downloading").into_response();
        }
        None => {
            return (StatusCode::NOT_FOUND, "Video not found or expired").into_response();
        }
    };
    let prepared = task;

    let bytes = match tokio::fs::read(&prepared.file_path).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("Failed to read video file: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read file").into_response();
        }
    };

    let filename = &prepared.filename;
    let content_disposition = format!("attachment; filename=\"{filename}\"");

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "video/mp4".to_string()),
            (header::CONTENT_DISPOSITION, content_disposition),
            (header::CONTENT_LENGTH, bytes.len().to_string()),
        ],
        bytes,
    )
        .into_response()
}

pub async fn video_file_part(
    State(state): State<AppState>,
    Path((token, part_index)): Path<(String, usize)>,
) -> impl IntoResponse {
    let downloads = state.video_downloads.lock().await;
    let task = match downloads.get(&token) {
        Some(t) if matches!(t.status, DownloadStatus::ReadyParts { .. }) => t,
        Some(_) => {
            return (StatusCode::CONFLICT, "Video is still processing").into_response();
        }
        None => {
            return (StatusCode::NOT_FOUND, "Video not found or expired").into_response();
        }
    };

    let part = match task.parts.get(part_index) {
        Some(p) => p,
        None => {
            return (StatusCode::NOT_FOUND, "Part not found").into_response();
        }
    };

    let bytes = match tokio::fs::read(&part.file_path).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("Failed to read part file: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read file").into_response();
        }
    };

    let content_disposition = format!("attachment; filename=\"{}\"", part.filename);

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "video/mp4".to_string()),
            (header::CONTENT_DISPOSITION, content_disposition),
            (header::CONTENT_LENGTH, bytes.len().to_string()),
        ],
        bytes,
    )
        .into_response()
}

// --- Recent downloads + Cleanup ---

#[derive(Serialize)]
pub struct RecentFile {
    filename: String,
    size_mb: f64,
    created: String,
}

#[derive(Serialize)]
pub struct CleanupResponse {
    deleted: usize,
    freed_mb: f64,
}

pub async fn video_recent(State(state): State<AppState>) -> Json<Vec<RecentFile>> {
    let downloads = state.video_downloads.lock().await;
    let mut files = Vec::new();

    let tmp_dir = std::path::PathBuf::from("/tmp/cr-videos");
    if let Ok(mut entries) = tokio::fs::read_dir(&tmp_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(meta) = entry.metadata().await {
                let filename = downloads
                    .values()
                    .find(|p| p.file_path == entry.path())
                    .map(|p| p.filename.clone())
                    .unwrap_or_else(|| entry.file_name().to_string_lossy().to_string());

                let size_mb = meta.len() as f64 / (1024.0 * 1024.0);
                let created = meta
                    .created()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| {
                        chrono::DateTime::from_timestamp(d.as_secs() as i64, 0)
                            .map(|dt| dt.format("%H:%M:%S").to_string())
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();

                files.push(RecentFile {
                    filename,
                    size_mb: (size_mb * 10.0).round() / 10.0,
                    created,
                });
            }
        }
    }

    Json(files)
}

pub async fn video_cleanup(State(state): State<AppState>) -> Json<CleanupResponse> {
    let mut downloads = state.video_downloads.lock().await;
    downloads.clear();

    let tmp_dir = std::path::PathBuf::from("/tmp/cr-videos");
    let mut deleted = 0;
    let mut freed: u64 = 0;

    if let Ok(mut entries) = tokio::fs::read_dir(&tmp_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(meta) = entry.metadata().await {
                freed += meta.len();
            }
            if tokio::fs::remove_file(entry.path()).await.is_ok() {
                deleted += 1;
            }
        }
    }

    let freed_mb = freed as f64 / (1024.0 * 1024.0);
    tracing::info!("Cleanup: deleted {deleted} files, freed {freed_mb:.1} MB");

    Json(CleanupResponse {
        deleted,
        freed_mb: (freed_mb * 10.0).round() / 10.0,
    })
}

// --- Thumbnail proxy ---

#[derive(Deserialize)]
pub struct ThumbQuery {
    url: String,
}

/// Allowed CDN domains for thumbnail proxy (prevents SSRF/open-proxy).
const THUMB_ALLOWED_DOMAINS: &[&str] = &["cdninstagram.com", "fbcdn.net", "sdn.cz"];

pub async fn video_thumb(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ThumbQuery>,
) -> impl IntoResponse {
    // Validate URL — only allow known CDN domains (prevent SSRF)
    let is_allowed = THUMB_ALLOWED_DOMAINS.iter().any(|d| query.url.contains(d));
    if !is_allowed || !query.url.starts_with("https://") {
        return (StatusCode::FORBIDDEN, "URL not allowed").into_response();
    }

    let resp = state
        .http_client
        .get(&query.url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Referer", "https://www.instagram.com/")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => {
            let ct = r
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("image/jpeg")
                .to_string();
            match r.bytes().await {
                Ok(bytes) => (StatusCode::OK, [(header::CONTENT_TYPE, ct)], bytes).into_response(),
                Err(_) => (StatusCode::BAD_GATEWAY, "Failed to read upstream body").into_response(),
            }
        }
        _ => (StatusCode::NOT_FOUND, "Thumbnail not available").into_response(),
    }
}

/// Sanitize yt-dlp error messages into user-friendly Czech text.
fn sanitize_error(raw: &str) -> String {
    if raw.contains("Sign in to confirm")
        || raw.contains("not a bot")
        || raw.contains("login required")
        || raw.contains("rate-limit reached")
    {
        return "Tento server vyžaduje přihlášení a momentálně není podporován. Zkuste jiný odkaz."
            .to_string();
    }
    if raw.contains("Unsupported URL") {
        return "Nepodporovaná URL — tento server zatím neumíme zpracovat.".to_string();
    }
    if raw.contains("Video unavailable") || raw.contains("not available") {
        return "Video není dostupné — mohlo být smazáno nebo je omezené.".to_string();
    }
    if raw.contains("Private video") {
        return "Toto video je soukromé a nelze ho stáhnout.".to_string();
    }
    if raw.contains("No video found") || raw.contains("could not find SDN") {
        return "Na této stránce nebylo nalezeno žádné video.".to_string();
    }
    if raw.contains("Failed to parse video manifest") {
        return "Nepodařilo se načíst video — server nevrátil platná data.".to_string();
    }
    // Generic fallback — don't expose raw yt-dlp output
    "Nepodařilo se získat informace o videu. Zkuste jiný odkaz.".to_string()
}
