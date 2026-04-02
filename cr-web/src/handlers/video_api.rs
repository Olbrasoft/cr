use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::{Json, extract::Path};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::state::AppState;

/// Shared state for tracking prepared video downloads.
pub type VideoDownloads = Arc<Mutex<HashMap<String, PreparedVideo>>>;

pub struct PreparedVideo {
    pub file_path: std::path::PathBuf,
    pub filename: String,
    #[allow(dead_code)]
    pub created_at: std::time::Instant,
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
    size_mb: f64,
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

    Ok(Json(VideoInfoResponse {
        title: info.title,
        thumbnail: info.thumbnail,
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

    // Find the requested quality
    let quality = &req.quality;
    let format = info
        .formats
        .iter()
        .find(|f| &f.format_id == quality)
        .or_else(|| info.formats.iter().find(|f| f.format_id == "480p"))
        .or(info.formats.last())
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
    let safe_title: String = info
        .title
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '-')
        .take(60)
        .collect();
    let filename = format!("{safe_title}.{}", format.ext);
    let file_path = tmp_dir.join(format!("{token}.{}", format.ext));

    // Download the video
    let size =
        cr_infra::video::download_video(&state.http_client, &url, &format.format_id, &file_path)
            .await
            .map_err(|e| {
                tracing::error!("Video download failed: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(VideoErrorResponse {
                        error: format!("Stažení se nezdařilo: {e}"),
                    }),
                )
            })?;

    let size_mb = size as f64 / (1024.0 * 1024.0);

    // Store download info
    state.video_downloads.lock().await.insert(
        token.clone(),
        PreparedVideo {
            file_path,
            filename,
            created_at: std::time::Instant::now(),
        },
    );

    tracing::info!("Video prepared: {token} ({size_mb:.1} MB) for {url}");

    Ok(Json(VideoPrepareResponse {
        token,
        size_mb: (size_mb * 10.0).round() / 10.0,
    }))
}

pub async fn video_file(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    let downloads = state.video_downloads.lock().await;
    let prepared = match downloads.get(&token) {
        Some(p) => p,
        None => {
            return (StatusCode::NOT_FOUND, "Video not found or expired").into_response();
        }
    };

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
