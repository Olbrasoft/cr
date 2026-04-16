//! yt-dlp download flow: info, prepare, status, file serving, recent, cleanup.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

use super::{
    DownloadStatus, PartInfo, PartResponse, TMP_VIDEO_DIR, VIDEO_DOWNLOAD_SEMAPHORE,
    VideoErrorResponse, VideoTask, content_type_for_filename, sanitize_error,
    sanitize_filename_ascii,
};

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
    /// Output container chosen by the user (#366). Accepted values
    /// are `"mp4"`, `"webm"`, or `"mkv"` (see [`ALLOWED_CONTAINERS`]);
    /// anything else is rejected as 400. The ready-link and filename
    /// always carry this container, regardless of what yt-dlp picked
    /// internally. The library row is always published as `"mp4"`
    /// because Streamtape re-encodes every upload.
    #[serde(default = "default_container")]
    container: String,
}

fn default_format() -> String {
    "video".to_string()
}
fn default_quality() -> String {
    "480p".to_string()
}
fn default_container() -> String {
    "mp4".to_string()
}

/// User-pickable containers on `/stahnout-video/` — all other values
/// are rejected at the handler boundary to keep the file-path and
/// ffmpeg codepaths bounded to shapes we have actually tested.
///
/// - `mp4`  — the universal default. Plays in every browser, every
///   editor, every chat app. H.264/AAC interior.
/// - `webm` — open-source stack for web embedding. VP9/Opus interior.
/// - `mkv`  — Matroska Swiss army knife. Holds practically any codec
///   so the fast `-c copy` remux path works for almost every source
///   and re-encode is rarely needed.
const ALLOWED_CONTAINERS: &[&str] = &["mp4", "webm", "mkv"];

#[derive(Serialize)]
pub struct VideoPrepareResponse {
    token: String,
}

// --- Recent downloads + Cleanup response types ---

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

    // #366 — validate container; reject anything outside the tested set.
    let container = req.container.trim().to_lowercase();
    if !ALLOWED_CONTAINERS.contains(&container.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(VideoErrorResponse {
                error: format!(
                    "Neznámý formát '{container}' — podporujeme pouze {}.",
                    ALLOWED_CONTAINERS.join(", ")
                ),
            }),
        ));
    }

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
    // WhatsApp always produces MP4 (H.264/AAC) via ffmpeg post-processing —
    // the user-picked container doesn't apply to that variant.
    let effective_container: &str = if is_whatsapp { "mp4" } else { &container };

    // --- #320/#366 Library lookup ---
    // Library rows are always MP4 (Streamtape re-encodes every upload
    // to H.264 MP4 regardless of the file we hand it), so the dedup
    // lookup is hard-coded to `format_ext = "mp4"`. Three outcomes:
    //
    //   1. Hit + user asked for MP4: redirect the ready-link at the
    //      library file proxy and skip yt-dlp entirely. `video_file`
    //      303s onto `/api/video/library/{id}/file` which streams
    //      from Streamtape.
    //
    //   2. Hit + user asked for WebM/MKV: we can't serve the stored
    //      row (bytes are MP4, badge would lie), so we go through
    //      the full yt-dlp + ffmpeg transcode path to produce a
    //      fresh temp file in the requested container. The existing
    //      library row is **not** republished — `should_publish`
    //      below is false — and we `touch` it so the card still
    //      slides to the top of the grid.
    //
    //   3. No hit: yt-dlp + ensure_container produce the temp file,
    //      publish fires on success, a new MP4 library row is born.
    //
    // Anything other than a successful hit keeps `should_publish = true`.
    let mut should_publish = true;
    if let Some(pipeline) = state.video_library.as_ref() {
        match pipeline.find_existing(&url, &req.quality, "mp4").await {
            Ok(Some(existing)) => {
                // Bump the card to the top of the grid, regardless of
                // which branch we take next.
                if let Err(e) = pipeline.touch(existing.id).await {
                    tracing::warn!("library touch failed: {e:?} — continuing");
                }
                should_publish = false;

                if effective_container == "mp4" {
                    // Branch 1 — serve directly from the library.
                    let token = uuid::Uuid::new_v4().to_string();
                    let size_mb = existing.file_size_bytes as f64 / (1024.0 * 1024.0);
                    let filename = format!(
                        "{}.{}",
                        sanitize_filename_ascii(&existing.title, 60),
                        existing.format_ext
                    );
                    state.video_downloads.lock().await.insert(
                        token.clone(),
                        VideoTask {
                            status: DownloadStatus::Ready {
                                size_mb: (size_mb * 10.0).round() / 10.0,
                                filename: filename.clone(),
                            },
                            progress: Arc::new(AtomicU8::new(100)),
                            // No local file — `video_file` will 303
                            // the client onto `/api/video/library/{id}/file`.
                            file_path: std::path::PathBuf::new(),
                            filename,
                            parts: Vec::new(),
                            created_at: std::time::Instant::now(),
                            library_id: Some(existing.id),
                        },
                    );
                    tracing::info!(
                        "video library dedup hit: token={token} streamtape_id={}",
                        existing.streamtape_file_id
                    );
                    return Ok(Json(VideoPrepareResponse { token }));
                }
                // Branch 2 — fall through to the fresh yt-dlp path
                // below. `should_publish` is now false so the spawn
                // task will skip the Streamtape re-upload.
                tracing::info!(
                    "library has MP4 for {url} but user asked for {effective_container} — \
                     doing fresh transcode without republish"
                );
            }
            Ok(None) => {}
            Err(e) => tracing::warn!("library dedup lookup failed: {e:?} — proceeding"),
        }
    }

    // Find the requested quality
    let quality = &req.quality;
    let format = if is_whatsapp {
        // #366 — WhatsApp downloads at ≤480p for a fast user-facing
        // turnaround. `convert_for_whatsapp` downscales / re-encodes
        // further as needed to fit the 16 MB WhatsApp limit; each
        // resulting wa file is independently published to the
        // library so users see one card per WhatsApp variant in the
        // "Stažená videa" grid — single-file output gives one card,
        // a 3-way split gives three cards, matching the "treat a
        // WhatsApp download like any other video" rule.
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
    let tmp_dir = std::path::PathBuf::from(TMP_VIDEO_DIR);
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
    // ASCII-only sanitiser — non-ASCII letters confuse HTTP header parsing
    // (Content-Disposition expects an ASCII token; the optional `filename*`
    // form for UTF-8 is not worth the complexity here). Emoji-only titles
    // collapse to an empty string and fall back to "video".
    let safe_title = sanitize_filename_ascii(&decoded_title, 60);
    // #366 — the on-disk file is always `.{container}` regardless of
    // what yt-dlp picks internally. `ensure_container` (infra) will
    // transcode via ffmpeg when needed so the invariant holds.
    let filename = format!("{safe_title}.{effective_container}");
    let file_path = tmp_dir.join(format!("{token}.{effective_container}"));

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
            library_id: None,
        },
    );

    // Spawn download in background — returns immediately
    let dl_token = token.clone();
    let dl_url = url.clone();
    let dl_format_id = format.format_id.clone();
    let dl_resolution = format.resolution.clone();
    let dl_container = effective_container.to_string();
    let dl_state = state.clone();
    let dl_client = state.http_client.clone();
    // Captured for the post-download library publish (#319). The publish
    // happens fire-and-forget after the user-facing Ready status is set,
    // so a slow Streamtape upload never blocks the local download flow.
    //
    // `format_ext` is hard-coded to `"mp4"` because Streamtape
    // re-encodes every upload to H.264 MP4 regardless of what file
    // we hand it (#366). Storing `"webm"` or `"mkv"` would be a lie
    // — the library bytes are always MP4. The user's container
    // choice only affects the local temp file served via the
    // ready-link; the library publish path is container-agnostic.
    //
    // `quality` comes from the picked format, not from the raw
    // request — this matters for WhatsApp, where the request
    // quality is the `"whatsapp"` sentinel but we actually download
    // a full-resolution MP4 (`"1080p"` / `"720p"` / ...) so the user
    // gets both a shareable WhatsApp file and a permanent library
    // card at the real source quality.
    let publish_meta_template = cr_infra::video_library::PublishMetadata {
        source_url: url.clone(),
        title: decoded_title.clone(),
        description: None,
        duration_sec: info.duration.map(|d| d as i32),
        source_extractor: info.uploader.clone(),
        quality: format.format_id.clone(),
        format_ext: "mp4".to_string(),
        // #366 — store the human-readable resolution yt-dlp reported
        // on the picked format (e.g. "720p"). Used by the library
        // card's top-right badge; falls back to hidden when yt-dlp
        // couldn't tell us a resolution for this source.
        resolution: Some(format.resolution.clone()).filter(|s| !s.is_empty()),
        upstream_thumbnail_url: info.thumbnail.clone(),
    };

    // Check concurrency limit before spawning. The semaphore permit must
    // move into the spawned task so it's released when the download
    // finishes; extract it here (the is_err() branch above already bailed,
    // so `ok()` is guaranteed to yield Some and gives us a panic-free path).
    let Ok(permit) = VIDEO_DOWNLOAD_SEMAPHORE.try_acquire() else {
        if let Some(t) = state.video_downloads.lock().await.get_mut(&token) {
            t.status = DownloadStatus::Failed {
                error: "Příliš mnoho souběžných stahování. Zkuste to za chvíli.".to_string(),
            };
        }
        return Ok(Json(VideoPrepareResponse { token }));
    };

    tokio::spawn(async move {
        let _permit = permit;

        let result = cr_infra::video::download_video_with_progress(
            &dl_client,
            &dl_url,
            &dl_format_id,
            &dl_resolution,
            &dl_container,
            &file_path,
            Some(progress.clone()),
        )
        .await;

        let mut downloads = dl_state.video_downloads.lock().await;
        if let Some(task) = downloads.get_mut(&dl_token) {
            match result {
                Ok(_size) if is_whatsapp => {
                    // WhatsApp: convert with ffmpeg. ASCII-only sanitiser to
                    // keep the resulting filename safe in HTTP headers.
                    let stem = task
                        .filename
                        .trim_end_matches(".mp4")
                        .trim_end_matches(".webm");
                    let safe = sanitize_filename_ascii(stem, 50);
                    task.status = DownloadStatus::Converting {
                        progress_percent: 0,
                    };
                    drop(downloads); // release lock during conversion

                    let wa_result = cr_infra::video::convert_for_whatsapp(
                        &file_path,
                        &std::path::PathBuf::from(TMP_VIDEO_DIR),
                        &dl_token,
                        Some(progress),
                    )
                    .await;

                    let mut downloads = dl_state.video_downloads.lock().await;
                    if let Some(task) = downloads.get_mut(&dl_token) {
                        match wa_result {
                            Ok(cr_infra::video::WhatsAppResult::Single { path, size }) => {
                                let size_mb = size as f64 / (1024.0 * 1024.0);
                                let fname = format!("{safe} (WhatsApp).mp4");
                                task.file_path = path.clone();
                                task.filename = fname.clone();
                                task.status = DownloadStatus::Ready {
                                    size_mb: (size_mb * 10.0).round() / 10.0,
                                    filename: fname,
                                };
                                tracing::info!(
                                    "WhatsApp video ready: {dl_token} ({size_mb:.1} MB)"
                                );
                                drop(downloads);

                                // #371 review — the pre-conversion yt-dlp
                                // temp file is no longer referenced by
                                // anything (task.file_path now points at
                                // the wa output). Unlink it immediately
                                // so disk usage drops right away instead
                                // of waiting for the 30-min reaper.
                                if let Err(e) = tokio::fs::remove_file(&file_path).await {
                                    tracing::warn!(
                                        "failed to unlink pre-WhatsApp temp file {:?}: {e}",
                                        file_path
                                    );
                                }

                                // #366 — publish the WhatsApp wa file
                                // to the library as its own row. The
                                // quality is `"whatsapp"` so it
                                // coexists with any regular-download
                                // row for the same URL under a
                                // different dedup key.
                                if should_publish
                                    && let Some(pipeline) = dl_state.video_library.clone()
                                {
                                    let mut meta = publish_meta_template.clone();
                                    meta.quality = "whatsapp".to_string();
                                    meta.resolution = Some("480p".to_string());
                                    let path = path.clone();
                                    tokio::spawn(async move {
                                        match pipeline.publish_local_video(&path, meta).await {
                                            Ok(rec) => tracing::info!(
                                                "whatsapp library publish OK: id={} streamtape_id={}",
                                                rec.id,
                                                rec.streamtape_file_id
                                            ),
                                            Err(e) => tracing::warn!(
                                                "whatsapp library publish failed: {e}"
                                            ),
                                        }
                                    });
                                }
                            }
                            Ok(cr_infra::video::WhatsAppResult::Parts(parts)) => {
                                let part_count = parts.len();
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
                                task.parts = part_infos.clone();
                                task.status = DownloadStatus::ReadyParts {
                                    parts: part_responses,
                                };
                                tracing::info!(
                                    "WhatsApp video ready: {dl_token} ({part_count} parts)"
                                );
                                drop(downloads);

                                // #371 review — the pre-conversion yt-dlp
                                // temp file is no longer referenced by
                                // anything (task.parts now point at the
                                // split wa outputs). Unlink it immediately
                                // so disk usage drops right away instead
                                // of waiting for the 30-min reaper.
                                if let Err(e) = tokio::fs::remove_file(&file_path).await {
                                    tracing::warn!(
                                        "failed to unlink pre-WhatsApp temp file {:?}: {e}",
                                        file_path
                                    );
                                }

                                // #366 — publish each WhatsApp part as
                                // its own library row. Quality is
                                // `"whatsapp-part{i}"` so all N parts
                                // live under distinct dedup keys and
                                // show up as N independent cards in
                                // the grid. Title carries an "X/N"
                                // suffix so the user can distinguish
                                // them at a glance.
                                if should_publish
                                    && let Some(pipeline) = dl_state.video_library.clone()
                                {
                                    let base_title = publish_meta_template.title.clone();
                                    let base_meta = publish_meta_template.clone();
                                    for p in &part_infos {
                                        let mut meta = base_meta.clone();
                                        meta.title = format!(
                                            "{base_title} — WhatsApp část {}/{}",
                                            p.index + 1,
                                            part_count
                                        );
                                        meta.quality = format!("whatsapp-part{}", p.index);
                                        meta.resolution = Some("480p".to_string());
                                        let path = p.file_path.clone();
                                        let pipeline = pipeline.clone();
                                        let idx = p.index;
                                        tokio::spawn(async move {
                                            match pipeline.publish_local_video(&path, meta).await {
                                                Ok(rec) => tracing::info!(
                                                    "whatsapp part{idx} library publish OK: id={} streamtape_id={}",
                                                    rec.id,
                                                    rec.streamtape_file_id
                                                ),
                                                Err(e) => tracing::warn!(
                                                    "whatsapp part{idx} library publish failed: {e}"
                                                ),
                                            }
                                        });
                                    }
                                }
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
                    // #319 — fire-and-forget publish to the library. Local
                    // file is intentionally kept on disk by `publish_local_video`
                    // (see #363) so the user can still download it via
                    // `/api/video/file/{token}` after the upload finishes.
                    // Temp files are reaped by `DELETE /api/video/cleanup`.
                    //
                    // #366 — skip publish entirely when the library
                    // already has a MP4 row for this URL+quality
                    // (`should_publish = false`). That happens when
                    // the user asked for WebM/MKV and we did a fresh
                    // yt-dlp + transcode alongside an existing MP4
                    // row: republishing would either hit the unique
                    // constraint or duplicate work Streamtape just
                    // did. The `touch` call already bumped the row
                    // to the top of the grid in the handler above.
                    if should_publish && let Some(pipeline) = dl_state.video_library.clone() {
                        let publish_path = file_path.clone();
                        let publish_meta = publish_meta_template.clone();
                        tokio::spawn(async move {
                            match pipeline
                                .publish_local_video(&publish_path, publish_meta)
                                .await
                            {
                                Ok(rec) => tracing::info!(
                                    "video library publish OK: id={} streamtape_id={}",
                                    rec.id,
                                    rec.streamtape_file_id
                                ),
                                Err(e) => {
                                    tracing::warn!("video library publish failed: {e} — local-only")
                                }
                            }
                        });
                    }
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
    // Extract what we need from the task and drop the lock before any
    // I/O so a slow filesystem read never blocks other handlers.
    let (file_path, filename, library_id) = {
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
        (
            task.file_path.clone(),
            task.filename.clone(),
            task.library_id,
        )
    };

    // Deduped downloads have no local temp file — point the client at
    // the library proxy instead so Streamtape serves the bytes. Using
    // 303 See Other preserves the GET method and avoids Chrome's POST
    // redirection quirks for any hypothetical future caller that uses
    // a non-GET verb. Browsers follow this transparently for the
    // `download` attribute on the anchor that triggered it.
    if let Some(id) = library_id {
        return axum::response::Redirect::to(&format!("/api/video/library/{id}/file"))
            .into_response();
    }

    let bytes = match tokio::fs::read(&file_path).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("Failed to read video file: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read file").into_response();
        }
    };

    let content_disposition = format!("attachment; filename=\"{filename}\"");
    // #366 — derive Content-Type from the filename extension so a
    // WebM/MKV download doesn't advertise itself as `video/mp4`.
    let content_type = content_type_for_filename(&filename).to_string();

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
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

pub async fn video_recent(State(state): State<AppState>) -> Json<Vec<RecentFile>> {
    let downloads = state.video_downloads.lock().await;
    let mut files = Vec::new();

    let tmp_dir = std::path::PathBuf::from(TMP_VIDEO_DIR);
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

    let tmp_dir = std::path::PathBuf::from(TMP_VIDEO_DIR);
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
