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

/// On-disk directory where yt-dlp drops freshly downloaded videos before
/// they are either served to the user via `/api/video/file/{token}` or
/// published to the library pipeline.
pub(crate) const TMP_VIDEO_DIR: &str = "/tmp/cr-videos";

/// Maximum age a temp video may sit on disk before the periodic reaper
/// removes it. See issue #192 — the VPS has limited disk and we can't
/// keep videos around forever after the user has finished downloading
/// them (the library copy on Streamtape + R2 is the canonical long-term
/// store).
pub(crate) const TMP_VIDEO_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// How often the periodic reaper wakes up to scan the temp dir.
pub(crate) const TMP_VIDEO_CLEANUP_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);

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
    /// Set for tasks that hit the library dedup path — the client's
    /// ready-link delegates to `/api/video/library/{id}/file` via a
    /// 303 See Other from `video_file` (`Redirect::to(...)`) because
    /// there is no local temp file for deduped downloads (the content
    /// lives only on Streamtape/R2). `None` for normal downloads
    /// where `file_path` carries the bytes.
    pub library_id: Option<i32>,
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

/// Map a file extension to a Content-Type mime string, defaulting to
/// `video/mp4` for anything we don't recognise so browsers at least
/// treat the response as some kind of video.
fn content_type_for_filename(name: &str) -> &'static str {
    let ext = std::path::Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("webm") => "video/webm",
        Some("mkv") => "video/x-matroska",
        _ => "video/mp4",
    }
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

// --- Periodic temp video cleanup (#192) ---

/// Scan `dir` once and delete any regular file whose last-modified
/// timestamp is older than `max_age`. Returns `(deleted_count,
/// bytes_freed)`.
///
/// Errors (failing to open the dir, failing to read an entry, failing
/// to delete a single file) are logged and skipped — the reaper runs
/// every few minutes so any transient issue will be retried on the
/// next tick.
pub(crate) async fn purge_stale_temp_videos(
    dir: &std::path::Path,
    max_age: std::time::Duration,
) -> (usize, u64) {
    let mut deleted = 0usize;
    let mut freed = 0u64;

    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (0, 0),
        Err(e) => {
            tracing::warn!("temp cleanup: cannot open {dir:?}: {e}");
            return (0, 0);
        }
    };

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => {
                tracing::warn!("temp cleanup: read_dir error: {e}");
                break;
            }
        };
        let meta = match entry.metadata().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("temp cleanup: stat {:?}: {e}", entry.path());
                continue;
            }
        };
        if !meta.is_file() {
            continue;
        }
        let age = meta
            .modified()
            .ok()
            .and_then(|m| m.elapsed().ok())
            .unwrap_or_default();
        if age < max_age {
            continue;
        }
        let path = entry.path();
        let size = meta.len();
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                deleted += 1;
                freed += size;
                tracing::debug!(
                    "temp cleanup: removed {path:?} (age={}s, size={} bytes)",
                    age.as_secs(),
                    size
                );
            }
            Err(e) => {
                tracing::warn!("temp cleanup: remove {path:?}: {e}");
            }
        }
    }

    (deleted, freed)
}

/// Prune in-memory `VideoDownloads` entries whose `created_at` is older
/// than `max_age`. Returns the number of tokens removed.
///
/// Runs alongside the on-disk reaper so that once a temp file is
/// deleted, the matching `/api/video/status/{token}` entry goes with
/// it — otherwise the handler would keep reporting `Ready` while
/// `/api/video/file/{token}` returns 500 for a missing file.
pub(crate) async fn prune_stale_video_downloads(
    downloads: &VideoDownloads,
    max_age: std::time::Duration,
) -> usize {
    let mut map = downloads.lock().await;
    let before = map.len();
    map.retain(|_, task| task.created_at.elapsed() < max_age);
    before - map.len()
}

/// Spawn the long-running periodic reaper. Call once at startup from
/// `main.rs`; the returned handle is detached (the task ends only when
/// the process exits).
///
/// Every `TMP_VIDEO_CLEANUP_INTERVAL` it scans [`TMP_VIDEO_DIR`] and
/// deletes any file older than `TMP_VIDEO_MAX_AGE`, then prunes the
/// corresponding in-memory `VideoDownloads` entries so the `status`
/// endpoint stops reporting `Ready` for tokens whose file is gone.
///
/// The first tick fires on the interval boundary — not immediately on
/// startup — which deliberately gives in-flight downloads time to
/// complete before the reaper runs the first time. The ticker uses
/// `MissedTickBehavior::Skip` so a slow sweep (lots of files / slow
/// I/O) can't trigger a back-to-back burst of catch-up sweeps.
pub fn spawn_temp_video_cleanup_loop(downloads: VideoDownloads) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let dir = std::path::PathBuf::from(TMP_VIDEO_DIR);
        let mut ticker = tokio::time::interval(TMP_VIDEO_CLEANUP_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate tick `interval` fires at t=0 — we want the
        // first scan to land `TMP_VIDEO_CLEANUP_INTERVAL` after startup
        // so any download racing the boot doesn't get swept mid-write.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let (deleted_files, freed) = purge_stale_temp_videos(&dir, TMP_VIDEO_MAX_AGE).await;
            let pruned_tokens = prune_stale_video_downloads(&downloads, TMP_VIDEO_MAX_AGE).await;
            if deleted_files > 0 || pruned_tokens > 0 {
                let freed_mb = freed as f64 / (1024.0 * 1024.0);
                tracing::info!(
                    "periodic temp cleanup: deleted {deleted_files} files ({freed_mb:.1} MB), pruned {pruned_tokens} stale tokens — age threshold {}m",
                    TMP_VIDEO_MAX_AGE.as_secs() / 60
                );
            }
        }
    })
}

#[cfg(test)]
mod temp_cleanup_tests {
    use super::purge_stale_temp_videos;
    use std::time::Duration;

    /// Create a file in `dir` whose mtime is `age_secs` in the past and
    /// whose body is `size` bytes of zeros.
    async fn write_aged_file(dir: &std::path::Path, name: &str, size: usize, age_secs: u64) {
        let path = dir.join(name);
        tokio::fs::write(&path, vec![0u8; size]).await.unwrap();
        // Back-date the mtime so the reaper thinks the file is stale.
        let past = std::time::SystemTime::now() - Duration::from_secs(age_secs);
        let ft = filetime::FileTime::from_system_time(past);
        filetime::set_file_mtime(&path, ft).unwrap();
    }

    #[tokio::test]
    async fn deletes_only_files_older_than_max_age() {
        let tmp = tempfile::tempdir().unwrap();
        // 2 stale files, 1 fresh file.
        write_aged_file(tmp.path(), "old1.mp4", 1024, 3600).await;
        write_aged_file(tmp.path(), "old2.mp4", 2048, 3600).await;
        write_aged_file(tmp.path(), "fresh.mp4", 512, 10).await;

        let (deleted, freed) =
            purge_stale_temp_videos(tmp.path(), Duration::from_secs(30 * 60)).await;

        assert_eq!(deleted, 2, "should delete the two old files");
        assert_eq!(freed, 1024 + 2048, "should free the exact byte count");
        assert!(
            tmp.path().join("fresh.mp4").exists(),
            "fresh file must survive"
        );
        assert!(!tmp.path().join("old1.mp4").exists());
        assert!(!tmp.path().join("old2.mp4").exists());
    }

    #[tokio::test]
    async fn missing_dir_is_a_no_op() {
        let missing = std::path::PathBuf::from("/tmp/cr-videos-periodic-cleanup-does-not-exist");
        let (deleted, freed) =
            purge_stale_temp_videos(&missing, Duration::from_secs(30 * 60)).await;
        assert_eq!(deleted, 0);
        assert_eq!(freed, 0);
    }

    #[tokio::test]
    async fn empty_dir_deletes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let (deleted, freed) =
            purge_stale_temp_videos(tmp.path(), Duration::from_secs(30 * 60)).await;
        assert_eq!(deleted, 0);
        assert_eq!(freed, 0);
    }

    #[tokio::test]
    async fn skips_subdirectories() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::create_dir(tmp.path().join("subdir"))
            .await
            .unwrap();
        write_aged_file(tmp.path(), "old.mp4", 100, 3600).await;

        let (deleted, _) = purge_stale_temp_videos(tmp.path(), Duration::from_secs(30 * 60)).await;
        assert_eq!(deleted, 1, "only the file should be deleted, not the dir");
        assert!(tmp.path().join("subdir").exists());
    }

    #[tokio::test]
    async fn prunes_video_downloads_older_than_max_age() {
        use super::{VideoDownloads, VideoTask, prune_stale_video_downloads};
        use std::sync::{
            Arc,
            atomic::{AtomicU8, Ordering},
        };

        let downloads: VideoDownloads =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));

        fn task_with_age(age: Duration) -> VideoTask {
            VideoTask {
                status: super::DownloadStatus::Ready {
                    size_mb: 1.0,
                    filename: "x.mp4".to_string(),
                },
                progress: Arc::new(AtomicU8::new(100)),
                file_path: std::path::PathBuf::new(),
                filename: "x.mp4".to_string(),
                parts: Vec::new(),
                // Back-date created_at by subtracting from Instant::now().
                created_at: std::time::Instant::now() - age,
                library_id: None,
            }
        }

        {
            let mut map = downloads.lock().await;
            map.insert("stale1".into(), task_with_age(Duration::from_secs(3600)));
            map.insert("stale2".into(), task_with_age(Duration::from_secs(3600)));
            map.insert("fresh".into(), task_with_age(Duration::from_secs(10)));
        }

        let pruned = prune_stale_video_downloads(&downloads, Duration::from_secs(30 * 60)).await;
        assert_eq!(pruned, 2, "both stale tokens should be pruned");

        let map = downloads.lock().await;
        assert!(map.contains_key("fresh"), "fresh token must survive");
        assert!(!map.contains_key("stale1"));
        assert!(!map.contains_key("stale2"));
        // sanity: progress atomic is unused in this test but Clippy might
        // complain about it being dead if we drop it silently.
        assert_eq!(
            map["fresh"].progress.load(Ordering::Relaxed),
            100,
            "progress atomic survives unchanged"
        );
    }
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

// --- #321 Video library API ---

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

#[derive(Serialize)]
pub struct LibraryPlayResponse {
    stream_url: String,
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

/// ASCII-only filename sanitiser used as the `filename="…"` fallback in
/// `Content-Disposition` headers (and as the on-disk filename for the
/// local-download flow).
///
/// Allowlist: ASCII alphanumerics + space + dash + underscore. Whitespace
/// is collapsed, the result is truncated to `max` characters, and an
/// empty result falls back to `"video"` (so emoji-only / Cyrillic-only /
/// CJK-only titles never produce a nameless `.mp4`).
fn sanitize_filename_ascii(input: &str, max: usize) -> String {
    let cleaned: String = input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == ' ' || c == '-' || c == '_' {
                c
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(max)
        .collect();
    if cleaned.is_empty() {
        "video".to_string()
    } else {
        cleaned
    }
}

/// Unicode-friendly filename sanitiser used as the `filename*=UTF-8''…`
/// value in `Content-Disposition` headers — keeps Czech, Cyrillic, CJK
/// and any other letters/digits intact while still stripping path
/// separators, control characters, quotes and the like.
///
/// Browsers prefer `filename*` over the ASCII `filename` fallback, so a
/// Czech title like `I když se vše vyřeší, KRIZE ZŮSTANE!` ends up
/// saved as `I když se vše vyřeší KRIZE ZŮSTANE.mp4` instead of the
/// mangled ASCII transliteration.
fn sanitize_filename_unicode(input: &str, max: usize) -> String {
    let cleaned: String = input
        .chars()
        .map(|c| {
            // Keep any letter/number from any script. Replace anything
            // else (punctuation, control chars, separators) with a space
            // and collapse whitespace afterwards.
            if c.is_alphanumeric() || c == ' ' || c == '-' || c == '_' {
                c
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(max)
        .collect();
    if cleaned.is_empty() {
        "video".to_string()
    } else {
        cleaned
    }
}

#[cfg(test)]
mod sanitize_tests {
    use super::{sanitize_filename_ascii, sanitize_filename_unicode};

    #[test]
    fn ascii_title_kept() {
        assert_eq!(sanitize_filename_ascii("Matrix (1999)", 60), "Matrix 1999");
    }

    #[test]
    fn cyrillic_only_falls_back() {
        assert_eq!(sanitize_filename_ascii("Москва", 60), "video");
    }

    #[test]
    fn emoji_only_falls_back() {
        assert_eq!(sanitize_filename_ascii("😭😭😭", 60), "video");
    }

    #[test]
    fn collapses_whitespace_and_truncates() {
        let long = "a".repeat(200);
        assert_eq!(sanitize_filename_ascii(&long, 80).len(), 80);
        assert_eq!(
            sanitize_filename_ascii("  hello   world  ", 60),
            "hello world"
        );
    }

    #[test]
    fn unicode_keeps_czech_diacritics() {
        assert_eq!(
            sanitize_filename_unicode("I když se vše vyřeší, KRIZE ZŮSTANE!", 80),
            "I když se vše vyřeší KRIZE ZŮSTANE"
        );
    }

    #[test]
    fn unicode_keeps_cyrillic_and_cjk() {
        assert_eq!(sanitize_filename_unicode("Москва", 80), "Москва");
        assert_eq!(sanitize_filename_unicode("世界", 80), "世界");
    }

    #[test]
    fn unicode_emoji_only_falls_back() {
        assert_eq!(sanitize_filename_unicode("😭😭😭", 80), "video");
    }
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
    if raw.contains("ensure_container")
        || raw.contains("ffmpeg")
        || raw.contains("full re-encode failed")
    {
        return "Požadovaný formát není dostupný a konverze se nezdařila — zkuste jiný formát."
            .to_string();
    }
    // Generic fallback — don't expose raw yt-dlp output
    "Nepodařilo se získat informace o videu. Zkuste jiný odkaz.".to_string()
}
