//! Video library upload pipeline.
//!
//! Glues the Streamtape client (#317), the R2 client (#318) and the
//! `PgVideoRepository` (#316) together: takes a freshly downloaded local
//! file and the metadata extracted from yt-dlp, uploads everything to the
//! external services and inserts the row into the `videos` table.
//!
//! See parent issue #314 for the architecture diagram.

use std::path::Path;

use bytes::Bytes;
use cr_domain::repository::{NewVideo, VideoRecord, VideoRepository};
use sha2::{Digest, Sha256};

use crate::r2::{R2Client, R2Error};
use crate::repositories::PgVideoRepository;
use crate::streamtape::{StreamtapeClient, StreamtapeError};

/// Metadata captured from yt-dlp / the user's Stáhnout click that the
/// pipeline needs in order to publish a video to the library.
#[derive(Debug, Clone)]
pub struct PublishMetadata {
    pub source_url: String,
    pub title: String,
    pub description: Option<String>,
    pub duration_sec: Option<i32>,
    pub source_extractor: Option<String>,
    pub quality: String,
    pub format_ext: String,
    /// Optional upstream thumbnail URL provided by yt-dlp. The pipeline
    /// downloads it and stores it on R2; if missing or download fails,
    /// the pipeline falls back to Streamtape's `getsplash`.
    pub upstream_thumbnail_url: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("streamtape upload failed: {0}")]
    Streamtape(#[from] StreamtapeError),
    #[error("R2 upload failed: {0}")]
    R2(#[from] R2Error),
    #[error("database insert failed: {0}")]
    Db(#[from] sqlx::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Orchestrator that owns the three external collaborators.
///
/// Cheap to clone — wraps `Arc`-shaped clients internally. The handler
/// builds one of these once at startup (when configs are present) and
/// stashes it on `AppState`.
#[derive(Clone)]
pub struct VideoLibraryPipeline {
    streamtape: StreamtapeClient,
    r2: R2Client,
    repo: std::sync::Arc<PgVideoRepository>,
    http: reqwest::Client,
}

impl VideoLibraryPipeline {
    pub fn new(
        streamtape: StreamtapeClient,
        r2: R2Client,
        repo: std::sync::Arc<PgVideoRepository>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            streamtape,
            r2,
            repo,
            http,
        }
    }

    /// Dedup check used by the handler before invoking yt-dlp at all.
    /// Returns `Some(record)` if `(source_url, quality)` already exists.
    pub async fn find_existing(
        &self,
        source_url: &str,
        quality: &str,
    ) -> Result<Option<VideoRecord>, sqlx::Error> {
        self.repo
            .find_by_source_and_quality(source_url, quality)
            .await
    }

    /// Borrowed view of the underlying Streamtape client — used by the
    /// library API endpoints to resolve playback URLs and to delete files
    /// when removing a library entry.
    pub fn streamtape_client(&self) -> &StreamtapeClient {
        &self.streamtape
    }

    /// Borrowed view of the underlying R2 client — used by the library
    /// delete endpoint to remove the orphaned thumbnail object.
    pub fn r2_client(&self) -> &R2Client {
        &self.r2
    }

    /// Publish a freshly downloaded local file to the library: upload to
    /// Streamtape, upload thumbnail to R2 (with `getsplash` fallback),
    /// insert into the `videos` table, and remove the local copy.
    pub async fn publish_local_video(
        &self,
        local_path: &Path,
        meta: PublishMetadata,
    ) -> Result<VideoRecord, PublishError> {
        let display_name = format!("{}.{}", sanitize_for_filename(&meta.title), meta.format_ext);

        // 1) Upload the actual video to Streamtape (the slow step).
        let uploaded = self
            .streamtape
            .upload(local_path, &display_name)
            .await
            .inspect_err(|e| tracing::error!("streamtape upload failed: {e}"))?;
        tracing::info!(
            "Streamtape upload OK: file_id={} size={}MB",
            uploaded.file_id,
            uploaded.size_bytes / (1024 * 1024)
        );

        // 2) Try to fetch + upload a thumbnail. Failure here is non-fatal:
        //    we still want the video in the library, just without a thumb.
        let (thumb_key, thumb_url) = match self
            .resolve_and_upload_thumbnail(&uploaded.file_id, meta.upstream_thumbnail_url.as_deref())
            .await
        {
            Ok((key, url)) => (Some(key), Some(url)),
            Err(e) => {
                tracing::warn!(
                    "thumbnail upload failed for {}: {e} — continuing without thumbnail",
                    uploaded.file_id
                );
                (None, None)
            }
        };

        // 3) Insert into DB.
        let new = NewVideo {
            source_url: meta.source_url,
            title: meta.title,
            description: meta.description,
            duration_sec: meta.duration_sec,
            source_extractor: meta.source_extractor,
            quality: meta.quality,
            format_ext: meta.format_ext,
            streamtape_file_id: uploaded.file_id.clone(),
            streamtape_url: uploaded.url,
            file_size_bytes: uploaded.size_bytes as i64,
            thumbnail_r2_key: thumb_key,
            thumbnail_url: thumb_url,
        };
        let id = self.repo.insert(new).await?;
        let record = self
            .repo
            .find_by_id(id)
            .await?
            .ok_or(sqlx::Error::RowNotFound)?;

        // 4) Best-effort cleanup of the temp file. Don't fail the publish
        //    if removal hits a permission error or the file is already gone.
        if let Err(e) = tokio::fs::remove_file(local_path).await {
            tracing::warn!("could not remove temp file {local_path:?}: {e}");
        }

        Ok(record)
    }

    async fn resolve_and_upload_thumbnail(
        &self,
        file_id: &str,
        upstream_url: Option<&str>,
    ) -> Result<(String, String), PublishError> {
        // Try the upstream (yt-dlp) thumbnail first; fall back to Streamtape
        // getsplash. Either way we re-host on R2 so the public URL is on our
        // domain and unaffected by upstream link rot.
        let bytes = if let Some(url) = upstream_url {
            match download_image(&self.http, url).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("upstream thumbnail fetch failed ({url}): {e}");
                    let splash = self.streamtape.get_splash_url(file_id).await?;
                    download_image(&self.http, &splash).await?
                }
            }
        } else {
            let splash = self.streamtape.get_splash_url(file_id).await?;
            download_image(&self.http, &splash).await?
        };

        let key = format!("videos/thumbs/{}.jpg", thumbnail_key_hash(file_id, &bytes));
        let url = self.r2.upload_thumbnail(&key, bytes, "image/jpeg").await?;
        Ok((key, url))
    }
}

async fn download_image(http: &reqwest::Client, url: &str) -> Result<Bytes, PublishError> {
    let resp = http
        .get(url)
        .header("User-Agent", "Mozilla/5.0 (cr-web video library)")
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|e| PublishError::Streamtape(StreamtapeError::Network(e)))?
        .error_for_status()
        .map_err(|e| PublishError::Streamtape(StreamtapeError::Network(e)))?;
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| PublishError::Streamtape(StreamtapeError::Network(e)))?;
    Ok(bytes)
}

/// SHA-256 prefix of `file_id || bytes` — short, stable, content-addressed.
fn thumbnail_key_hash(file_id: &str, bytes: &Bytes) -> String {
    let mut h = Sha256::new();
    h.update(file_id.as_bytes());
    h.update(bytes);
    let digest = h.finalize();
    let mut hex = String::with_capacity(16);
    for b in &digest[..8] {
        use std::fmt::Write;
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

/// Filesystem-friendly name derived from a video title; the upload
/// endpoint uses this as the user-visible filename on Streamtape.
fn sanitize_for_filename(title: &str) -> String {
    title
        .chars()
        .map(|c| {
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
        .take(80)
        .collect()
}
