//! Video publishing service — orchestrates Streamtape upload + R2
//! thumbnail upload + DB record creation for the hosted video library.
//!
//! Before #445, this orchestration lived in `cr-infra::video_library`
//! which directly composed `StreamtapeClient`, `R2Client`, and
//! `PgVideoRepository`. That works at runtime but makes unit testing
//! impossible (all three are concrete types with real network I/O).
//!
//! This service takes trait-object dependencies so tests can inject
//! in-memory fakes. The handler (`video_prepare`) delegates here
//! instead of driving the pipeline itself.
//!
//! ## Migration plan
//!
//! 1. (this commit) Create the service skeleton with the public API.
//! 2. (follow-up) Move the upload + dedup logic from
//!    `cr-infra::video_library::VideoLibraryPipeline::publish` into
//!    `VideoPublishingService::publish`.
//! 3. (follow-up) Add a test using in-memory fakes.

use std::sync::Arc;

use cr_domain::repository::{NewVideo, VideoRecord, VideoRepository};

use crate::error::AppError;

/// Orchestrates the "download → upload → persist" flow for the hosted
/// video library.
pub struct VideoPublishingService<R: VideoRepository> {
    repo: Arc<R>,
}

impl<R: VideoRepository> VideoPublishingService<R>
where
    R::Error: std::fmt::Display,
{
    pub fn new(repo: Arc<R>) -> Self {
        Self { repo }
    }

    /// Check if a video from `source_url` at `quality`+`format_ext`
    /// already exists. Returns the existing record if found so the
    /// handler can skip the upload and serve the cached entry directly.
    pub async fn find_existing(
        &self,
        source_url: &str,
        quality: &str,
        format_ext: &str,
    ) -> Result<Option<VideoRecord>, AppError> {
        self.repo
            .find_by_source_quality_and_format(source_url, quality, format_ext)
            .await
            .map_err(|e| AppError::Repository(format!("{e}")))
    }

    /// Persist a freshly uploaded video. Called after the Streamtape
    /// upload + R2 thumbnail upload have both succeeded.
    pub async fn record_upload(&self, video: NewVideo) -> Result<i32, AppError> {
        self.repo
            .insert(video)
            .await
            .map_err(|e| AppError::Repository(format!("{e}")))
    }

    /// Bump `last_accessed_at` so the card slides to the top of the
    /// library grid when the same video is requested again.
    pub async fn touch(&self, id: i32) -> Result<(), AppError> {
        self.repo
            .touch(id)
            .await
            .map_err(|e| AppError::Repository(format!("{e}")))
    }
}
