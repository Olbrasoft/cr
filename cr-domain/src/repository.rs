//! Repository trait definitions (ports).
//!
//! These traits define the contract between the application layer and
//! infrastructure layer. Implementations live in `cr-infra`.
//!
//! The row-shaped DTOs each trait returns (`RegionRecord`, `OrpRecord`,
//! …) live in `cr-domain::dto` — split out of this module in #446 so the
//! trait file stays short and focused on ports.

use crate::id::*;

// Back-compat re-exports so existing `cr_domain::repository::{RegionRecord, …}`
// import paths keep working after the DTO split (#446).
pub use crate::dto::*;

/// Repository for region queries.
#[allow(async_fn_in_trait)]
pub trait RegionRepository {
    type Error: std::fmt::Debug;
    async fn find_all(&self) -> Result<Vec<RegionRecord>, Self::Error>;
    async fn find_by_slug(&self, slug: &str) -> Result<Option<RegionRecord>, Self::Error>;
}

/// Repository for ORP queries.
#[allow(async_fn_in_trait)]
pub trait OrpRepository {
    type Error: std::fmt::Debug;
    async fn find_by_slug(&self, slug: &str) -> Result<Option<OrpRecord>, Self::Error>;
    async fn find_by_region(&self, region_id: RegionId) -> Result<Vec<OrpRecord>, Self::Error>;
    async fn exists_by_slug(&self, slug: &str) -> Result<bool, Self::Error>;
    /// Find the region slug for an ORP by its slug (joins through districts).
    async fn region_slug_for_orp(&self, orp_slug: &str) -> Result<Option<String>, Self::Error>;
}

/// Repository for municipality queries.
#[allow(async_fn_in_trait)]
pub trait MunicipalityRepository {
    type Error: std::fmt::Debug;
    async fn find_by_slug_and_orp(
        &self,
        slug: &str,
        orp_id: OrpId,
    ) -> Result<Option<MunicipalityRecord>, Self::Error>;
    async fn find_by_orp(&self, orp_id: OrpId) -> Result<Vec<MunicipalityRecord>, Self::Error>;
}

/// Repository for landmark queries.
#[allow(async_fn_in_trait)]
pub trait LandmarkRepository {
    type Error: std::fmt::Debug;
    async fn find_by_slug_and_orp(
        &self,
        slug: &str,
        orp_id: OrpId,
    ) -> Result<Option<LandmarkRecord>, Self::Error>;
    async fn find_by_orp(&self, orp_id: OrpId) -> Result<Vec<LandmarkSummary>, Self::Error>;
    async fn count_by_type(&self, type_slug: &str) -> Result<i64, Self::Error>;
}

/// Repository for pool queries.
#[allow(async_fn_in_trait)]
pub trait PoolRepository {
    type Error: std::fmt::Debug;
    async fn find_by_slug_and_orp(
        &self,
        slug: &str,
        orp_id: OrpId,
    ) -> Result<Option<PoolRecord>, Self::Error>;
    async fn find_by_orp(&self, orp_id: OrpId) -> Result<Vec<PoolSummary>, Self::Error>;
}

/// Repository for photo metadata queries.
#[allow(async_fn_in_trait)]
pub trait PhotoRepository {
    type Error: std::fmt::Debug;
    async fn find_by_entity(
        &self,
        entity_type: &str,
        entity_id: i32,
    ) -> Result<Vec<PhotoRecord>, Self::Error>;
}

/// Repository for the hosted video library (`videos` table).
#[allow(async_fn_in_trait)]
pub trait VideoRepository {
    type Error: std::fmt::Debug;
    /// Insert a new video and return the id assigned by the DB.
    async fn insert(&self, video: NewVideo) -> Result<i32, Self::Error>;
    /// Look up an existing library entry for the dedup key
    /// `(source_url, quality, format_ext)`. Returns `None` if no row
    /// matches. In practice (#366) library rows are always MP4 because
    /// Streamtape re-encodes uploads to H.264 MP4 regardless of the
    /// file we hand it, so the `format_ext` parameter is effectively
    /// always `"mp4"` from the handler — we keep it parameterised
    /// anyway so the trait stays symmetric with `NewVideo.format_ext`.
    async fn find_by_source_quality_and_format(
        &self,
        source_url: &str,
        quality: &str,
        format_ext: &str,
    ) -> Result<Option<VideoRecord>, Self::Error>;
    /// Most recent `limit` library entries, ordered by
    /// `last_accessed_at DESC`. A row slides to the top of the list
    /// every time [`touch`](Self::touch) bumps its timestamp — either
    /// directly when the user re-requests an existing URL+quality
    /// (see the dedup path in `video_prepare`) or indirectly via the
    /// unique constraint swallowing a duplicate publish.
    async fn list_recent(&self, limit: i64) -> Result<Vec<VideoRecord>, Self::Error>;
    /// Look up by primary id.
    async fn find_by_id(&self, id: i32) -> Result<Option<VideoRecord>, Self::Error>;
    /// Delete by primary id; returns `true` if a row was removed.
    async fn delete(&self, id: i32) -> Result<bool, Self::Error>;
    /// Bump `last_accessed_at` to `NOW()` on the row identified by
    /// `id`. Used whenever the handler spots an existing library
    /// entry for the requested URL (even if the user asked for a
    /// different container) so the card slides to the top of the
    /// library grid — see #366.
    async fn touch(&self, id: i32) -> Result<(), Self::Error>;
}
