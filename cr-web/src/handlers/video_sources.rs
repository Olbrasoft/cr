//! Data types for the unified `video_sources` schema (issue #607 / #608).
//!
//! PR1 only lands these structs so `cargo check` and `cargo sqlx prepare`
//! verify they compile against the migrated schema. Readers and writers
//! are wired up in the follow-up PRs (#611 reader switch, #610 dual-write).
//!
//! Schema reference: `cr-infra/migrations/20260529_058_video_sources_unified.sql`.

use chrono::{DateTime, Utc};
use serde::Serialize;

/// One row in `video_providers`. Lookup table, seeded with 3 rows
/// (sktorrent / prehrajto / sledujteto). Adding a fourth is a data change.
#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
#[allow(dead_code)] // Landed in PR1; consumed in PR3 (#611 reader switch).
pub(crate) struct VideoProvider {
    pub(crate) id: i16,
    pub(crate) slug: String,
    pub(crate) host: String,
    pub(crate) display_name: String,
    pub(crate) sort_priority: i16,
    pub(crate) is_active: bool,
}

/// One row in `video_sources`. Polymorphic over three parent columns — exactly
/// one of `film_id` / `episode_id` / `tv_episode_id` is `Some` (enforced by
/// the `video_sources_one_parent_check` CHECK constraint at the DB level).
#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
#[allow(dead_code)] // Landed in PR1; consumed in PR3 (#611 reader switch).
pub(crate) struct VideoSource {
    pub(crate) id: i32,
    pub(crate) provider_id: i16,
    pub(crate) film_id: Option<i32>,
    pub(crate) episode_id: Option<i32>,
    pub(crate) tv_episode_id: Option<i32>,
    pub(crate) external_id: String,
    pub(crate) title: Option<String>,
    pub(crate) duration_sec: Option<i32>,
    pub(crate) resolution_hint: Option<String>,
    pub(crate) filesize_bytes: Option<i64>,
    pub(crate) view_count: Option<i32>,
    pub(crate) lang_class: String,
    pub(crate) audio_lang: Option<String>,
    pub(crate) audio_confidence: Option<f32>,
    pub(crate) audio_detected_by: Option<String>,
    pub(crate) cdn: Option<String>,
    pub(crate) is_primary: bool,
    pub(crate) is_alive: bool,
    pub(crate) last_seen: Option<DateTime<Utc>>,
    pub(crate) last_checked: Option<DateTime<Utc>>,
    pub(crate) metadata: Option<serde_json::Value>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

/// One row in `video_source_subtitles`. Persisted even when `url` is NULL
/// (sledujteto resolves URLs live at play time) so filters + badges can
/// work from the DB without a live resolve.
#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
#[allow(dead_code)] // Landed in PR1; consumed in PR3 (#611 reader switch).
pub(crate) struct VideoSourceSubtitle {
    pub(crate) id: i32,
    pub(crate) source_id: i32,
    pub(crate) lang: String,
    pub(crate) label: Option<String>,
    pub(crate) format: Option<String>,
    pub(crate) url: Option<String>,
    pub(crate) is_default: bool,
    pub(crate) is_forced: bool,
    pub(crate) created_at: DateTime<Utc>,
}
