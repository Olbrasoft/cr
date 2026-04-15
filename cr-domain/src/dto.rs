//! Repository data-transfer objects (DTOs).
//!
//! These are NOT domain entities — they're plain row-shaped records
//! returned by repository queries. Kept separate from `repository.rs`
//! (which holds the trait definitions / ports) so the trait module
//! doesn't balloon past 100 lines when new repositories are added.
//!
//! Entities with invariants live in `entities/`; DTOs here carry only
//! column-shaped data.

// --- Record types returned by repositories ---
// These are plain data records (not domain entities) for query results.

/// Region data as stored in the database.
#[derive(Debug, Clone)]
pub struct RegionRecord {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub region_code: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub coat_of_arms_ext: Option<String>,
    pub flag_ext: Option<String>,
    pub description: Option<String>,
    pub hero_photo_r2_key: Option<String>,
    pub hero_municipality_code: Option<String>,
    pub hero_municipality_photo_index: Option<i16>,
}

/// ORP data as stored in the database.
#[derive(Debug, Clone)]
pub struct OrpRecord {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub orp_code: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub description: Option<String>,
}

/// Municipality data as stored in the database.
#[derive(Debug, Clone)]
pub struct MunicipalityRecord {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub municipality_code: String,
    pub pou_code: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub wikipedia_url: Option<String>,
    pub official_website: Option<String>,
    pub coat_of_arms_ext: Option<String>,
    pub flag_ext: Option<String>,
    pub population: Option<i32>,
    pub elevation: Option<f64>,
}

/// Landmark summary for listings.
#[derive(Debug, Clone)]
pub struct LandmarkSummary {
    pub name: String,
    pub slug: String,
    pub type_name: String,
    pub municipality_name: String,
    pub municipality_slug: String,
    pub is_main: bool,
}

/// Landmark full record for detail pages.
#[derive(Debug, Clone)]
pub struct LandmarkRecord {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub description: Option<String>,
    pub wikipedia_url: Option<String>,
    pub image_ext: Option<String>,
    pub npu_catalog_id: Option<String>,
    pub npu_description: Option<String>,
    pub type_slug: String,
    pub type_name: String,
    pub municipality_name: Option<String>,
    pub municipality_slug: Option<String>,
    pub orp_slug: Option<String>,
    pub region_slug: Option<String>,
    pub municipality_code: Option<String>,
    pub municipality_coat_of_arms_ext: Option<String>,
}

/// Pool summary for listings.
#[derive(Debug, Clone)]
pub struct PoolSummary {
    pub name: String,
    pub slug: String,
    pub is_aquapark: bool,
    pub is_indoor: bool,
    pub is_outdoor: bool,
    pub is_natural: bool,
}

/// Pool full record for detail pages.
#[derive(Debug, Clone)]
pub struct PoolRecord {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub description: Option<String>,
    pub address: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub website: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub facebook: Option<String>,
    pub facilities: Option<String>,
    pub pool_length_m: Option<i32>,
    pub is_aquapark: bool,
    pub is_indoor: bool,
    pub is_outdoor: bool,
    pub is_natural: bool,
    pub photo_count: i16,
    pub municipality_name: Option<String>,
}

/// Photo metadata record.
#[derive(Debug, Clone)]
pub struct PhotoRecord {
    pub r2_key: String,
    pub width: i16,
    pub height: i16,
}

/// Hosted video as stored in the `videos` table.
///
/// `created_at` is stored as an ISO 8601 string so this record stays
/// dependency-free (cr-domain has zero external deps by design).
#[derive(Debug, Clone)]
pub struct VideoRecord {
    pub id: i32,
    pub source_url: String,
    pub title: String,
    pub description: Option<String>,
    pub duration_sec: Option<i32>,
    pub source_extractor: Option<String>,
    pub quality: String,
    pub format_ext: String,
    pub streamtape_file_id: String,
    pub streamtape_url: String,
    pub file_size_bytes: i64,
    pub thumbnail_r2_key: Option<String>,
    pub thumbnail_url: Option<String>,
    pub created_at: String,
    /// Bumped to `NOW()` whenever the handler spots an existing
    /// library entry for the requested URL+quality, so the card
    /// slides to the top of the library grid. See #366.
    pub last_accessed_at: String,
    /// Human-readable resolution like `"1080p"` or `"720p"` parsed
    /// from yt-dlp's `format.resolution` field. Separate from
    /// `quality` because `quality` is the raw yt-dlp format_id
    /// which is source-specific garbage (`"137"` on YouTube,
    /// `"bytevc1_720p_..."` on TikTok, `"mp4"` on Seznam). `None`
    /// on legacy rows where the backfill regex couldn't find a
    /// `\d+p` substring. See #366.
    pub resolution: Option<String>,
}

/// Insertion payload for the `videos` table — everything the upload
/// pipeline knows about a freshly uploaded video. The DB assigns `id`
/// and `created_at`.
#[derive(Debug, Clone)]
pub struct NewVideo {
    pub source_url: String,
    pub title: String,
    pub description: Option<String>,
    pub duration_sec: Option<i32>,
    pub source_extractor: Option<String>,
    pub quality: String,
    pub format_ext: String,
    pub streamtape_file_id: String,
    pub streamtape_url: String,
    pub file_size_bytes: i64,
    pub thumbnail_r2_key: Option<String>,
    pub thumbnail_url: Option<String>,
    /// Human-readable resolution (e.g. `"720p"`). See
    /// [`VideoRecord::resolution`] for rationale.
    pub resolution: Option<String>,
}
