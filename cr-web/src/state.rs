use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use cr_infra::r2::R2Config;
use cr_infra::repositories::{
    PgLandmarkRepository, PgMunicipalityRepository, PgOrpRepository, PgPhotoRepository,
    PgPoolRepository, PgRegionRepository, PgVideoRepository,
};
use cr_infra::streamtape::StreamtapeConfig;
use cr_infra::video_library::VideoLibraryPipeline;
use sqlx::PgPool;

use crate::cache::BoundedTtlCache;
use crate::handlers::SktorrentSource;
use crate::handlers::video_api::VideoDownloads;

#[derive(Clone)]
pub struct AppState {
    /// Process-wide configuration. Read once at startup. Handlers consult
    /// this instead of reaching into `std::env` so paths and toggles live
    /// in one place.
    pub config: Arc<crate::config::AppConfig>,
    pub db: PgPool,
    pub geojson_index: Arc<GeoJsonIndex>,
    /// Mirrors `config.image_base_url` for the many templates that already
    /// take `&img: String`. New code should prefer `state.config` directly.
    pub image_base_url: String,
    /// Shared HTTP client for image proxy (reuse connections).
    pub http_client: reqwest::Client,
    // Repositories (cr-infra) — used progressively as handlers are refactored
    pub region_repo: Arc<PgRegionRepository>,
    pub orp_repo: Arc<PgOrpRepository>,
    pub municipality_repo: Arc<PgMunicipalityRepository>,
    pub landmark_repo: Arc<PgLandmarkRepository>,
    pub pool_repo: Arc<PgPoolRepository>,
    pub photo_repo: Arc<PgPhotoRepository>,
    /// Repository for the hosted video library (`videos` table).
    pub video_repo: Arc<PgVideoRepository>,
    /// Prepared video downloads waiting to be served.
    pub video_downloads: VideoDownloads,
    /// Streamtape API credentials for the video library. `None` until the
    /// `STREAMTAPE_LOGIN`/`STREAMTAPE_KEY` env vars are provisioned. Read
    /// at startup only — actual use happens via [`AppState::video_library`].
    #[allow(dead_code)]
    pub streamtape_config: Option<Arc<StreamtapeConfig>>,
    /// Cloudflare R2 credentials for storing video thumbnails. `None` until
    /// the `R2_*` env vars are provisioned. Read at startup only.
    #[allow(dead_code)]
    pub r2_config: Option<Arc<R2Config>>,
    /// Orchestrator that owns the Streamtape + R2 + DB collaborators for
    /// the hosted video library. `Some` only when both configs are present.
    pub video_library: Option<VideoLibraryPipeline>,
    /// In-memory cache of resolved Streamtape CDN URLs keyed by file_id.
    /// Streamtape's `dlticket` flow forces a 5 s wait per call which makes
    /// every HTML5 `<video>` Range request unbearable; the resolved URL is
    /// good for ~50 min so caching it makes seek/buffer feel instant.
    pub streamtape_url_cache:
        Arc<tokio::sync::Mutex<HashMap<String, (String, std::time::Instant)>>>,
    /// Bounded TTL cache for resolved filemoon / stream m3u8 URLs. Key is
    /// `"{provider}:{code}"`, value is the resolved playback URL (optionally
    /// followed by `\n{cookies}`, matching the pre-refactor convention).
    /// Before #443 this was an unbounded module-level LazyLock.
    pub filemoon_cache: BoundedTtlCache<String, String>,
    /// Bounded TTL cache for SK Torrent per-video source lists. Key is
    /// `sktorrent_video_id`; value is the resolved source list.
    pub sktorrent_cache: BoundedTtlCache<i32, Vec<SktorrentSource>>,
    /// Resolved prehraj.to CDN URLs, keyed by `upload_id`. Cached value
    /// carries its own deadline because token lifetimes vary per upload —
    /// the cache's own TTL is just a conservative upper bound.
    pub prehrajto_stream_cache: BoundedTtlCache<String, CachedStreamUrl>,
    /// Per-`upload_id` async locks for in-flight scrape deduplication. On
    /// cache miss a handler takes the per-key lock, re-checks the cache,
    /// then scrapes once — concurrent requests for the same upload block
    /// on the lock and pick up the cached URL after release.
    pub prehrajto_in_flight: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

/// Cached resolved CDN URL for a single prehraj.to upload.
///
/// `expires_at` is the token's own deadline (extracted from the URL's
/// `expires=` query param) minus a safety margin; reads treat the entry as
/// stale once `expires_at` is past.
#[derive(Clone)]
pub struct CachedStreamUrl {
    pub url: String,
    pub expires_at: Instant,
}

/// In-memory index of GeoJSON features for fast API lookups.
/// Loaded at startup from simplified GeoJSON files.
pub struct GeoJsonIndex {
    /// Municipality code -> GeoJSON Feature (as raw JSON string)
    pub municipalities: HashMap<String, String>,
    /// ORP code -> GeoJSON Feature (as raw JSON string)
    pub orp: HashMap<String, String>,
}

impl GeoJsonIndex {
    pub fn load(data_dir: &str) -> anyhow::Result<Self> {
        let mut municipalities = HashMap::new();
        let mut orp = HashMap::new();

        // Load municipalities
        let muni_path = format!("{data_dir}/obce_simple.geojson");
        if let Ok(content) = std::fs::read_to_string(&muni_path) {
            let data: serde_json::Value = serde_json::from_str(&content)?;
            if let Some(features) = data["features"].as_array() {
                for feat in features {
                    if let Some(code) = feat["properties"]["kod_obec_p"].as_str() {
                        municipalities.insert(code.to_string(), feat.to_string());
                    }
                }
            }
            tracing::info!("Loaded {} municipality polygons", municipalities.len());
        } else {
            tracing::warn!("Municipality GeoJSON not found at {muni_path}");
        }

        // Load ORP
        let orp_path = format!("{data_dir}/orp_simple.geojson");
        if let Ok(content) = std::fs::read_to_string(&orp_path) {
            let data: serde_json::Value = serde_json::from_str(&content)?;
            if let Some(features) = data["features"].as_array() {
                for feat in features {
                    if let Some(code) = feat["properties"]["kod_orp_p"].as_str() {
                        orp.insert(code.to_string(), feat.to_string());
                    }
                }
            }
            tracing::info!("Loaded {} ORP polygons", orp.len());
        } else {
            tracing::warn!("ORP GeoJSON not found at {orp_path}");
        }

        Ok(Self {
            municipalities,
            orp,
        })
    }
}
