//! Centralized application configuration.
//!
//! One source of truth for every environment variable the web process reads.
//! `AppConfig::from_env()` runs once at startup and either succeeds or panics
//! with a clear message, so a missing `DATABASE_URL` or `TMDB_API_KEY` fails
//! the process boot instead of surfacing as a request-time 500.
//!
//! Handlers read config through `AppState.config` (an `Arc<AppConfig>`) — no
//! handler calls `std::env::var` directly.

use anyhow::Context;
use std::sync::Arc;

/// Everything the web process learns from its environment.
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// Postgres DSN (required).
    pub database_url: String,
    /// HTTP port to listen on (default 3000).
    pub port: u16,
    /// Dev override — proxy image requests through this base URL in lieu of
    /// the local data dir. Empty in production.
    pub image_base_url: String,
    /// Root for GeoJSON blobs served through /api/geojson.
    pub geojson_dir: String,
    /// Root for /static/*.
    pub static_dir: String,
    /// Film WebP covers (small variant).
    pub film_covers_dir: String,
    /// Series WebP covers (small variant).
    pub series_covers_dir: String,
    /// Series episode stills (small variant).
    pub series_stills_dir: String,
    /// Series people (actor/crew portraits).
    pub series_people_dir: String,
    /// Repo checkout root — where scripts/auto-import.py lives when the
    /// admin Run-now button spawns it as a subprocess.
    pub cr_repo_root: String,
    /// Hard gate on POST /admin/import/run. Set `ADMIN_IMPORT_RUN_ENABLED=1`
    /// to allow the dashboard button to spawn the importer.
    pub admin_import_run_enabled: bool,
    /// Optional CZ-hosted proxy for scraping geo-blocked sources (prehraj.to,
    /// SK Torrent). None if unconfigured.
    pub cz_proxy: Option<CzProxyConfig>,
}

#[derive(Debug, Clone)]
pub struct CzProxyConfig {
    pub url: String,
    pub key: String,
}

impl AppConfig {
    /// Load from the process environment. Required variables fail-fast at
    /// startup; optional ones fall through to sensible defaults.
    pub fn from_env() -> anyhow::Result<Arc<Self>> {
        let database_url =
            std::env::var("DATABASE_URL").context("DATABASE_URL must be set in .env")?;

        let port: u16 = match std::env::var("PORT") {
            Ok(p) => p
                .parse()
                .with_context(|| format!("PORT=\"{p}\" is not a valid port number"))?,
            Err(_) => 3000,
        };

        let image_base_url = std::env::var("IMAGE_BASE_URL").unwrap_or_default();

        let geojson_dir =
            std::env::var("GEOJSON_DATA_DIR").unwrap_or_else(|_| "data/geojson".to_string());
        let static_dir =
            std::env::var("STATIC_DIR").unwrap_or_else(|_| "cr-web/static".to_string());
        let film_covers_dir =
            std::env::var("COVERS_DIR").unwrap_or_else(|_| "data/movies/covers-webp".to_string());
        let series_covers_dir = std::env::var("SERIES_COVERS_DIR")
            .unwrap_or_else(|_| "data/series/covers-webp".to_string());
        let series_stills_dir = std::env::var("SERIES_STILLS_DIR")
            .unwrap_or_else(|_| "data/series/episode-stills".to_string());
        let series_people_dir =
            std::env::var("SERIES_PEOPLE_DIR").unwrap_or_else(|_| "data/series/people".to_string());
        let cr_repo_root = std::env::var("CR_REPO_ROOT").unwrap_or_else(|_| "/opt/cr".to_string());

        let admin_import_run_enabled = matches!(
            std::env::var("ADMIN_IMPORT_RUN_ENABLED").as_deref(),
            Ok("1")
        );

        let cz_proxy = match (
            std::env::var("CZ_PROXY_URL").ok().filter(|s| !s.is_empty()),
            std::env::var("CZ_PROXY_KEY").ok().filter(|s| !s.is_empty()),
        ) {
            (Some(url), Some(key)) => Some(CzProxyConfig { url, key }),
            _ => None,
        };

        Ok(Arc::new(Self {
            database_url,
            port,
            image_base_url,
            geojson_dir,
            static_dir,
            film_covers_dir,
            series_covers_dir,
            series_stills_dir,
            series_people_dir,
            cr_repo_root,
            admin_import_run_enabled,
            cz_proxy,
        }))
    }
}
