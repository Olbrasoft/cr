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
    /// Legacy film WebP covers dir. Kept in config so an old env var doesn't
    /// crash boot, but the serving path now proxies R2 via the cr-img-proxy
    /// Worker (see docs/IMAGE_PIPELINE.md rule 1). Will be dropped alongside
    /// the volume mount in epic #552.
    #[allow(dead_code)]
    pub film_covers_dir: String,
    /// Legacy series WebP covers dir. Same rationale as `film_covers_dir`.
    #[allow(dead_code)]
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
    /// Hard gate on POST /admin/cache/purge. Set `ADMIN_CACHE_PURGE_ENABLED=1`
    /// to allow the dashboard button to call the Cloudflare purge API. Even
    /// with a token present the POST 403s without this flag — prevents a
    /// stray/CSRF request on the unauthenticated /admin/ path from flushing
    /// the CDN.
    pub admin_cache_purge_enabled: bool,
    /// Optional CZ-hosted proxy for scraping geo-blocked sources (prehraj.to,
    /// SK Torrent). None if unconfigured.
    pub cz_proxy: Option<CzProxyConfig>,
    /// Hard gate on the sledujteto.cz POC surface (issue #551): the
    /// `/admin/test-sledujteto/` diagnostic page and the `/api/sledujteto/*`
    /// endpoints it consumes. Both register only when
    /// `SLEDUJTETO_POC_ENABLED=1`. Off by default so a forgotten POC can't
    /// drive uncached upstream load on production. The proper, cached
    /// three-source handler lands in #547 and replaces these.
    pub sledujteto_poc_enabled: bool,
    /// Serve the detail-page "Další zdroje" block from
    /// `film_prehrajto_uploads` (DB) instead of the legacy live
    /// search + per-result validate against prehraj.to. Set
    /// `PREHRAJTO_SOURCES_FROM_DB=1` to enable. Rollback is just
    /// flipping the flag back — the old search/validate handlers stay
    /// routed so the template's fallback path keeps working.
    pub prehrajto_sources_from_db: bool,
    /// Cloudflare API token scoped to Zone.Cache Purge. Enables the admin
    /// `/admin/cache/` page to invalidate CDN cache on demand. `None` when the
    /// env vars aren't provisioned — UI hides the purge actions in that case.
    pub cf_cache_purge: Option<CfCachePurgeConfig>,
}

#[derive(Debug, Clone)]
pub struct CfCachePurgeConfig {
    pub token: String,
    pub zone_id: String,
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

        let admin_cache_purge_enabled = matches!(
            std::env::var("ADMIN_CACHE_PURGE_ENABLED").as_deref(),
            Ok("1")
        );

        let cz_proxy = match (
            std::env::var("CZ_PROXY_URL").ok().filter(|s| !s.is_empty()),
            std::env::var("CZ_PROXY_KEY").ok().filter(|s| !s.is_empty()),
        ) {
            (Some(url), Some(key)) => Some(CzProxyConfig { url, key }),
            _ => None,
        };

        let prehrajto_sources_from_db = matches!(
            std::env::var("PREHRAJTO_SOURCES_FROM_DB").as_deref(),
            Ok("1")
        );

        let sledujteto_poc_enabled =
            matches!(std::env::var("SLEDUJTETO_POC_ENABLED").as_deref(), Ok("1"));

        let cf_cache_purge = match (
            std::env::var("CF_CACHE_PURGE_TOKEN")
                .ok()
                .filter(|s| !s.is_empty()),
            std::env::var("CF_ZONE_ID").ok().filter(|s| !s.is_empty()),
        ) {
            (Some(token), Some(zone_id)) => Some(CfCachePurgeConfig { token, zone_id }),
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
            admin_cache_purge_enabled,
            cz_proxy,
            prehrajto_sources_from_db,
            sledujteto_poc_enabled,
            cf_cache_purge,
        }))
    }
}
