use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use cr_infra::r2::{R2Client, R2Config};
use cr_infra::repositories::{
    PgLandmarkRepository, PgMunicipalityRepository, PgOrpRepository, PgPhotoRepository,
    PgPoolRepository, PgRegionRepository, PgVideoRepository,
};
use cr_infra::streamtape::{StreamtapeClient, StreamtapeConfig};
use cr_infra::video_library::VideoLibraryPipeline;
use sqlx::postgres::PgPoolOptions;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

mod error;
mod handlers;
mod img_proxy;
mod state;

use state::{AppState, GeoJsonIndex};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    dotenvy::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set in .env")?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("Failed to connect to database")?;

    // Run migrations on startup
    sqlx::migrate!("../cr-infra/migrations")
        .run(&pool)
        .await
        .context("Failed to run database migrations")?;
    tracing::info!("Database migrations applied");

    // Load GeoJSON index into memory for API endpoints
    let geojson_dir =
        std::env::var("GEOJSON_DATA_DIR").unwrap_or_else(|_| "data/geojson".to_string());
    let geojson_index = GeoJsonIndex::load(&geojson_dir).context("Failed to load GeoJSON index")?;

    // IMAGE_BASE_URL: empty in production, "https://ceskarepublika.wiki" in dev
    let image_base_url = std::env::var("IMAGE_BASE_URL").unwrap_or_default();
    if !image_base_url.is_empty() {
        tracing::info!("Dev mode: images proxied from {image_base_url}");
    }

    // Streamtape + R2 credentials for the video library. Optional during
    // rollout — when env vars are unset, the rest of the app keeps running
    // and library-related endpoints will refuse the operation cleanly.
    let streamtape_config = StreamtapeConfig::from_env();
    let r2_config = R2Config::from_env();
    match (&streamtape_config, &r2_config) {
        (Some(_), Some(r2)) => {
            tracing::info!(
                "Video library: Streamtape + R2 configured (bucket: {})",
                r2.bucket
            )
        }
        (None, _) => tracing::warn!(
            "Video library: STREAMTAPE_LOGIN/STREAMTAPE_KEY missing — uploads disabled"
        ),
        (_, None) => {
            tracing::warn!("Video library: R2_* env vars missing — thumbnail upload disabled")
        }
    }

    let video_repo = Arc::new(PgVideoRepository::new(pool.clone()));

    // Build the orchestrator only when both halves of the config are present.
    // The handler treats `None` as "library disabled" and falls back to the
    // legacy local-only download flow.
    let video_library = match (streamtape_config.clone(), r2_config.clone()) {
        (Some(stc), Some(r2c)) => Some(VideoLibraryPipeline::new(
            StreamtapeClient::new(reqwest::Client::new(), stc),
            R2Client::new(r2c),
            video_repo.clone(),
            reqwest::Client::new(),
        )),
        _ => None,
    };

    let video_downloads: handlers::video_api::VideoDownloads =
        Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));

    // #192 — periodic reaper for /tmp/cr-videos/. Deletes anything older
    // than 30 minutes so temp videos left behind by `publish_local_video`
    // (kept on disk on purpose for the /api/video/file/{token} ready-link,
    // see #363) don't accumulate and exhaust the VPS disk. Also prunes
    // matching in-memory `video_downloads` entries so `/status/{token}`
    // doesn't report `Ready` for a file that's already been reaped.
    let _cleanup_task = handlers::video_api::spawn_temp_video_cleanup_loop(video_downloads.clone());

    let state = AppState {
        region_repo: Arc::new(PgRegionRepository::new(pool.clone())),
        orp_repo: Arc::new(PgOrpRepository::new(pool.clone())),
        municipality_repo: Arc::new(PgMunicipalityRepository::new(pool.clone())),
        landmark_repo: Arc::new(PgLandmarkRepository::new(pool.clone())),
        pool_repo: Arc::new(PgPoolRepository::new(pool.clone())),
        photo_repo: Arc::new(PgPhotoRepository::new(pool.clone())),
        video_repo,
        db: pool,
        geojson_index: Arc::new(geojson_index),
        image_base_url,
        http_client: reqwest::Client::new(),
        video_downloads,
        streamtape_config: streamtape_config.map(Arc::new),
        r2_config: r2_config.map(Arc::new),
        video_library,
        streamtape_url_cache: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
    };

    // API routes with CORS
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    let api_routes = Router::new()
        .route(
            "/geojson/municipality/{code}",
            axum::routing::get(handlers::geojson_municipality),
        )
        .route(
            "/geojson/orp/{code}",
            axum::routing::get(handlers::geojson_orp),
        )
        .route("/landmarks", axum::routing::get(handlers::api_landmarks))
        .route("/video/info", axum::routing::post(handlers::video_info))
        .route(
            "/video/prepare",
            axum::routing::post(handlers::video_prepare),
        )
        .route(
            "/video/status/{token}",
            axum::routing::get(handlers::video_status),
        )
        .route(
            "/video/file/{token}",
            axum::routing::get(handlers::video_file),
        )
        .route(
            "/video/file/{token}/{part_index}",
            axum::routing::get(handlers::video_file_part),
        )
        .route("/video/recent", axum::routing::get(handlers::video_recent))
        .route("/video/thumb", axum::routing::get(handlers::video_thumb))
        .route(
            "/video/cleanup",
            axum::routing::delete(handlers::video_cleanup),
        )
        // --- #321 Video library API ---
        .route("/video/library", axum::routing::get(handlers::library_list))
        .route(
            "/video/library/{id}/play",
            axum::routing::get(handlers::library_play),
        )
        .route(
            "/video/library/{id}/stream",
            axum::routing::get(handlers::library_stream),
        )
        .route(
            "/video/library/{id}/file",
            axum::routing::get(handlers::library_file),
        )
        .route(
            "/video/library/{id}",
            axum::routing::delete(handlers::library_delete),
        )
        .route(
            "/movies/search",
            axum::routing::get(handlers::movies_api::movies_search),
        )
        .route(
            "/movies/video-url",
            axum::routing::get(handlers::movies_api::movies_video_url),
        )
        .route(
            "/movies/validate",
            axum::routing::get(handlers::movies_api::movies_validate),
        )
        .route(
            "/movies/stream",
            axum::routing::get(handlers::movies_api::movies_stream),
        )
        .route(
            "/movies/thumb",
            axum::routing::get(handlers::movies_api::movies_thumb),
        )
        .route(
            "/movies/filemoon-resolve",
            axum::routing::get(handlers::movies_api::filemoon_resolve),
        )
        .route(
            "/movies/stream-resolve",
            axum::routing::get(handlers::movies_api::stream_resolve),
        )
        .route(
            "/movies/proxy-stream",
            axum::routing::get(handlers::movies_api::movies_proxy_stream),
        )
        .route("/films/search", axum::routing::get(handlers::films_search))
        .route(
            "/films/sktorrent-resolve",
            axum::routing::get(handlers::sktorrent_resolve),
        )
        .layer(cors);

    let app = Router::new()
        .route("/", axum::routing::get(handlers::homepage))
        .route("/health", axum::routing::get(handlers::health))
        .nest("/api", api_routes)
        .route(
            "/admin/import/",
            axum::routing::get(handlers::admin_import::admin_import_list),
        )
        .route(
            "/admin/import",
            axum::routing::get(handlers::admin_import::admin_import_list),
        )
        .route(
            "/admin/import/failures",
            axum::routing::get(handlers::admin_import::admin_import_failures),
        )
        .route(
            "/admin/import/{run_id}",
            axum::routing::get(handlers::admin_import::admin_import_detail),
        )
        .route("/pamatky", axum::routing::get(handlers::landmarks_index))
        .route("/pamatky/", axum::routing::get(handlers::landmarks_index))
        .route("/audioknihy", axum::routing::get(handlers::audiobooks))
        .route("/audioknihy/", axum::routing::get(handlers::audiobooks))
        .route(
            "/stahnout-video",
            axum::routing::get(handlers::download_video),
        )
        .route(
            "/stahnout-video/",
            axum::routing::get(handlers::download_video),
        )
        .route("/filmy-online", axum::routing::get(handlers::films_list))
        .route("/filmy-online/", axum::routing::get(handlers::films_list))
        .route(
            "/filmy-online/{slug}",
            axum::routing::get(handlers::films_detail),
        )
        .route(
            "/filmy-online/{slug}/",
            axum::routing::get(handlers::films_detail),
        )
        .route(
            "/filmy-a-serialy",
            axum::routing::get(handlers::filmy_serialy),
        )
        .route(
            "/filmy-a-serialy/",
            axum::routing::get(handlers::filmy_serialy),
        )
        .route("/koupani", axum::routing::get(handlers::pools_hub))
        .route("/koupani/", axum::routing::get(handlers::pools_hub))
        .route(
            "/aquaparky",
            axum::routing::get(handlers::pools_by_category),
        )
        .route(
            "/aquaparky/",
            axum::routing::get(handlers::pools_by_category),
        )
        .route("/bazeny", axum::routing::get(handlers::pools_by_category))
        .route("/bazeny/", axum::routing::get(handlers::pools_by_category))
        .route(
            "/koupaliste",
            axum::routing::get(handlers::pools_by_category),
        )
        .route(
            "/koupaliste/",
            axum::routing::get(handlers::pools_by_category),
        )
        .route(
            "/prirodni-koupaliste",
            axum::routing::get(handlers::pools_by_category),
        )
        .route(
            "/prirodni-koupaliste/",
            axum::routing::get(handlers::pools_by_category),
        )
        .route("/img/{*path}", axum::routing::get(img_proxy::img_proxy))
        .nest_service(
            "/static",
            ServeDir::new(
                std::env::var("STATIC_DIR").unwrap_or_else(|_| "cr-web/static".to_string()),
            ),
        )
        .fallback(axum::routing::get(handlers::resolve_path))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let port: u16 = match std::env::var("PORT") {
        Ok(p) => p.parse().context("PORT must be a valid port number")?,
        Err(_) => 3000,
    };
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Shutdown signal received, starting graceful shutdown");
}
