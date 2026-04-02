use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use cr_infra::repositories::{
    PgLandmarkRepository, PgMunicipalityRepository, PgOrpRepository, PgPhotoRepository,
    PgPoolRepository, PgRegionRepository,
};
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

    let state = AppState {
        region_repo: Arc::new(PgRegionRepository::new(pool.clone())),
        orp_repo: Arc::new(PgOrpRepository::new(pool.clone())),
        municipality_repo: Arc::new(PgMunicipalityRepository::new(pool.clone())),
        landmark_repo: Arc::new(PgLandmarkRepository::new(pool.clone())),
        pool_repo: Arc::new(PgPoolRepository::new(pool.clone())),
        photo_repo: Arc::new(PgPhotoRepository::new(pool.clone())),
        db: pool,
        geojson_index: Arc::new(geojson_index),
        image_base_url,
        http_client: reqwest::Client::new(),
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
        .layer(cors);

    let app = Router::new()
        .route("/", axum::routing::get(handlers::homepage))
        .route("/health", axum::routing::get(handlers::health))
        .nest("/api", api_routes)
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
