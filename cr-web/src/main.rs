use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use sqlx::postgres::PgPoolOptions;
use tower_http::services::ServeDir;

mod handlers;
mod state;

use state::{AppState, GeoJsonIndex};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    dotenvy::dotenv().ok();
    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL must be set in .env")?;

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
    let geojson_dir = std::env::var("GEOJSON_DATA_DIR")
        .unwrap_or_else(|_| "data/geojson".to_string());
    let geojson_index = GeoJsonIndex::load(&geojson_dir)
        .context("Failed to load GeoJSON index")?;

    // IMAGE_BASE_URL: empty in production, "https://ceskarepublika.wiki" in dev
    let image_base_url = std::env::var("IMAGE_BASE_URL").unwrap_or_default();
    if !image_base_url.is_empty() {
        tracing::info!("Dev mode: images proxied from {image_base_url}");
    }

    let state = AppState {
        db: pool,
        geojson_index: Arc::new(geojson_index),
        image_base_url,
    };

    let app = Router::new()
        .route("/", axum::routing::get(handlers::homepage))
        .route("/health", axum::routing::get(handlers::health))
        .route("/api/geojson/municipality/{code}", axum::routing::get(handlers::geojson_municipality))
        .route("/api/geojson/orp/{code}", axum::routing::get(handlers::geojson_orp))
        .route("/audioknihy", axum::routing::get(handlers::audiobooks))
        .route("/audioknihy/", axum::routing::get(handlers::audiobooks))
        .nest_service("/static", ServeDir::new(
            std::env::var("STATIC_DIR").unwrap_or_else(|_| "cr-web/static".to_string())
        ))
        .fallback(axum::routing::get(handlers::resolve_path))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("Listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
