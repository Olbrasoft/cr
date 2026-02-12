use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::Router;
use sqlx::postgres::PgPoolOptions;
use tower_http::services::ServeDir;

mod handlers;
mod state;

use state::AppState;

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

    let state = AppState { db: pool };

    let app = Router::new()
        .route("/", axum::routing::get(handlers::homepage))
        .route("/health", axum::routing::get(handlers::health))
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
