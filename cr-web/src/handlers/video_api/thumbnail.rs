//! CDN thumbnail proxy — prevents SSRF by allow-listing known domains.

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct ThumbQuery {
    url: String,
}

/// Allowed CDN domains for thumbnail proxy (prevents SSRF/open-proxy).
const THUMB_ALLOWED_DOMAINS: &[&str] = &["cdninstagram.com", "fbcdn.net", "sdn.cz"];

pub async fn video_thumb(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ThumbQuery>,
) -> impl IntoResponse {
    // Validate URL — only allow known CDN domains (prevent SSRF)
    let is_allowed = THUMB_ALLOWED_DOMAINS.iter().any(|d| query.url.contains(d));
    if !is_allowed || !query.url.starts_with("https://") {
        return (StatusCode::FORBIDDEN, "URL not allowed").into_response();
    }

    let resp = state
        .http_client
        .get(&query.url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Referer", "https://www.instagram.com/")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => {
            let ct = r
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("image/jpeg")
                .to_string();
            match r.bytes().await {
                Ok(bytes) => (StatusCode::OK, [(header::CONTENT_TYPE, ct)], bytes).into_response(),
                Err(_) => (StatusCode::BAD_GATEWAY, "Failed to read upstream body").into_response(),
            }
        }
        _ => (StatusCode::NOT_FOUND, "Thumbnail not available").into_response(),
    }
}
