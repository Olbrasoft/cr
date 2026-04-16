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
    // Parse URL and validate host against allow-list with dot-boundary
    // to prevent bypasses like `attacker.tld/?q=cdninstagram.com`.
    let parsed = match reqwest::Url::parse(&query.url) {
        Ok(u) => u,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid URL").into_response(),
    };
    if parsed.scheme() != "https" {
        return (StatusCode::FORBIDDEN, "URL not allowed").into_response();
    }
    let host = parsed.host_str().unwrap_or("").to_ascii_lowercase();
    let is_allowed = THUMB_ALLOWED_DOMAINS
        .iter()
        .any(|d| host == *d || host.ends_with(&format!(".{d}")));
    if !is_allowed {
        return (StatusCode::FORBIDDEN, "URL not allowed").into_response();
    }

    let resp = state
        .http_client
        .get(parsed.as_str())
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
