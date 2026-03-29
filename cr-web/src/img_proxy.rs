//! On-the-fly image resize proxy.
//!
//! Fetches the original image from R2 (via Cloudflare Worker in production
//! or IMAGE_BASE_URL in dev), resizes to the requested width, caches
//! the result on disk, and returns with immutable cache headers.
//!
//! Route: GET /img/*path?w=360
//! Allowed widths: 360, 720. Without ?w, proxies the original.

use axum::extract::{Query, State};
use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use image::ImageReader;
use std::io::Cursor;
use std::path::PathBuf;

use crate::state::AppState;

const ALLOWED_WIDTHS: &[u32] = &[360, 720];
const CACHE_DIR: &str = "/tmp/cr-img-cache";

#[derive(serde::Deserialize)]
pub struct ResizeParams {
    w: Option<u32>,
}

pub async fn img_proxy(
    State(state): State<AppState>,
    uri: Uri,
    Query(params): Query<ResizeParams>,
) -> Response {
    let path = uri.path();

    // Only handle /img/ paths
    if !path.starts_with("/img/") {
        return StatusCode::NOT_FOUND.into_response();
    }

    let img_path = &path[5..]; // strip "/img/"
    if img_path.is_empty() || img_path.contains("..") {
        return StatusCode::BAD_REQUEST.into_response();
    }

    // Validate path: only allow safe characters and image extensions
    let valid_path = img_path.chars().all(|c| c.is_ascii_alphanumeric() || "-_/.".contains(c));
    let valid_ext = img_path.ends_with(".webp") || img_path.ends_with(".jpg")
        || img_path.ends_with(".jpeg") || img_path.ends_with(".png") || img_path.ends_with(".svg");
    if !valid_path || !valid_ext {
        return StatusCode::BAD_REQUEST.into_response();
    }

    let target_width = params.w;

    // Validate requested width
    if let Some(w) = target_width {
        if !ALLOWED_WIDTHS.contains(&w) {
            return (StatusCode::BAD_REQUEST, "Allowed widths: 360, 720").into_response();
        }
    }

    // Build cache key
    let cache_path = match target_width {
        Some(w) => {
            let dir = PathBuf::from(CACHE_DIR).join(format!("{w}"));
            dir.join(img_path)
        }
        None => PathBuf::from(CACHE_DIR).join("orig").join(img_path),
    };

    // Serve from disk cache if available
    if cache_path.exists() {
        if let Ok(data) = tokio::fs::read(&cache_path).await {
            // Resized images are always JPEG
            let content_type = if target_width.is_some() {
                "image/jpeg"
            } else if img_path.ends_with(".webp") {
                "image/webp"
            } else if img_path.ends_with(".jpg") || img_path.ends_with(".jpeg") {
                "image/jpeg"
            } else if img_path.ends_with(".png") {
                "image/png"
            } else {
                "application/octet-stream"
            };

            return (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, content_type),
                    (header::CACHE_CONTROL, "public, max-age=86400, s-maxage=604800"),
                ],
                data,
            )
                .into_response();
        }
    }

    // Fetch original image from upstream
    let upstream_url = if state.image_base_url.is_empty() {
        // Production: fetch from ourselves (Cloudflare Worker serves /img/)
        // But we ARE the server, so fetch from R2 directly isn't possible here.
        // In production, the Cloudflare Worker handles /img/ before Axum.
        // This proxy only runs when there's a ?w= param that the Worker doesn't handle.
        // We need to fetch from the Worker URL.
        format!("https://ceskarepublika.wiki/img/{img_path}")
    } else {
        format!("{}/img/{img_path}", state.image_base_url)
    };

    let client = reqwest::Client::new();
    let resp = match client
        .get(&upstream_url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Failed to fetch upstream image {upstream_url}: {e}");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    if !resp.status().is_success() {
        return StatusCode::NOT_FOUND.into_response();
    }

    let original_bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("Failed to read image bytes: {e}");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    // If no resize requested, cache and return original
    // When resizing: always encode as JPEG (lossy, quality 82) because the image crate's
    // WebP encoder only supports lossless VP8L, which produces larger files than originals.
    let (output_bytes, is_resized) = if let Some(w) = target_width {
        // Decode and resize
        let reader = match ImageReader::new(Cursor::new(&original_bytes))
            .with_guessed_format()
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to guess image format: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };

        let img = match reader.decode() {
            Ok(img) => img,
            Err(e) => {
                tracing::error!("Failed to decode image: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };

        let orig_w = img.width();
        if orig_w <= w {
            // Already small enough, return original as-is
            (original_bytes.to_vec(), false)
        } else {
            let resized = img.resize(w, u32::MAX, image::imageops::FilterType::Lanczos3);

            // Encode as JPEG with quality 82 (lossy, much smaller than lossless WebP)
            let mut buf = Vec::new();
            let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buf, 82);
            if let Err(e) = resized.write_with_encoder(encoder) {
                tracing::error!("Failed to encode resized JPEG: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }

            (buf, true)
        }
    } else {
        (original_bytes.to_vec(), false)
    };

    // Save to disk cache (async, don't block response)
    let cache_path_clone = cache_path.clone();
    let output_clone = output_bytes.clone();
    tokio::spawn(async move {
        if let Some(parent) = cache_path_clone.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let _ = tokio::fs::write(&cache_path_clone, &output_clone).await;
    });

    let content_type = if is_resized {
        "image/jpeg"
    } else if img_path.ends_with(".webp") {
        "image/webp"
    } else if img_path.ends_with(".jpg") || img_path.ends_with(".jpeg") {
        "image/jpeg"
    } else if img_path.ends_with(".png") {
        "image/png"
    } else {
        "application/octet-stream"
    };

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "public, max-age=86400, s-maxage=604800"),
        ],
        output_bytes,
    )
        .into_response()
}
