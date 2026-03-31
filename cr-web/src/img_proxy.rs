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
    pub w: Option<u32>,
}

/// Handler for `/img/*` route.
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
    serve_image_inner(&state, img_path, params.w).await
}

/// Serve an image by path. Called from resolve_path for SEO URLs without /img/ prefix.
pub async fn serve_image(state: &AppState, img_path: &str, width: Option<u32>) -> Response {
    serve_image_inner(state, img_path, width).await
}

async fn serve_image_inner(
    state: &AppState,
    img_path: &str,
    target_width: Option<u32>,
) -> Response {
    if img_path.is_empty() || img_path.contains("..") {
        return StatusCode::BAD_REQUEST.into_response();
    }

    // Validate path: only allow safe characters and image extensions
    let valid_path = img_path
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "-_/.".contains(c));
    let valid_ext = img_path.ends_with(".webp")
        || img_path.ends_with(".jpg")
        || img_path.ends_with(".jpeg")
        || img_path.ends_with(".png")
        || img_path.ends_with(".svg");
    if !valid_path || !valid_ext {
        return StatusCode::BAD_REQUEST.into_response();
    }

    // SEO URL translation: /{orp}/{municipality}/{photo}.webp → municipalities/{code}/{photo}.webp
    // Pattern: 3 path segments where first is not a known prefix
    let img_path = resolve_seo_path(&state.db, img_path).await;
    let img_path = img_path.as_str();

    let is_svg = img_path.ends_with(".svg");

    // SVGs cannot be raster-resized
    if target_width.is_some() && is_svg {
        return (StatusCode::BAD_REQUEST, "SVG resize not supported").into_response();
    }

    // Validate requested width
    if let Some(w) = target_width
        && !ALLOWED_WIDTHS.contains(&w)
    {
        return (StatusCode::BAD_REQUEST, "Allowed widths: 360, 720").into_response();
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
    if cache_path.exists()
        && let Ok(data) = tokio::fs::read(&cache_path).await
    {
        // Resized images are always JPEG
        let content_type = if target_width.is_some() {
            "image/jpeg"
        } else if img_path.ends_with(".webp") {
            "image/webp"
        } else if img_path.ends_with(".jpg") || img_path.ends_with(".jpeg") {
            "image/jpeg"
        } else if img_path.ends_with(".png") {
            "image/png"
        } else if img_path.ends_with(".svg") {
            "image/svg+xml"
        } else {
            "application/octet-stream"
        };

        return (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, content_type),
                (
                    header::CACHE_CONTROL,
                    "public, max-age=86400, s-maxage=604800",
                ),
            ],
            data,
        )
            .into_response();
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

    let resp = match state
        .http_client
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
        let reader = match ImageReader::new(Cursor::new(&original_bytes)).with_guessed_format() {
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
            (
                header::CACHE_CONTROL,
                "public, max-age=86400, s-maxage=604800",
            ),
        ],
        output_bytes,
    )
        .into_response()
}

/// Translate SEO-friendly image paths to R2 storage paths.
///
/// Supported patterns:
/// - `{orp}/{landmark-slug}.webp` (landmark in main municipality)
///   → `landmarks/{npu_catalog_id}.webp`
/// - `{orp}/{photo-slug}.webp` (municipality photo, main municipality)
///   → `municipalities/{municipality_code}/{photo-slug}.webp`
/// - `{orp}/{municipality}/{landmark-slug}.webp` (landmark in specific municipality)
///   → `landmarks/{npu_catalog_id}.webp`
/// - `{orp}/{municipality}/{photo-slug}.webp` (municipality photo)
///   → `municipalities/{municipality_code}/{photo-slug}.webp`
///
/// Known prefixes (municipalities/, landmarks/, pools/, regions/) pass through unchanged.
async fn resolve_seo_path(db: &sqlx::PgPool, path: &str) -> String {
    let known_prefixes = ["municipalities/", "landmarks/", "pools/", "regions/"];
    if known_prefixes.iter().any(|p| path.starts_with(p)) {
        return path.to_string();
    }

    let segments: Vec<&str> = path.split('/').collect();

    let (orp_slug, muni_slug, file_with_ext) = match segments.len() {
        2 => (segments[0], segments[0], segments[1]),
        3 => (segments[0], segments[1], segments[2]),
        _ => return path.to_string(),
    };

    // Strip extension to get the slug
    let slug = file_with_ext
        .rsplit_once('.')
        .map(|(s, _)| s)
        .unwrap_or(file_with_ext);

    // Single query: try landmark photo first, always return municipality_code for fallback
    #[derive(sqlx::FromRow)]
    struct SeoLookup {
        municipality_code: String,
        landmark_r2_key: Option<String>,
    }

    let lookup = sqlx::query_as::<_, SeoLookup>(
        "SELECT m.municipality_code, \
         (SELECT pm.r2_key FROM photo_metadata pm \
          JOIN landmarks l ON pm.entity_type = 'landmark' AND pm.entity_id = l.id \
          WHERE l.municipality_id = m.id AND l.slug = $3 AND pm.photo_index = 1 \
          LIMIT 1) as landmark_r2_key \
         FROM municipalities m \
         JOIN orp o ON m.orp_id = o.id \
         WHERE o.slug = $1 AND m.slug = $2",
    )
    .bind(orp_slug)
    .bind(muni_slug)
    .bind(slug)
    .fetch_optional(db)
    .await;

    match lookup {
        Ok(Some(row)) => {
            if let Some(r2_key) = row.landmark_r2_key {
                // Sanitize: ensure r2_key has no path traversal
                if !r2_key.contains("..") {
                    return r2_key;
                }
            }
            // Municipality photo fallback
            format!("municipalities/{}/{file_with_ext}", row.municipality_code)
        }
        Ok(None) => path.to_string(),
        Err(e) => {
            tracing::error!("DB error resolving SEO path '{path}': {e}");
            path.to_string()
        }
    }
}
