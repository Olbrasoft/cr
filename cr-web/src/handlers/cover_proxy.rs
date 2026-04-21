//! Shared helpers for the `/filmy-online/{slug}.webp`,
//! `/serialy-online/{slug}.webp` and `/tv-porady/{slug}.webp` cover
//! routes (plus their `-large` variants).
//!
//! Post sub-issue #576 we stopped storing covers on the cr-web container
//! and moved to an id-keyed R2 layout:
//!
//!     cr-images:films/{id}/cover.webp       (200×300)
//!     cr-images:films/{id}/cover-large.webp (780×1170)
//!     cr-images:series/{id}/cover.webp      …
//!     cr-images:tv-shows/{id}/cover.webp    …
//!
//! These are fronted by the `cr-img-proxy` Cloudflare Worker at the
//! `/img/{prefix}/…` path. The handler translates the user-facing
//! `/filmy-online/{slug}.webp` URL to `/img/films/{id}/{variant}` and
//! proxies the worker response back to the browser. The small
//! round-trip is paid only once per (edge, cover) — CF then caches the
//! response for a year thanks to our `immutable` response header.
//!
//! During the rollout window we also fall back to the OLD layout
//! (`{prefix}/{cover_filename}.webp` without the `{id}/` segment) so
//! covers keep serving if a migration step fails or is in flight. Sub B
//! (#577) removes the fallback once all three tables have been
//! verified.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::state::AppState;

/// `image/webp` bytes wrapped with a `public, max-age=31536000, immutable`
/// cache header. The immutable part is safe because every new cover
/// lives at a distinct `{id}/cover.webp` URL — re-uploaded covers MUST
/// purge via the existing `/admin/cache/` flow.
pub fn immutable_webp(bytes: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/webp"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        bytes,
    )
        .into_response()
}

/// `image/webp` bytes wrapped with `no-store`. Used when we serve
/// *something* under a URL that may get a better answer soon — e.g.
/// the `-large.webp` endpoint falling back to the small variant while
/// the large is not yet on R2. If we returned those bytes with
/// `immutable`, CF and browsers would cache the low-res image for a
/// year and never pick up the large file that imports later.
pub fn no_store_webp(bytes: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/webp"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        bytes,
    )
        .into_response()
}

/// Tiny 1×1 transparent WebP. Same bytes as the pre-#576 handlers used.
/// `no-store` is deliberate — a missing cover is a transient state
/// (import will fill it in shortly), caching the placeholder pins an
/// empty card in the browser for hours after the real WebP lands.
pub fn placeholder_webp() -> Response {
    static BYTES: &[u8] = &[
        0x52, 0x49, 0x46, 0x46, 0x1a, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38,
        0x4c, 0x0d, 0x00, 0x00, 0x00, 0x2f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ];
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/webp"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        BYTES.to_vec(),
    )
        .into_response()
}

/// Return a 1×1 placeholder whose MIME type matches `ext` (`"jpg"`,
/// `"png"` or `"webp"`). Used by the dynamic-TMDB handlers so a request
/// to `…-large.jpg` that can't reach TMDB still gets bytes the browser
/// (and OG scrapers) can parse as JPEG — a WebP fallback would mismatch
/// the declared `og:image:type`.
pub fn placeholder_for_ext(ext: &str) -> Response {
    match ext {
        // Smallest baseline JPEG that decodes cleanly in every browser.
        "jpg" | "jpeg" => {
            static BYTES: &[u8] = &[
                0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00,
                0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43, 0x00, 0x08, 0x06, 0x06,
                0x07, 0x06, 0x05, 0x08, 0x07, 0x07, 0x07, 0x09, 0x09, 0x08, 0x0A, 0x0C, 0x14, 0x0D,
                0x0C, 0x0B, 0x0B, 0x0C, 0x19, 0x12, 0x13, 0x0F, 0x14, 0x1D, 0x1A, 0x1F, 0x1E, 0x1D,
                0x1A, 0x1C, 0x1C, 0x20, 0x24, 0x2E, 0x27, 0x20, 0x22, 0x2C, 0x23, 0x1C, 0x1C, 0x28,
                0x37, 0x29, 0x2C, 0x30, 0x31, 0x34, 0x34, 0x34, 0x1F, 0x27, 0x39, 0x3D, 0x38, 0x32,
                0x3C, 0x2E, 0x33, 0x34, 0x32, 0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01, 0x00, 0x01,
                0x01, 0x01, 0x11, 0x00, 0xFF, 0xC4, 0x00, 0x1F, 0x00, 0x00, 0x01, 0x05, 0x01, 0x01,
                0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
                0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0xFF, 0xC4, 0x00, 0xB5, 0x10,
                0x00, 0x02, 0x01, 0x03, 0x03, 0x02, 0x04, 0x03, 0x05, 0x05, 0x04, 0x04, 0x00, 0x00,
                0x01, 0x7D, 0x01, 0x02, 0x03, 0x00, 0x04, 0x11, 0x05, 0x12, 0x21, 0x31, 0x41, 0x06,
                0x13, 0x51, 0x61, 0x07, 0x22, 0x71, 0x14, 0x32, 0x81, 0x91, 0xA1, 0x08, 0x23, 0x42,
                0xB1, 0xC1, 0x15, 0x52, 0xD1, 0xF0, 0x24, 0x33, 0x62, 0x72, 0x82, 0x09, 0x0A, 0x16,
                0x17, 0x18, 0x19, 0x1A, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x34, 0x35, 0x36, 0x37,
                0x38, 0x39, 0x3A, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x53, 0x54, 0x55,
                0x56, 0x57, 0x58, 0x59, 0x5A, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6A, 0x73,
                0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7A, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89,
                0x8A, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0xA2, 0xA3, 0xA4, 0xA5,
                0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA,
                0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6,
                0xD7, 0xD8, 0xD9, 0xDA, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA,
                0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFF, 0xDA, 0x00, 0x08,
                0x01, 0x01, 0x00, 0x00, 0x3F, 0x00, 0xFB, 0xD0, 0x07, 0xFF, 0xD9,
            ];
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "image/jpeg"),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                BYTES.to_vec(),
            )
                .into_response()
        }
        "png" => {
            static BYTES: &[u8] = &[
                0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
                0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x04, 0x00, 0x00,
                0x00, 0xB5, 0x1C, 0x0C, 0x02, 0x00, 0x00, 0x00, 0x0B, 0x49, 0x44, 0x41, 0x54, 0x78,
                0x9C, 0x63, 0xFA, 0xCF, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0xE5, 0x27, 0xDE, 0xFC,
                0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
            ];
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "image/png"),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                BYTES.to_vec(),
            )
                .into_response()
        }
        _ => placeholder_webp(),
    }
}

fn public_base(state: &AppState) -> &str {
    // Prod: `image_base_url` is empty and we fall back to the canonical
    // site hostname. Dev: set IMAGE_BASE_URL=http://dev.localhost:3000
    // or similar to route image fetches at a dev-facing worker.
    let b = state.image_base_url.as_str();
    if b.is_empty() {
        "https://ceskarepublika.wiki"
    } else {
        b
    }
}

pub async fn try_fetch_r2(state: &AppState, key: &str) -> Option<Vec<u8>> {
    let url = format!("{}/img/{key}", public_base(state));
    let resp = state
        .http_client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.bytes().await.ok().map(|b| b.to_vec())
}

/// Fetch a cover from R2 via the img-proxy worker, preferring the new
/// id-keyed layout and falling back to the old name-keyed one.
///
/// `new_key` / `old_key` are R2 object keys like `films/29876/cover.webp`
/// and `films/children-of-the-sea.webp`. Returns a ready `Response` —
/// the placeholder 1×1 WebP when both keys miss, so clients never see a
/// 404 for a cover URL.
pub async fn fetch_cover(state: &AppState, new_key: &str, old_key: Option<&str>) -> Response {
    if let Some(bytes) = try_fetch_r2(state, new_key).await {
        return immutable_webp(bytes);
    }
    if let Some(old) = old_key
        && let Some(bytes) = try_fetch_r2(state, old).await
    {
        return immutable_webp(bytes);
    }
    placeholder_webp()
}

/// Strip the `.webp` / `-large.webp` suffix from an incoming path param
/// and return `(slug, is_large)`.
pub fn parse_cover_slug(slug_webp: &str) -> (String, bool) {
    if let Some(s) = slug_webp.strip_suffix("-large.webp") {
        (s.to_string(), true)
    } else if let Some(s) = slug_webp.strip_suffix(".webp") {
        (s.to_string(), false)
    } else {
        (slug_webp.to_string(), false)
    }
}

/// Returns the R2 key for a cover given `table_prefix`, `id`, and
/// `is_large`. `table_prefix` is the top-level R2 prefix
/// (`films`, `series`, `tv-shows`).
pub fn new_r2_key(table_prefix: &str, id: i32, is_large: bool) -> String {
    let variant = if is_large { "cover-large" } else { "cover" };
    format!("{table_prefix}/{id}/{variant}.webp")
}

/// Old-layout key we try as a fallback when the new key isn't on R2
/// yet. `cover_filename` is the pre-#576 slug-ish identifier stored in
/// the `{table}.cover_filename` column. Large variants historically
/// lived under `/large/` with either the row's `slug` or its
/// `cover_filename` — we accept the `cover_filename` form here and let
/// the handler try the slug as a second fallback if it wants.
pub fn old_r2_key(table_prefix: &str, cover_filename: &str, is_large: bool) -> String {
    if is_large {
        format!("{table_prefix}/large/{cover_filename}.webp")
    } else {
        format!("{table_prefix}/{cover_filename}.webp")
    }
}
