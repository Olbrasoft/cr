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
