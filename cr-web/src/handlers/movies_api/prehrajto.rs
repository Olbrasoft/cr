//! Resolve a prehraj.to upload to a tokenized CDN URL.
//!
//! `GET /api/movies/stream/{upload_id}` replaces the old live-scrape path
//! (issue #522). On cache hit it 302-redirects without hitting prehraj.to;
//! on miss it scrapes once via `cz_proxy` (action=video), caches for
//! `token_expires - 60 s`, and redirects. Concurrent requests for the same
//! `upload_id` share a per-key async mutex so only one scrape runs.
//!
//! When the upload is dead (proxy says no `contentUrl`), the row is marked
//! `is_alive = FALSE` and we walk up to three fallback uploads for the same
//! `film_id`, ranked the same way the importer picks the primary
//! (lang-class, resolution hint, view count — see
//! `scripts/import-prehrajto-uploads.py::rank`).
//!
//! Parent epic: #518. Depends on schema migration `20260508_048`.
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use serde::Deserialize;
use serde_json::json;
use sqlx::FromRow;
use tokio::sync::Semaphore;

use crate::state::{AppState, CachedStreamUrl};

use super::cz_proxy::cz_proxy_config;
use super::thumbnail::is_allowed_stream_url;

/// Outbound concurrency cap against prehraj.to (via the CZ proxy). Three
/// concurrent scrapes keep peak load modest while still letting unrelated
/// upload_ids progress in parallel. Shared with future prehraj.to code
/// paths — defined here to live close to its first user.
static PREHRAJTO_SCRAPE_SEMAPHORE: Semaphore = Semaphore::const_new(3);

/// Maximum number of resolve attempts per request — the initial upload
/// plus up to N-1 dead-upload fallbacks. Three is a pragmatic ceiling:
/// if the top three alive uploads for a film all 404 on the CDN,
/// re-running the sitemap sync is the right fix, not endless fallback
/// hops at request time.
const MAX_RESOLVE_ATTEMPTS: usize = 3;

/// Safety margin subtracted from the token's reported `expires=` timestamp
/// before caching. Prevents serving a URL that will 403 between cache
/// check and client request.
const TOKEN_SAFETY_MARGIN: Duration = Duration::from_secs(60);

/// Conservative fallback when `expires=` is missing/unparseable — two
/// hours matches the typical observed prehraj.to token lifetime.
const DEFAULT_TOKEN_LIFETIME: Duration = Duration::from_secs(2 * 3600);

/// prehraj.to upload ids are 13-hex (older) or 16-hex (newer); anything
/// else is definitely not a real upload and we can reject it early.
pub(crate) fn is_valid_upload_id(s: &str) -> bool {
    matches!(s.len(), 13 | 16)
        && s.chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

#[derive(FromRow)]
struct UploadRow {
    film_id: i32,
    url: String,
}

#[derive(Deserialize)]
struct ProxyVideoResponse {
    success: Option<bool>,
    #[serde(rename = "videoUrl")]
    video_url: Option<String>,
}

/// Pull `expires=<unix-sec>` out of a tokenized CDN URL and translate it
/// to an `Instant` on the monotonic clock. Returns `None` when the query
/// param is missing or already elapsed — caller falls back to a default
/// lifetime.
fn token_expiry_instant(url: &str) -> Option<Instant> {
    let parsed = reqwest::Url::parse(url).ok()?;
    let exp_sec: u64 = parsed
        .query_pairs()
        .find(|(k, _)| k == "expires")?
        .1
        .parse()
        .ok()?;
    let target_wall = UNIX_EPOCH + Duration::from_secs(exp_sec);
    let remaining = target_wall.duration_since(SystemTime::now()).ok()?;
    Some(Instant::now() + remaining)
}

/// `entry.expires_at` already has [`TOKEN_SAFETY_MARGIN`] subtracted when
/// inserted (see scrape-success branch in `do_scrape`), so the freshness
/// check is a plain comparison against `now` — applying the margin again
/// here would make cached entries go stale ~60 s early.
fn is_fresh_enough(entry: &CachedStreamUrl, now: Instant) -> bool {
    entry.expires_at > now
}

async fn cached_fresh(state: &AppState, upload_id: &str) -> Option<String> {
    let entry = state
        .prehrajto_stream_cache
        .get(&upload_id.to_string())
        .await?;
    if is_fresh_enough(&entry, Instant::now()) {
        Some(entry.url)
    } else {
        None
    }
}

async fn per_key_lock(state: &AppState, upload_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut map = state.prehrajto_in_flight.lock().await;
    map.entry(upload_id.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

/// One scrape pass against the CZ proxy. Returns:
/// - `Ok(Some(url))` on success (real tokenized CDN URL),
/// - `Ok(None)` only when the proxy explicitly reports `success: false`
///   (prehraj.to said "gone" — the upload-is-dead signal),
/// - `Err(code)` on any other outcome: proxy unreachable, non-2xx HTTP,
///   JSON parse failure, or ambiguous/missing `success` field. Callers
///   must not flip `is_alive=FALSE` on `Err`, so keeping a transient
///   proxy blip or a truncated response out of this branch matters.
///
/// Error codes are **coarse and URL-free**: the proxy URL contains the
/// shared `key=` secret, so we never embed the raw reqwest Display
/// output (which includes that URL) in the returned message or logs.
async fn scrape_content_url(state: &AppState, detail_url: &str) -> Result<Option<String>, String> {
    let Some((proxy_url, proxy_key)) = cz_proxy_config(&state.config) else {
        return Err("proxy-not-configured".to_string());
    };
    let api_url = format!(
        "{}?action=video&url={}&key={}",
        proxy_url,
        urlencoding::encode(detail_url),
        proxy_key,
    );
    let _permit = PREHRAJTO_SCRAPE_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| "semaphore-closed".to_string())?;
    let resp = state
        .http_client
        .get(&api_url)
        .timeout(Duration::from_secs(25))
        .send()
        .await
        .map_err(|e| format!("proxy-transport-{}", classify_reqwest_error(&e)))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("proxy-http-{}", status.as_u16()));
    }
    let data: ProxyVideoResponse = resp
        .json()
        .await
        .map_err(|e| format!("proxy-parse-{}", classify_reqwest_error(&e)))?;
    match data.success {
        Some(true) => Ok(data.video_url.filter(|u| !u.is_empty())),
        Some(false) => Ok(None),
        None => Err("proxy-malformed".to_string()),
    }
}

/// Coarse, URL-free category for a `reqwest::Error`. The default `Display`
/// impl includes the full request URL — which for us carries the CZ proxy
/// `key=` secret — so we never stringify the raw error; we only report
/// which stage failed.
fn classify_reqwest_error(e: &reqwest::Error) -> &'static str {
    if e.is_timeout() {
        "timeout"
    } else if e.is_connect() {
        "connect"
    } else if e.is_decode() {
        "decode"
    } else if e.is_body() {
        "body"
    } else if e.is_request() {
        "request"
    } else {
        "other"
    }
}

/// Result of a single resolve attempt: either a playable URL, a "this
/// upload is dead — try the next one on its film" signal, or a hard error.
enum TryResolveOutcome {
    Resolved(String),
    DeadUpload { film_id: i32 },
    HardError(Response),
}

async fn try_resolve_one(state: &AppState, upload_id: &str) -> TryResolveOutcome {
    // Fast path: fresh cached URL — no DB, no lock.
    if let Some(url) = cached_fresh(state, upload_id).await {
        tracing::debug!(upload_id, result = "cache-hit", "resolved");
        return TryResolveOutcome::Resolved(url);
    }

    // DB lookup first. The per-key in-flight map is an unbounded
    // HashMap<String, Arc<Mutex>>; inserting an entry for every
    // valid-looking hex id a client throws at us would be a memory-growth
    // vector. Confirming the upload exists (and is alive) before reserving
    // a lock slot bounds the map to real rows in `film_prehrajto_uploads`.
    let row = match sqlx::query_as::<_, UploadRow>(
        "SELECT film_id, url FROM film_prehrajto_uploads \
         WHERE upload_id = $1 AND is_alive = TRUE",
    )
    .bind(upload_id)
    .fetch_optional(&state.db)
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            tracing::warn!(upload_id, result = "not-found", "no alive row in DB");
            return TryResolveOutcome::HardError(no_sources_response());
        }
        Err(e) => {
            tracing::error!(upload_id, error = ?e, "db lookup failed");
            return TryResolveOutcome::HardError(
                (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response(),
            );
        }
    };

    // Per-key exclusion so only one scrape per upload_id runs concurrently.
    let lock = per_key_lock(state, upload_id).await;
    let outcome = {
        let _guard = lock.lock().await;
        if let Some(url) = cached_fresh(state, upload_id).await {
            tracing::debug!(upload_id, result = "cache-hit-after-wait", "resolved");
            TryResolveOutcome::Resolved(url)
        } else {
            do_scrape(state, upload_id, &row).await
        }
    };

    // Evict the in-flight entry once we're the last task holding it so the
    // map doesn't accumulate one entry per ever-requested upload over the
    // process lifetime.
    release_per_key_lock(state, upload_id, lock).await;

    outcome
}

async fn do_scrape(state: &AppState, upload_id: &str, row: &UploadRow) -> TryResolveOutcome {
    let scrape_started = Instant::now();
    match scrape_content_url(state, &row.url).await {
        Ok(Some(video_url)) => {
            // Belt-and-suspenders: the CZ proxy is a trusted component,
            // but a compromise or upstream change should not turn this
            // endpoint into an open redirect. Refuse anything off the
            // CDN allow-list (`premiumcdn.net`).
            if !is_allowed_stream_url(&video_url) {
                tracing::error!(
                    upload_id,
                    "resolved URL not on CDN allow-list — refusing to redirect"
                );
                return TryResolveOutcome::HardError(
                    (StatusCode::BAD_GATEWAY, "resolved URL rejected").into_response(),
                );
            }
            let latency_ms = scrape_started.elapsed().as_millis();
            let deadline = token_expiry_instant(&video_url)
                .unwrap_or_else(|| Instant::now() + DEFAULT_TOKEN_LIFETIME);
            let expires_at = deadline
                .checked_sub(TOKEN_SAFETY_MARGIN)
                .unwrap_or(deadline);
            state
                .prehrajto_stream_cache
                .insert(
                    upload_id.to_string(),
                    CachedStreamUrl {
                        url: video_url.clone(),
                        expires_at,
                    },
                )
                .await;
            tracing::info!(
                upload_id,
                latency_ms,
                result = "scraped",
                "cache miss resolved"
            );
            TryResolveOutcome::Resolved(video_url)
        }
        Ok(None) => {
            tracing::warn!(
                upload_id,
                film_id = row.film_id,
                result = "dead",
                "no contentUrl — marking is_alive=FALSE"
            );
            if let Err(e) = sqlx::query(
                "UPDATE film_prehrajto_uploads SET is_alive = FALSE WHERE upload_id = $1",
            )
            .bind(upload_id)
            .execute(&state.db)
            .await
            {
                tracing::error!(upload_id, error = ?e, "failed to mark dead");
            }
            TryResolveOutcome::DeadUpload {
                film_id: row.film_id,
            }
        }
        Err(e) => {
            // `e` is one of the coarse codes emitted by
            // `scrape_content_url` (`proxy-transport-timeout`,
            // `proxy-http-502`, …) — never a raw reqwest Display, which
            // would include the CZ proxy `key=`. Safe to log and still
            // hidden from the client behind a generic status+message.
            tracing::error!(upload_id, error = %e, "scrape failed");
            let (status, message) = if e == "proxy-not-configured" {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "scrape proxy not configured",
                )
            } else {
                (StatusCode::BAD_GATEWAY, "scrape failed")
            };
            TryResolveOutcome::HardError((status, message).into_response())
        }
    }
}

/// Drop the map entry for `upload_id` when no other task is still using
/// the same `Arc<Mutex>`. We hold the map lock during the check, so no
/// new clones can appear; existing clones can only be dropped during the
/// check, which makes the count monotone-decreasing — so `<= 2` (our
/// local + the map entry) is a safe upper bound for "no other holders".
async fn release_per_key_lock(
    state: &AppState,
    upload_id: &str,
    lock: Arc<tokio::sync::Mutex<()>>,
) {
    let mut map = state.prehrajto_in_flight.lock().await;
    if Arc::strong_count(&lock) <= 2 {
        map.remove(upload_id);
    }
}

async fn next_best_upload(
    state: &AppState,
    film_id: i32,
    tried: &HashSet<String>,
) -> Option<String> {
    let tried_vec: Vec<String> = tried.iter().cloned().collect();
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT upload_id FROM film_prehrajto_uploads
        WHERE film_id = $1
          AND is_alive = TRUE
          AND upload_id <> ALL($2)
        ORDER BY
          CASE lang_class
            WHEN 'CZ_DUB'    THEN 6
            WHEN 'CZ_NATIVE' THEN 5
            WHEN 'CZ_SUB'    THEN 4
            WHEN 'SK_DUB'    THEN 3
            WHEN 'SK_SUB'    THEN 2
            WHEN 'UNKNOWN'   THEN 1
            ELSE 0
          END DESC,
          CASE LOWER(COALESCE(resolution_hint, ''))
            WHEN '2160p'  THEN 6
            WHEN 'bluray' THEN 5
            WHEN '1080p'  THEN 5
            WHEN '720p'   THEN 4
            WHEN 'bdrip'  THEN 4
            WHEN 'webrip' THEN 4
            WHEN 'web-dl' THEN 4
            WHEN 'hdrip'  THEN 3
            WHEN 'hdtv'   THEN 3
            WHEN '480p'   THEN 2
            WHEN 'dvdrip' THEN 2
            WHEN 'tvrip'  THEN 2
            ELSE 1
          END DESC,
          COALESCE(view_count, 0) DESC
        LIMIT 1
        "#,
    )
    .bind(film_id)
    .bind(&tried_vec)
    .fetch_optional(&state.db)
    .await
    .ok()
    .flatten()
}

async fn resolve_with_fallback(state: &AppState, initial: String) -> Result<String, Response> {
    let mut tried: HashSet<String> = HashSet::new();
    let mut current = initial;

    for _ in 0..MAX_RESOLVE_ATTEMPTS {
        if !tried.insert(current.clone()) {
            break;
        }
        match try_resolve_one(state, &current).await {
            TryResolveOutcome::Resolved(url) => return Ok(url),
            TryResolveOutcome::DeadUpload { film_id } => {
                match next_best_upload(state, film_id, &tried).await {
                    Some(next) => current = next,
                    None => return Err(no_sources_response()),
                }
            }
            TryResolveOutcome::HardError(resp) => return Err(resp),
        }
    }
    Err(no_sources_response())
}

fn no_sources_response() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": "no-sources"}))).into_response()
}

/// `GET /api/movies/stream/{upload_id}` — resolves to a fresh tokenized
/// CDN URL (cached when possible) and 302-redirects.
pub async fn prehrajto_stream_upload(
    State(state): State<AppState>,
    Path(upload_id): Path<String>,
) -> Response {
    let upload_id = upload_id.trim().to_ascii_lowercase();
    if !is_valid_upload_id(&upload_id) {
        return (StatusCode::BAD_REQUEST, "invalid upload_id").into_response();
    }

    match resolve_with_fallback(&state, upload_id).await {
        Ok(url) => Redirect::temporary(&url).into_response(),
        Err(resp) => resp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_13_hex_and_16_hex_lowercase() {
        assert!(is_valid_upload_id("558bd2364b350"));
        assert!(is_valid_upload_id("558bd2364b350abc"));
        assert!(is_valid_upload_id("0123456789abc"));
    }

    #[test]
    fn rejects_other_lengths_or_non_hex() {
        assert!(!is_valid_upload_id(""));
        assert!(!is_valid_upload_id("abc"));
        assert!(!is_valid_upload_id("558bd2364b35")); // 12
        assert!(!is_valid_upload_id("558bd2364b3500ab1")); // 17
        assert!(!is_valid_upload_id("558bd2364b35z")); // non-hex
        assert!(!is_valid_upload_id("558BD2364B350")); // upper-case
    }

    #[test]
    fn token_expiry_parses_expires_query() {
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let url = format!("https://cdn.example.com/x.mp4?expires={future}&signature=abc");
        let instant = token_expiry_instant(&url).expect("should parse");
        let remaining = instant.saturating_duration_since(Instant::now());
        // Allow a large margin — test just needs to confirm it landed near +1h.
        assert!(remaining > Duration::from_secs(60 * 59));
        assert!(remaining < Duration::from_secs(60 * 61));
    }

    #[test]
    fn token_expiry_returns_none_for_past_or_missing_expires() {
        assert_eq!(token_expiry_instant("https://cdn.example.com/x.mp4"), None);
        assert_eq!(
            token_expiry_instant("https://cdn.example.com/x.mp4?expires=1"),
            None,
            "past timestamp should fall through"
        );
    }

    #[test]
    fn freshness_predicate_uses_adjusted_expiry() {
        // `expires_at` is already stored with TOKEN_SAFETY_MARGIN subtracted,
        // so the check is a plain `> now` — applying the margin again here
        // would make cache entries go stale ~60 s early.
        let now = Instant::now();
        let entry_fresh = CachedStreamUrl {
            url: "https://cdn/x".to_string(),
            expires_at: now + Duration::from_secs(10),
        };
        assert!(is_fresh_enough(&entry_fresh, now));

        let entry_at_now = CachedStreamUrl {
            url: "https://cdn/x".to_string(),
            expires_at: now,
        };
        assert!(
            !is_fresh_enough(&entry_at_now, now),
            "expires_at == now counts as stale (strict >)"
        );

        let entry_stale = CachedStreamUrl {
            url: "https://cdn/x".to_string(),
            expires_at: now.checked_sub(Duration::from_secs(1)).unwrap_or(now),
        };
        assert!(!is_fresh_enough(&entry_stale, now));
    }
}
