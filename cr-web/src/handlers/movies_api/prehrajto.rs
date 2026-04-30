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
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use serde::{Deserialize, Serialize};
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
/// Shared with [`super::prehrajto_resolver`] — both code paths hit the
/// same upstream (CZ proxy → prehraj.to), so the cap must be process-wide
/// rather than per-handler. `PREHRAJTO_VIDEO_SEMAPHORE` re-exported here
/// under the historical name to keep the legacy call sites unchanged.
pub(super) static PREHRAJTO_SCRAPE_SEMAPHORE: Semaphore = Semaphore::const_new(3);

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

/// Single source of truth for the "best upload first" ranking. Used by
/// both the listing endpoint (`/api/films/{id}/prehrajto-sources`) and
/// the stream endpoint's dead-upload fallback (`next_best_upload`); if
/// this changes, both paths change together so the first row of the
/// list is always the same upload the stream endpoint would try next.
///
/// The template's `parseResolutionHint` helper mirrors the resolution
/// buckets here — keep them in sync too.
///
/// After the #611 reader switch this references `video_sources` columns
/// (prefixed with `vs.` or unprefixed, depending on which SELECT uses it).
const PREHRAJTO_RANK_ORDER_BY: &str = r#"
    CASE vs.lang_class
      WHEN 'CZ_DUB'    THEN 6
      WHEN 'CZ_NATIVE' THEN 5
      WHEN 'CZ_SUB'    THEN 4
      WHEN 'SK_DUB'    THEN 3
      WHEN 'SK_SUB'    THEN 2
      WHEN 'UNKNOWN'   THEN 1
      ELSE 0
    END DESC,
    CASE LOWER(COALESCE(vs.resolution_hint, ''))
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
    COALESCE(vs.view_count, 0) DESC
"#;

/// Filter clause used by every prehrajto SELECT to read only rows from
/// the `prehrajto` provider in `video_sources`. Joins on `video_providers`
/// instead of hard-coding a provider_id so a fresh DB setup doesn't have
/// to care about the lookup row order.
const PREHRAJTO_JOIN: &str = "FROM video_sources vs \
                              JOIN video_providers p ON p.id = vs.provider_id \
                              WHERE p.slug = 'prehrajto'";

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
/// - `Ok(None)` **only** when the proxy explicitly reports `success: false`
///   (prehraj.to said "gone" — the upload-is-dead signal),
/// - `Err(code)` on any other outcome: proxy unreachable, non-2xx HTTP,
///   JSON parse failure, `success: true` with a missing/empty `videoUrl`,
///   or ambiguous/missing `success` field. Callers must not flip
///   `is_alive=FALSE` on `Err`, so keeping a transient proxy blip or a
///   truncated response out of that branch matters.
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
    interpret_proxy_response(&data)
}

/// Pure interpretation of a decoded proxy payload — split out of
/// [`scrape_content_url`] so the edge cases can be unit-tested without
/// spinning up an HTTP server.
///
/// * `success: true` with a non-empty `video_url` → `Ok(Some(url))` (hit).
/// * `success: false` → `Ok(None)` — the proxy's "prehraj.to says gone"
///   signal, the only shape the dead-upload path should react to.
/// * `success: true` with missing/empty `video_url`, or `success: null`
///   / missing → `Err("proxy-malformed")`. Both indicate a 2xx payload
///   that violates the contract, not a deleted upload, so callers must
///   leave `is_alive` unchanged.
fn interpret_proxy_response(data: &ProxyVideoResponse) -> Result<Option<String>, String> {
    match data.success {
        Some(true) => match data.video_url.as_deref() {
            Some(u) if !u.is_empty() => Ok(Some(u.to_string())),
            _ => Err("proxy-malformed".to_string()),
        },
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
    // a lock slot bounds the map to real rows in the prehrajto provider's
    // `video_sources` set.
    let row = match sqlx::query_as::<_, UploadRow>(
        "SELECT vs.film_id AS film_id, \
                COALESCE(vs.metadata->>'url', 'https://prehraj.to/' || vs.external_id) AS url \
           FROM video_sources vs \
           JOIN video_providers p ON p.id = vs.provider_id \
          WHERE p.slug = 'prehrajto' AND vs.external_id = $1 AND vs.is_alive = TRUE",
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
            // Classify the resolved URL once, and opportunistically
            // persist the flag for the "Další zdroje" DB listing (#521)
            // before deciding whether to redirect. The DB update happens
            // on both paths so a proxy upload eventually flips to
            // `is_direct = FALSE` even though we still refuse to
            // redirect to it — the list endpoint then shows it with the
            // "proxy" badge instead of optimistic "direct".
            let is_direct = is_allowed_stream_url(&video_url);
            persist_is_direct(state, upload_id, is_direct).await;

            // Belt-and-suspenders: the CZ proxy is a trusted component,
            // but a compromise or upstream change should not turn this
            // endpoint into an open redirect. Refuse anything off the
            // CDN allow-list (`premiumcdn.net`).
            if !is_direct {
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
                "UPDATE video_sources SET is_alive = FALSE, updated_at = NOW() \
                   WHERE provider_id = (SELECT id FROM video_providers WHERE slug = 'prehrajto') \
                     AND external_id = $1",
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

/// Write `is_direct` for this `upload_id` when it changes. Called on
/// every successful scrape so the DB-backed "Další zdroje" listing
/// (#521) reflects the latest direct/proxy classification prehraj.to
/// reports — no importer-time validate needed. A DB failure here is
/// non-fatal (the next scrape will try again); log and move on.
async fn persist_is_direct(state: &AppState, upload_id: &str, is_direct: bool) {
    // After the #611 reader switch, `is_direct` is stored inside
    // `video_sources.metadata` as a JSONB key rather than a typed column.
    // The UPDATE merges a single-key patch so other metadata fields
    // (url, qualities, …) stay intact. `jsonb_build_object` avoids string
    // interpolation and is immune to injection even if `is_direct` ever
    // came from an untrusted source (it doesn't — it's a bool we parsed
    // from the proxy response above).
    //
    // The WHERE clause checks current value via `metadata->'is_direct'`
    // to keep the UPDATE a no-op when already correct, preserving the
    // same "don't churn rows on cache hits" behavior the legacy query
    // had via `IS DISTINCT FROM`.
    let res = sqlx::query(
        "UPDATE video_sources \
           SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('is_direct', $1::boolean), \
               updated_at = NOW() \
         WHERE provider_id = (SELECT id FROM video_providers WHERE slug = 'prehrajto') \
           AND external_id = $2 \
           AND (metadata->>'is_direct')::BOOLEAN IS DISTINCT FROM $1",
    )
    .bind(is_direct)
    .bind(upload_id)
    .execute(&state.db)
    .await;
    if let Err(e) = res {
        tracing::warn!(upload_id, is_direct, error = ?e, "persist_is_direct failed");
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

static NEXT_BEST_UPLOAD_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT vs.external_id AS upload_id \
           {PREHRAJTO_JOIN} \
           AND vs.film_id = $1 AND vs.is_alive = TRUE \
           AND vs.external_id <> ALL($2) \
         ORDER BY {PREHRAJTO_RANK_ORDER_BY} LIMIT 1"
    )
});

async fn next_best_upload(
    state: &AppState,
    film_id: i32,
    tried: &HashSet<String>,
) -> Option<String> {
    let tried_vec: Vec<String> = tried.iter().cloned().collect();
    sqlx::query_scalar::<_, String>(&NEXT_BEST_UPLOAD_SQL)
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

/// One row in the "Další zdroje" listing. Public so the Serialize impl
/// produces a stable JSON contract for the template JS that consumes it.
#[derive(Serialize, FromRow)]
pub struct PrehrajtoSourceRow {
    pub upload_id: String,
    pub url: String,
    pub title: String,
    pub duration_sec: Option<i32>,
    pub resolution_hint: Option<String>,
    pub lang_class: String,
    /// `Some(true)` = resolved to a `premiumcdn.net` URL on the last hit,
    /// `Some(false)` = proxied, `None` = never resolved yet by the server
    /// (the template renders this as "unknown" and optimistically treats
    /// it like `direct` so existing playback flows keep working).
    pub is_direct: Option<bool>,
}

static PREHRAJTO_SOURCES_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT vs.external_id AS upload_id, \
                COALESCE(vs.metadata->>'url', 'https://prehraj.to/' || vs.external_id) AS url, \
                COALESCE(vs.title, '') AS title, \
                vs.duration_sec, vs.resolution_hint, vs.lang_class, \
                (vs.metadata->>'is_direct')::BOOLEAN AS is_direct \
           {PREHRAJTO_JOIN} \
           AND vs.film_id = $1 AND vs.is_alive = TRUE \
         ORDER BY {PREHRAJTO_RANK_ORDER_BY}"
    )
});

/// `GET /api/films/{film_id}/prehrajto-sources` — list of all alive
/// uploads for a film, ranked identically to the stream endpoint's
/// fallback (`next_best_upload`) by reusing [`PREHRAJTO_RANK_ORDER_BY`].
/// Replaces the legacy live-scrape path for the detail-page "Další
/// zdroje" block once `PREHRAJTO_SOURCES_FROM_DB=1`.
pub async fn prehrajto_sources(
    State(state): State<AppState>,
    Path(film_id): Path<i32>,
) -> Response {
    match sqlx::query_as::<_, PrehrajtoSourceRow>(&PREHRAJTO_SOURCES_SQL)
        .bind(film_id)
        .fetch_all(&state.db)
        .await
    {
        Ok(rows) => Json(json!({
            "film_id": film_id,
            "count": rows.len(),
            "sources": rows,
        }))
        .into_response(),
        Err(e) => {
            tracing::error!(film_id, error = ?e, "prehrajto_sources DB query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response()
        }
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

    // --- `interpret_proxy_response` -----------------------------------
    // These guard the dead-vs-malformed distinction: only `success: false`
    // may flip `is_alive=FALSE`; everything else must surface as `Err` so
    // callers leave the row alone.

    #[test]
    fn interpret_success_true_with_url_is_hit() {
        let data = ProxyVideoResponse {
            success: Some(true),
            video_url: Some("https://cdn.example/x.mp4?expires=1".to_string()),
        };
        assert_eq!(
            interpret_proxy_response(&data).unwrap(),
            Some("https://cdn.example/x.mp4?expires=1".to_string())
        );
    }

    #[test]
    fn interpret_success_false_is_dead() {
        let data = ProxyVideoResponse {
            success: Some(false),
            video_url: None,
        };
        assert_eq!(interpret_proxy_response(&data).unwrap(), None);

        // Belt-and-suspenders: `success: false` with a stray URL still
        // means dead — we trust the explicit "gone" signal over the URL.
        let with_stray = ProxyVideoResponse {
            success: Some(false),
            video_url: Some("https://cdn.example/x.mp4".to_string()),
        };
        assert_eq!(interpret_proxy_response(&with_stray).unwrap(), None);
    }

    #[test]
    fn interpret_success_null_is_malformed() {
        let data = ProxyVideoResponse {
            success: None,
            video_url: Some("https://cdn.example/x.mp4".to_string()),
        };
        assert_eq!(
            interpret_proxy_response(&data).unwrap_err(),
            "proxy-malformed"
        );
    }

    #[test]
    fn interpret_success_true_without_url_is_malformed() {
        let missing = ProxyVideoResponse {
            success: Some(true),
            video_url: None,
        };
        assert_eq!(
            interpret_proxy_response(&missing).unwrap_err(),
            "proxy-malformed"
        );

        let empty = ProxyVideoResponse {
            success: Some(true),
            video_url: Some(String::new()),
        };
        assert_eq!(
            interpret_proxy_response(&empty).unwrap_err(),
            "proxy-malformed"
        );
    }

    // --- `PrehrajtoSourceRow` JSON contract -----------------------------
    // Locks the JSON shape the film-detail template JS depends on. If a
    // field is renamed or dropped here the template stops rendering the
    // "Další zdroje" block, so this test is cheap insurance.

    #[test]
    fn source_row_serializes_with_expected_fields() {
        let row = PrehrajtoSourceRow {
            upload_id: "558bd2364b350".to_string(),
            url: "https://prehraj.to/some-slug-558bd2364b350".to_string(),
            title: "Example Movie 1080p CZ".to_string(),
            duration_sec: Some(5412),
            resolution_hint: Some("1080p".to_string()),
            lang_class: "CZ_DUB".to_string(),
            is_direct: Some(true),
        };
        let json = serde_json::to_value(&row).expect("serialize");
        assert_eq!(json["upload_id"], "558bd2364b350");
        assert_eq!(json["url"], "https://prehraj.to/some-slug-558bd2364b350");
        assert_eq!(json["title"], "Example Movie 1080p CZ");
        assert_eq!(json["duration_sec"], 5412);
        assert_eq!(json["resolution_hint"], "1080p");
        assert_eq!(json["lang_class"], "CZ_DUB");
        assert_eq!(json["is_direct"], true);
    }

    #[test]
    fn source_row_serializes_null_is_direct_as_json_null() {
        let row = PrehrajtoSourceRow {
            upload_id: "0123456789abc".to_string(),
            url: "https://prehraj.to/x-0123456789abc".to_string(),
            title: "Unknown".to_string(),
            duration_sec: None,
            resolution_hint: None,
            lang_class: "UNKNOWN".to_string(),
            is_direct: None,
        };
        let json = serde_json::to_value(&row).expect("serialize");
        assert!(
            json["is_direct"].is_null(),
            "template JS treats null as unknown/optimistic direct"
        );
        assert!(json["duration_sec"].is_null());
        assert!(json["resolution_hint"].is_null());
    }
}
