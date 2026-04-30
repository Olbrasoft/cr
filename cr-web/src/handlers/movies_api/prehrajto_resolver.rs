//! Search-then-video resolver for prehraj.to (issue #633, parent #631).
//!
//! Replaces the cached-`external_id` flow in [`prehrajto.rs`] with a
//! search-first one. prehraj.to rotates upload IDs on every re-upload, so a
//! cached ID goes 404 within days/weeks; the only stable inputs are the
//! search query + variant (CZ_DUB / CZ_SUB / RES_2160P / RES_1080P) we
//! persist in `prehrajto_search_hints` (#632).
//!
//! Flow per request:
//!
//!   1. Look up the hint by `(owner_id, variant)`.
//!   2. Hit the proxy `action=search` (cached for 30 min in
//!      [`AppState::prehrajto_search_cache`]) to get fresh upload candidates.
//!   3. Filter candidates by variant (regex on title) and rank.
//!   4. For the top N candidates, hit `action=video` to get a tokenized
//!      CDN URL — reuses the existing [`AppState::prehrajto_stream_cache`]
//!      so a successful resolve from the legacy endpoint also serves this
//!      one and vice versa.
//!   5. 302 to the first working candidate; 404 with "Žádný funkční zdroj"
//!      if the top N all fail.
//!
//! Endpoint: `GET /api/movies/stream/by-hint/{owner_kind}/{owner_id}/{variant}`
//! where `owner_kind ∈ {"film", "episode", "tv-episode"}` and `variant` is
//! the table CHECK value verbatim.

use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use regex::Regex;
use serde::Deserialize;

use crate::state::{AppState, CachedStreamUrl};

use super::cz_proxy::cz_proxy_config;
use super::prehrajto_hints::{
    PrehrajtoSearchHint, find_for_episode, find_for_film, find_for_tv_episode,
};
use super::thumbnail::is_allowed_stream_url;

/// Maximum candidates to attempt per request before giving up. The first
/// hit wins; the loop only advances on dead-upload signals (proxy
/// `success: false`) or hard CDN allow-list rejections — transient proxy
/// blips bubble up as 502 instead of silently masking real outages.
const MAX_CANDIDATES_PER_REQUEST: usize = 3;

/// Safety margin under the token's `expires=` timestamp before we treat a
/// cached entry as stale. Mirrors the legacy resolver in `prehrajto.rs`.
const TOKEN_SAFETY_MARGIN: Duration = Duration::from_secs(60);

/// Conservative fallback when `expires=` is missing/unparseable. Matches
/// observed prehraj.to token lifetime (~2 h).
const DEFAULT_TOKEN_LIFETIME: Duration = Duration::from_secs(2 * 3600);

// --- Cache types ---------------------------------------------------------

/// Slimmed-down search candidate (just what the resolver needs).
/// Cached as a list per `search_query`; a single search round-trip can
/// serve every variant of the same hint.
#[derive(Clone, Debug)]
pub struct SearchCandidate {
    /// Detail-page URL we hand back to `action=video`.
    pub url: String,
    /// Filename / human title — used by the variant regex matcher.
    pub title: String,
    /// 13- or 16-hex upload id parsed out of `url`. Used as the cache key
    /// for [`AppState::prehrajto_stream_cache`] so the search-then-video
    /// flow shares cached CDN URLs with the legacy upload-id endpoint.
    pub upload_id: String,
}

// --- URL parsing helpers -------------------------------------------------

/// Pull the trailing `/<13|16-hex>` segment out of a prehraj.to detail URL.
/// Returns `None` for unexpected shapes — caller drops the candidate.
pub(crate) fn parse_upload_id(url: &str) -> Option<String> {
    let trimmed = url.trim_end_matches('/');
    let last = trimmed.rsplit_once('/').map(|(_, t)| t)?;
    let last = last.split(['?', '#']).next().unwrap_or(last);
    if matches!(last.len(), 13 | 16)
        && last
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        Some(last.to_string())
    } else {
        None
    }
}

/// Translate `expires=<unix-sec>` from a tokenized CDN URL into a
/// monotonic `Instant`. `None` when missing/unparseable/already-elapsed.
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

// --- Variant matcher -----------------------------------------------------

/// Compiled regexes are reused across requests. Patterns are deliberately
/// permissive — prehraj.to filenames are messy ("CZ.dabing", "Češt.",
/// "cz-titulky", "1080p", "FullHD", etc.). False positives are cheap (we
/// fall through to the next candidate); false negatives mean the resolver
/// returns 404 even though a working source exists.
static RE_CZ_DUB: LazyLock<Regex> = LazyLock::new(|| {
    // Covers "CZ DABING", "cz-dab", "Česká/Český/české dabing", "ceština",
    // "cestina". Cyrillic-style "č" → use a UCS char class so any Czech
    // adjective ending (á/é/ý/ému/...) before "dab" matches.
    Regex::new(r"(?i)(cz[\s\-_.]*dab|česk\w*[\s\-_.]*dab|cestin|češtin)").unwrap()
});
static RE_CZ_SUB: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(cz[\s\-_.]*tit|cz[\s\-_.]*sub|titulky[\s\-_.]*cz|cz\.tit)").unwrap()
});
static RE_RES_2160P: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)(2160p|4k|uhd)").unwrap());
static RE_RES_1080P: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)1080p").unwrap());

/// True when `title` looks like a match for the requested variant. The
/// per-hint `title_filter_regex` (when set) acts as an *additional*
/// filter — both must match.
pub(crate) fn variant_matches(title: &str, variant: &str, custom: Option<&str>) -> bool {
    let primary = match variant {
        "CZ_DUB" => RE_CZ_DUB.is_match(title),
        "CZ_SUB" => RE_CZ_SUB.is_match(title),
        "RES_2160P" => RE_RES_2160P.is_match(title),
        "RES_1080P" => RE_RES_1080P.is_match(title),
        _ => false,
    };
    if !primary {
        return false;
    }
    match custom.and_then(|p| Regex::new(p).ok()) {
        Some(re) => re.is_match(title),
        None => true,
    }
}

// --- Proxy types ---------------------------------------------------------

#[derive(Deserialize)]
struct ProxySearchResponse {
    success: Option<bool>,
    movies: Option<Vec<ProxySearchMovie>>,
}

#[derive(Deserialize)]
struct ProxySearchMovie {
    url: Option<String>,
    title: Option<String>,
}

#[derive(Deserialize)]
struct ProxyVideoResponse {
    success: Option<bool>,
    #[serde(rename = "videoUrl")]
    video_url: Option<String>,
}

// --- Public route handler ------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ResolveParams {
    /// "film" | "episode" | "tv-episode"
    pub owner_kind: String,
    pub owner_id: i32,
    /// Variant key from the hints table (e.g. "CZ_DUB").
    pub variant: String,
}

/// `GET /api/movies/stream/by-hint/{owner_kind}/{owner_id}/{variant}`
///
/// 302 → fresh tokenized CDN URL, or 404 / 502 on failure. Never 5xx-leaks
/// the proxy URL (which carries the shared `key=`).
pub async fn prehrajto_resolve_by_hint(
    State(state): State<AppState>,
    Path(params): Path<ResolveParams>,
) -> Response {
    let ResolveParams {
        owner_kind,
        owner_id,
        variant,
    } = params;

    // 1. Hint lookup
    let hints = match owner_kind.as_str() {
        "film" => find_for_film(&state.db, owner_id).await,
        "episode" => find_for_episode(&state.db, owner_id).await,
        "tv-episode" => find_for_tv_episode(&state.db, owner_id).await,
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Unknown owner_kind: {other}"),
            )
                .into_response();
        }
    };
    let Ok(hints) = hints else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response();
    };
    let Some(hint) = hints.into_iter().find(|h| h.variant == variant) else {
        return (StatusCode::NOT_FOUND, "Variant nedostupný").into_response();
    };

    // 2-3. Search (cached) + filter by variant
    let candidates = match search_candidates(&state, &hint.search_query).await {
        Ok(list) => list,
        Err(reason) => {
            tracing::warn!(
                hint_id = hint.id,
                reason,
                "prehrajto resolver: search failed"
            );
            return (StatusCode::BAD_GATEWAY, "Vyhledávání selhalo").into_response();
        }
    };
    let filtered: Vec<SearchCandidate> = candidates
        .into_iter()
        .filter(|c| variant_matches(&c.title, &variant, hint.title_filter_regex.as_deref()))
        .take(MAX_CANDIDATES_PER_REQUEST)
        .collect();

    if filtered.is_empty() {
        tracing::info!(
            hint_id = hint.id,
            variant = %variant,
            "prehrajto resolver: no matching candidates"
        );
        return (StatusCode::NOT_FOUND, "Žádný funkční zdroj").into_response();
    }

    // 4-5. action=video on each candidate, first hit wins.
    for candidate in &filtered {
        match resolve_candidate(&state, candidate).await {
            Ok(Some(url)) => {
                opportunistically_persist_last_resolved(&state, &hint, &candidate.upload_id).await;
                return Redirect::temporary(&url).into_response();
            }
            Ok(None) => {
                tracing::debug!(
                    upload_id = candidate.upload_id,
                    "prehrajto resolver: candidate dead, trying next"
                );
            }
            Err(reason) => {
                tracing::warn!(
                    upload_id = candidate.upload_id,
                    reason,
                    "prehrajto resolver: candidate transient error, trying next"
                );
            }
        }
    }

    (StatusCode::NOT_FOUND, "Žádný funkční zdroj").into_response()
}

// --- Search step ---------------------------------------------------------

/// Hit the proxy with `action=search`, parse to [`SearchCandidate`], cache.
/// Caching is keyed by the raw search query so every variant of the same
/// hint shares one round-trip per 30-min window.
async fn search_candidates(
    state: &AppState,
    query: &str,
) -> Result<Vec<SearchCandidate>, &'static str> {
    if let Some(cached) = state.prehrajto_search_cache.get(&query.to_string()).await {
        return Ok(cached);
    }

    let (proxy_url, proxy_key) = cz_proxy_config(&state.config).ok_or("proxy-not-configured")?;
    let url = format!(
        "{}?action=search&q={}&key={}",
        proxy_url,
        urlencoding::encode(query),
        proxy_key,
    );
    let resp = state
        .http_client
        .get(&url)
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .map_err(|_| "proxy-transport")?;
    if !resp.status().is_success() {
        return Err("proxy-http");
    }
    let data: ProxySearchResponse = resp.json().await.map_err(|_| "proxy-parse")?;
    if data.success != Some(true) {
        return Err("proxy-not-success");
    }

    let candidates: Vec<SearchCandidate> = data
        .movies
        .unwrap_or_default()
        .into_iter()
        .filter_map(|m| {
            let url = m.url?;
            let title = m.title.unwrap_or_default();
            let upload_id = parse_upload_id(&url)?;
            Some(SearchCandidate {
                url,
                title,
                upload_id,
            })
        })
        .collect();

    state
        .prehrajto_search_cache
        .insert(query.to_string(), candidates.clone())
        .await;
    Ok(candidates)
}

// --- Resolve step --------------------------------------------------------

/// Resolve a single candidate to a tokenized CDN URL. Reuses
/// [`AppState::prehrajto_stream_cache`] so the legacy upload-id endpoint
/// and this resolver share cached URLs.
///
/// Returns:
/// - `Ok(Some(url))` on success (302-able).
/// - `Ok(None)` on `success: false` from the proxy (upload is dead).
/// - `Err(reason)` on transient proxy errors / malformed responses /
///   non-allow-listed CDN — caller advances to the next candidate.
async fn resolve_candidate(
    state: &AppState,
    candidate: &SearchCandidate,
) -> Result<Option<String>, &'static str> {
    if let Some(entry) = state.prehrajto_stream_cache.get(&candidate.upload_id).await
        && entry.expires_at > Instant::now()
    {
        return Ok(Some(entry.url));
    }

    let (proxy_url, proxy_key) = cz_proxy_config(&state.config).ok_or("proxy-not-configured")?;
    let api_url = format!(
        "{}?action=video&url={}&key={}",
        proxy_url,
        urlencoding::encode(&candidate.url),
        proxy_key,
    );
    let resp = state
        .http_client
        .get(&api_url)
        .timeout(Duration::from_secs(25))
        .send()
        .await
        .map_err(|_| "proxy-transport")?;
    if !resp.status().is_success() {
        return Err("proxy-http");
    }
    let data: ProxyVideoResponse = resp.json().await.map_err(|_| "proxy-parse")?;

    match data.success {
        Some(true) => {
            let raw = data.video_url.unwrap_or_default();
            if raw.is_empty() {
                return Err("proxy-empty-url");
            }
            if !is_allowed_stream_url(&raw) {
                tracing::warn!(
                    upload_id = candidate.upload_id,
                    "prehrajto resolver: refusing non-CDN URL"
                );
                return Err("cdn-allowlist");
            }
            let expires_at = token_expiry_instant(&raw)
                .map(|i| i.checked_sub(TOKEN_SAFETY_MARGIN).unwrap_or(i))
                .unwrap_or_else(|| Instant::now() + DEFAULT_TOKEN_LIFETIME);
            state
                .prehrajto_stream_cache
                .insert(
                    candidate.upload_id.clone(),
                    CachedStreamUrl {
                        url: raw.clone(),
                        expires_at,
                    },
                )
                .await;
            Ok(Some(raw))
        }
        Some(false) => Ok(None),
        None => Err("proxy-malformed"),
    }
}

/// Best-effort write-through of `last_resolved_id` / `last_resolved_at`
/// for ops visibility. Failures are logged but never propagated — the
/// user has already received their redirect.
async fn opportunistically_persist_last_resolved(
    state: &AppState,
    hint: &PrehrajtoSearchHint,
    upload_id: &str,
) {
    let _ = sqlx::query(
        "UPDATE prehrajto_search_hints \
            SET last_resolved_id = $1, last_resolved_at = now() \
          WHERE id = $2",
    )
    .bind(upload_id)
    .bind(hint.id)
    .execute(&state.db)
    .await
    .map_err(|e| {
        tracing::debug!(hint_id = hint.id, error = ?e, "failed to update last_resolved_*");
    });
}

// --- Tests ----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_upload_id_handles_slug_and_id() {
        assert_eq!(
            parse_upload_id("https://prehraj.to/spasitel-2026/20127347c6682258"),
            Some("20127347c6682258".to_string())
        );
        assert_eq!(
            parse_upload_id("https://prehraj.to/x/62f69ba1c4691"),
            Some("62f69ba1c4691".to_string())
        );
        assert_eq!(
            parse_upload_id("https://prehraj.to/spasitel-2026/20127347c6682258?player=jwplayer"),
            Some("20127347c6682258".to_string())
        );
    }

    #[test]
    fn parse_upload_id_rejects_malformed() {
        assert_eq!(parse_upload_id("https://prehraj.to/spasitel-2026/"), None);
        assert_eq!(
            parse_upload_id("https://prehraj.to/spasitel-2026/notvalid"),
            None
        );
        assert_eq!(parse_upload_id("https://prehraj.to/"), None);
    }

    #[test]
    fn cz_dub_matches_common_phrases() {
        assert!(variant_matches(
            "Spasitel - Project Hail Mary HD 2026 CZ DABING.mkv",
            "CZ_DUB",
            None
        ));
        assert!(variant_matches("Lego film cz dab.mp4", "CZ_DUB", None));
        assert!(variant_matches("Český dabing - Cobra Kai", "CZ_DUB", None));
        assert!(!variant_matches(
            "Spasitel.Project Hail Mary (2026) CZ Titulky.mkv",
            "CZ_DUB",
            None
        ));
    }

    #[test]
    fn cz_sub_matches_common_phrases() {
        assert!(variant_matches(
            "Spasitel.Project Hail Mary (2026) CZ Titulky.mkv",
            "CZ_SUB",
            None
        ));
        assert!(variant_matches("Spasitel CZ-titulky 1080p", "CZ_SUB", None));
        assert!(variant_matches("Movie titulky cz", "CZ_SUB", None));
        assert!(!variant_matches(
            "Spasitel - Project Hail Mary HD 2026 CZ DABING.mkv",
            "CZ_SUB",
            None
        ));
    }

    #[test]
    fn resolution_variants_match_resolution_hints() {
        assert!(variant_matches(
            "Spasitel.Project Hail Mary (2026).2160p.WEBrip",
            "RES_2160P",
            None
        ));
        assert!(variant_matches("Movie 4K UHD edition", "RES_2160P", None));
        assert!(variant_matches(
            "Spasitel.Project Hail Mary (2026).1080p.WEBrip",
            "RES_1080P",
            None
        ));
        assert!(!variant_matches("Movie 720p only", "RES_1080P", None));
    }

    #[test]
    fn custom_filter_narrows_match() {
        // Both filters must match.
        assert!(variant_matches(
            "Spasitel 2026 CZ DABING",
            "CZ_DUB",
            Some(r"(?i)2026")
        ));
        // Custom filter rejects matching primary.
        assert!(!variant_matches(
            "Spasitel 1981 CZ DABING",
            "CZ_DUB",
            Some(r"(?i)2026")
        ));
        // Invalid regex falls through to primary-only.
        assert!(variant_matches(
            "Spasitel CZ DABING",
            "CZ_DUB",
            Some("[unbalanced")
        ));
    }
}
