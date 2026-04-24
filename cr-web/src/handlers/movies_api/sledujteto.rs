//! API endpoints for the sledujteto.cz source on /admin/test-sledujteto/.
//!
//! Observed upstream behaviour (see `~/Dokumenty/sledujteto-cz-integrace.md`
//! and `/home/jirka/sledujteto/INTEGRACE-DO-CR-WEB.md`):
//!   - `POST /services/add-file-link` (hash-gen) works from any ASN,
//!     including Hetzner AS24940 — no CZ proxy needed.
//!   - `GET /api/web/videos` (search) rate-limits known datacenter ASNs to
//!     an empty result set — Hetzner and Oracle currently get `files: []`,
//!     aspone AS43541 is allowed through. We fall back to the SledujteToCzProxy
//!     mirror (`SLEDUJTETO_PROXY_URL` + `SLEDUJTETO_PROXY_KEY`; no built-in
//!     default, prod currently points at `sledujteto.aspfree.cz`). Behaviour
//!     when the proxy env vars aren't configured: empty-direct returns an
//!     empty result silently (useful in local dev), direct-failure returns
//!     an explicit error so callers can tell why search broke.
//!   - Playback hostname varies per upload: `www.sledujteto.cz/player/...`
//!     serves 206 Partial Content from Hetzner; `data{N}.sledujteto.cz/...`
//!     responds with a redirect to invalid-file for datacenter ASNs (and
//!     occasionally also from CZ IPs when an upload has been deleted).
//!
//! Routes:
//!   GET /api/sledujteto/search?q=<query>     — search upstream, aspone fallback (POC-gated)
//!   GET /api/sledujteto/resolve?id=<filesId> — turn files_id into playback URL
//!   GET /api/films/{film_id}/sledujteto-sources — list alive uploads from DB

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::FromRow;

use super::subtitles::SubtitleTrack;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SearchQuery {
    q: String,
}

#[derive(Serialize)]
pub struct SearchResultItem {
    /// sledujteto `files_id` (stable DB key) — pass back to `/resolve`.
    files_id: i64,
    name: String,
    preview: Option<String>,
    filesize: Option<String>,
    duration: Option<String>,
    resolution: Option<String>,
    is_hd: bool,
    views: Option<i64>,
    /// Full page URL on sledujteto.cz (for external link / reference).
    full_url: Option<String>,
}

#[derive(Serialize)]
pub struct SearchResponse {
    success: bool,
    movies: Vec<SearchResultItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Fetch + parse one upstream JSON into our flattened search items.
/// Returns `Err(_)` only on transport/parse failure — an empty `files` array
/// is a legit upstream response and returned as `Ok(vec![])`.
async fn fetch_sledujteto_search(
    client: &reqwest::Client,
    url: &str,
) -> Result<Vec<SearchResultItem>, String> {
    #[derive(Deserialize)]
    struct UpstreamResp {
        data: Option<UpstreamData>,
    }
    #[derive(Deserialize)]
    struct UpstreamData {
        files: Option<Vec<UpstreamFile>>,
    }
    #[derive(Deserialize)]
    struct UpstreamFile {
        id: i64,
        name: Option<String>,
        preview: Option<String>,
        filesize: Option<String>,
        duration: Option<String>,
        resolution: Option<String>,
        is_hd: Option<bool>,
        views: Option<i64>,
        full_url: Option<String>,
    }

    let resp = client
        .get(url)
        .header("Accept", "application/json")
        .header("X-Requested-With", "XMLHttpRequest")
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        )
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| format!("upstream: {e}"))?;

    // Distinguish "upstream returned non-JSON / rate-limit HTML" from an
    // application-level empty result so the caller can decide whether to
    // fall back (only on transport failure) vs accept the response.
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("HTTP {}", status.as_u16()));
    }

    let parsed: UpstreamResp = resp.json().await.map_err(|e| format!("parse: {e}"))?;
    let files = parsed.data.and_then(|d| d.files).unwrap_or_default();

    Ok(files
        .into_iter()
        .map(|f| SearchResultItem {
            files_id: f.id,
            name: f.name.unwrap_or_default(),
            preview: f.preview,
            filesize: f.filesize,
            duration: f.duration,
            resolution: f.resolution,
            is_hd: f.is_hd.unwrap_or(false),
            views: f.views,
            full_url: f.full_url,
        })
        .collect())
}

pub async fn sledujteto_search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> Json<SearchResponse> {
    let q = params.q.trim();
    if q.is_empty() {
        return Json(SearchResponse {
            success: false,
            movies: vec![],
            error: Some("empty query".into()),
        });
    }

    // Sledujteto rate-limits the search endpoint per ASN: most datacenter
    // ranges (Hetzner AS24940, Oracle AS31898, …) hit an empty-result
    // blocklist, while aspone (AS43541) is allowed through. So we try the
    // direct call first and fall back to the aspone mirror on empty.
    // Mirror source: /home/jirka/sledujteto/oracle/server.mjs.
    let direct_url = format!(
        "https://www.sledujteto.cz/api/web/videos?query={}&page=1&limit=30&collection=suggestions&sort=relevance&me=0",
        urlencoding::encode(q)
    );

    let direct = fetch_sledujteto_search(&state.http_client, &direct_url).await;

    // Build the aspone fallback URL whenever the proxy is configured so both
    // the empty-direct and direct-error branches below have it ready. `None`
    // when no proxy is configured — local dev then skips the fallback entirely.
    // `SLEDUJTETO_PROXY_URL` is the site root; we append `/Search.ashx` here
    // (IIS is case-insensitive, but we match the file casing we actually ship).
    let fallback_url = state.config.sledujteto_proxy.as_ref().map(|proxy| {
        format!(
            "{}/Search.ashx?q={}&key={}",
            proxy.url.trim_end_matches('/'),
            urlencoding::encode(q),
            urlencoding::encode(&proxy.key),
        )
    });

    match direct {
        Ok(movies) if !movies.is_empty() => Json(SearchResponse {
            success: true,
            movies,
            error: None,
        }),
        Ok(_empty) => {
            // Empty result might be a real zero-hit query or a silent ASN
            // blocklist — the caller can't tell, so try the aspone mirror.
            let Some(url) = fallback_url else {
                return Json(SearchResponse {
                    success: true,
                    movies: vec![],
                    error: None,
                });
            };
            match fetch_sledujteto_search(&state.http_client, &url).await {
                Ok(movies) => Json(SearchResponse {
                    success: true,
                    movies,
                    error: None,
                }),
                Err(e) => {
                    tracing::warn!("sledujteto search fallback (aspone) failed: {e}");
                    Json(SearchResponse {
                        success: true,
                        movies: vec![],
                        error: None,
                    })
                }
            }
        }
        Err(e) => {
            tracing::warn!("sledujteto search direct failed: {e}");
            let Some(url) = fallback_url else {
                return Json(SearchResponse {
                    success: false,
                    movies: vec![],
                    // `e` is from the upstream www.sledujteto.cz URL (no
                    // secrets), so echoing it to the client is fine.
                    error: Some(format!("direct={e}; no proxy configured")),
                });
            };
            match fetch_sledujteto_search(&state.http_client, &url).await {
                Ok(movies) => Json(SearchResponse {
                    success: true,
                    movies,
                    error: None,
                }),
                Err(fallback_err) => {
                    // Log full detail server-side — fallback_err can contain
                    // the proxy URL with `key=…` because `reqwest::Error`'s
                    // Display includes the request URL on transport failures.
                    tracing::warn!(
                        "sledujteto search fallback (aspone) failed after direct failure: direct={e}; fallback={fallback_err}"
                    );
                    Json(SearchResponse {
                        success: false,
                        movies: vec![],
                        // Client-facing error is intentionally sanitized to
                        // avoid leaking `SLEDUJTETO_PROXY_KEY`. Operators read
                        // the full reason from the log line above.
                        error: Some("upstream search failed and proxy fallback unavailable".into()),
                    })
                }
            }
        }
    }
}

#[derive(Deserialize)]
pub struct ResolveQuery {
    id: i64,
}

#[derive(Serialize)]
pub struct ResolveResponse {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    video_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    download_url: Option<String>,
    /// HTML `<track>` entries. Matches the shape returned by the prehraj.to
    /// resolve endpoint so the shared `addSubtitles(player, data.subtitles)`
    /// client code works verbatim for both providers. Empty vec when the
    /// upload has no subtitle attachments.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    subtitles: Vec<SubtitleTrack>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

pub async fn sledujteto_resolve(
    State(state): State<AppState>,
    Query(params): Query<ResolveQuery>,
) -> Json<ResolveResponse> {
    // Abuse guard: the endpoint is CORS-`Any` (same policy as the rest
    // of /api), so without a DB check it would act as a free
    // `add-file-link` hash generator for any integer. Only accept ids
    // present in `film_sledujteto_uploads` — that bounds traffic to
    // files the import pipeline has already classified, and turns any
    // drive-by into a DB read with no upstream side effect.
    //
    // We also pull `lang_class` in the same round-trip so we can map
    // subtitle tracks to an HTML `srclang` below (sledujteto doesn't
    // expose track language in its add-file-link response).
    let lang_class: Option<String> = match sqlx::query_scalar::<_, String>(
        "SELECT vs.lang_class \
           FROM video_sources vs \
           JOIN video_providers p ON p.id = vs.provider_id \
          WHERE p.slug = 'sledujteto' AND vs.external_id = $1::TEXT",
    )
    .bind(params.id as i32)
    .fetch_optional(&state.db)
    .await
    {
        Ok(Some(lc)) => Some(lc),
        Ok(None) => {
            return Json(ResolveResponse {
                success: false,
                video_url: None,
                download_url: None,
                subtitles: vec![],
                error: Some("unknown file_id".into()),
            });
        }
        Err(e) => {
            tracing::error!("sledujteto resolve DB check failed: {e}");
            return Json(ResolveResponse {
                success: false,
                video_url: None,
                download_url: None,
                subtitles: vec![],
                error: Some("db error".into()),
            });
        }
    };

    let body = json!({ "params": { "id": params.id } });

    let resp = state
        .http_client
        .post("https://www.sledujteto.cz/services/add-file-link")
        .header("Content-Type", "application/json;charset=UTF-8")
        .header("Accept", "application/json, text/plain, */*")
        .header("Requested-With-AngularJS", "true")
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        )
        .json(&body)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await;

    let resp = match resp {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("sledujteto resolve upstream failed: {e}");
            return Json(ResolveResponse {
                success: false,
                video_url: None,
                download_url: None,
                subtitles: vec![],
                error: Some(format!("upstream: {e}")),
            });
        }
    };

    // Guard against rate-limit HTML / redirects — upstream returns 200 +
    // JSON on success, anything else is an opaque failure mode we should
    // not try to json-parse.
    let status = resp.status();
    if !status.is_success() {
        tracing::warn!("sledujteto resolve non-2xx: {status}");
        return Json(ResolveResponse {
            success: false,
            video_url: None,
            download_url: None,
            subtitles: vec![],
            error: Some(format!("HTTP {}", status.as_u16())),
        });
    }

    #[derive(Deserialize)]
    struct UpstreamResp {
        error: Option<bool>,
        msg: Option<String>,
        video_url: Option<String>,
        download_url: Option<String>,
        #[serde(default)]
        subtitles: Vec<UpstreamSubtitle>,
    }
    #[derive(Deserialize)]
    struct UpstreamSubtitle {
        /// Sledujteto returns a proxied VTT URL of the form
        /// `https://www.sledujteto.cz/file/subtitles/?file=<raw>` — that
        /// outer URL is what actually serves valid WEBVTT bytes, so we
        /// forward it as-is (wrapped by our own CORS proxy below).
        file: Option<String>,
        /// Human-readable label (usually the original .srt filename).
        label: Option<String>,
    }

    let data: UpstreamResp = match resp.json().await {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!("sledujteto resolve parse failed: {e}");
            return Json(ResolveResponse {
                success: false,
                video_url: None,
                download_url: None,
                subtitles: vec![],
                error: Some(format!("parse: {e}")),
            });
        }
    };

    if data.error.unwrap_or(false) {
        return Json(ResolveResponse {
            success: false,
            video_url: None,
            download_url: None,
            subtitles: vec![],
            error: data.msg.or(Some("upstream error".into())),
        });
    }

    // A 200 with no `video_url` is still a failure from the caller's POV —
    // the POC page would otherwise wire `None` into `<video src>` and show
    // a misleading "playing" state. Treat it as an explicit failure.
    let video_url = match data.video_url {
        Some(v) if !v.trim().is_empty() => v,
        _ => {
            return Json(ResolveResponse {
                success: false,
                video_url: None,
                download_url: data.download_url,
                subtitles: vec![],
                error: Some("missing video_url in upstream response".into()),
            });
        }
    };

    // Map upstream subtitle descriptors to our shared `<track>` shape.
    //  - `url` stays raw; the browser-side `addSubtitles` helper already
    //    wraps it in `/api/movies/subtitle?url=…` (shared allowlist now
    //    covers both `premiumcdn.net` and `www.sledujteto.cz`, so the
    //    same wrapper serves both providers and adds the CORS / VTT
    //    content-type the CDN omits).
    //  - `lang` is derived from the upload's classified `lang_class`:
    //    CZ_SUB → cs, SK_SUB → sk, anything else defaults to `cs` because
    //    99 % of attached tracks on sledujteto are Czech and the user can
    //    still toggle them off via native CC menu.
    //  - `label` falls back to "Titulky" when upstream omits the filename.
    let sub_lang = match lang_class.as_deref() {
        Some("SK_SUB") => "sk",
        _ => "cs",
    };
    let subtitles: Vec<SubtitleTrack> = data
        .subtitles
        .into_iter()
        .filter_map(|s| {
            let raw = s.file?;
            if raw.trim().is_empty() {
                return None;
            }
            Some(SubtitleTrack {
                url: raw,
                lang: sub_lang.to_string(),
                label: s.label.unwrap_or_else(|| "Titulky".to_string()),
            })
        })
        .collect();

    Json(ResolveResponse {
        success: true,
        video_url: Some(video_url),
        download_url: data.download_url,
        subtitles,
        error: None,
    })
}

/// One row in the "Další zdroje" listing for sledujteto.cz. Mirrors the
/// prehraj.to shape so the detail-page template can reuse the same JS
/// rendering path. `file_id` is an INT (vs prehraj.to's hex upload_id),
/// and `cdn` is first-class because the data{N} copies are blocked from
/// datacenter ASNs (#549).
#[derive(Serialize, FromRow)]
pub struct SledujtetoSourceRow {
    pub file_id: i32,
    pub title: String,
    pub duration_sec: Option<i32>,
    pub resolution_hint: Option<String>,
    pub filesize_bytes: Option<i64>,
    pub lang_class: String,
    pub cdn: String,
}

/// `GET /api/films/{film_id}/sledujteto-sources` — ranked list of alive
/// uploads from `film_sledujteto_uploads`. Ranking mirrors the primary-
/// upload picker in the import script:
///
///   1. `cdn = 'www'` first (datacenter-ASN streamable).
///   2. Then by language priority (CZ_DUB > CZ_NATIVE > CZ_SUB > SK_DUB >
///      SK_SUB > UNKNOWN > EN).
///   3. Then by rough resolution score parsed from `resolution_hint`.
///
/// Exposes the DB-backed "Další zdroje" source list for follow-up UI/API
/// consumers. The current film detail template does not fetch this
/// endpoint on render — it uses the server-rendered
/// `film.sledujteto_primary_file_id` and calls `/api/sledujteto/resolve`
/// directly. This endpoint lands ahead of the unified source-picker UI
/// so API consumers can already enumerate alternatives.
pub async fn sledujteto_sources(
    State(state): State<AppState>,
    Path(film_id): Path<i32>,
) -> Response {
    // After the #611 reader switch, this reads from `video_sources` with
    // a `provider='sledujteto'` filter. The `external_id` column is the
    // sledujteto file_id stored as TEXT — cast to INT for the output so
    // the JSON contract (and `SledujtetoSourceRow.file_id: i32`) stay
    // unchanged.
    let sql = r#"
        SELECT vs.external_id::INTEGER AS file_id,
               COALESCE(vs.title, '') AS title,
               vs.duration_sec, vs.resolution_hint, vs.filesize_bytes,
               vs.lang_class,
               COALESCE(vs.cdn, 'unknown') AS cdn
          FROM video_sources vs
          JOIN video_providers p ON p.id = vs.provider_id
         WHERE p.slug = 'sledujteto'
           AND vs.film_id = $1 AND vs.is_alive = TRUE
         ORDER BY
            CASE vs.cdn WHEN 'www' THEN 0 ELSE 1 END,
            CASE vs.lang_class
                WHEN 'CZ_DUB'    THEN 0
                WHEN 'CZ_NATIVE' THEN 1
                WHEN 'CZ_SUB'    THEN 2
                WHEN 'SK_DUB'    THEN 3
                WHEN 'SK_SUB'    THEN 4
                WHEN 'UNKNOWN'   THEN 5
                ELSE 6
            END,
            CASE
                WHEN vs.resolution_hint ILIKE '%2160%' OR vs.resolution_hint ILIKE '%4k%' THEN 0
                WHEN vs.resolution_hint ILIKE '%1080%' THEN 1
                WHEN vs.resolution_hint ILIKE '%720%'  THEN 2
                WHEN vs.resolution_hint ILIKE '%480%'  THEN 3
                ELSE 4
            END,
            vs.external_id::INTEGER
    "#;

    match sqlx::query_as::<_, SledujtetoSourceRow>(sql)
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
            tracing::error!(film_id, error = ?e, "sledujteto_sources DB query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "db error").into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- `SledujtetoSourceRow` JSON contract -----------------------------
    // Locks the field names/types the `/api/films/{id}/sledujteto-sources`
    // endpoint exposes. Any rename or drop here breaks downstream API
    // consumers (and the eventual unified source-picker UI in #548). Same
    // pattern as the `PrehrajtoSourceRow` contract tests above.

    #[test]
    fn source_row_serializes_with_expected_fields() {
        let row = SledujtetoSourceRow {
            file_id: 16824,
            title: "Vecirek 2017 CZ titulky HD".to_string(),
            duration_sec: Some(4242),
            resolution_hint: Some("1280x534".to_string()),
            filesize_bytes: Some(562_047_221),
            lang_class: "CZ_SUB".to_string(),
            cdn: "www".to_string(),
        };
        let json = serde_json::to_value(&row).expect("serialize");
        assert_eq!(json["file_id"], 16824);
        assert_eq!(json["title"], "Vecirek 2017 CZ titulky HD");
        assert_eq!(json["duration_sec"], 4242);
        assert_eq!(json["resolution_hint"], "1280x534");
        assert_eq!(json["filesize_bytes"], 562_047_221_i64);
        assert_eq!(json["lang_class"], "CZ_SUB");
        assert_eq!(json["cdn"], "www");
    }

    #[test]
    fn source_row_serializes_optional_nulls_as_json_null() {
        let row = SledujtetoSourceRow {
            file_id: 1,
            title: "Unknown".to_string(),
            duration_sec: None,
            resolution_hint: None,
            filesize_bytes: None,
            lang_class: "UNKNOWN".to_string(),
            cdn: "unknown".to_string(),
        };
        let json = serde_json::to_value(&row).expect("serialize");
        assert!(json["duration_sec"].is_null());
        assert!(json["resolution_hint"].is_null());
        assert!(json["filesize_bytes"].is_null());
    }
}
