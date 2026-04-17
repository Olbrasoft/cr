//! Admin page `/admin/cache/` — on-demand Cloudflare cache purge.
//!
//! Solves the "posters updated but CDN still serves stale" problem: after we
//! re-upload covers (films / series / TV pořady) the CF edge keeps the old
//! response until its TTL (max-age=3600) expires. This handler calls the CF
//! API's `zones/{id}/purge_cache` endpoint to invalidate specific URLs (or
//! the whole zone) immediately.
//!
//! Scopes:
//!   - "everything"         → `{"purge_everything": true}` — nuclear option
//!   - "series_covers"      → all /serialy-online/{slug}.webp
//!   - "films_covers"       → all /filmy-online/{slug}.webp
//!   - "tv_shows_covers"    → all /tv-porady/{slug}.webp
//!   - "urls"               → user-provided list (one URL per line, textarea)
//!
//! Free plan limit is 30 URLs per purge_cache call, so section purges are
//! chunked. Everything/hostname-style purges are a single call.

use askama::Template;
use axum::extract::{Form, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use serde::Deserialize;

use crate::config::CfCachePurgeConfig;
use crate::error::WebResult;
use crate::state::AppState;

const CF_PURGE_BATCH: usize = 30;
const SITE_ORIGIN: &str = "https://ceskarepublika.wiki";

#[derive(Template)]
#[template(path = "admin_cache.html")]
struct AdminCacheTemplate {
    img: String,
    /// Whether CF token is configured. When false the form is disabled and the
    /// page explains the missing env vars.
    configured: bool,
    /// Zone ID (or empty string) — shown in the info box so an admin can tell
    /// at a glance which zone this would purge.
    zone_id: String,
    /// Flash message from a previous POST redirect. None on a fresh GET.
    flash: Option<FlashMessage>,
}

#[derive(Clone)]
struct FlashMessage {
    kind: &'static str, // "ok" | "error"
    text: String,
}

fn noindex(html: String) -> Response {
    let mut resp = Html(html).into_response();
    resp.headers_mut().insert(
        "X-Robots-Tag",
        axum::http::HeaderValue::from_static("noindex, nofollow"),
    );
    resp
}

/// Parse flash message out of the `?msg=...&kind=ok|error` query string.
/// We use query-string flash instead of sessions because /admin/ has no
/// session layer today; the tradeoff is a uglier URL for a split-second.
fn parse_flash(query: &str) -> Option<FlashMessage> {
    let mut kind = "";
    let mut text = String::new();
    for pair in query.split('&') {
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = it.next().unwrap_or("");
        let v = urlencoding::decode(v)
            .map(|c| c.into_owned())
            .unwrap_or_default();
        match k {
            "kind" => kind = if v == "error" { "error" } else { "ok" },
            "msg" => text = v,
            _ => {}
        }
    }
    if text.is_empty() {
        None
    } else {
        let kind_static: &'static str = if kind == "error" { "error" } else { "ok" };
        Some(FlashMessage {
            kind: kind_static,
            text,
        })
    }
}

/// GET /admin/cache/ — render form.
pub async fn admin_cache_form(
    State(state): State<AppState>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
) -> WebResult<Response> {
    let (configured, zone_id) = match &state.config.cf_cache_purge {
        Some(cfg) => (true, cfg.zone_id.clone()),
        None => (false, String::new()),
    };

    let flash = query.as_deref().and_then(parse_flash);

    let tmpl = AdminCacheTemplate {
        img: state.image_base_url.clone(),
        configured,
        zone_id,
        flash,
    };
    Ok(noindex(tmpl.render()?))
}

#[derive(Debug, Deserialize)]
pub struct PurgeForm {
    /// "everything" | "series_covers" | "films_covers" | "tv_shows_covers" | "urls"
    scope: String,
    /// URL list from textarea — one per line. Only honored when scope == "urls".
    #[serde(default)]
    urls: String,
    /// Typed "SMAZAT" to guard the "everything" scope from accidental clicks.
    #[serde(default)]
    confirm: String,
}

/// POST /admin/cache/purge — execute purge, redirect back with flash.
pub async fn admin_cache_purge(
    State(state): State<AppState>,
    Form(form): Form<PurgeForm>,
) -> WebResult<Response> {
    let cfg = match &state.config.cf_cache_purge {
        Some(c) => c.clone(),
        None => {
            return Ok(redirect_with_flash(
                "error",
                "CF cache purge není nakonfigurovaný (chybí CF_CACHE_PURGE_TOKEN / CF_ZONE_ID).",
            ));
        }
    };

    // Dispatch by scope — each branch either returns a flash redirect or falls
    // through to a common "chunked URL list" purge path.
    let urls: Vec<String> = match form.scope.as_str() {
        "everything" => {
            if form.confirm.trim() != "SMAZAT" {
                return Ok(redirect_with_flash(
                    "error",
                    "Pro \"Smaž vše\" je nutné do pole potvrzení napsat SMAZAT.",
                ));
            }
            return match purge_everything(&state.http_client, &cfg).await {
                Ok(()) => Ok(redirect_with_flash(
                    "ok",
                    "Celá zóna vyčištěna (purge_everything).",
                )),
                Err(e) => Ok(redirect_with_flash(
                    "error",
                    &format!("purge_everything selhal: {e}"),
                )),
            };
        }
        "series_covers" => match collect_series_cover_urls(&state.db).await {
            Ok(u) => u,
            Err(e) => {
                return Ok(redirect_with_flash(
                    "error",
                    &format!("DB query (series) selhala: {e}"),
                ));
            }
        },
        "films_covers" => match collect_film_cover_urls(&state.db).await {
            Ok(u) => u,
            Err(e) => {
                return Ok(redirect_with_flash(
                    "error",
                    &format!("DB query (films) selhala: {e}"),
                ));
            }
        },
        "tv_shows_covers" => match collect_tv_show_cover_urls(&state.db).await {
            Ok(u) => u,
            Err(e) => {
                return Ok(redirect_with_flash(
                    "error",
                    &format!("DB query (tv_shows) selhala: {e}"),
                ));
            }
        },
        "urls" => parse_url_list(&form.urls),
        other => {
            return Ok(redirect_with_flash(
                "error",
                &format!("Neznámý scope: {other}"),
            ));
        }
    };

    if urls.is_empty() {
        return Ok(redirect_with_flash(
            "error",
            "Nebyla poslána žádná URL k promazání.",
        ));
    }

    match purge_urls_batched(&state.http_client, &cfg, &urls).await {
        Ok(n) => Ok(redirect_with_flash(
            "ok",
            &format!("Promazáno {n} URL ({} dávek).", n.div_ceil(CF_PURGE_BATCH)),
        )),
        Err(e) => Ok(redirect_with_flash("error", &format!("Purge selhal: {e}"))),
    }
}

fn redirect_with_flash(kind: &str, msg: &str) -> Response {
    let url = format!(
        "/admin/cache/?kind={}&msg={}",
        kind,
        urlencoding::encode(msg)
    );
    (StatusCode::SEE_OTHER, [(axum::http::header::LOCATION, url)]).into_response()
}

fn parse_url_list(raw: &str) -> Vec<String> {
    raw.lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && (l.starts_with("http://") || l.starts_with("https://")))
        .collect()
}

async fn collect_series_cover_urls(pool: &sqlx::PgPool) -> Result<Vec<String>, sqlx::Error> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT slug FROM series WHERE slug IS NOT NULL AND slug <> ''")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(slug,)| format!("{SITE_ORIGIN}/serialy-online/{slug}.webp"))
        .collect())
}

async fn collect_film_cover_urls(pool: &sqlx::PgPool) -> Result<Vec<String>, sqlx::Error> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT slug FROM films WHERE slug IS NOT NULL AND slug <> ''")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(slug,)| format!("{SITE_ORIGIN}/filmy-online/{slug}.webp"))
        .collect())
}

async fn collect_tv_show_cover_urls(pool: &sqlx::PgPool) -> Result<Vec<String>, sqlx::Error> {
    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT slug FROM tv_shows WHERE slug IS NOT NULL AND slug <> ''")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(slug,)| format!("{SITE_ORIGIN}/tv-porady/{slug}.webp"))
        .collect())
}

async fn purge_everything(
    client: &reqwest::Client,
    cfg: &CfCachePurgeConfig,
) -> anyhow::Result<()> {
    let url = format!(
        "https://api.cloudflare.com/client/v4/zones/{}/purge_cache",
        cfg.zone_id
    );
    let res = client
        .post(&url)
        .bearer_auth(&cfg.token)
        .json(&serde_json::json!({ "purge_everything": true }))
        .send()
        .await?;
    let status = res.status();
    let body = res.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("HTTP {status}: {body}");
    }
    // CF returns { "success": true, ... } even on semantic errors with 200
    let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    if v.get("success").and_then(|s| s.as_bool()) != Some(true) {
        anyhow::bail!("CF reported failure: {body}");
    }
    Ok(())
}

async fn purge_urls_batched(
    client: &reqwest::Client,
    cfg: &CfCachePurgeConfig,
    urls: &[String],
) -> anyhow::Result<usize> {
    let api = format!(
        "https://api.cloudflare.com/client/v4/zones/{}/purge_cache",
        cfg.zone_id
    );
    let mut done = 0usize;
    for chunk in urls.chunks(CF_PURGE_BATCH) {
        let res = client
            .post(&api)
            .bearer_auth(&cfg.token)
            .json(&serde_json::json!({ "files": chunk }))
            .send()
            .await?;
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("HTTP {status} at batch starting {done}: {body}");
        }
        let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
        if v.get("success").and_then(|s| s.as_bool()) != Some(true) {
            anyhow::bail!("CF reported failure at batch starting {done}: {body}");
        }
        done += chunk.len();
    }
    Ok(done)
}
