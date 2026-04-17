//! Admin landing page `/admin/` — rozcestník pro všechny administrační sekce.
//!
//! Zobrazuje dlaždice:
//!   - Auto-import (SK Torrent → films/series/tv_shows)
//!   - Zálohy DB (pg_dump → Cloudflare R2)
//!
//! Každá dlaždice ukazuje stav poslední akce, aby admin ráno věděl, jestli
//! noční pipeline proběhla v pořádku.
//!
//! Routes:
//!   GET /admin/   — dashboard
//!   GET /admin    — dashboard (bez lomítka)

use askama::Template;
use axum::extract::State;
use axum::response::{Html, IntoResponse, Response};

use crate::error::WebResult;
use crate::state::AppState;

/// Snapshot jednoho posledního běhu — společný tvar pro import i zálohy.
struct LastRunTile {
    /// 'ok', 'error', 'partial', 'running', 'none'
    status: &'static str,
    /// Lidsky formátovaná zpráva ("Před 4 hodinami — ok, 47 filmů přidáno").
    message: String,
}

impl LastRunTile {
    fn css_class(&self) -> &'static str {
        match self.status {
            "ok" => "tile-ok",
            "partial" => "tile-warn",
            "error" => "tile-error",
            "running" => "tile-running",
            _ => "tile-unknown",
        }
    }
}

#[derive(Template)]
#[template(path = "admin_dashboard.html")]
struct AdminDashboardTemplate {
    img: String,
    import_tile: LastRunTile,
    backup_tile: LastRunTile,
    cache_tile: LastRunTile,
}

fn noindex(html: String) -> Response {
    let mut resp = Html(html).into_response();
    resp.headers_mut().insert(
        "X-Robots-Tag",
        axum::http::HeaderValue::from_static("noindex, nofollow"),
    );
    resp
}

fn hours_ago(t: chrono::DateTime<chrono::Utc>) -> i64 {
    (chrono::Utc::now() - t).num_hours().max(0)
}

async fn fetch_import_tile(state: &AppState) -> LastRunTile {
    #[derive(sqlx::FromRow)]
    struct LastImport {
        started_at: chrono::DateTime<chrono::Utc>,
        status: String,
        added_films: i32,
        added_series: i32,
        added_episodes: i32,
        failed_count: i32,
    }

    // Rozlišuj mezi "prázdná tabulka" (= Ok(None)) a "query selhala" (= Err).
    // Při chybě ukáž červenou dlaždici, ne klidné „zatím žádný běh" — incident
    // s DB bychom jinak na dashboardu přehlédli.
    let row = match sqlx::query_as::<_, LastImport>(
        "SELECT started_at, status, added_films, added_series, added_episodes, failed_count \
         FROM import_runs ORDER BY started_at DESC LIMIT 1",
    )
    .fetch_optional(&state.db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            // Detail do journaldu, obecná hláška do HTML — /admin/ je zatím
            // bez auth a je na veřejném vhostu, neleakovat DB hostname /
            // SQLx driver detaily návštěvníkům.
            tracing::error!("admin dashboard: import_runs query failed: {e}");
            return LastRunTile {
                status: "error",
                message: "Chyba DB (import_runs) — viz journald.".to_string(),
            };
        }
    };

    match row {
        None => LastRunTile {
            status: "none",
            message: "Zatím žádný běh — scanner nebyl spuštěn.".to_string(),
        },
        Some(r) => {
            let h = hours_ago(r.started_at);
            let added = r.added_films + r.added_series + r.added_episodes;
            let status: &'static str = match r.status.as_str() {
                "ok" => "ok",
                "partial" => "partial",
                "error" => "error",
                "running" => "running",
                _ => "none",
            };
            let summary = if r.status == "running" {
                "právě běží".to_string()
            } else if r.failed_count > 0 {
                format!("+{added} / ⚠ {} selhalo", r.failed_count)
            } else {
                format!("+{added} položek")
            };
            LastRunTile {
                status,
                message: format!("Před {h}h — {} ({summary})", r.status),
            }
        }
    }
}

async fn fetch_backup_tile(state: &AppState) -> LastRunTile {
    #[derive(sqlx::FromRow)]
    struct LastBackup {
        started_at: chrono::DateTime<chrono::Utc>,
        status: String,
        size_bytes: Option<i64>,
    }

    // Stejně jako u importu — "query selhala" ≠ "prázdná tabulka".
    let row = match sqlx::query_as::<_, LastBackup>(
        "SELECT started_at, status, size_bytes \
         FROM backup_runs ORDER BY started_at DESC LIMIT 1",
    )
    .fetch_optional(&state.db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            // Stejně jako u import_runs — obecná hláška, detaily do journaldu.
            tracing::error!("admin dashboard: backup_runs query failed: {e}");
            return LastRunTile {
                status: "error",
                message: "Chyba DB (backup_runs) — viz journald.".to_string(),
            };
        }
    };

    match row {
        None => LastRunTile {
            status: "none",
            message: "Zatím žádná záloha — pipeline ještě neběžela.".to_string(),
        },
        Some(r) => {
            let h = hours_ago(r.started_at);
            let status: &'static str = match r.status.as_str() {
                "ok" => "ok",
                "error" => "error",
                "running" => "running",
                _ => "none",
            };
            let size = match r.size_bytes {
                Some(b) if b >= 1024 * 1024 => format!("{:.1} MB", b as f64 / (1024.0 * 1024.0)),
                Some(b) => format!("{} B", b),
                None => "—".to_string(),
            };
            LastRunTile {
                status,
                message: format!("Před {h}h — {} ({size})", r.status),
            }
        }
    }
}

fn cache_tile(state: &AppState) -> LastRunTile {
    // Dashboard tile reflects config state — tracking actual purge history
    // would require a new table; for now admins just see whether the feature
    // is wired up.
    match &state.config.cf_cache_purge {
        Some(_) => LastRunTile {
            status: "ok",
            message: "Nakonfigurováno — klikni pro promazání.".to_string(),
        },
        None => LastRunTile {
            status: "none",
            message: "Chybí CF_CACHE_PURGE_TOKEN / CF_ZONE_ID.".to_string(),
        },
    }
}

/// GET /admin/ — landing page with per-section status tiles.
pub async fn admin_dashboard(State(state): State<AppState>) -> WebResult<Response> {
    let (import_tile, backup_tile) =
        tokio::join!(fetch_import_tile(&state), fetch_backup_tile(&state));

    let tmpl = AdminDashboardTemplate {
        img: state.image_base_url.clone(),
        import_tile,
        backup_tile,
        cache_tile: cache_tile(&state),
    };
    Ok(noindex(tmpl.render()?))
}
