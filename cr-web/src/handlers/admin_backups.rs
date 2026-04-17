//! Admin dashboard for the PostgreSQL auto-backup pipeline.
//!
//! Read-only UI listing the last 30 `pg_dump` runs that `scripts/backup-db.sh`
//! pushed to Cloudflare R2. Mounted under `/admin/backups/` — same auth story
//! as `/admin/import/` (currently public, see `admin_import.rs`).
//!
//! Routes:
//!   GET /admin/backups/  — last 30 runs (table + freshness banner)
//!   GET /admin/backups   — same, without trailing slash

use askama::Template;
use axum::extract::State;
use axum::response::{Html, IntoResponse, Response};

use crate::error::WebResult;
use crate::state::AppState;

#[derive(sqlx::FromRow)]
struct BackupRunRow {
    id: i32,
    started_at: chrono::DateTime<chrono::Utc>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    status: String,
    trigger: String,
    size_bytes: Option<i64>,
    dump_filename: Option<String>,
    error_message: Option<String>,
}

impl BackupRunRow {
    fn duration_sec(&self) -> i64 {
        let end = self.finished_at.unwrap_or_else(chrono::Utc::now);
        (end - self.started_at).num_seconds().max(0)
    }
    fn started_at_str(&self) -> String {
        self.started_at.format("%Y-%m-%d %H:%M").to_string()
    }
    fn status_class(&self) -> &'static str {
        match self.status.as_str() {
            "ok" => "status-ok",
            "error" => "status-error",
            "running" => "status-running",
            _ => "status-unknown",
        }
    }
    /// Human-friendly size (e.g. "24.3 MB") or "—" when pg_dump failed
    /// before producing the file.
    fn size_human(&self) -> String {
        match self.size_bytes {
            None => "—".to_string(),
            Some(b) if b < 1024 => format!("{} B", b),
            Some(b) if b < 1024 * 1024 => format!("{:.1} KB", b as f64 / 1024.0),
            Some(b) if b < 1024 * 1024 * 1024 => format!("{:.1} MB", b as f64 / (1024.0 * 1024.0)),
            Some(b) => format!("{:.2} GB", b as f64 / (1024.0 * 1024.0 * 1024.0)),
        }
    }
}

/// Freshness banner at the top of the page. Daily timer runs at 03:00 UTC, so
/// anything over ~26h means yesterday's run was skipped/failed.
struct FreshnessBanner {
    css_class: &'static str,
    text: String,
}

impl FreshnessBanner {
    fn from_hours(hours: Option<i64>) -> Self {
        match hours {
            None => Self {
                css_class: "banner-error",
                text: "🚨 Dosud žádná úspěšná záloha — pipeline ještě neběžela nebo každá proběhla s chybou.".to_string(),
            },
            Some(h) if h <= 26 => Self {
                css_class: "banner-ok",
                text: format!("✅ Poslední úspěšná záloha před {h} hodinami — vše v pořádku."),
            },
            Some(h) if h <= 48 => Self {
                css_class: "banner-warn",
                text: format!("⚠️ Poslední úspěšná záloha před {h} hodinami — očekáváme denní běh."),
            },
            Some(h) => Self {
                css_class: "banner-error",
                text: format!("🚨 Poslední úspěšná záloha před {h} hodinami — zkontroluj systemd timer a R2 token."),
            },
        }
    }
}

#[derive(Template)]
#[template(path = "admin_backups.html")]
struct AdminBackupsTemplate {
    img: String,
    runs: Vec<BackupRunRow>,
    banner: FreshnessBanner,
}

/// Apply X-Robots-Tag noindex — mirror what admin_import does.
fn noindex(html: String) -> Response {
    let mut resp = Html(html).into_response();
    resp.headers_mut().insert(
        "X-Robots-Tag",
        axum::http::HeaderValue::from_static("noindex, nofollow"),
    );
    resp
}

/// GET /admin/backups/  — last 30 backup runs.
pub async fn admin_backups_list(State(state): State<AppState>) -> WebResult<Response> {
    let runs = sqlx::query_as::<_, BackupRunRow>(
        "SELECT id, started_at, finished_at, status, trigger, size_bytes, \
         dump_filename, error_message \
         FROM backup_runs ORDER BY started_at DESC LIMIT 30",
    )
    .fetch_all(&state.db)
    .await?;

    // Separate query so the banner stays honest even if the last 30 rows are
    // all failures (a real scenario when R2 credentials rotate).
    let last_ok_started_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT started_at FROM backup_runs WHERE status = 'ok' \
         ORDER BY started_at DESC LIMIT 1",
    )
    .fetch_optional(&state.db)
    .await?;

    let hours_since_last_ok =
        last_ok_started_at.map(|t| (chrono::Utc::now() - t).num_hours().max(0));

    let tmpl = AdminBackupsTemplate {
        img: state.image_base_url.clone(),
        runs,
        banner: FreshnessBanner::from_hours(hours_since_last_ok),
    };
    Ok(noindex(tmpl.render()?))
}
