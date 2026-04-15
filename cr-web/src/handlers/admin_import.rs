//! Admin dashboard for the SK Torrent auto-import pipeline (Issue #421).
//!
//! Read-only UI listing recent scanner runs and per-item details. Mounted under
//! `/admin/import/` — currently public, will move under auth in a follow-up.
//!
//! Routes:
//!   GET /admin/import/             — last 30 runs (table)
//!   GET /admin/import/{run_id}     — detail with 4 tabs (added/updated/failed/skipped)
//!   GET /admin/import/{run_id}.json — JSON dump for Claude Code debugging

use askama::Template;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use serde::Serialize;
use sqlx::types::JsonValue;

use crate::error::WebResult;
use crate::state::AppState;

#[derive(sqlx::FromRow)]
struct ImportRunRow {
    id: i32,
    started_at: chrono::DateTime<chrono::Utc>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    status: String,
    trigger: String,
    scanned_pages: i32,
    scanned_videos: i32,
    checkpoint_before: Option<i32>,
    checkpoint_after: Option<i32>,
    added_films: i32,
    added_series: i32,
    added_episodes: i32,
    updated_films: i32,
    updated_episodes: i32,
    failed_count: i32,
    skipped_count: i32,
    error_message: Option<String>,
}

impl ImportRunRow {
    /// Duration in seconds (started → finished, or now if still running).
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
            "partial" => "status-partial",
            "running" => "status-running",
            _ => "status-unknown",
        }
    }
}

#[derive(sqlx::FromRow)]
struct ImportItemRow {
    id: i32,
    sktorrent_video_id: i32,
    sktorrent_url: String,
    sktorrent_title: String,
    detected_type: Option<String>,
    imdb_id: Option<String>,
    season: Option<i16>,
    episode: Option<i16>,
    action: String,
    target_film_id: Option<i32>,
    target_series_id: Option<i32>,
    target_episode_id: Option<i32>,
    failure_step: Option<String>,
    failure_message: Option<String>,
    raw_log: Option<JsonValue>,
}

impl ImportItemRow {
    /// URL on our own site (link to the imported/updated entity).
    fn target_url(&self) -> Option<String> {
        if let Some(_eid) = self.target_episode_id {
            // Need slug + season/episode; without a JOIN we settle for series id
            // (template shows raw fields). This stays usable for debugging.
            return self
                .target_series_id
                .map(|sid| format!("/admin/import/series/{}", sid));
        }
        if let Some(fid) = self.target_film_id {
            return Some(format!("/admin/import/film/{}", fid));
        }
        if let Some(sid) = self.target_series_id {
            return Some(format!("/admin/import/series/{}", sid));
        }
        None
    }
    fn imdb_url(&self) -> Option<String> {
        self.imdb_id
            .as_ref()
            .map(|i| format!("https://www.imdb.com/title/{}/", i))
    }
    fn raw_log_pretty(&self) -> String {
        self.raw_log
            .as_ref()
            .and_then(|v| serde_json::to_string_pretty(v).ok())
            .unwrap_or_default()
    }
    fn season_episode(&self) -> Option<String> {
        match (self.season, self.episode) {
            (Some(s), Some(e)) => Some(format!("S{:02}E{:02}", s, e)),
            _ => None,
        }
    }
}

#[derive(Template)]
#[template(path = "admin_import_list.html")]
struct AdminImportListTemplate {
    img: String,
    runs: Vec<ImportRunRow>,
}

#[derive(Template)]
#[template(path = "admin_import_detail.html")]
struct AdminImportDetailTemplate {
    img: String,
    run: ImportRunRow,
    added: Vec<ImportItemRow>,
    updated: Vec<ImportItemRow>,
    failed: Vec<ImportItemRow>,
    skipped: Vec<ImportItemRow>,
}

/// GET /admin/import/  — list last 30 runs.
pub async fn admin_import_list(State(state): State<AppState>) -> WebResult<Response> {
    let runs = sqlx::query_as::<_, ImportRunRow>(
        "SELECT id, started_at, finished_at, status, trigger, scanned_pages, \
         scanned_videos, checkpoint_before, checkpoint_after, added_films, \
         added_series, added_episodes, updated_films, updated_episodes, \
         failed_count, skipped_count, error_message \
         FROM import_runs ORDER BY started_at DESC LIMIT 30",
    )
    .fetch_all(&state.db)
    .await?;

    let tmpl = AdminImportListTemplate {
        img: state.image_base_url.clone(),
        runs,
    };
    Ok(Html(tmpl.render()?).into_response())
}

/// GET /admin/import/{run_id}  — detail with 4 tabs.
/// Also handles GET /admin/import/{run_id}.json for the Claude Code debugger.
pub async fn admin_import_detail(
    State(state): State<AppState>,
    Path(run_id_str): Path<String>,
) -> WebResult<Response> {
    let (id_part, as_json) = if let Some(stripped) = run_id_str.strip_suffix(".json") {
        (stripped, true)
    } else {
        (run_id_str.as_str(), false)
    };

    let Ok(run_id) = id_part.parse::<i32>() else {
        return Ok((StatusCode::BAD_REQUEST, "Invalid run id").into_response());
    };

    let run = sqlx::query_as::<_, ImportRunRow>(
        "SELECT id, started_at, finished_at, status, trigger, scanned_pages, \
         scanned_videos, checkpoint_before, checkpoint_after, added_films, \
         added_series, added_episodes, updated_films, updated_episodes, \
         failed_count, skipped_count, error_message \
         FROM import_runs WHERE id = $1",
    )
    .bind(run_id)
    .fetch_optional(&state.db)
    .await?;

    let Some(run) = run else {
        return Ok((StatusCode::NOT_FOUND, "Run not found").into_response());
    };

    let items = sqlx::query_as::<_, ImportItemRow>(
        "SELECT id, sktorrent_video_id, sktorrent_url, sktorrent_title, \
         detected_type, imdb_id, season, episode, action, target_film_id, \
         target_series_id, target_episode_id, failure_step, failure_message, \
         raw_log \
         FROM import_items WHERE run_id = $1 ORDER BY id",
    )
    .bind(run_id)
    .fetch_all(&state.db)
    .await?;

    if as_json {
        // Strojitelný export pro Claude Code debugger.
        return Ok(Json(serialize_run_detail(&run, &items)).into_response());
    }

    // Group by action category
    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut failed = Vec::new();
    let mut skipped = Vec::new();
    for it in items {
        match it.action.as_str() {
            "added_film" | "added_series" | "added_episode" => added.push(it),
            "updated_film" | "updated_episode" => updated.push(it),
            "failed" => failed.push(it),
            "skipped" => skipped.push(it),
            _ => {}
        }
    }

    let tmpl = AdminImportDetailTemplate {
        img: state.image_base_url.clone(),
        run,
        added,
        updated,
        failed,
        skipped,
    };
    Ok(Html(tmpl.render()?).into_response())
}

#[derive(Serialize)]
struct RunDetailJson<'a> {
    run: RunJson<'a>,
    items: Vec<ItemJson<'a>>,
}

#[derive(Serialize)]
struct RunJson<'a> {
    id: i32,
    started_at: String,
    finished_at: Option<String>,
    status: &'a str,
    trigger: &'a str,
    scanned_pages: i32,
    scanned_videos: i32,
    checkpoint_before: Option<i32>,
    checkpoint_after: Option<i32>,
    added_films: i32,
    added_series: i32,
    added_episodes: i32,
    updated_films: i32,
    updated_episodes: i32,
    failed_count: i32,
    skipped_count: i32,
    error_message: Option<&'a str>,
}

#[derive(Serialize)]
struct ItemJson<'a> {
    id: i32,
    sktorrent_video_id: i32,
    sktorrent_url: &'a str,
    sktorrent_title: &'a str,
    detected_type: Option<&'a str>,
    imdb_id: Option<&'a str>,
    season: Option<i16>,
    episode: Option<i16>,
    action: &'a str,
    target_film_id: Option<i32>,
    target_series_id: Option<i32>,
    target_episode_id: Option<i32>,
    failure_step: Option<&'a str>,
    failure_message: Option<&'a str>,
    raw_log: Option<&'a JsonValue>,
}

fn serialize_run_detail<'a>(
    run: &'a ImportRunRow,
    items: &'a [ImportItemRow],
) -> RunDetailJson<'a> {
    RunDetailJson {
        run: RunJson {
            id: run.id,
            started_at: run.started_at.to_rfc3339(),
            finished_at: run.finished_at.map(|t| t.to_rfc3339()),
            status: &run.status,
            trigger: &run.trigger,
            scanned_pages: run.scanned_pages,
            scanned_videos: run.scanned_videos,
            checkpoint_before: run.checkpoint_before,
            checkpoint_after: run.checkpoint_after,
            added_films: run.added_films,
            added_series: run.added_series,
            added_episodes: run.added_episodes,
            updated_films: run.updated_films,
            updated_episodes: run.updated_episodes,
            failed_count: run.failed_count,
            skipped_count: run.skipped_count,
            error_message: run.error_message.as_deref(),
        },
        items: items
            .iter()
            .map(|it| ItemJson {
                id: it.id,
                sktorrent_video_id: it.sktorrent_video_id,
                sktorrent_url: &it.sktorrent_url,
                sktorrent_title: &it.sktorrent_title,
                detected_type: it.detected_type.as_deref(),
                imdb_id: it.imdb_id.as_deref(),
                season: it.season,
                episode: it.episode,
                action: &it.action,
                target_film_id: it.target_film_id,
                target_series_id: it.target_series_id,
                target_episode_id: it.target_episode_id,
                failure_step: it.failure_step.as_deref(),
                failure_message: it.failure_message.as_deref(),
                raw_log: it.raw_log.as_ref(),
            })
            .collect(),
    }
}
