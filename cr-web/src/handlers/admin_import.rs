//! Admin dashboard for the SK Torrent auto-import pipeline (Issue #421).
//!
//! Read-only UI listing recent scanner runs and per-item details. Mounted under
//! `/admin/import/` — currently public, will move under auth in a follow-up.
//!
//! Routes:
//!   GET /admin/import/             — last 30 runs (table)
//!   GET /admin/import/summary      — all-time aggregated view (films/series/failures)
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
    added_tv_shows: i32,
    added_tv_episodes: i32,
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
    target_tv_show_id: Option<i32>,
    target_tv_episode_id: Option<i32>,
    #[sqlx(default)]
    target_film_slug: Option<String>,
    #[sqlx(default)]
    target_series_slug: Option<String>,
    #[sqlx(default)]
    target_episode_slug: Option<String>,
    /// Series slug reached via target_episode_id → episodes.series_id → series.slug.
    /// Used to build /serialy-online/{series-slug}/{episode-slug}/ URLs.
    #[sqlx(default)]
    target_episode_series_slug: Option<String>,
    #[sqlx(default)]
    target_tv_show_slug: Option<String>,
    /// Episode slug on tv_episodes (e.g. "s01e03"). Used alongside the show
    /// slug to build /tv-porady/{show}/{ep}/ URLs.
    #[sqlx(default)]
    target_tv_episode_slug: Option<String>,
    /// tv_shows.slug reached via target_tv_episode_id → tv_episodes.tv_show_id.
    /// Required to build the episode URL when only target_tv_episode_id is set.
    #[sqlx(default)]
    target_tv_episode_show_slug: Option<String>,
    failure_step: Option<String>,
    failure_message: Option<String>,
    raw_log: Option<JsonValue>,
}

impl ImportItemRow {
    /// URL on our own site linking back to the imported/updated entity.
    /// Slugs come from the detail handler's JOIN so this is a pure format.
    fn target_url(&self) -> Option<String> {
        if let Some(slug) = self.target_film_slug.as_deref() {
            return Some(format!("/filmy-online/{slug}/"));
        }
        if let (Some(series_slug), Some(ep_slug)) = (
            self.target_episode_series_slug.as_deref(),
            self.target_episode_slug.as_deref(),
        ) {
            return Some(format!("/serialy-online/{series_slug}/{ep_slug}/"));
        }
        if let (Some(series_slug), Some(season), Some(episode)) = (
            self.target_episode_series_slug.as_deref(),
            self.season,
            self.episode,
        ) {
            // Episode slug not set yet — fall back to legacy N×M URL.
            return Some(format!("/serialy-online/{series_slug}/{season}x{episode}/"));
        }
        if let Some(slug) = self.target_series_slug.as_deref() {
            return Some(format!("/serialy-online/{slug}/"));
        }
        if let (Some(show_slug), Some(ep_slug)) = (
            self.target_tv_episode_show_slug.as_deref(),
            self.target_tv_episode_slug.as_deref(),
        ) {
            return Some(format!("/tv-porady/{show_slug}/{ep_slug}/"));
        }
        if let (Some(show_slug), Some(season), Some(episode)) = (
            self.target_tv_episode_show_slug.as_deref(),
            self.season,
            self.episode,
        ) {
            // TV episode slug not set yet — fall back to legacy N×M URL.
            return Some(format!("/tv-porady/{show_slug}/{season}x{episode}/"));
        }
        if let Some(slug) = self.target_tv_show_slug.as_deref() {
            return Some(format!("/tv-porady/{slug}/"));
        }
        None
    }
    fn imdb_url(&self) -> Option<String> {
        self.imdb_id
            .as_ref()
            .map(|i| format!("https://www.imdb.com/title/{}/", i))
    }
    /// Pretty-printed JSON, truncated to 4 KB so failure-tab pages don't bloat
    /// even when raw_log carries large TMDB/Gemma payloads. Full payload is
    /// always available via the .json export endpoint.
    fn raw_log_pretty(&self) -> String {
        const MAX_LEN: usize = 4096;
        let full = self
            .raw_log
            .as_ref()
            .and_then(|v| serde_json::to_string_pretty(v).ok())
            .unwrap_or_default();
        if full.len() <= MAX_LEN {
            full
        } else {
            format!(
                "{}\n... [truncated; {} bytes total — see /admin/import/{{run_id}}.json for full payload]",
                &full[..MAX_LEN],
                full.len()
            )
        }
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

/// Apply X-Robots-Tag noindex header on every admin response so admin pages
/// stay out of search indexes regardless of inherited HTML meta tags.
fn noindex(html: String) -> Response {
    let mut resp = Html(html).into_response();
    resp.headers_mut().insert(
        "X-Robots-Tag",
        axum::http::HeaderValue::from_static("noindex, nofollow"),
    );
    resp
}

/// GET /admin/import/  — list last 30 runs.
pub async fn admin_import_list(State(state): State<AppState>) -> WebResult<Response> {
    let runs = sqlx::query_as::<_, ImportRunRow>(
        "SELECT id, started_at, finished_at, status, trigger, scanned_pages, \
         scanned_videos, checkpoint_before, checkpoint_after, added_films, \
         added_series, added_episodes, added_tv_shows, added_tv_episodes, \
         updated_films, updated_episodes, failed_count, skipped_count, error_message \
         FROM import_runs ORDER BY started_at DESC LIMIT 30",
    )
    .fetch_all(&state.db)
    .await?;

    let tmpl = AdminImportListTemplate {
        img: state.image_base_url.clone(),
        runs,
    };
    Ok(noindex(tmpl.render()?))
}

#[derive(Template)]
#[template(path = "admin_import_failures.html")]
struct AdminImportFailuresTemplate {
    img: String,
    failures: Vec<FailureItemRow>,
}

#[derive(sqlx::FromRow)]
struct FailureItemRow {
    run_id: i32,
    #[allow(dead_code)]
    sktorrent_video_id: i32,
    sktorrent_url: String,
    sktorrent_title: String,
    failure_step: Option<String>,
    failure_message: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl FailureItemRow {
    fn created_at_str(&self) -> String {
        self.created_at.format("%Y-%m-%d %H:%M").to_string()
    }
}

/// GET /admin/import/failures — aggregated failures across all runs.
pub async fn admin_import_failures(State(state): State<AppState>) -> WebResult<Response> {
    let failures = sqlx::query_as::<_, FailureItemRow>(
        "SELECT run_id, sktorrent_video_id, sktorrent_url, sktorrent_title, \
         failure_step, failure_message, created_at \
         FROM import_items WHERE action = 'failed' \
         ORDER BY created_at DESC LIMIT 100",
    )
    .fetch_all(&state.db)
    .await?;

    let tmpl = AdminImportFailuresTemplate {
        img: state.image_base_url.clone(),
        failures,
    };
    Ok(noindex(tmpl.render()?))
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
         added_series, added_episodes, added_tv_shows, added_tv_episodes, \
         updated_films, updated_episodes, failed_count, skipped_count, error_message \
         FROM import_runs WHERE id = $1",
    )
    .bind(run_id)
    .fetch_optional(&state.db)
    .await?;

    let Some(run) = run else {
        return Ok((StatusCode::NOT_FOUND, "Run not found").into_response());
    };

    let items = sqlx::query_as::<_, ImportItemRow>(
        "SELECT i.id, i.sktorrent_video_id, i.sktorrent_url, i.sktorrent_title, \
         i.detected_type, i.imdb_id, i.season, i.episode, i.action, \
         i.target_film_id, i.target_series_id, i.target_episode_id, \
         i.target_tv_show_id, i.target_tv_episode_id, \
         f.slug AS target_film_slug, \
         s.slug AS target_series_slug, \
         e.slug AS target_episode_slug, \
         es.slug AS target_episode_series_slug, \
         ts.slug AS target_tv_show_slug, \
         te.slug AS target_tv_episode_slug, \
         tes.slug AS target_tv_episode_show_slug, \
         i.failure_step, i.failure_message, i.raw_log \
         FROM import_items i \
         LEFT JOIN films f     ON f.id   = i.target_film_id \
         LEFT JOIN series s    ON s.id   = i.target_series_id \
         LEFT JOIN episodes e  ON e.id   = i.target_episode_id \
         LEFT JOIN series es   ON es.id  = e.series_id \
         LEFT JOIN tv_shows ts ON ts.id  = i.target_tv_show_id \
         LEFT JOIN tv_episodes te ON te.id = i.target_tv_episode_id \
         LEFT JOIN tv_shows tes ON tes.id = te.tv_show_id \
         WHERE i.run_id = $1 \
         ORDER BY i.id",
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
            "added_film" | "added_series" | "added_episode" | "added_tv_show"
            | "added_tv_episode" => added.push(it),
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
    Ok(noindex(tmpl.render()?))
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
    target_tv_show_id: Option<i32>,
    target_tv_episode_id: Option<i32>,
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
                target_tv_show_id: it.target_tv_show_id,
                target_tv_episode_id: it.target_tv_episode_id,
                failure_step: it.failure_step.as_deref(),
                failure_message: it.failure_message.as_deref(),
                raw_log: it.raw_log.as_ref(),
            })
            .collect(),
    }
}

// --- Manual trigger (#422) ---

#[derive(serde::Deserialize)]
pub struct RunNowForm {
    /// Hard cap on processed videos. Default 5 keeps ad-hoc runs safe;
    /// the daily cron passes a higher value. Server clamps to 1..=100 so a
    /// stray submission can't trigger a marathon scan.
    #[serde(default = "default_max_new")]
    pub max_new: u32,
}

fn default_max_new() -> u32 {
    5
}

const MAX_NEW_HARD_CAP: u32 = 100;

/// POST /admin/import/run — fire-and-redirect manual scanner trigger.
///
/// Spawns `python3 scripts/auto-import.py --trigger manual --max-new N` as
/// a detached subprocess and 303-redirects to the dashboard. The script
/// INSERTs an `import_runs` row immediately so the new run appears in the
/// list within a second or two.
///
/// Guarded by env `ADMIN_IMPORT_RUN_ENABLED=1` — without it we 403, so a
/// stray POST on production can't kick off an unbounded subprocess. Set
/// the flag in the production .env once the cron rollout is signed off.
pub async fn admin_import_run(
    State(state): State<AppState>,
    axum::extract::Form(form): axum::extract::Form<RunNowForm>,
) -> WebResult<Response> {
    if !state.config.admin_import_run_enabled {
        return Ok((
            StatusCode::FORBIDDEN,
            "Manual run is disabled. Set ADMIN_IMPORT_RUN_ENABLED=1 in the env to enable.",
        )
            .into_response());
    }

    // Clamp user input to a sane range so we don't accidentally scan
    // thousands of pages on a fat-fingered submission.
    let max_new = form.max_new.clamp(1, MAX_NEW_HARD_CAP);

    let repo_root = state.config.cr_repo_root.clone();
    let script = format!("{}/scripts/auto-import.py", repo_root);

    let spawn_result = tokio::process::Command::new("python3")
        .arg("-u")
        .arg(&script)
        .arg("--trigger")
        .arg("manual")
        .arg("--max-new")
        .arg(max_new.to_string())
        .current_dir(&repo_root)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();

    if let Err(e) = spawn_result {
        tracing::error!("admin_import_run: spawn failed: {e}");
        return Ok((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Cannot spawn scanner: {e}"),
        )
            .into_response());
    }

    Ok((
        StatusCode::SEE_OTHER,
        [(axum::http::header::LOCATION, "/admin/import/")],
    )
        .into_response())
}

// ---- GET /admin/import/summary — aggregated view across ALL runs ----

#[derive(sqlx::FromRow)]
struct NewFilmRow {
    title: String,
    slug: String,
    year: Option<i16>,
    runtime_min: Option<i16>,
    imdb_id: Option<String>,
    sktorrent_title: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl NewFilmRow {
    fn when_str(&self) -> String {
        self.created_at.format("%Y-%m-%d %H:%M").to_string()
    }
    fn year_str(&self) -> String {
        self.year.map(|y| format!(" ({y})")).unwrap_or_default()
    }
    fn runtime_str(&self) -> String {
        self.runtime_min
            .map(|m| format!("{m} min"))
            .unwrap_or_default()
    }
}

#[derive(sqlx::FromRow)]
struct UpdatedFilmRow {
    title: String,
    slug: String,
    year: Option<i16>,
    sktorrent_title: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl UpdatedFilmRow {
    fn when_str(&self) -> String {
        self.created_at.format("%Y-%m-%d %H:%M").to_string()
    }
    fn year_str(&self) -> String {
        self.year.map(|y| format!(" ({y})")).unwrap_or_default()
    }
}

#[derive(sqlx::FromRow)]
struct NewSeriesRow {
    series_title: String,
    series_slug: String,
    first_air_year: Option<i16>,
    imdb_id: Option<String>,
    episode_count: i64,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl NewSeriesRow {
    fn when_str(&self) -> String {
        self.created_at.format("%Y-%m-%d %H:%M").to_string()
    }
    fn year_str(&self) -> String {
        self.first_air_year
            .map(|y| format!(" ({y})"))
            .unwrap_or_default()
    }
}

#[derive(sqlx::FromRow)]
struct EpisodeForExistingRow {
    series_title: String,
    series_slug: String,
    season: i16,
    episode: i16,
    episode_name: Option<String>,
    sktorrent_title: String,
    action: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl EpisodeForExistingRow {
    fn when_str(&self) -> String {
        self.created_at.format("%Y-%m-%d %H:%M").to_string()
    }
    fn ep_code(&self) -> String {
        format!("S{:02}E{:02}", self.season, self.episode)
    }
    fn is_added(&self) -> bool {
        self.action == "added_episode"
    }
}

#[derive(sqlx::FromRow)]
struct SkippedRow {
    sktorrent_title: String,
    sktorrent_url: String,
    detected_type: Option<String>,
    failure_step: Option<String>,
    failure_message: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl SkippedRow {
    fn when_str(&self) -> String {
        self.created_at.format("%Y-%m-%d %H:%M").to_string()
    }
    fn reason(&self) -> String {
        match (
            self.failure_step.as_deref(),
            self.failure_message.as_deref(),
        ) {
            (Some(s), Some(m)) => format!("{s}: {m}"),
            (Some(s), None) => s.to_string(),
            (_, Some(m)) => m.to_string(),
            _ => "-".to_string(),
        }
    }
}

#[derive(Template)]
#[template(path = "admin_import_summary.html")]
struct AdminImportSummaryTemplate {
    img: String,
    new_films: Vec<NewFilmRow>,
    updated_films: Vec<UpdatedFilmRow>,
    new_series: Vec<NewSeriesRow>,
    episodes_for_existing: Vec<EpisodeForExistingRow>,
    skipped: Vec<SkippedRow>,
    failed: Vec<FailureItemRow>,
    total_runs: i64,
    /// Issue #566 — TV pořady don't have card panels yet (those are
    /// follow-up to #563), so for now we surface them as scalar counts in
    /// the stats row alongside the other "all-time" tallies.
    new_tv_shows_count: i64,
    new_tv_episodes_count: i64,
}

/// GET /admin/import/summary — single page aggregating every decision
/// the auto-import has ever made, broken down by outcome category.
pub async fn admin_import_summary(State(state): State<AppState>) -> WebResult<Response> {
    // Films created by auto-import (joined via target_film_id).
    let new_films = sqlx::query_as::<_, NewFilmRow>(
        "SELECT f.title, f.slug, f.year, f.runtime_min, \
         f.imdb_id, i.sktorrent_title, i.created_at \
         FROM import_items i JOIN films f ON f.id = i.target_film_id \
         WHERE i.action = 'added_film' \
         ORDER BY i.created_at DESC LIMIT 200",
    )
    .fetch_all(&state.db)
    .await?;

    // Films where we only attached a new SKT playback source.
    let updated_films = sqlx::query_as::<_, UpdatedFilmRow>(
        "SELECT f.title, f.slug, f.year, i.sktorrent_title, i.created_at \
         FROM import_items i JOIN films f ON f.id = i.target_film_id \
         WHERE i.action = 'updated_film' \
         ORDER BY i.created_at DESC LIMIT 200",
    )
    .fetch_all(&state.db)
    .await?;

    // Brand-new series — detect via the heuristic "series.added_at equals
    // the earliest import_items.created_at referencing any of its episodes".
    // The dedicated action='added_series' row is only written for runs
    // #9 onwards; this keeps historical runs (#3–#8) visible too by looking
    // at the episode trail instead.
    let new_series = sqlx::query_as::<_, NewSeriesRow>(
        "WITH per_series AS ( \
            SELECT e.series_id, MIN(i.created_at) AS first_touch, COUNT(*) AS ep_cnt \
            FROM import_items i JOIN episodes e ON e.id = i.target_episode_id \
            WHERE i.action IN ('added_episode') \
            GROUP BY e.series_id \
         ) \
         SELECT s.title AS series_title, s.slug AS series_slug, \
                s.first_air_year, s.imdb_id, \
                ps.ep_cnt AS episode_count, \
                ps.first_touch AS created_at \
         FROM per_series ps JOIN series s ON s.id = ps.series_id \
         WHERE s.added_at <= ps.first_touch + INTERVAL '2 minutes' \
           AND s.added_at >= ps.first_touch - INTERVAL '2 minutes' \
         ORDER BY ps.first_touch DESC LIMIT 100",
    )
    .fetch_all(&state.db)
    .await?;

    // Collect ids of series the auto-import CREATED so the "episodes for
    // existing series" panel can exclude them (their episodes show under
    // the new-series panel instead). Same 2-minute window heuristic.
    let new_series_ids: Vec<i32> = sqlx::query_scalar(
        "SELECT s.id \
         FROM series s JOIN ( \
             SELECT e.series_id, MIN(i.created_at) AS first_touch \
             FROM import_items i JOIN episodes e ON e.id = i.target_episode_id \
             WHERE i.action IN ('added_episode') \
             GROUP BY e.series_id \
         ) ps ON ps.series_id = s.id \
         WHERE s.added_at <= ps.first_touch + INTERVAL '2 minutes' \
           AND s.added_at >= ps.first_touch - INTERVAL '2 minutes'",
    )
    .fetch_all(&state.db)
    .await?;

    let episodes_for_existing = sqlx::query_as::<_, EpisodeForExistingRow>(
        "SELECT s.title AS series_title, s.slug AS series_slug, \
         e.season, e.episode, e.episode_name, i.sktorrent_title, i.action, i.created_at \
         FROM import_items i \
         JOIN episodes e ON e.id = i.target_episode_id \
         JOIN series s ON s.id = e.series_id \
         WHERE i.action IN ('added_episode', 'updated_episode') \
           AND (e.series_id IS NULL OR NOT (e.series_id = ANY($1))) \
         ORDER BY i.created_at DESC LIMIT 300",
    )
    .bind(&new_series_ids)
    .fetch_all(&state.db)
    .await?;

    let skipped = sqlx::query_as::<_, SkippedRow>(
        "SELECT sktorrent_title, sktorrent_url, detected_type, \
         failure_step, failure_message, created_at \
         FROM import_items WHERE action = 'skipped' \
         ORDER BY created_at DESC LIMIT 200",
    )
    .fetch_all(&state.db)
    .await?;

    let failed = sqlx::query_as::<_, FailureItemRow>(
        "SELECT run_id, sktorrent_video_id, sktorrent_url, sktorrent_title, \
         failure_step, failure_message, created_at \
         FROM import_items WHERE action = 'failed' \
         ORDER BY created_at DESC LIMIT 100",
    )
    .fetch_all(&state.db)
    .await?;

    let total_runs = sqlx::query_scalar::<_, Option<i64>>("SELECT COUNT(*) FROM import_runs")
        .fetch_one(&state.db)
        .await?
        .unwrap_or(0);

    let new_tv_shows_count = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT COUNT(*) FROM import_items WHERE action = 'added_tv_show'",
    )
    .fetch_one(&state.db)
    .await?
    .unwrap_or(0);
    let new_tv_episodes_count = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT COUNT(*) FROM import_items WHERE action = 'added_tv_episode'",
    )
    .fetch_one(&state.db)
    .await?
    .unwrap_or(0);

    let tmpl = AdminImportSummaryTemplate {
        img: state.image_base_url.clone(),
        new_films,
        updated_films,
        new_series,
        episodes_for_existing,
        skipped,
        failed,
        total_runs,
        new_tv_shows_count,
        new_tv_episodes_count,
    };
    Ok(noindex(tmpl.render()?))
}
