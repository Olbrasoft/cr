//! Admin dashboard for prehraj.to importer observability (#657, parent #656).
//!
//! Routes:
//!   GET /admin/prehrajto/unmatched      — list unresolved unmatched clusters
//!   GET /admin/prehrajto/unmatched.csv  — CSV export of the same dataset
//!
//! Source data: `prehrajto_unmatched_clusters` table (see migration
//! 20260604_064). Rows land there during the daily sitemap import for
//! every film-shape cluster the importer could not match against the
//! `films` table. Operator uses this view to decide which titles are
//! worth backfilling manually or where the matching heuristic is too
//! strict.

use askama::Template;
use axum::extract::State;
use axum::http::HeaderValue;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse, Response};

use crate::error::WebResult;
use crate::state::AppState;

#[derive(sqlx::FromRow)]
struct UnmatchedRow {
    id: i32,
    cluster_key: String,
    year: Option<i32>,
    duration_bucket: Option<i32>,
    sample_title: String,
    sample_url: String,
    upload_count: i32,
    first_seen_at: chrono::DateTime<chrono::Utc>,
    last_seen_at: chrono::DateTime<chrono::Utc>,
    attempt_count: i32,
}

impl UnmatchedRow {
    fn first_seen_at_str(&self) -> String {
        self.first_seen_at.format("%Y-%m-%d %H:%M").to_string()
    }
    fn last_seen_at_str(&self) -> String {
        self.last_seen_at.format("%Y-%m-%d %H:%M").to_string()
    }
    fn duration_min(&self) -> Option<i32> {
        // Bucket size is 3 minutes (see scripts/import-prehrajto-uploads.py
        // ::cluster_key — duration_sec / (3 * 60)). Multiplying back gives the
        // lower bound of the bucket in minutes — close enough for human eyes.
        self.duration_bucket.map(|b| b * 3)
    }
    fn year_str(&self) -> String {
        self.year.map(|y| y.to_string()).unwrap_or_default()
    }
}

#[derive(Template)]
#[template(path = "admin_prehrajto_unmatched.html")]
struct AdminPrehrajtoUnmatchedTemplate {
    img: String,
    rows: Vec<UnmatchedRow>,
    total_rows: i64,
    total_uploads: i64,
}

/// HTML table is capped — operator only ever needs the loudest entries.
const QUERY_LIST_UNRESOLVED: &str = "SELECT id, cluster_key, year, duration_bucket, \
     sample_title, sample_url, upload_count, first_seen_at, last_seen_at, \
     attempt_count \
     FROM prehrajto_unmatched_clusters \
     WHERE resolved_at IS NULL \
     ORDER BY upload_count DESC, last_seen_at DESC \
     LIMIT 500";

/// CSV export streams every unresolved row — no LIMIT — because the
/// whole point of CSV is offline analysis where the top-500 slice would
/// hide long-tail entries the operator might want to grep through.
const QUERY_FULL_UNRESOLVED: &str = "SELECT id, cluster_key, year, duration_bucket, \
     sample_title, sample_url, upload_count, first_seen_at, last_seen_at, \
     attempt_count \
     FROM prehrajto_unmatched_clusters \
     WHERE resolved_at IS NULL \
     ORDER BY upload_count DESC, last_seen_at DESC";

fn noindex(html: String) -> Response {
    let mut resp = Html(html).into_response();
    resp.headers_mut().insert(
        "X-Robots-Tag",
        HeaderValue::from_static("noindex, nofollow"),
    );
    resp
}

/// GET /admin/prehrajto/unmatched — table of unresolved clusters, capped at
/// the top 500 rows by `upload_count` / `last_seen_at`. No pagination
/// controls — the long tail belongs in the CSV export, not in the UI.
pub async fn admin_prehrajto_unmatched(State(state): State<AppState>) -> WebResult<Response> {
    let rows = sqlx::query_as::<_, UnmatchedRow>(QUERY_LIST_UNRESOLVED)
        .fetch_all(&state.db)
        .await?;

    // Aggregate stats over the full unresolved set (not just the displayed
    // 500), so the operator sees the true scope even when the table is
    // truncated. Two cheap COUNTs in one round-trip via UNION ALL would also
    // work; the two queries here are clearer and still sub-millisecond on
    // an indexed table this small.
    let (total_rows, total_uploads): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT, COALESCE(SUM(upload_count), 0)::BIGINT \
         FROM prehrajto_unmatched_clusters WHERE resolved_at IS NULL",
    )
    .fetch_one(&state.db)
    .await?;

    let tmpl = AdminPrehrajtoUnmatchedTemplate {
        img: state.image_base_url.clone(),
        rows,
        total_rows,
        total_uploads,
    };
    Ok(noindex(tmpl.render()?))
}

/// GET /admin/prehrajto/unmatched.csv — CSV export of EVERY unresolved row.
pub async fn admin_prehrajto_unmatched_csv(State(state): State<AppState>) -> WebResult<Response> {
    let rows = sqlx::query_as::<_, UnmatchedRow>(QUERY_FULL_UNRESOLVED)
        .fetch_all(&state.db)
        .await?;

    let mut out = String::with_capacity(64 + rows.len() * 200);
    out.push_str(
        "id,cluster_key,year,duration_min,upload_count,attempt_count,\
         first_seen_at,last_seen_at,sample_title,sample_url\n",
    );
    for r in &rows {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{}\n",
            r.id,
            csv_escape(&r.cluster_key),
            r.year_str(),
            r.duration_min().map(|m| m.to_string()).unwrap_or_default(),
            r.upload_count,
            r.attempt_count,
            r.first_seen_at_str(),
            r.last_seen_at_str(),
            csv_escape(&r.sample_title),
            csv_escape(&r.sample_url),
        ));
    }

    let mut resp = out.into_response();
    resp.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/csv; charset=utf-8"),
    );
    resp.headers_mut().insert(
        "X-Robots-Tag",
        HeaderValue::from_static("noindex, nofollow"),
    );
    resp.headers_mut().insert(
        axum::http::header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"prehrajto-unmatched.csv\""),
    );
    Ok(resp)
}

/// RFC 4180-ish CSV cell quoting — wrap in double quotes and escape any
/// embedded double quotes by doubling them. Only emit quotes when the
/// value contains a comma, quote, or newline.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        let escaped = s.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        s.to_string()
    }
}
