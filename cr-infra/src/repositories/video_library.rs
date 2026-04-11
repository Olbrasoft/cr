//! PostgreSQL implementation of [`VideoRepository`].
//!
//! Backed by the `videos` table created in
//! `migrations/20260412_021_create_videos.sql` and evolved in
//! `migrations/20260413_022_videos_unique_by_container.sql`. The unique
//! constraint is `(source_url, quality, format_ext)` — which is the
//! dedup key. `find_by_source_quality_and_format` is the cheap path the
//! upload pipeline calls before invoking yt-dlp.

use chrono::{DateTime, Utc};
use cr_domain::repository::{NewVideo, VideoRecord, VideoRepository};

/// PostgreSQL implementation of [`VideoRepository`].
pub struct PgVideoRepository {
    pool: sqlx::PgPool,
}

impl PgVideoRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct VideoRow {
    id: i32,
    source_url: String,
    title: String,
    description: Option<String>,
    duration_sec: Option<i32>,
    source_extractor: Option<String>,
    quality: String,
    format_ext: String,
    streamtape_file_id: String,
    streamtape_url: String,
    file_size_bytes: i64,
    thumbnail_r2_key: Option<String>,
    thumbnail_url: Option<String>,
    created_at: DateTime<Utc>,
    last_accessed_at: DateTime<Utc>,
    resolution: Option<String>,
}

impl From<VideoRow> for VideoRecord {
    fn from(r: VideoRow) -> Self {
        Self {
            id: r.id,
            source_url: r.source_url,
            title: r.title,
            description: r.description,
            duration_sec: r.duration_sec,
            source_extractor: r.source_extractor,
            quality: r.quality,
            format_ext: r.format_ext,
            streamtape_file_id: r.streamtape_file_id,
            streamtape_url: r.streamtape_url,
            file_size_bytes: r.file_size_bytes,
            thumbnail_r2_key: r.thumbnail_r2_key,
            thumbnail_url: r.thumbnail_url,
            // ISO 8601 + offset, e.g. 2026-04-07T14:30:00.123Z
            created_at: r.created_at.to_rfc3339(),
            last_accessed_at: r.last_accessed_at.to_rfc3339(),
            resolution: r.resolution,
        }
    }
}

const SELECT_COLUMNS: &str = "id, source_url, title, description, duration_sec, \
                              source_extractor, quality, format_ext, streamtape_file_id, \
                              streamtape_url, file_size_bytes, thumbnail_r2_key, \
                              thumbnail_url, created_at, last_accessed_at, resolution";

impl VideoRepository for PgVideoRepository {
    type Error = sqlx::Error;

    async fn insert(&self, video: NewVideo) -> Result<i32, Self::Error> {
        let id: i32 = sqlx::query_scalar(
            "INSERT INTO videos (\
                source_url, title, description, duration_sec, source_extractor, \
                quality, format_ext, streamtape_file_id, streamtape_url, \
                file_size_bytes, thumbnail_r2_key, thumbnail_url, resolution\
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING id",
        )
        .bind(&video.source_url)
        .bind(&video.title)
        .bind(&video.description)
        .bind(video.duration_sec)
        .bind(&video.source_extractor)
        .bind(&video.quality)
        .bind(&video.format_ext)
        .bind(&video.streamtape_file_id)
        .bind(&video.streamtape_url)
        .bind(video.file_size_bytes)
        .bind(&video.thumbnail_r2_key)
        .bind(&video.thumbnail_url)
        .bind(&video.resolution)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    async fn find_by_source_quality_and_format(
        &self,
        source_url: &str,
        quality: &str,
        format_ext: &str,
    ) -> Result<Option<VideoRecord>, Self::Error> {
        let sql = format!(
            "SELECT {SELECT_COLUMNS} FROM videos \
             WHERE source_url = $1 AND quality = $2 AND format_ext = $3"
        );
        let row: Option<VideoRow> = sqlx::query_as(&sql)
            .bind(source_url)
            .bind(quality)
            .bind(format_ext)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(VideoRecord::from))
    }

    async fn list_recent(&self, limit: i64) -> Result<Vec<VideoRecord>, Self::Error> {
        // Recency-ordered (#366) — `last_accessed_at` starts equal to
        // `created_at` on insert and gets bumped on every dedup hit.
        let sql = format!(
            "SELECT {SELECT_COLUMNS} FROM videos \
             ORDER BY last_accessed_at DESC LIMIT $1"
        );
        let rows: Vec<VideoRow> = sqlx::query_as(&sql)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(VideoRecord::from).collect())
    }

    async fn find_by_id(&self, id: i32) -> Result<Option<VideoRecord>, Self::Error> {
        let sql = format!("SELECT {SELECT_COLUMNS} FROM videos WHERE id = $1");
        let row: Option<VideoRow> = sqlx::query_as(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(VideoRecord::from))
    }

    async fn delete(&self, id: i32) -> Result<bool, Self::Error> {
        let result = sqlx::query("DELETE FROM videos WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn touch(&self, id: i32) -> Result<(), Self::Error> {
        sqlx::query("UPDATE videos SET last_accessed_at = NOW() WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
