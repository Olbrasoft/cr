use cr_domain::repository::{PhotoRecord, PhotoRepository};

/// PostgreSQL implementation of [`PhotoRepository`].
pub struct PgPhotoRepository {
    pool: sqlx::PgPool,
}

impl PgPhotoRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct PhotoRow {
    r2_key: String,
    width: i16,
    height: i16,
}

impl From<PhotoRow> for PhotoRecord {
    fn from(r: PhotoRow) -> Self {
        Self {
            r2_key: r.r2_key,
            width: r.width,
            height: r.height,
        }
    }
}

impl PhotoRepository for PgPhotoRepository {
    type Error = sqlx::Error;

    async fn find_by_entity(
        &self,
        entity_type: &str,
        entity_id: i32,
    ) -> Result<Vec<PhotoRecord>, Self::Error> {
        let rows = sqlx::query_as::<_, PhotoRow>(
            "SELECT r2_key, width, height FROM photo_metadata \
             WHERE entity_type = $1 AND entity_id = $2 ORDER BY photo_index",
        )
        .bind(entity_type)
        .bind(entity_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(PhotoRecord::from).collect())
    }
}
