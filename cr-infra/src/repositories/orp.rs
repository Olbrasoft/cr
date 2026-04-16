use cr_domain::id::RegionId;
use cr_domain::repository::{OrpRecord, OrpRepository};

/// SELECT column list for ORP queries (with `o.` table alias).
pub(crate) const ORP_COLUMNS: &str =
    "o.id, o.name, o.slug, o.orp_code, o.latitude, o.longitude, o.description";

/// PostgreSQL implementation of [`OrpRepository`].
pub struct PgOrpRepository {
    pool: sqlx::PgPool,
}

impl PgOrpRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct OrpRow {
    id: i32,
    name: String,
    slug: String,
    orp_code: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
    description: Option<String>,
}

impl From<OrpRow> for OrpRecord {
    fn from(r: OrpRow) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            orp_code: r.orp_code,
            latitude: r.latitude,
            longitude: r.longitude,
            description: r.description,
        }
    }
}

impl OrpRepository for PgOrpRepository {
    type Error = sqlx::Error;

    async fn find_by_slug(&self, slug: &str) -> Result<Option<OrpRecord>, Self::Error> {
        let row = sqlx::query_as::<_, OrpRow>(&format!(
            "SELECT {ORP_COLUMNS} FROM orp o \
                 JOIN districts d ON o.district_id = d.id \
                 WHERE o.slug = $1"
        ))
        .bind(slug)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(OrpRecord::from))
    }

    async fn find_by_region(&self, region_id: RegionId) -> Result<Vec<OrpRecord>, Self::Error> {
        let rows = sqlx::query_as::<_, OrpRow>(&format!(
            "SELECT {ORP_COLUMNS} FROM orp o \
                 JOIN districts d ON o.district_id = d.id \
                 WHERE d.region_id = $1 ORDER BY o.name"
        ))
        .bind(region_id.value())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(OrpRecord::from).collect())
    }

    async fn exists_by_slug(&self, slug: &str) -> Result<bool, Self::Error> {
        let exists =
            sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM orp WHERE slug = $1)")
                .bind(slug)
                .fetch_one(&self.pool)
                .await?;

        Ok(exists)
    }

    async fn region_slug_for_orp(&self, orp_slug: &str) -> Result<Option<String>, Self::Error> {
        sqlx::query_scalar::<_, String>(
            "SELECT r.slug FROM regions r \
             JOIN districts d ON d.region_id = r.id \
             JOIN orp o ON o.district_id = d.id \
             WHERE o.slug = $1",
        )
        .bind(orp_slug)
        .fetch_optional(&self.pool)
        .await
    }
}
