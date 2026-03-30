use cr_domain::id::OrpId;
use cr_domain::repository::{LandmarkRecord, LandmarkRepository, LandmarkSummary};

/// PostgreSQL implementation of [`LandmarkRepository`].
pub struct PgLandmarkRepository {
    pool: sqlx::PgPool,
}

impl PgLandmarkRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct LandmarkRow {
    id: i32,
    name: String,
    slug: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
    description: Option<String>,
    wikipedia_url: Option<String>,
    image_ext: Option<String>,
    npu_catalog_id: Option<String>,
    npu_description: Option<String>,
    type_slug: String,
    type_name: String,
    municipality_name: Option<String>,
    municipality_slug: Option<String>,
    orp_slug: Option<String>,
    region_slug: Option<String>,
}

impl From<LandmarkRow> for LandmarkRecord {
    fn from(r: LandmarkRow) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            latitude: r.latitude,
            longitude: r.longitude,
            description: r.description,
            wikipedia_url: r.wikipedia_url,
            image_ext: r.image_ext,
            npu_catalog_id: r.npu_catalog_id,
            npu_description: r.npu_description,
            type_slug: r.type_slug,
            type_name: r.type_name,
            municipality_name: r.municipality_name,
            municipality_slug: r.municipality_slug,
            orp_slug: r.orp_slug,
            region_slug: r.region_slug,
        }
    }
}

#[derive(sqlx::FromRow)]
struct LandmarkSummaryRow {
    name: String,
    slug: String,
    type_name: String,
    municipality_name: String,
    municipality_slug: String,
    is_main: bool,
}

impl From<LandmarkSummaryRow> for LandmarkSummary {
    fn from(r: LandmarkSummaryRow) -> Self {
        Self {
            name: r.name,
            slug: r.slug,
            type_name: r.type_name,
            municipality_name: r.municipality_name,
            municipality_slug: r.municipality_slug,
            is_main: r.is_main,
        }
    }
}

impl LandmarkRepository for PgLandmarkRepository {
    type Error = sqlx::Error;

    async fn find_by_slug_and_orp(
        &self,
        slug: &str,
        orp_id: OrpId,
    ) -> Result<Option<LandmarkRecord>, Self::Error> {
        let row = sqlx::query_as::<_, LandmarkRow>(
            "SELECT l.id, l.name, l.slug, l.latitude, l.longitude, l.description, \
             l.wikipedia_url, l.image_ext, l.npu_catalog_id, l.npu_description, \
             lt.slug as type_slug, lt.name as type_name, \
             m.name as municipality_name, m.slug as municipality_slug, \
             o2.slug as orp_slug, r2.slug as region_slug \
             FROM landmarks l \
             JOIN landmark_types lt ON l.type_id = lt.id \
             LEFT JOIN municipalities m ON l.municipality_id = m.id \
             LEFT JOIN orp o2 ON m.orp_id = o2.id \
             LEFT JOIN districts d2 ON o2.district_id = d2.id \
             LEFT JOIN regions r2 ON d2.region_id = r2.id \
             WHERE l.slug = $1 AND m.orp_id = $2",
        )
        .bind(slug)
        .bind(orp_id.value())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(LandmarkRecord::from))
    }

    async fn find_by_orp(&self, orp_id: OrpId) -> Result<Vec<LandmarkSummary>, Self::Error> {
        let rows = sqlx::query_as::<_, LandmarkSummaryRow>(
            "SELECT l.name, l.slug, lt.name as type_name, m.name as municipality_name, \
             m.slug as municipality_slug, false as is_main \
             FROM landmarks l \
             JOIN landmark_types lt ON l.type_id = lt.id \
             JOIN municipalities m ON l.municipality_id = m.id \
             WHERE m.orp_id = $1 \
             ORDER BY lt.name, l.name",
        )
        .bind(orp_id.value())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(LandmarkSummary::from).collect())
    }

    async fn count_by_type(&self, type_slug: &str) -> Result<i64, Self::Error> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM landmarks l \
             JOIN landmark_types lt ON l.type_id = lt.id \
             WHERE lt.slug = $1",
        )
        .bind(type_slug)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }
}
