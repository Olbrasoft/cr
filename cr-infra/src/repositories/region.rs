use cr_domain::repository::{RegionRecord, RegionRepository};

/// PostgreSQL implementation of [`RegionRepository`].
pub struct PgRegionRepository {
    pool: sqlx::PgPool,
}

impl PgRegionRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct RegionRow {
    id: i32,
    name: String,
    slug: String,
    region_code: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
    coat_of_arms_ext: Option<String>,
    flag_ext: Option<String>,
    description: Option<String>,
}

impl From<RegionRow> for RegionRecord {
    fn from(r: RegionRow) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            region_code: r.region_code,
            latitude: r.latitude,
            longitude: r.longitude,
            coat_of_arms_ext: r.coat_of_arms_ext,
            flag_ext: r.flag_ext,
            description: r.description,
        }
    }
}

impl RegionRepository for PgRegionRepository {
    type Error = sqlx::Error;

    async fn find_all(&self) -> Result<Vec<RegionRecord>, Self::Error> {
        let rows = sqlx::query_as::<_, RegionRow>(
            "SELECT id, name, slug, region_code, latitude, longitude, \
             coat_of_arms_ext, flag_ext, description \
             FROM regions ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(RegionRecord::from).collect())
    }

    async fn find_by_slug(&self, slug: &str) -> Result<Option<RegionRecord>, Self::Error> {
        let row = sqlx::query_as::<_, RegionRow>(
            "SELECT id, name, slug, region_code, latitude, longitude, \
             coat_of_arms_ext, flag_ext, description \
             FROM regions WHERE slug = $1",
        )
        .bind(slug)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(RegionRecord::from))
    }
}
