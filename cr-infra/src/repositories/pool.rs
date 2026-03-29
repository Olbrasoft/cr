use cr_domain::id::OrpId;
use cr_domain::repository::{PoolRecord, PoolRepository, PoolSummary};

/// PostgreSQL implementation of [`PoolRepository`].
pub struct PgPoolRepository {
    pool: sqlx::PgPool,
}

impl PgPoolRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct PoolDetailRow {
    id: i32,
    name: String,
    slug: String,
    description: Option<String>,
    address: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    website: Option<String>,
    email: Option<String>,
    phone: Option<String>,
    facebook: Option<String>,
    facilities: Option<String>,
    pool_length_m: Option<i32>,
    is_aquapark: bool,
    is_indoor: bool,
    is_outdoor: bool,
    is_natural: bool,
    photo_count: i16,
    municipality_name: Option<String>,
}

impl From<PoolDetailRow> for PoolRecord {
    fn from(r: PoolDetailRow) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            description: r.description,
            address: r.address,
            latitude: r.latitude,
            longitude: r.longitude,
            website: r.website,
            email: r.email,
            phone: r.phone,
            facebook: r.facebook,
            facilities: r.facilities,
            pool_length_m: r.pool_length_m,
            is_aquapark: r.is_aquapark,
            is_indoor: r.is_indoor,
            is_outdoor: r.is_outdoor,
            is_natural: r.is_natural,
            photo_count: r.photo_count,
            municipality_name: r.municipality_name,
        }
    }
}

#[derive(sqlx::FromRow)]
struct PoolSummaryRow {
    name: String,
    slug: String,
    is_aquapark: bool,
    is_indoor: bool,
    is_outdoor: bool,
    is_natural: bool,
}

impl From<PoolSummaryRow> for PoolSummary {
    fn from(r: PoolSummaryRow) -> Self {
        Self {
            name: r.name,
            slug: r.slug,
            is_aquapark: r.is_aquapark,
            is_indoor: r.is_indoor,
            is_outdoor: r.is_outdoor,
            is_natural: r.is_natural,
        }
    }
}

impl PoolRepository for PgPoolRepository {
    type Error = sqlx::Error;

    async fn find_by_slug_and_orp(
        &self,
        slug: &str,
        orp_id: OrpId,
    ) -> Result<Option<PoolRecord>, Self::Error> {
        let row = sqlx::query_as::<_, PoolDetailRow>(
            "SELECT p.id, p.name, p.slug, p.description, p.address, p.latitude, p.longitude, \
             p.website, p.email, p.phone, p.facebook, p.facilities, p.pool_length_m, \
             p.is_aquapark, p.is_indoor, p.is_outdoor, p.is_natural, p.photo_count, \
             m.name as municipality_name \
             FROM pools p \
             LEFT JOIN municipalities m ON p.municipality_id = m.id \
             WHERE p.slug = $1 AND p.orp_id = $2",
        )
        .bind(slug)
        .bind(orp_id.value())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(PoolRecord::from))
    }

    async fn find_by_orp(&self, orp_id: OrpId) -> Result<Vec<PoolSummary>, Self::Error> {
        let rows = sqlx::query_as::<_, PoolSummaryRow>(
            "SELECT name, slug, is_aquapark, is_indoor, is_outdoor, is_natural \
             FROM pools WHERE orp_id = $1 ORDER BY name",
        )
        .bind(orp_id.value())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(PoolSummary::from).collect())
    }
}
