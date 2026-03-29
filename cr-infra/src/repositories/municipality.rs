use cr_domain::id::OrpId;
use cr_domain::repository::{MunicipalityRecord, MunicipalityRepository};

/// PostgreSQL implementation of [`MunicipalityRepository`].
pub struct PgMunicipalityRepository {
    pool: sqlx::PgPool,
}

impl PgMunicipalityRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct MunicipalityRow {
    id: i32,
    name: String,
    slug: String,
    municipality_code: String,
    pou_code: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
    wikipedia_url: Option<String>,
    official_website: Option<String>,
    coat_of_arms_ext: Option<String>,
    flag_ext: Option<String>,
    population: Option<i32>,
    elevation: Option<f64>,
}

impl From<MunicipalityRow> for MunicipalityRecord {
    fn from(r: MunicipalityRow) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            municipality_code: r.municipality_code,
            pou_code: r.pou_code,
            latitude: r.latitude,
            longitude: r.longitude,
            wikipedia_url: r.wikipedia_url,
            official_website: r.official_website,
            coat_of_arms_ext: r.coat_of_arms_ext,
            flag_ext: r.flag_ext,
            population: r.population,
            elevation: r.elevation,
        }
    }
}

impl MunicipalityRepository for PgMunicipalityRepository {
    type Error = sqlx::Error;

    async fn find_by_slug_and_orp(
        &self,
        slug: &str,
        orp_id: OrpId,
    ) -> Result<Option<MunicipalityRecord>, Self::Error> {
        let row = sqlx::query_as::<_, MunicipalityRow>(
            "SELECT id, name, slug, municipality_code, pou_code, latitude, longitude, \
             wikipedia_url, official_website, coat_of_arms_ext, flag_ext, population, elevation \
             FROM municipalities WHERE orp_id = $1 AND slug = $2",
        )
        .bind(orp_id.value())
        .bind(slug)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(MunicipalityRecord::from))
    }

    async fn find_by_orp(&self, orp_id: OrpId) -> Result<Vec<MunicipalityRecord>, Self::Error> {
        let rows = sqlx::query_as::<_, MunicipalityRow>(
            "SELECT id, name, slug, municipality_code, pou_code, latitude, longitude, \
             wikipedia_url, official_website, coat_of_arms_ext, flag_ext, population, elevation \
             FROM municipalities WHERE orp_id = $1 ORDER BY name",
        )
        .bind(orp_id.value())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(MunicipalityRecord::from).collect())
    }
}
