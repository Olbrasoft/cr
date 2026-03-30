//! Repository trait definitions (ports).
//!
//! These traits define the contract between the application layer and
//! infrastructure layer. Implementations live in `cr-infra`.

use crate::id::*;

/// Repository for region queries.
#[allow(async_fn_in_trait)]
pub trait RegionRepository {
    type Error: std::fmt::Debug;
    async fn find_all(&self) -> Result<Vec<RegionRecord>, Self::Error>;
    async fn find_by_slug(&self, slug: &str) -> Result<Option<RegionRecord>, Self::Error>;
}

/// Repository for ORP queries.
#[allow(async_fn_in_trait)]
pub trait OrpRepository {
    type Error: std::fmt::Debug;
    async fn find_by_slug(&self, slug: &str) -> Result<Option<OrpRecord>, Self::Error>;
    async fn find_by_region(&self, region_id: RegionId) -> Result<Vec<OrpRecord>, Self::Error>;
    async fn exists_by_slug(&self, slug: &str) -> Result<bool, Self::Error>;
}

/// Repository for municipality queries.
#[allow(async_fn_in_trait)]
pub trait MunicipalityRepository {
    type Error: std::fmt::Debug;
    async fn find_by_slug_and_orp(
        &self,
        slug: &str,
        orp_id: OrpId,
    ) -> Result<Option<MunicipalityRecord>, Self::Error>;
    async fn find_by_orp(&self, orp_id: OrpId) -> Result<Vec<MunicipalityRecord>, Self::Error>;
}

/// Repository for landmark queries.
#[allow(async_fn_in_trait)]
pub trait LandmarkRepository {
    type Error: std::fmt::Debug;
    async fn find_by_slug_and_orp(
        &self,
        slug: &str,
        orp_id: OrpId,
    ) -> Result<Option<LandmarkRecord>, Self::Error>;
    async fn find_by_orp(&self, orp_id: OrpId) -> Result<Vec<LandmarkSummary>, Self::Error>;
    async fn count_by_type(&self, type_slug: &str) -> Result<i64, Self::Error>;
}

/// Repository for pool queries.
#[allow(async_fn_in_trait)]
pub trait PoolRepository {
    type Error: std::fmt::Debug;
    async fn find_by_slug_and_orp(
        &self,
        slug: &str,
        orp_id: OrpId,
    ) -> Result<Option<PoolRecord>, Self::Error>;
    async fn find_by_orp(&self, orp_id: OrpId) -> Result<Vec<PoolSummary>, Self::Error>;
}

/// Repository for photo metadata queries.
#[allow(async_fn_in_trait)]
pub trait PhotoRepository {
    type Error: std::fmt::Debug;
    async fn find_by_entity(
        &self,
        entity_type: &str,
        entity_id: i32,
    ) -> Result<Vec<PhotoRecord>, Self::Error>;
}

// --- Record types returned by repositories ---
// These are plain data records (not domain entities) for query results.

/// Region data as stored in the database.
#[derive(Debug, Clone)]
pub struct RegionRecord {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub region_code: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub coat_of_arms_ext: Option<String>,
    pub flag_ext: Option<String>,
    pub description: Option<String>,
}

/// ORP data as stored in the database.
#[derive(Debug, Clone)]
pub struct OrpRecord {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub orp_code: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

/// Municipality data as stored in the database.
#[derive(Debug, Clone)]
pub struct MunicipalityRecord {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub municipality_code: String,
    pub pou_code: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub wikipedia_url: Option<String>,
    pub official_website: Option<String>,
    pub coat_of_arms_ext: Option<String>,
    pub flag_ext: Option<String>,
    pub population: Option<i32>,
    pub elevation: Option<f64>,
}

/// Landmark summary for listings.
#[derive(Debug, Clone)]
pub struct LandmarkSummary {
    pub name: String,
    pub slug: String,
    pub type_name: String,
    pub municipality_name: String,
    pub municipality_slug: String,
    pub is_main: bool,
}

/// Landmark full record for detail pages.
#[derive(Debug, Clone)]
pub struct LandmarkRecord {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub description: Option<String>,
    pub wikipedia_url: Option<String>,
    pub image_ext: Option<String>,
    pub npu_catalog_id: Option<String>,
    pub npu_description: Option<String>,
    pub type_slug: String,
    pub type_name: String,
    pub municipality_name: Option<String>,
    pub municipality_slug: Option<String>,
    pub orp_slug: Option<String>,
    pub region_slug: Option<String>,
}

/// Pool summary for listings.
#[derive(Debug, Clone)]
pub struct PoolSummary {
    pub name: String,
    pub slug: String,
    pub is_aquapark: bool,
    pub is_indoor: bool,
    pub is_outdoor: bool,
    pub is_natural: bool,
}

/// Pool full record for detail pages.
#[derive(Debug, Clone)]
pub struct PoolRecord {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub description: Option<String>,
    pub address: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub website: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub facebook: Option<String>,
    pub facilities: Option<String>,
    pub pool_length_m: Option<i32>,
    pub is_aquapark: bool,
    pub is_indoor: bool,
    pub is_outdoor: bool,
    pub is_natural: bool,
    pub photo_count: i16,
    pub municipality_name: Option<String>,
}

/// Photo metadata record.
#[derive(Debug, Clone)]
pub struct PhotoRecord {
    pub r2_key: String,
    pub width: i16,
    pub height: i16,
}
