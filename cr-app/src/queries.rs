//! Query (read) use-cases.
//!
//! Each function takes repository trait references and returns data.
//! No framework dependencies — pure business logic orchestration.

use crate::error::AppError;
use cr_domain::repository::*;

/// Get all regions for the homepage.
pub async fn get_all_regions<R: RegionRepository>(repo: &R) -> Result<Vec<RegionRecord>, AppError> {
    repo.find_all()
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))
}

/// Get a region by slug.
pub async fn get_region_by_slug<R: RegionRepository>(
    repo: &R,
    slug: &str,
) -> Result<RegionRecord, AppError> {
    repo.find_by_slug(slug)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))?
        .ok_or(AppError::NotFound)
}

/// Get ORP list for a region.
pub async fn get_orps_by_region<O: OrpRepository>(
    repo: &O,
    region_id: cr_domain::RegionId,
) -> Result<Vec<OrpRecord>, AppError> {
    repo.find_by_region(region_id)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))
}

/// Get ORP by slug.
pub async fn get_orp_by_slug<O: OrpRepository>(
    repo: &O,
    slug: &str,
) -> Result<OrpRecord, AppError> {
    repo.find_by_slug(slug)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))?
        .ok_or(AppError::NotFound)
}

/// Check if a slug is an ORP.
pub async fn is_orp_slug<O: OrpRepository>(repo: &O, slug: &str) -> Result<bool, AppError> {
    repo.exists_by_slug(slug)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))
}

/// Get municipality by slug within an ORP.
pub async fn get_municipality<M: MunicipalityRepository>(
    repo: &M,
    slug: &str,
    orp_id: cr_domain::OrpId,
) -> Result<MunicipalityRecord, AppError> {
    repo.find_by_slug_and_orp(slug, orp_id)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))?
        .ok_or(AppError::NotFound)
}

/// Get municipalities for an ORP.
pub async fn get_municipalities_by_orp<M: MunicipalityRepository>(
    repo: &M,
    orp_id: cr_domain::OrpId,
) -> Result<Vec<MunicipalityRecord>, AppError> {
    repo.find_by_orp(orp_id)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))
}

/// Get landmark detail by slug within an ORP.
pub async fn get_landmark<L: LandmarkRepository>(
    repo: &L,
    slug: &str,
    orp_id: cr_domain::OrpId,
) -> Result<LandmarkRecord, AppError> {
    repo.find_by_slug_and_orp(slug, orp_id)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))?
        .ok_or(AppError::NotFound)
}

/// Get landmarks for an ORP.
pub async fn get_landmarks_by_orp<L: LandmarkRepository>(
    repo: &L,
    orp_id: cr_domain::OrpId,
) -> Result<Vec<LandmarkSummary>, AppError> {
    repo.find_by_orp(orp_id)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))
}

/// Get pool detail by slug within an ORP.
pub async fn get_pool<P: PoolRepository>(
    repo: &P,
    slug: &str,
    orp_id: cr_domain::OrpId,
) -> Result<PoolRecord, AppError> {
    repo.find_by_slug_and_orp(slug, orp_id)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))?
        .ok_or(AppError::NotFound)
}

/// Get pools for an ORP.
pub async fn get_pools_by_orp<P: PoolRepository>(
    repo: &P,
    orp_id: cr_domain::OrpId,
) -> Result<Vec<PoolSummary>, AppError> {
    repo.find_by_orp(orp_id)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))
}

/// Get photos for an entity.
pub async fn get_photos<Ph: PhotoRepository>(
    repo: &Ph,
    entity_type: &str,
    entity_id: i32,
) -> Result<Vec<PhotoRecord>, AppError> {
    repo.find_by_entity(entity_type, entity_id)
        .await
        .map_err(|e| AppError::Repository(format!("{e:?}")))
}
