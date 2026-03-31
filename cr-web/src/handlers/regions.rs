use cr_domain::id::RegionId;
use cr_domain::repository::{OrpRepository, RegionRepository};

use super::*;

impl From<cr_domain::repository::RegionRecord> for RegionRow {
    fn from(r: cr_domain::repository::RegionRecord) -> Self {
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
            hero_photo_url: None,
        }
    }
}

impl From<cr_domain::repository::OrpRecord> for OrpRow {
    fn from(r: cr_domain::repository::OrpRecord) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            orp_code: r.orp_code,
            latitude: r.latitude,
            longitude: r.longitude,
        }
    }
}

pub(crate) async fn render_region(
    state: &AppState,
    region_slug: &str,
) -> (StatusCode, Html<String>) {
    let region = state
        .region_repo
        .find_by_slug(region_slug)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("render_region region query failed: {e}");
            None
        });

    let Some(region) = region else {
        return not_found(&state.image_base_url);
    };

    let region_id = region.id;
    let hero_r2_direct = region.hero_photo_r2_key.clone();
    let hero_muni_code = region.hero_municipality_code.clone();
    let hero_muni_idx = region.hero_municipality_photo_index;
    let mut region_row: RegionRow = region.into();

    // Resolve hero photo URL — priority: landmark → municipality → direct r2_key
    let hero_r2 = sqlx::query_scalar::<_, String>(
        "SELECT COALESCE( \
           (SELECT lp.r2_key FROM landmark_photos lp \
            JOIN landmarks l ON l.npu_catalog_id = lp.npu_catalog_id \
            JOIN regions r ON r.hero_landmark_id = l.id \
            WHERE r.id = $1 AND lp.photo_index = r.hero_photo_index), \
           (SELECT mp.r2_key FROM municipality_photos mp \
            WHERE mp.municipality_code = $2 AND mp.photo_index = $3) \
         )",
    )
    .bind(region_id)
    .bind(&hero_muni_code)
    .bind(hero_muni_idx.unwrap_or(2) as i32)
    .fetch_optional(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_region hero photo query failed: {e}");
        None
    });

    if let Some(r2_key) = hero_r2 {
        region_row.hero_photo_url = Some(format!("/img/{}", r2_key));
    } else if let Some(r2_key) = hero_r2_direct {
        region_row.hero_photo_url = Some(format!("/img/{}", r2_key));
    }

    let orps: Vec<OrpRow> = state
        .orp_repo
        .find_by_region(RegionId::from(region_id))
        .await
        .unwrap_or_else(|e| {
            tracing::error!("render_region orps query failed: {e}");
            Vec::new()
        })
        .into_iter()
        .map(OrpRow::from)
        .collect();

    // Special case: region with single ORP (e.g. Praha) — render ORP page directly
    if orps.len() == 1 {
        return orp::render_orp(state, region_slug, &orps[0].slug).await;
    }

    let tmpl = RegionTemplate {
        img: state.image_base_url.clone(),
        region: region_row,
        orps,
    };
    match tmpl.render() {
        Ok(html) => (StatusCode::OK, Html(html)),
        Err(e) => {
            tracing::error!("template render failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Html(String::new()))
        }
    }
}
