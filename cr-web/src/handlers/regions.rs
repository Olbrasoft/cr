use cr_domain::id::RegionId;
use cr_domain::repository::{OrpRepository, RegionRepository};

use super::*;

impl From<cr_domain::repository::RegionRecord> for RegionRow {
    fn from(r: cr_domain::repository::RegionRecord) -> Self {
        let hero_photo_url = r.hero_photo_r2_key.map(|k| format!("/img/{}", k));
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
            hero_photo_url,
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
    let mut region_row: RegionRow = region.into();

    // Override hero_photo_url with landmark photo if hero_landmark_id is set
    let landmark_hero = sqlx::query_scalar::<_, String>(
        "SELECT lp.r2_key FROM landmark_photos lp \
         JOIN landmarks l ON l.npu_catalog_id = lp.npu_catalog_id \
         JOIN regions r ON r.hero_landmark_id = l.id \
         WHERE r.id = $1 AND lp.photo_index = r.hero_photo_index",
    )
    .bind(region_id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_region hero photo query failed: {e}");
        None
    });
    if let Some(r2_key) = landmark_hero {
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
