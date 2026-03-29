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
    let region_row: RegionRow = region.into();

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
