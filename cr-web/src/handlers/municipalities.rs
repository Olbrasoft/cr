use cr_domain::id::OrpId;
use cr_domain::repository::{MunicipalityRepository, OrpRepository, RegionRepository};

use super::*;

pub(crate) async fn render_municipality_short(
    state: &AppState,
    orp_slug: &str,
    muni_slug: &str,
) -> (StatusCode, Html<String>) {
    let Some(region_slug) = region_slug_for_orp(&state.db, orp_slug).await else {
        return not_found(&state.image_base_url);
    };
    render_municipality(state, &region_slug, orp_slug, muni_slug).await
}

pub(crate) async fn render_municipality(
    state: &AppState,
    region_slug: &str,
    orp_slug: &str,
    municipality_slug: &str,
) -> (StatusCode, Html<String>) {
    // If municipality slug = ORP slug, the ORP page already shows this municipality
    if municipality_slug == orp_slug {
        return not_found(&state.image_base_url);
    }

    let region = state
        .region_repo
        .find_by_slug(region_slug)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("render_municipality region query failed: {e}");
            None
        });

    let Some(region) = region else {
        return not_found(&state.image_base_url);
    };

    let region_row: RegionRow = region.into();

    let orp = state
        .orp_repo
        .find_by_slug(orp_slug)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("render_municipality orp query failed: {e}");
            None
        });

    let Some(orp) = orp else {
        return not_found(&state.image_base_url);
    };

    let orp_id = orp.id;
    let orp_row: OrpRow = orp.into();

    let municipality = state
        .municipality_repo
        .find_by_slug_and_orp(municipality_slug, OrpId::from(orp_id))
        .await
        .unwrap_or_else(|e| {
            tracing::error!("render_municipality municipality query failed: {e}");
            None
        });

    let Some(municipality) = municipality else {
        return not_found(&state.image_base_url);
    };

    let municipality_id = municipality.id;
    let municipality_row: MunicipalityRow = municipality.into();

    // Keep direct sqlx: MunicipalityLandmarkRow is a complex JOIN with no matching domain record
    let landmarks = sqlx::query_as::<_, MunicipalityLandmarkRow>(
        "SELECT l.name, l.slug, lt.name as type_name \
         FROM landmarks l \
         JOIN landmark_types lt ON l.type_id = lt.id \
         WHERE l.municipality_id = $1 \
         ORDER BY lt.name, l.name",
    )
    .bind(municipality_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_municipality landmarks query failed: {e}");
        Vec::new()
    });

    let photo = fetch_municipality_photo(
        &state.db,
        &municipality_row.municipality_code,
        orp_slug,
        municipality_slug,
    )
    .await;

    let gallery_photos = fetch_municipality_gallery(
        &state.db,
        &municipality_row.municipality_code,
        orp_slug,
        municipality_slug,
    )
    .await;

    let tmpl = MunicipalityTemplate {
        img: state.image_base_url.clone(),
        region: region_row,
        orp: orp_row,
        municipality: municipality_row,
        landmarks,
        photo,
        gallery_photos,
    };
    match tmpl.render() {
        Ok(html) => (StatusCode::OK, Html(html)),
        Err(e) => {
            tracing::error!("template render failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Html(String::new()))
        }
    }
}
