use cr_domain::id::OrpId;
use cr_domain::repository::{MunicipalityRepository, OrpRepository, RegionRepository};

use super::*;

pub(crate) async fn render_orp_by_slug(
    state: &AppState,
    orp_slug: &str,
) -> (StatusCode, Html<String>) {
    let Some(region_slug) = region_slug_for_orp(&state.db, orp_slug).await else {
        return not_found(&state.image_base_url);
    };
    render_orp(state, &region_slug, orp_slug).await
}

pub(crate) async fn render_orp(
    state: &AppState,
    region_slug: &str,
    orp_slug: &str,
) -> (StatusCode, Html<String>) {
    let region = state
        .region_repo
        .find_by_slug(region_slug)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("render_orp region query failed: {e}");
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
            tracing::error!("render_orp orp query failed: {e}");
            None
        });

    let Some(orp) = orp else {
        return not_found(&state.image_base_url);
    };

    let orp_id = orp.id;
    let orp_row: OrpRow = orp.into();

    let all_municipalities: Vec<MunicipalityRow> = state
        .municipality_repo
        .find_by_orp(OrpId::from(orp_id))
        .await
        .unwrap_or_else(|e| {
            tracing::error!("render_orp municipalities query failed: {e}");
            Vec::new()
        })
        .into_iter()
        .map(MunicipalityRow::from)
        .collect();

    let mut main_municipality = None;
    let mut other_municipalities = Vec::new();
    for m in all_municipalities {
        if main_municipality.is_none() && m.slug == orp_row.slug {
            main_municipality = Some(m);
        } else {
            other_municipalities.push(m);
        }
    }

    let Some(main_municipality) = main_municipality else {
        return not_found(&state.image_base_url);
    };

    // TODO: add count_by_orp to LandmarkRepository to replace this direct query
    let landmarks_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM landmarks WHERE municipality_id IN \
         (SELECT id FROM municipalities WHERE orp_id = $1)",
    )
    .bind(orp_id)
    .fetch_one(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_orp landmarks_count query failed: {e}");
        0
    });

    // Landmarks in entire ORP area — main municipality first, then others
    // Keep direct sqlx: OrpLandmarkRow is a complex JOIN with no matching domain record
    let landmarks = sqlx::query_as::<_, OrpLandmarkRow>(
        "SELECT l.name, l.slug, lt.name as type_name, m.name as municipality_name, \
         m.slug as municipality_slug, (m.id = $2) as is_main \
         FROM landmarks l \
         JOIN landmark_types lt ON l.type_id = lt.id \
         JOIN municipalities m ON l.municipality_id = m.id \
         WHERE m.orp_id = $1 \
         ORDER BY CASE WHEN m.id = $2 THEN 0 ELSE 1 END, lt.name, l.name",
    )
    .bind(orp_id)
    .bind(main_municipality.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_orp landmarks query failed: {e}");
        Vec::new()
    });

    let (main_landmarks, other_landmarks): (Vec<_>, Vec<_>) =
        landmarks.into_iter().partition(|l| l.is_main);

    // Pools in this ORP — keep direct sqlx: OrpPoolRow has no matching domain record
    let pools = sqlx::query_as::<_, OrpPoolRow>(
        "SELECT name, slug, is_aquapark, is_indoor, is_outdoor, is_natural \
         FROM pools WHERE orp_id = $1 ORDER BY name",
    )
    .bind(orp_id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_orp pools query failed: {e}");
        Vec::new()
    });

    let tmpl = OrpTemplate {
        img: state.image_base_url.clone(),
        region: region_row,
        orp: orp_row,
        main_municipality,
        other_municipalities,
        main_landmarks,
        other_landmarks,
        landmarks_count,
        pools,
    };
    match tmpl.render() {
        Ok(html) => (StatusCode::OK, Html(html)),
        Err(e) => {
            tracing::error!("template render failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Html(String::new()))
        }
    }
}
