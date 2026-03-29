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
    let region = sqlx::query_as::<_, RegionRow>(
        "SELECT id, name, slug, region_code, latitude, longitude, coat_of_arms_ext, flag_ext, description FROM regions WHERE slug = $1",
    )
    .bind(region_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or_else(|e| { tracing::error!("render_orp region query failed: {e}"); None });

    let Some(region) = region else {
        return not_found(&state.image_base_url);
    };

    let orp = sqlx::query_as::<_, OrpRow>(
        "SELECT o.id, o.name, o.slug, o.orp_code, o.latitude, o.longitude FROM orp o \
         JOIN districts d ON o.district_id = d.id \
         WHERE d.region_id = $1 AND o.slug = $2",
    )
    .bind(region.id)
    .bind(orp_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_orp orp query failed: {e}");
        None
    });

    let Some(orp) = orp else {
        return not_found(&state.image_base_url);
    };

    let all_municipalities = sqlx::query_as::<_, MunicipalityRow>(
        "SELECT id, name, slug, municipality_code, pou_code, latitude, longitude, \
         wikipedia_url, official_website, coat_of_arms_ext, flag_ext, population, elevation \
         FROM municipalities WHERE orp_id = $1 ORDER BY name",
    )
    .bind(orp.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_orp municipalities query failed: {e}");
        Vec::new()
    });

    let mut main_municipality = None;
    let mut other_municipalities = Vec::new();
    for m in all_municipalities {
        if main_municipality.is_none() && m.slug == orp.slug {
            main_municipality = Some(m);
        } else {
            other_municipalities.push(m);
        }
    }

    let Some(main_municipality) = main_municipality else {
        return not_found(&state.image_base_url);
    };

    let landmarks_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM landmarks WHERE municipality_id IN \
         (SELECT id FROM municipalities WHERE orp_id = $1)",
    )
    .bind(orp.id)
    .fetch_one(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_orp landmarks_count query failed: {e}");
        0
    });

    // Landmarks in entire ORP area — main municipality first, then others
    let landmarks = sqlx::query_as::<_, OrpLandmarkRow>(
        "SELECT l.name, l.slug, lt.name as type_name, m.name as municipality_name, \
         m.slug as municipality_slug, (m.id = $2) as is_main \
         FROM landmarks l \
         JOIN landmark_types lt ON l.type_id = lt.id \
         JOIN municipalities m ON l.municipality_id = m.id \
         WHERE m.orp_id = $1 \
         ORDER BY CASE WHEN m.id = $2 THEN 0 ELSE 1 END, lt.name, l.name",
    )
    .bind(orp.id)
    .bind(main_municipality.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_orp landmarks query failed: {e}");
        Vec::new()
    });

    let (main_landmarks, other_landmarks): (Vec<_>, Vec<_>) =
        landmarks.into_iter().partition(|l| l.is_main);

    // Pools in this ORP
    let pools = sqlx::query_as::<_, OrpPoolRow>(
        "SELECT name, slug, is_aquapark, is_indoor, is_outdoor, is_natural \
         FROM pools WHERE orp_id = $1 ORDER BY name",
    )
    .bind(orp.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_orp pools query failed: {e}");
        Vec::new()
    });

    let tmpl = OrpTemplate {
        img: state.image_base_url.clone(),
        region,
        orp,
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
