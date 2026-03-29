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

    let region = sqlx::query_as::<_, RegionRow>(
        "SELECT id, name, slug, region_code, latitude, longitude, coat_of_arms_ext, flag_ext, description FROM regions WHERE slug = $1",
    )
    .bind(region_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or_else(|e| { tracing::error!("render_municipality region query failed: {e}"); None });

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
        tracing::error!("render_municipality orp query failed: {e}");
        None
    });

    let Some(orp) = orp else {
        return not_found(&state.image_base_url);
    };

    let municipality = sqlx::query_as::<_, MunicipalityRow>(
        "SELECT id, name, slug, municipality_code, pou_code, latitude, longitude, \
         wikipedia_url, official_website, coat_of_arms_ext, flag_ext, population, elevation \
         FROM municipalities WHERE orp_id = $1 AND slug = $2",
    )
    .bind(orp.id)
    .bind(municipality_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_municipality municipality query failed: {e}");
        None
    });

    let Some(municipality) = municipality else {
        return not_found(&state.image_base_url);
    };

    let landmarks = sqlx::query_as::<_, MunicipalityLandmarkRow>(
        "SELECT l.name, l.slug, lt.name as type_name \
         FROM landmarks l \
         JOIN landmark_types lt ON l.type_id = lt.id \
         WHERE l.municipality_id = $1 \
         ORDER BY lt.name, l.name",
    )
    .bind(municipality.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_municipality landmarks query failed: {e}");
        Vec::new()
    });

    let tmpl = MunicipalityTemplate {
        img: state.image_base_url.clone(),
        region,
        orp,
        municipality,
        landmarks,
    };
    match tmpl.render() {
        Ok(html) => (StatusCode::OK, Html(html)),
        Err(e) => {
            tracing::error!("template render failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Html(String::new()))
        }
    }
}
