use super::*;

pub(crate) async fn render_region(state: &AppState, region_slug: &str) -> (StatusCode, Html<String>) {
    let region = sqlx::query_as::<_, RegionRow>(
        "SELECT id, name, slug, region_code, latitude, longitude, coat_of_arms_ext, flag_ext, description FROM regions WHERE slug = $1",
    )
    .bind(region_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or_else(|e| { tracing::error!("render_region region query failed: {e}"); None });

    let Some(region) = region else {
        return not_found(&state.image_base_url);
    };

    let orps = sqlx::query_as::<_, OrpRow>(
        "SELECT o.id, o.name, o.slug, o.orp_code, o.latitude, o.longitude FROM orp o \
         JOIN districts d ON o.district_id = d.id \
         WHERE d.region_id = $1 ORDER BY o.name",
    )
    .bind(region.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_else(|e| { tracing::error!("render_region orps query failed: {e}"); Vec::new() });

    // Special case: region with single ORP (e.g. Praha) — render ORP page directly
    if orps.len() == 1 {
        return orp::render_orp(state, region_slug, &orps[0].slug).await;
    }

    let tmpl = RegionTemplate { img: state.image_base_url.clone(), region, orps };
    match tmpl.render() {
        Ok(html) => (StatusCode::OK, Html(html)),
        Err(e) => {
            tracing::error!("template render failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Html(String::new()))
        }
    }
}
