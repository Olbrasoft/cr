use super::*;

pub async fn pools_hub(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let aquapark_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pools WHERE is_aquapark")
        .fetch_one(&state.db).await?;
    let indoor_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pools WHERE is_indoor AND NOT is_aquapark")
        .fetch_one(&state.db).await?;
    let outdoor_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pools WHERE is_outdoor AND NOT is_aquapark")
        .fetch_one(&state.db).await?;
    let natural_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pools WHERE is_natural")
        .fetch_one(&state.db).await?;

    let tmpl = PoolsHubTemplate {
        img: state.image_base_url.clone(),
        aquapark_count, indoor_count, outdoor_count, natural_count,
    };
    Ok(Html(tmpl.render()?))
}

pub async fn pools_by_category(
    State(state): State<AppState>,
    uri: Uri,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<impl IntoResponse> {
    let path = uri.path().trim_matches('/');
    let (filter_col, category_name) = match path {
        "aquaparky" => ("is_aquapark", "Aquaparky"),
        "bazeny" => ("is_indoor", "Kryté bazény"),
        "koupaliste" => ("is_outdoor", "Venkovní koupaliště"),
        "prirodni-koupaliste" => ("is_natural", "Přírodní koupaliště"),
        _ => ("is_indoor", "Bazény"),
    };

    let page: i64 = params.get("strana").and_then(|s| s.parse().ok()).unwrap_or(1).max(1);
    let per_page: i64 = 20;

    // Use separate queries per category to avoid SQL string interpolation
    let total_count = match filter_col {
        "is_aquapark" => sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pools WHERE is_aquapark").fetch_one(&state.db).await?,
        "is_indoor" => sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pools WHERE is_indoor AND NOT is_aquapark").fetch_one(&state.db).await?,
        "is_outdoor" => sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pools WHERE is_outdoor AND NOT is_aquapark").fetch_one(&state.db).await?,
        _ => sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM pools WHERE is_natural").fetch_one(&state.db).await?,
    };
    let total_pages = (total_count + per_page - 1) / per_page;
    let offset = (page - 1) * per_page;

    let base_query = "SELECT p.name, p.slug, p.description, m.name as municipality_name, \
         o.slug as orp_slug, r.slug as region_slug, p.photo_count \
         FROM pools p \
         LEFT JOIN municipalities m ON p.municipality_id = m.id \
         LEFT JOIN orp o ON p.orp_id = o.id \
         LEFT JOIN districts d ON o.district_id = d.id \
         LEFT JOIN regions r ON d.region_id = r.id";

    let pools = match filter_col {
        "is_aquapark" => sqlx::query_as::<_, PoolListRow>(&format!("{base_query} WHERE p.is_aquapark ORDER BY p.name LIMIT $1 OFFSET $2"))
            .bind(per_page).bind(offset).fetch_all(&state.db).await?,
        "is_indoor" => sqlx::query_as::<_, PoolListRow>(&format!("{base_query} WHERE p.is_indoor AND NOT p.is_aquapark ORDER BY p.name LIMIT $1 OFFSET $2"))
            .bind(per_page).bind(offset).fetch_all(&state.db).await?,
        "is_outdoor" => sqlx::query_as::<_, PoolListRow>(&format!("{base_query} WHERE p.is_outdoor AND NOT p.is_aquapark ORDER BY p.name LIMIT $1 OFFSET $2"))
            .bind(per_page).bind(offset).fetch_all(&state.db).await?,
        _ => sqlx::query_as::<_, PoolListRow>(&format!("{base_query} WHERE p.is_natural ORDER BY p.name LIMIT $1 OFFSET $2"))
            .bind(per_page).bind(offset).fetch_all(&state.db).await?,
    };

    let tmpl = PoolsListTemplate {
        img: state.image_base_url.clone(),
        category_name: category_name.to_string(),
        category_slug: path.to_string(),
        pools,
        page,
        total_pages,
        total_count,
    };
    Ok(Html(tmpl.render()?))
}

pub(crate) async fn render_pool(
    state: &AppState,
    region_slug: &str,
    orp_slug: &str,
    pool_slug: &str,
) -> (StatusCode, Html<String>) {
    let region = sqlx::query_as::<_, RegionRow>(
        "SELECT id, name, slug, region_code, latitude, longitude, coat_of_arms_ext, flag_ext, description FROM regions WHERE slug = $1",
    )
    .bind(region_slug)
    .fetch_optional(&state.db).await
    .unwrap_or_else(|e| { tracing::error!("render_pool region query failed: {e}"); None });

    let Some(region) = region else {
        return not_found(&state.image_base_url);
    };

    let orp = sqlx::query_as::<_, OrpRow>(
        "SELECT o.id, o.name, o.slug, o.orp_code, o.latitude, o.longitude FROM orp o \
         JOIN districts d ON o.district_id = d.id \
         WHERE d.region_id = $1 AND o.slug = $2",
    )
    .bind(region.id).bind(orp_slug)
    .fetch_optional(&state.db).await
    .unwrap_or_else(|e| { tracing::error!("render_pool orp query failed: {e}"); None });

    let Some(orp) = orp else {
        return not_found(&state.image_base_url);
    };

    let pool = sqlx::query_as::<_, PoolDetailRow>(
        "SELECT p.id, p.name, p.slug, p.description, p.address, p.latitude, p.longitude, \
         p.website, p.email, p.phone, p.facebook, p.facilities, p.pool_length_m, \
         p.is_aquapark, p.is_indoor, p.is_outdoor, p.is_natural, p.photo_count, \
         m.name as municipality_name \
         FROM pools p \
         LEFT JOIN municipalities m ON p.municipality_id = m.id \
         WHERE p.slug = $1 AND p.orp_id = $2",
    )
    .bind(pool_slug).bind(orp.id)
    .fetch_optional(&state.db).await
    .unwrap_or_else(|e| { tracing::error!("render_pool pool query failed: {e}"); None });

    let Some(pool) = pool else {
        return not_found(&state.image_base_url);
    };

    let photos = fetch_photos(&state.db, &state.image_base_url, "pool", pool.id, &pool.slug).await;

    let tmpl = PoolDetailTemplate {
        img: state.image_base_url.clone(),
        pool, region, orp, photos,
    };
    match tmpl.render() {
        Ok(html) => (StatusCode::OK, Html(html)),
        Err(e) => {
            tracing::error!("template render failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Html(String::new()))
        }
    }
}

pub(crate) async fn render_pool_short(state: &AppState, orp_slug: &str, pool_slug: &str) -> (StatusCode, Html<String>) {
    let Some(region_slug) = region_slug_for_orp(&state.db, orp_slug).await else {
        return not_found(&state.image_base_url);
    };
    render_pool(state, &region_slug, orp_slug, pool_slug).await
}
