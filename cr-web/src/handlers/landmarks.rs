use super::*;

const LANDMARKS_PER_PAGE: i64 = 10;

pub(crate) async fn render_landmark_short(
    state: &AppState,
    orp_slug: &str,
    landmark_slug: &str,
) -> (StatusCode, Html<String>) {
    let Some(region_slug) = region_slug_for_orp(&state.db, orp_slug).await else {
        return not_found(&state.image_base_url);
    };
    render_landmark(state, &region_slug, orp_slug, landmark_slug).await
}

pub(crate) async fn render_landmark_in_municipality(
    state: &AppState,
    orp_slug: &str,
    landmark_slug: &str,
    _orp_id: i32,
) -> (StatusCode, Html<String>) {
    let Some(region_slug) = region_slug_for_orp(&state.db, orp_slug).await else {
        return not_found(&state.image_base_url);
    };
    render_landmark(state, &region_slug, orp_slug, landmark_slug).await
}

pub(crate) async fn render_landmark(
    state: &AppState,
    region_slug: &str,
    orp_slug: &str,
    landmark_slug: &str,
) -> (StatusCode, Html<String>) {
    let region = sqlx::query_as::<_, RegionRow>(
        "SELECT id, name, slug, region_code, latitude, longitude, coat_of_arms_ext, flag_ext, description FROM regions WHERE slug = $1",
    )
    .bind(region_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or_else(|e| { tracing::error!("render_landmark region query failed: {e}"); None });

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
        tracing::error!("render_landmark orp query failed: {e}");
        None
    });

    let Some(orp) = orp else {
        return not_found(&state.image_base_url);
    };

    // Find landmark by slug within municipalities of this ORP
    let landmark = sqlx::query_as::<_, LandmarkRow>(
        "SELECT l.id, l.name, l.slug, l.latitude, l.longitude, l.description, \
         l.wikipedia_url, l.image_ext, l.npu_catalog_id, \
         lt.slug as type_slug, lt.name as type_name, \
         m.name as municipality_name, m.slug as municipality_slug, \
         o2.slug as orp_slug, r2.slug as region_slug \
         FROM landmarks l \
         JOIN landmark_types lt ON l.type_id = lt.id \
         LEFT JOIN municipalities m ON l.municipality_id = m.id \
         LEFT JOIN orp o2 ON m.orp_id = o2.id \
         LEFT JOIN districts d2 ON o2.district_id = d2.id \
         LEFT JOIN regions r2 ON d2.region_id = r2.id \
         WHERE l.slug = $1 AND m.orp_id = $2",
    )
    .bind(landmark_slug)
    .bind(orp.id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_landmark landmark query failed: {e}");
        None
    });

    let Some(landmark) = landmark else {
        return not_found(&state.image_base_url);
    };

    let photos = fetch_photos(
        &state.db,
        &state.image_base_url,
        "landmark",
        landmark.id,
        &landmark.slug,
    )
    .await;

    let tmpl = LandmarkDetailTemplate {
        img: state.image_base_url.clone(),
        landmark,
        region,
        orp,
        photos,
    };
    match tmpl.render() {
        Ok(html) => (StatusCode::OK, Html(html)),
        Err(e) => {
            tracing::error!("template render failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Html(String::new()))
        }
    }
}

pub async fn landmarks_index(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let rows = sqlx::query_as::<_, LandmarkTypeCountRow>(
        "SELECT lt.slug, lt.name, lt.name_plural, COUNT(l.id) as count \
         FROM landmark_types lt \
         JOIN landmarks l ON l.type_id = lt.id \
         GROUP BY lt.slug, lt.name, lt.name_plural \
         ORDER BY count DESC",
    )
    .fetch_all(&state.db)
    .await?;

    let types: Vec<LandmarkTypeCount> = rows
        .into_iter()
        .map(|r| {
            let url_path = format!("/{}/", type_slug_to_url(&r.slug));
            let display_name = r.name_plural.clone().unwrap_or_else(|| r.name.clone());
            LandmarkTypeCount {
                slug: r.slug,
                name: display_name,
                count: r.count,
                url_path,
            }
        })
        .collect();

    let tmpl = LandmarksIndexTemplate {
        img: state.image_base_url.clone(),
        types,
    };
    Ok(Html(tmpl.render()?))
}

pub async fn landmarks_by_url(
    State(state): State<AppState>,
    Path(url_slug): Path<String>,
    query: axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    let Some(type_slug) = url_slug_to_type_slug(&url_slug) else {
        return Ok(not_found(&state.image_base_url).into_response());
    };
    landmarks_by_type(State(state), Path(type_slug.to_string()), query).await
}

pub async fn landmarks_by_type(
    State(state): State<AppState>,
    Path(type_slug): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    let page: i64 = params
        .get("strana")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1);
    let offset = (page - 1) * LANDMARKS_PER_PAGE;

    let type_row = sqlx::query_as::<_, LandmarkTypeCountRow>(
        "SELECT lt.slug, lt.name, lt.name_plural, COUNT(l.id) as count \
         FROM landmark_types lt \
         JOIN landmarks l ON l.type_id = lt.id \
         WHERE lt.slug = $1 \
         GROUP BY lt.slug, lt.name, lt.name_plural",
    )
    .bind(&type_slug)
    .fetch_optional(&state.db)
    .await?;

    let Some(type_row) = type_row else {
        return Ok(not_found(&state.image_base_url).into_response());
    };
    let display_name = type_row
        .name_plural
        .clone()
        .unwrap_or_else(|| type_row.name.clone());
    let type_info = LandmarkTypeCount {
        url_path: format!("/{}/", type_slug_to_url(&type_row.slug)),
        slug: type_row.slug,
        name: display_name,
        count: type_row.count,
    };

    let total_pages = (type_info.count + LANDMARKS_PER_PAGE - 1) / LANDMARKS_PER_PAGE;

    let landmarks = sqlx::query_as::<_, LandmarkRow>(
        "SELECT l.id, l.name, l.slug, l.latitude, l.longitude, l.description, \
         l.wikipedia_url, l.image_ext, l.npu_catalog_id, \
         lt.slug as type_slug, lt.name as type_name, \
         m.name as municipality_name, m.slug as municipality_slug, \
         o.slug as orp_slug, r.slug as region_slug \
         FROM landmarks l \
         JOIN landmark_types lt ON l.type_id = lt.id \
         LEFT JOIN municipalities m ON l.municipality_id = m.id \
         LEFT JOIN orp o ON m.orp_id = o.id \
         LEFT JOIN districts d ON o.district_id = d.id \
         LEFT JOIN regions r ON d.region_id = r.id \
         WHERE lt.slug = $1 \
         ORDER BY l.name \
         LIMIT $2 OFFSET $3",
    )
    .bind(&type_slug)
    .bind(LANDMARKS_PER_PAGE)
    .bind(offset)
    .fetch_all(&state.db)
    .await?;

    let tmpl = LandmarksListTemplate {
        img: state.image_base_url.clone(),
        type_name: type_info.name,
        type_slug: type_info.slug,
        landmarks,
        page,
        total_pages,
        total_count: type_info.count,
    };
    Ok(Html(tmpl.render()?).into_response())
}

pub async fn api_landmarks(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    let type_slug = params.get("type").cloned().unwrap_or_default();
    let page: i64 = params
        .get("page")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1);
    let offset = (page - 1) * LANDMARKS_PER_PAGE;

    let landmarks = sqlx::query_as::<_, LandmarkRow>(
        "SELECT l.id, l.name, l.slug, l.latitude, l.longitude, l.description, \
         l.wikipedia_url, l.image_ext, l.npu_catalog_id, \
         lt.slug as type_slug, lt.name as type_name, \
         m.name as municipality_name, m.slug as municipality_slug, \
         o.slug as orp_slug, r.slug as region_slug \
         FROM landmarks l \
         JOIN landmark_types lt ON l.type_id = lt.id \
         LEFT JOIN municipalities m ON l.municipality_id = m.id \
         LEFT JOIN orp o ON m.orp_id = o.id \
         LEFT JOIN districts d ON o.district_id = d.id \
         LEFT JOIN regions r ON d.region_id = r.id \
         WHERE ($1 = '' OR lt.slug = $1) \
         ORDER BY l.name \
         LIMIT $2 OFFSET $3",
    )
    .bind(&type_slug)
    .bind(LANDMARKS_PER_PAGE)
    .bind(offset)
    .fetch_all(&state.db)
    .await?;

    let items: Vec<serde_json::Value> = landmarks
        .iter()
        .map(|l| {
            let url = match (&l.orp_slug, &l.municipality_slug) {
                (Some(o), Some(m)) if o == m => format!("/{o}/{}/", l.slug),
                (Some(o), Some(m)) => format!("/{o}/{m}/{}/", l.slug),
                _ => String::new(),
            };
            serde_json::json!({
                "name": l.name,
                "slug": l.slug,
                "type": l.type_name,
                "municipality": l.municipality_name,
                "url": url,
                "latitude": l.latitude,
                "longitude": l.longitude,
            })
        })
        .collect();

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&serde_json::json!({
            "items": items,
            "page": page,
            "hasMore": landmarks.len() as i64 == LANDMARKS_PER_PAGE,
        }))?,
    )
        .into_response())
}
