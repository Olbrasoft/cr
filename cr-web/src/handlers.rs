#![allow(clippy::manual_div_ceil)]
use askama::Template;
use axum::extract::{Path, State};
use axum::http::{StatusCode, Uri, header};
use axum::response::{Html, IntoResponse, Response};

use crate::state::AppState;

// --- DB row types ---

#[derive(sqlx::FromRow)]
struct RegionRow {
    id: i32,
    name: String,
    slug: String,
    region_code: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
    coat_of_arms_ext: Option<String>,
    flag_ext: Option<String>,
    description: Option<String>,
}

#[derive(sqlx::FromRow)]
struct OrpRow {
    id: i32,
    name: String,
    slug: String,
    orp_code: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
}

#[derive(sqlx::FromRow)]
struct MunicipalityRow {
    #[allow(dead_code)]
    id: i32,
    name: String,
    slug: String,
    municipality_code: String,
    pou_code: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
    wikipedia_url: Option<String>,
    official_website: Option<String>,
    coat_of_arms_ext: Option<String>,
    flag_ext: Option<String>,
    population: Option<i32>,
    #[allow(dead_code)]
    elevation: Option<f64>,
}

#[derive(sqlx::FromRow)]
struct LandmarkRow {
    #[allow(dead_code)]
    id: i32,
    name: String,
    slug: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
    description: Option<String>,
    wikipedia_url: Option<String>,
    #[allow(dead_code)]
    image_ext: Option<String>,
    #[allow(dead_code)]
    type_slug: String,
    type_name: String,
    municipality_name: Option<String>,
    #[allow(dead_code)]
    municipality_slug: Option<String>,
    orp_slug: Option<String>,
    region_slug: Option<String>,
}

#[derive(sqlx::FromRow)]
struct LandmarkTypeCount {
    slug: String,
    name: String,
    count: i64,
}

// --- Templates ---
// All templates receive `img` — the image base URL prefix.
// Production: "" (images via Cloudflare Worker at /img/)
// Dev: "https://ceskarepublika.wiki" (images fetched from production)

#[derive(Template)]
#[template(path = "homepage.html")]
struct HomepageTemplate {
    img: String,
    regions: Vec<RegionRow>,
}

#[derive(Template)]
#[template(path = "region.html")]
struct RegionTemplate {
    img: String,
    region: RegionRow,
    orps: Vec<OrpRow>,
}

#[derive(Template)]
#[template(path = "orp.html")]
struct OrpTemplate {
    img: String,
    region: RegionRow,
    orp: OrpRow,
    main_municipality: MunicipalityRow,
    other_municipalities: Vec<MunicipalityRow>,
}

#[derive(Template)]
#[template(path = "municipality.html")]
struct MunicipalityTemplate {
    img: String,
    region: RegionRow,
    orp: OrpRow,
    municipality: MunicipalityRow,
}

#[derive(Template)]
#[template(path = "404.html")]
struct NotFoundTemplate {
    img: String,
}

#[derive(Template)]
#[template(path = "landmarks_index.html")]
struct LandmarksIndexTemplate {
    img: String,
    types: Vec<LandmarkTypeCount>,
}

#[derive(Template)]
#[template(path = "landmarks_list.html")]
struct LandmarksListTemplate {
    img: String,
    type_name: String,
    type_slug: String,
    landmarks: Vec<LandmarkRow>,
    page: i64,
    total_pages: i64,
    total_count: i64,
}

#[derive(Template)]
#[template(path = "landmark_detail.html")]
struct LandmarkDetailTemplate {
    img: String,
    landmark: LandmarkRow,
    region: RegionRow,
    orp: OrpRow,
}

#[derive(sqlx::FromRow)]
struct AudiobookRow {
    #[allow(dead_code)]
    id: i32,
    title: String,
    author: String,
    narrator: String,
    year: i16,
    duration: String,
    archive_id: String,
    cover_filename: String,
}

#[derive(Template)]
#[template(path = "audiobooks.html")]
struct AudiobooksTemplate {
    img: String,
    audiobooks: Vec<AudiobookRow>,
}

// --- Handlers ---

pub async fn health() -> &'static str {
    "OK"
}

pub async fn homepage(State(state): State<AppState>) -> impl IntoResponse {
    let regions = sqlx::query_as::<_, RegionRow>("SELECT id, name, slug, region_code, latitude, longitude, coat_of_arms_ext, flag_ext, description FROM regions ORDER BY name")
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();

    let tmpl = HomepageTemplate { img: state.image_base_url.clone(), regions };
    Html(tmpl.render().unwrap_or_default())
}

pub async fn audiobooks(State(state): State<AppState>) -> impl IntoResponse {
    let audiobooks = sqlx::query_as::<_, AudiobookRow>(
        "SELECT id, title, author, narrator, year, duration, archive_id, cover_filename \
         FROM audiobooks ORDER BY year, title",
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let tmpl = AudiobooksTemplate { img: state.image_base_url.clone(), audiobooks };
    Html(tmpl.render().unwrap_or_default())
}

pub async fn resolve_path(
    State(state): State<AppState>,
    uri: Uri,
) -> impl IntoResponse {
    let path = uri.path().trim_matches('/');
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    match segments.len() {
        1 => render_region(&state, segments[0]).await,
        2 => render_orp(&state, segments[0], segments[1]).await,
        3 => {
            // Try municipality first, then landmark
            let result = render_municipality(&state, segments[0], segments[1], segments[2]).await;
            if result.0 == StatusCode::NOT_FOUND {
                render_landmark(&state, segments[0], segments[1], segments[2]).await
            } else {
                result
            }
        }
        _ => not_found(&state.image_base_url),
    }
}

async fn render_region(state: &AppState, region_slug: &str) -> (StatusCode, Html<String>) {
    let region = sqlx::query_as::<_, RegionRow>(
        "SELECT id, name, slug, region_code, latitude, longitude, coat_of_arms_ext, flag_ext, description FROM regions WHERE slug = $1",
    )
    .bind(region_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

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
    .unwrap_or_default();

    let tmpl = RegionTemplate { img: state.image_base_url.clone(), region, orps };
    (StatusCode::OK, Html(tmpl.render().unwrap_or_default()))
}

async fn render_orp(
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
    .unwrap_or(None);

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
    .unwrap_or(None);

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
    .unwrap_or_default();

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

    let tmpl = OrpTemplate {
        img: state.image_base_url.clone(),
        region,
        orp,
        main_municipality,
        other_municipalities,
    };
    (StatusCode::OK, Html(tmpl.render().unwrap_or_default()))
}

async fn render_municipality(
    state: &AppState,
    region_slug: &str,
    orp_slug: &str,
    municipality_slug: &str,
) -> (StatusCode, Html<String>) {
    let region = sqlx::query_as::<_, RegionRow>(
        "SELECT id, name, slug, region_code, latitude, longitude, coat_of_arms_ext, flag_ext, description FROM regions WHERE slug = $1",
    )
    .bind(region_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

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
    .unwrap_or(None);

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
    .unwrap_or(None);

    let Some(municipality) = municipality else {
        return not_found(&state.image_base_url);
    };

    let tmpl = MunicipalityTemplate {
        img: state.image_base_url.clone(),
        region,
        orp,
        municipality,
    };
    (StatusCode::OK, Html(tmpl.render().unwrap_or_default()))
}

async fn render_landmark(
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
    .unwrap_or(None);

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
    .unwrap_or(None);

    let Some(orp) = orp else {
        return not_found(&state.image_base_url);
    };

    // Find landmark by slug within municipalities of this ORP
    let landmark = sqlx::query_as::<_, LandmarkRow>(
        "SELECT l.id, l.name, l.slug, l.latitude, l.longitude, l.description, \
         l.wikipedia_url, l.image_ext, \
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
    .unwrap_or(None);

    let Some(landmark) = landmark else {
        return not_found(&state.image_base_url);
    };

    let tmpl = LandmarkDetailTemplate {
        img: state.image_base_url.clone(),
        landmark,
        region,
        orp,
    };
    (StatusCode::OK, Html(tmpl.render().unwrap_or_default()))
}

fn not_found(image_base_url: &str) -> (StatusCode, Html<String>) {
    let tmpl = NotFoundTemplate { img: image_base_url.to_string() };
    (
        StatusCode::NOT_FOUND,
        Html(tmpl.render().unwrap_or_default()),
    )
}

// --- GeoJSON API handlers ---

pub async fn geojson_municipality(
    State(state): State<AppState>,
    Path(code): Path<String>,
) -> Response {
    match state.geojson_index.municipalities.get(&code) {
        Some(geojson) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/geo+json"),
             (header::CACHE_CONTROL, "public, max-age=86400")],
            geojson.clone(),
        ).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub async fn geojson_orp(
    State(state): State<AppState>,
    Path(code): Path<String>,
) -> Response {
    match state.geojson_index.orp.get(&code) {
        Some(geojson) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/geo+json"),
             (header::CACHE_CONTROL, "public, max-age=86400")],
            geojson.clone(),
        ).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// --- Landmark handlers ---

const LANDMARKS_PER_PAGE: i64 = 10;

pub async fn landmarks_index(State(state): State<AppState>) -> impl IntoResponse {
    let types = sqlx::query_as::<_, LandmarkTypeCount>(
        "SELECT lt.slug, lt.name, COUNT(l.id) as count \
         FROM landmark_types lt \
         JOIN landmarks l ON l.type_id = lt.id \
         GROUP BY lt.slug, lt.name \
         ORDER BY count DESC",
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let tmpl = LandmarksIndexTemplate { img: state.image_base_url.clone(), types };
    Html(tmpl.render().unwrap_or_default())
}

pub async fn landmarks_by_type(
    State(state): State<AppState>,
    Path(type_slug): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let page: i64 = params.get("strana").and_then(|s| s.parse().ok()).unwrap_or(1).max(1);
    let offset = (page - 1) * LANDMARKS_PER_PAGE;

    let type_info = sqlx::query_as::<_, LandmarkTypeCount>(
        "SELECT lt.slug, lt.name, COUNT(l.id) as count \
         FROM landmark_types lt \
         JOIN landmarks l ON l.type_id = lt.id \
         WHERE lt.slug = $1 \
         GROUP BY lt.slug, lt.name",
    )
    .bind(&type_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let Some(type_info) = type_info else {
        return not_found(&state.image_base_url).into_response();
    };

    let total_pages = (type_info.count + LANDMARKS_PER_PAGE - 1) / LANDMARKS_PER_PAGE;

    let landmarks = sqlx::query_as::<_, LandmarkRow>(
        "SELECT l.id, l.name, l.slug, l.latitude, l.longitude, l.description, \
         l.wikipedia_url, l.image_ext, \
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
    .await
    .unwrap_or_default();

    let tmpl = LandmarksListTemplate {
        img: state.image_base_url.clone(),
        type_name: type_info.name,
        type_slug: type_info.slug,
        landmarks,
        page,
        total_pages,
        total_count: type_info.count,
    };
    Html(tmpl.render().unwrap_or_default()).into_response()
}

pub async fn api_landmarks(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let type_slug = params.get("type").cloned().unwrap_or_default();
    let page: i64 = params.get("page").and_then(|s| s.parse().ok()).unwrap_or(1).max(1);
    let offset = (page - 1) * LANDMARKS_PER_PAGE;

    let landmarks = sqlx::query_as::<_, LandmarkRow>(
        "SELECT l.id, l.name, l.slug, l.latitude, l.longitude, l.description, \
         l.wikipedia_url, l.image_ext, \
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
    .await
    .unwrap_or_default();

    let items: Vec<serde_json::Value> = landmarks.iter().map(|l| {
        let url = match (&l.region_slug, &l.orp_slug) {
            (Some(r), Some(o)) => format!("/{r}/{o}/{}/", l.slug),
            _ => format!("/pamatky/{}/", l.slug),
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
    }).collect();

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&serde_json::json!({
            "items": items,
            "page": page,
            "hasMore": landmarks.len() as i64 == LANDMARKS_PER_PAGE,
        })).unwrap_or_default(),
    ).into_response()
}
