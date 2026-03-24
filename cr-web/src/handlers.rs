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
}

// --- Templates ---

#[derive(Template)]
#[template(path = "homepage.html")]
struct HomepageTemplate {
    regions: Vec<RegionRow>,
}

#[derive(Template)]
#[template(path = "region.html")]
struct RegionTemplate {
    region: RegionRow,
    orps: Vec<OrpRow>,
}

#[derive(Template)]
#[template(path = "orp.html")]
struct OrpTemplate {
    region: RegionRow,
    orp: OrpRow,
    main_municipality: MunicipalityRow,
    other_municipalities: Vec<MunicipalityRow>,
}

#[derive(Template)]
#[template(path = "municipality.html")]
struct MunicipalityTemplate {
    region: RegionRow,
    orp: OrpRow,
    municipality: MunicipalityRow,
}

#[derive(Template)]
#[template(path = "404.html")]
struct NotFoundTemplate;

// --- Handlers ---

pub async fn health() -> &'static str {
    "OK"
}

pub async fn homepage(State(state): State<AppState>) -> impl IntoResponse {
    let regions = sqlx::query_as::<_, RegionRow>("SELECT id, name, slug, region_code, latitude, longitude FROM regions ORDER BY name")
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();

    let tmpl = HomepageTemplate { regions };
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
        3 => render_municipality(&state, segments[0], segments[1], segments[2]).await,
        _ => not_found(),
    }
}

async fn render_region(state: &AppState, region_slug: &str) -> (StatusCode, Html<String>) {
    let region = sqlx::query_as::<_, RegionRow>(
        "SELECT id, name, slug, region_code, latitude, longitude FROM regions WHERE slug = $1",
    )
    .bind(region_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let Some(region) = region else {
        return not_found();
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

    let tmpl = RegionTemplate { region, orps };
    (StatusCode::OK, Html(tmpl.render().unwrap_or_default()))
}

async fn render_orp(
    state: &AppState,
    region_slug: &str,
    orp_slug: &str,
) -> (StatusCode, Html<String>) {
    let region = sqlx::query_as::<_, RegionRow>(
        "SELECT id, name, slug, region_code, latitude, longitude FROM regions WHERE slug = $1",
    )
    .bind(region_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let Some(region) = region else {
        return not_found();
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
        return not_found();
    };

    let all_municipalities = sqlx::query_as::<_, MunicipalityRow>(
        "SELECT id, name, slug, municipality_code, pou_code, latitude, longitude \
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
        return not_found();
    };

    let tmpl = OrpTemplate {
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
        "SELECT id, name, slug, region_code, latitude, longitude FROM regions WHERE slug = $1",
    )
    .bind(region_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let Some(region) = region else {
        return not_found();
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
        return not_found();
    };

    let municipality = sqlx::query_as::<_, MunicipalityRow>(
        "SELECT id, name, slug, municipality_code, pou_code, latitude, longitude \
         FROM municipalities WHERE orp_id = $1 AND slug = $2",
    )
    .bind(orp.id)
    .bind(municipality_slug)
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let Some(municipality) = municipality else {
        return not_found();
    };

    let tmpl = MunicipalityTemplate {
        region,
        orp,
        municipality,
    };
    (StatusCode::OK, Html(tmpl.render().unwrap_or_default()))
}

fn not_found() -> (StatusCode, Html<String>) {
    let tmpl = NotFoundTemplate;
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
