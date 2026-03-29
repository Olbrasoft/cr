#![allow(clippy::manual_div_ceil)]
use askama::Template;
use axum::extract::{Path, State};
use axum::http::{StatusCode, Uri, header};
use axum::response::{Html, IntoResponse, Response};

use crate::error::WebResult;
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
    npu_catalog_id: Option<String>,
    #[allow(dead_code)]
    type_slug: String,
    type_name: String,
    municipality_name: Option<String>,
    #[allow(dead_code)]
    municipality_slug: Option<String>,
    orp_slug: Option<String>,
    region_slug: Option<String>,
}

struct LandmarkTypeCount {
    slug: String,
    name: String,
    count: i64,
    url_path: String,
}

#[derive(sqlx::FromRow)]
struct LandmarkTypeCountRow {
    slug: String,
    name: String,
    name_plural: Option<String>,
    count: i64,
}

// --- Photo info for gallery display ---

struct PhotoInfo {
    url: String,
    thumb_url: String,
    width: i16,
    height: i16,
}

#[derive(sqlx::FromRow)]
struct PhotoMetadataRow {
    r2_key: String,
    width: i16,
    height: i16,
}

async fn fetch_photos(
    db: &sqlx::PgPool,
    img_base: &str,
    entity_type: &str,
    entity_id: i32,
    slug: &str,
) -> Vec<PhotoInfo> {
    let rows = sqlx::query_as::<_, PhotoMetadataRow>(
        "SELECT r2_key, width, height FROM photo_metadata \
         WHERE entity_type = $1 AND entity_id = $2 ORDER BY photo_index",
    )
    .bind(entity_type)
    .bind(entity_id)
    .fetch_all(db)
    .await
    .unwrap_or_else(|e| { tracing::error!("fetch_photos query failed: {e}"); Vec::new() });

    rows.into_iter()
        .map(|r| {
            let url = if entity_type == "landmark" {
                // SEO URL: /img/landmarks/{slug}-{r2_filename}
                let filename = r.r2_key.strip_prefix("landmarks/").unwrap_or(&r.r2_key);
                format!("{}/img/landmarks/{}-{}", img_base, slug, filename)
            } else {
                // Pools: /img/{r2_key} (slug already in filename)
                format!("{}/img/{}", img_base, r.r2_key)
            };
            let thumb_url = format!("{}?w=360", &url);
            PhotoInfo { url, thumb_url, width: r.width, height: r.height }
        })
        .collect()
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
    main_landmarks: Vec<OrpLandmarkRow>,
    other_landmarks: Vec<OrpLandmarkRow>,
    landmarks_count: i64,
    pools: Vec<OrpPoolRow>,
}

#[derive(sqlx::FromRow)]
struct OrpPoolRow {
    name: String,
    slug: String,
    is_aquapark: bool,
    is_indoor: bool,
    is_outdoor: bool,
    is_natural: bool,
}

#[derive(Template)]
#[template(path = "municipality.html")]
struct MunicipalityTemplate {
    img: String,
    region: RegionRow,
    orp: OrpRow,
    municipality: MunicipalityRow,
    landmarks: Vec<MunicipalityLandmarkRow>,
}

#[derive(sqlx::FromRow)]
struct MunicipalityLandmarkRow {
    name: String,
    slug: String,
    type_name: String,
}

#[derive(sqlx::FromRow)]
struct OrpLandmarkRow {
    name: String,
    slug: String,
    type_name: String,
    municipality_name: String,
    municipality_slug: String,
    is_main: bool,
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
    photos: Vec<PhotoInfo>,
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

// --- Pool types ---

#[derive(sqlx::FromRow)]
struct PoolListRow {
    name: String,
    slug: String,
    description: Option<String>,
    municipality_name: Option<String>,
    orp_slug: Option<String>,
    region_slug: Option<String>,
    photo_count: i16,
}

#[derive(sqlx::FromRow)]
struct PoolDetailRow {
    #[allow(dead_code)]
    id: i32,
    name: String,
    slug: String,
    description: Option<String>,
    address: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    website: Option<String>,
    email: Option<String>,
    phone: Option<String>,
    facebook: Option<String>,
    facilities: Option<String>,
    pool_length_m: Option<i32>,
    is_aquapark: bool,
    is_indoor: bool,
    is_outdoor: bool,
    is_natural: bool,
    photo_count: i16,
    municipality_name: Option<String>,
}

#[derive(Template)]
#[template(path = "pools_hub.html")]
struct PoolsHubTemplate {
    img: String,
    aquapark_count: i64,
    indoor_count: i64,
    outdoor_count: i64,
    natural_count: i64,
}

#[derive(Template)]
#[template(path = "pools_list.html")]
struct PoolsListTemplate {
    img: String,
    category_name: String,
    category_slug: String,
    pools: Vec<PoolListRow>,
    page: i64,
    total_pages: i64,
    total_count: i64,
}

#[derive(Template)]
#[template(path = "pool_detail.html")]
struct PoolDetailTemplate {
    img: String,
    pool: PoolDetailRow,
    region: RegionRow,
    orp: OrpRow,
    photos: Vec<PhotoInfo>,
}

// --- Handlers ---

pub async fn health() -> &'static str {
    "OK"
}

pub async fn homepage(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let regions = sqlx::query_as::<_, RegionRow>("SELECT id, name, slug, region_code, latitude, longitude, coat_of_arms_ext, flag_ext, description FROM regions ORDER BY name")
        .fetch_all(&state.db)
        .await?;

    let tmpl = HomepageTemplate { img: state.image_base_url.clone(), regions };
    Ok(Html(tmpl.render()?))
}

pub async fn audiobooks(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let audiobooks = sqlx::query_as::<_, AudiobookRow>(
        "SELECT id, title, author, narrator, year, duration, archive_id, cover_filename \
         FROM audiobooks ORDER BY year, title",
    )
    .fetch_all(&state.db)
    .await?;

    let tmpl = AudiobooksTemplate { img: state.image_base_url.clone(), audiobooks };
    Ok(Html(tmpl.render()?))
}

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

async fn render_pool(
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

pub async fn resolve_path(
    State(state): State<AppState>,
    uri: Uri,
) -> WebResult<Response> {
    let path = uri.path().trim_matches('/');
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    match segments.len() {
        1 => {
            // Try landmark type first (e.g. /hrady, /zamky)
            if url_slug_to_type_slug(segments[0]).is_some() {
                let query = uri.query().map(|q| {
                    q.split('&')
                        .filter_map(|p| {
                            let mut kv = p.splitn(2, '=');
                            Some((kv.next()?.to_string(), kv.next().unwrap_or("").to_string()))
                        })
                        .collect::<std::collections::HashMap<String, String>>()
                }).unwrap_or_default();
                return landmarks_by_url(
                    State(state.clone()),
                    Path(segments[0].to_string()),
                    axum::extract::Query(query),
                ).await;
            }
            // Try ORP first (short URL: /{orp}/), then region
            let orp_check = sqlx::query_scalar::<_, i32>(
                "SELECT id FROM orp WHERE slug = $1",
            )
            .bind(segments[0])
            .fetch_optional(&state.db)
            .await?;

            if orp_check.is_some() {
                return Ok(render_orp_by_slug(&state, segments[0]).await.into_response());
            }
            Ok(render_region(&state, segments[0]).await.into_response())
        }
        2 => {
            // New primary: /{orp}/{entity}/ — try ORP + entity
            let orp_check = sqlx::query_scalar::<_, i32>(
                "SELECT id FROM orp WHERE slug = $1",
            )
            .bind(segments[0])
            .fetch_optional(&state.db)
            .await?;

            if orp_check.is_some() {
                // /{orp}/{entity}/ — try municipality, landmark, pool
                let result = render_municipality_short(&state, segments[0], segments[1]).await;
                if result.0 == StatusCode::NOT_FOUND {
                    let landmark_result = render_landmark_short(&state, segments[0], segments[1]).await;
                    if landmark_result.0 == StatusCode::NOT_FOUND {
                        return Ok(render_pool_short(&state, segments[0], segments[1]).await.into_response());
                    }
                    return Ok(landmark_result.into_response());
                }
                return Ok(result.into_response());
            }
            // Legacy: /{region}/{orp}/ → 301 redirect to /{orp}/
            let is_region = sqlx::query_scalar::<_, i32>(
                "SELECT id FROM regions WHERE slug = $1",
            )
            .bind(segments[0])
            .fetch_optional(&state.db)
            .await?;

            if is_region.is_some() {
                let new_url = format!("/{}/", segments[1]);
                return Ok(axum::response::Redirect::permanent(&new_url).into_response());
            }
            Ok(not_found(&state.image_base_url).into_response())
        }
        3 => {
            // First check: /{orp}/{municipality}/{entity}/ — landmark or pool in specific municipality
            let orp_id = sqlx::query_scalar::<_, i32>(
                "SELECT id FROM orp WHERE slug = $1",
            )
            .bind(segments[0])
            .fetch_optional(&state.db)
            .await?;

            if let Some(oid) = orp_id {
                // Check if second segment is a municipality in this ORP
                let muni_id = sqlx::query_scalar::<_, i32>(
                    "SELECT id FROM municipalities WHERE slug = $1 AND orp_id = $2",
                )
                .bind(segments[1])
                .bind(oid)
                .fetch_optional(&state.db)
                .await?;

                if muni_id.is_some() {
                    // If municipality slug == ORP slug, redirect to short URL
                    if segments[1] == segments[0] {
                        let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
                        let new_url = format!("/{}/{}/{query}", segments[0], segments[2]);
                        return Ok(axum::response::Redirect::permanent(&new_url).into_response());
                    }
                    // Try landmark in this municipality, then pool
                    let landmark_result = render_landmark_in_municipality(&state, segments[0], segments[2], oid).await;
                    if landmark_result.0 != StatusCode::NOT_FOUND {
                        return Ok(landmark_result.into_response());
                    }
                    return Ok(render_pool_short(&state, segments[0], segments[2]).await.into_response());
                }
            }

            // Legacy fallback: /{region}/{orp}/{entity}/ → redirect to /{orp}/{entity}/
            let is_region = sqlx::query_scalar::<_, i32>(
                "SELECT id FROM regions WHERE slug = $1",
            )
            .bind(segments[0])
            .fetch_optional(&state.db)
            .await?;

            if is_region.is_some() {
                let new_url = format!("/{}/{}/", segments[1], segments[2]);
                return Ok(axum::response::Redirect::permanent(&new_url).into_response());
            }
            Ok(not_found(&state.image_base_url).into_response())
        }
        _ => Ok(not_found(&state.image_base_url).into_response()),
    }
}

// Helper: find region slug for an ORP slug
async fn region_slug_for_orp(db: &sqlx::PgPool, orp_slug: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT r.slug FROM regions r \
         JOIN districts d ON d.region_id = r.id \
         JOIN orp o ON o.district_id = d.id \
         WHERE o.slug = $1",
    )
    .bind(orp_slug)
    .fetch_optional(db)
    .await
    .unwrap_or_else(|e| { tracing::error!("region_slug_for_orp query failed: {e}"); None })
}

async fn render_orp_by_slug(state: &AppState, orp_slug: &str) -> (StatusCode, Html<String>) {
    let Some(region_slug) = region_slug_for_orp(&state.db, orp_slug).await else {
        return not_found(&state.image_base_url);
    };
    render_orp(state, &region_slug, orp_slug).await
}

async fn render_municipality_short(state: &AppState, orp_slug: &str, muni_slug: &str) -> (StatusCode, Html<String>) {
    let Some(region_slug) = region_slug_for_orp(&state.db, orp_slug).await else {
        return not_found(&state.image_base_url);
    };
    render_municipality(state, &region_slug, orp_slug, muni_slug).await
}

async fn render_landmark_short(state: &AppState, orp_slug: &str, landmark_slug: &str) -> (StatusCode, Html<String>) {
    let Some(region_slug) = region_slug_for_orp(&state.db, orp_slug).await else {
        return not_found(&state.image_base_url);
    };
    render_landmark(state, &region_slug, orp_slug, landmark_slug).await
}

async fn render_pool_short(state: &AppState, orp_slug: &str, pool_slug: &str) -> (StatusCode, Html<String>) {
    let Some(region_slug) = region_slug_for_orp(&state.db, orp_slug).await else {
        return not_found(&state.image_base_url);
    };
    render_pool(state, &region_slug, orp_slug, pool_slug).await
}

async fn render_landmark_in_municipality(
    state: &AppState,
    orp_slug: &str,
    landmark_slug: &str,
    orp_id: i32,
) -> (StatusCode, Html<String>) {
    let Some(region_slug) = region_slug_for_orp(&state.db, orp_slug).await else {
        return not_found(&state.image_base_url);
    };
    render_landmark(state, &region_slug, orp_slug, landmark_slug).await
}

async fn render_region(state: &AppState, region_slug: &str) -> (StatusCode, Html<String>) {
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
        return render_orp(state, region_slug, &orps[0].slug).await;
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
    .unwrap_or_else(|e| { tracing::error!("render_orp orp query failed: {e}"); None });

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
    .unwrap_or_else(|e| { tracing::error!("render_orp municipalities query failed: {e}"); Vec::new() });

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
    .unwrap_or_else(|e| { tracing::error!("render_orp landmarks_count query failed: {e}"); 0 });

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
    .unwrap_or_else(|e| { tracing::error!("render_orp landmarks query failed: {e}"); Vec::new() });

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
    .unwrap_or_else(|e| { tracing::error!("render_orp pools query failed: {e}"); Vec::new() });

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

async fn render_municipality(
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
    .unwrap_or_else(|e| { tracing::error!("render_municipality orp query failed: {e}"); None });

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
    .unwrap_or_else(|e| { tracing::error!("render_municipality municipality query failed: {e}"); None });

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
    .unwrap_or_else(|e| { tracing::error!("render_municipality landmarks query failed: {e}"); Vec::new() });

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
    .unwrap_or_else(|e| { tracing::error!("render_landmark orp query failed: {e}"); None });

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
    .unwrap_or_else(|e| { tracing::error!("render_landmark landmark query failed: {e}"); None });

    let Some(landmark) = landmark else {
        return not_found(&state.image_base_url);
    };

    let photos = fetch_photos(&state.db, &state.image_base_url, "landmark", landmark.id, &landmark.slug).await;

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

/// Map URL path segments to database type slugs
fn url_slug_to_type_slug(url_slug: &str) -> Option<&'static str> {
    match url_slug {
        "hrady" => Some("castle"),
        "zamky" => Some("chateau"),
        "kostely" => Some("church"),
        "kaple" => Some("chapel"),
        "tvrze" => Some("fortress"),
        "sochy" => Some("statue"),
        "krize" => Some("cross"),
        "sloupy" => Some("column"),
        "pomniky" => Some("monument"),
        "vodni-mlyny" => Some("watermill"),
        "vetrne-mlyny" => Some("windmill"),
        "rozhledny" => Some("lookout_tower"),
        "klastery" => Some("monastery"),
        "zriceniny" => Some("ruins"),
        "vily" => Some("villa"),
        "sousosi" => Some("sculpture_group"),
        "fary" => Some("rectory"),
        "sypky" => Some("granary"),
        "hrbitovy" => Some("cemetery"),
        "mosty" => Some("bridge"),
        "radnice" => Some("town_hall"),
        "skoly" => Some("school"),
        "kasny" => Some("fountain"),
        "zvonice" => Some("belfry"),
        "brany" => Some("gate"),
        "pivovary" => Some("brewery"),
        "synagogy" => Some("synagogue"),
        "hotely" => Some("hotel"),
        "veze" => Some("tower"),
        "hrobky" => Some("tomb"),
        "divadla" => Some("theater"),
        "parky" => Some("park"),
        "palace" => Some("palace"),
        "tovarny" => Some("factory"),
        "rotundy" => Some("rotunda"),
        _ => None,
    }
}

/// Map database type slug to URL path segment
fn type_slug_to_url(type_slug: &str) -> &'static str {
    match type_slug {
        "castle" => "hrady",
        "chateau" => "zamky",
        "church" => "kostely",
        "chapel" => "kaple",
        "fortress" => "tvrze",
        "statue" => "sochy",
        "cross" => "krize",
        "column" => "sloupy",
        "monument" => "pomniky",
        "watermill" => "vodni-mlyny",
        "windmill" => "vetrne-mlyny",
        "lookout_tower" => "rozhledny",
        "monastery" => "klastery",
        "ruins" => "zriceniny",
        "villa" => "vily",
        "sculpture_group" => "sousosi",
        "rectory" => "fary",
        "granary" => "sypky",
        "cemetery" => "hrbitovy",
        "bridge" => "mosty",
        "town_hall" => "radnice",
        "school" => "skoly",
        "fountain" => "kasny",
        "belfry" => "zvonice",
        "gate" => "brany",
        "brewery" => "pivovary",
        "synagogue" => "synagogy",
        "hotel" => "hotely",
        "tower" => "veze",
        "tomb" => "hrobky",
        "theater" => "divadla",
        "park" => "parky",
        "palace" => "palace",
        "factory" => "tovarny",
        "rotunda" => "rotundy",
        _ => "ostatni",
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

    let types: Vec<LandmarkTypeCount> = rows.into_iter().map(|r| {
        let url_path = format!("/{}/", type_slug_to_url(&r.slug));
        let display_name = r.name_plural.clone().unwrap_or_else(|| r.name.clone());
        LandmarkTypeCount { slug: r.slug, name: display_name, count: r.count, url_path }
    }).collect();

    let tmpl = LandmarksIndexTemplate { img: state.image_base_url.clone(), types };
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
    landmarks_by_type(
        State(state),
        Path(type_slug.to_string()),
        query,
    ).await
}

pub async fn landmarks_by_type(
    State(state): State<AppState>,
    Path(type_slug): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    let page: i64 = params.get("strana").and_then(|s| s.parse().ok()).unwrap_or(1).max(1);
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
    let display_name = type_row.name_plural.clone().unwrap_or_else(|| type_row.name.clone());
    let type_info = LandmarkTypeCount {
        url_path: format!("/{}/", type_slug_to_url(&type_row.slug)),
        slug: type_row.slug, name: display_name, count: type_row.count,
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
    let page: i64 = params.get("page").and_then(|s| s.parse().ok()).unwrap_or(1).max(1);
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

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&serde_json::json!({
            "items": items,
            "page": page,
            "hasMore": landmarks.len() as i64 == LANDMARKS_PER_PAGE,
        }))?,
    ).into_response())
}
