// Allow manual_div_ceil in askama-generated derive code (askama 0.12 issue, fixed in 0.15+)
#![allow(clippy::manual_div_ceil)]
use askama::Template;
use axum::extract::{Path, State};
use axum::http::{StatusCode, Uri, header};
use axum::response::{Html, IntoResponse, Response};
use cr_domain::repository::{PhotoRepository, RegionRepository};

use crate::error::WebResult;
use crate::state::AppState;

mod audiobooks;
mod geojson;
mod landmarks;
mod municipalities;
mod orp;
mod pools;
mod regions;

// Re-export all public handlers so main.rs doesn't need changes
pub use audiobooks::audiobooks;
pub use geojson::{geojson_municipality, geojson_orp};
pub use landmarks::{api_landmarks, landmarks_by_url, landmarks_index};
pub use pools::{pools_by_category, pools_hub};

// --- DB row types ---

#[derive(sqlx::FromRow)]
pub(crate) struct RegionRow {
    #[allow(dead_code)]
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) region_code: String,
    #[allow(dead_code)]
    pub(crate) latitude: Option<f64>,
    #[allow(dead_code)]
    pub(crate) longitude: Option<f64>,
    pub(crate) coat_of_arms_ext: Option<String>,
    pub(crate) flag_ext: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) hero_photo_url: Option<String>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct OrpRow {
    #[allow(dead_code)]
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) orp_code: String,
    pub(crate) latitude: Option<f64>,
    pub(crate) longitude: Option<f64>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct MunicipalityRow {
    #[allow(dead_code)]
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) municipality_code: String,
    #[allow(dead_code)]
    pub(crate) pou_code: String,
    pub(crate) latitude: Option<f64>,
    pub(crate) longitude: Option<f64>,
    pub(crate) wikipedia_url: Option<String>,
    pub(crate) official_website: Option<String>,
    pub(crate) coat_of_arms_ext: Option<String>,
    pub(crate) flag_ext: Option<String>,
    pub(crate) population: Option<i32>,
    #[allow(dead_code)]
    pub(crate) elevation: Option<f64>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct LandmarkRow {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) latitude: Option<f64>,
    pub(crate) longitude: Option<f64>,
    #[allow(dead_code)]
    pub(crate) description: Option<String>,
    pub(crate) wikipedia_url: Option<String>,
    #[allow(dead_code)]
    pub(crate) image_ext: Option<String>,
    #[allow(dead_code)]
    pub(crate) npu_catalog_id: Option<String>,
    #[sqlx(default)]
    pub(crate) npu_description: Option<String>,
    #[allow(dead_code)]
    pub(crate) type_slug: String,
    pub(crate) type_name: String,
    pub(crate) municipality_name: Option<String>,
    #[allow(dead_code)]
    pub(crate) municipality_slug: Option<String>,
    pub(crate) orp_slug: Option<String>,
    #[allow(dead_code)]
    pub(crate) region_slug: Option<String>,
    #[sqlx(default)]
    pub(crate) municipality_code: Option<String>,
    #[sqlx(default)]
    pub(crate) municipality_coat_of_arms_ext: Option<String>,
}

pub(crate) struct LandmarkTypeCount {
    pub(crate) slug: String,
    pub(crate) name: String,
    pub(crate) count: i64,
    pub(crate) url_path: String,
}

#[derive(sqlx::FromRow)]
pub(crate) struct LandmarkTypeCountRow {
    pub(crate) slug: String,
    pub(crate) name: String,
    pub(crate) name_plural: Option<String>,
    pub(crate) count: i64,
}

// --- From impls: domain record → handler row types ---

impl From<cr_domain::repository::MunicipalityRecord> for MunicipalityRow {
    fn from(r: cr_domain::repository::MunicipalityRecord) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            municipality_code: r.municipality_code,
            pou_code: r.pou_code,
            latitude: r.latitude,
            longitude: r.longitude,
            wikipedia_url: r.wikipedia_url,
            official_website: r.official_website,
            coat_of_arms_ext: r.coat_of_arms_ext,
            flag_ext: r.flag_ext,
            population: r.population,
            elevation: r.elevation,
        }
    }
}

impl From<cr_domain::repository::LandmarkRecord> for LandmarkRow {
    fn from(r: cr_domain::repository::LandmarkRecord) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            latitude: r.latitude,
            longitude: r.longitude,
            description: r.description,
            wikipedia_url: r.wikipedia_url,
            image_ext: r.image_ext,
            npu_catalog_id: r.npu_catalog_id,
            npu_description: r.npu_description,
            type_slug: r.type_slug,
            type_name: r.type_name,
            municipality_name: r.municipality_name,
            municipality_slug: r.municipality_slug,
            orp_slug: r.orp_slug,
            region_slug: r.region_slug,
            municipality_code: r.municipality_code,
            municipality_coat_of_arms_ext: r.municipality_coat_of_arms_ext,
        }
    }
}

impl From<cr_domain::repository::PoolRecord> for PoolDetailRow {
    fn from(r: cr_domain::repository::PoolRecord) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            description: r.description,
            address: r.address,
            latitude: r.latitude,
            longitude: r.longitude,
            website: r.website,
            email: r.email,
            phone: r.phone,
            facebook: r.facebook,
            facilities: r.facilities,
            pool_length_m: r.pool_length_m,
            is_aquapark: r.is_aquapark,
            is_indoor: r.is_indoor,
            is_outdoor: r.is_outdoor,
            is_natural: r.is_natural,
            photo_count: r.photo_count,
            municipality_name: r.municipality_name,
        }
    }
}

// --- Photo info for gallery display ---

pub(crate) struct PhotoInfo {
    pub(crate) url: String,
    pub(crate) thumb_url: String,
    #[allow(dead_code)]
    pub(crate) width: i16,
    #[allow(dead_code)]
    pub(crate) height: i16,
}

pub(crate) async fn fetch_photos(
    state: &AppState,
    entity_type: &str,
    entity_id: i32,
    slug: &str,
    orp_slug: Option<&str>,
    municipality_slug: Option<&str>,
) -> Vec<PhotoInfo> {
    let records = state
        .photo_repo
        .find_by_entity(entity_type, entity_id)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("fetch_photos query failed: {e}");
            Vec::new()
        });

    records
        .into_iter()
        .map(|r| {
            let url = if entity_type == "landmark" {
                if let (Some(orp), Some(muni)) = (orp_slug, municipality_slug) {
                    // SEO URL: /{orp}/{municipality}/{landmark-slug}.webp
                    // or /{orp}/{landmark-slug}.webp for main municipality
                    if orp == muni {
                        format!("/{}/{}.webp", orp, slug)
                    } else {
                        format!("/{}/{}/{}.webp", orp, muni, slug)
                    }
                } else {
                    // Fallback to old URL pattern
                    let filename = r.r2_key.strip_prefix("landmarks/").unwrap_or(&r.r2_key);
                    format!(
                        "{}/img/landmarks/{}-{}",
                        state.image_base_url, slug, filename
                    )
                }
            } else {
                // Pools: /img/{r2_key}
                format!("{}/img/{}", state.image_base_url, r.r2_key)
            };
            let thumb_url = format!("{}?w=360", &url);
            PhotoInfo {
                url,
                thumb_url,
                width: r.width,
                height: r.height,
            }
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
pub(crate) struct RegionTemplate {
    pub(crate) img: String,
    pub(crate) region: RegionRow,
    pub(crate) orps: Vec<OrpRow>,
}

#[derive(Template)]
#[template(path = "orp.html")]
pub(crate) struct OrpTemplate {
    pub(crate) img: String,
    pub(crate) region: RegionRow,
    pub(crate) orp: OrpRow,
    pub(crate) main_municipality: MunicipalityRow,
    pub(crate) other_municipalities: Vec<MunicipalityRow>,
    pub(crate) main_landmarks: Vec<OrpLandmarkRow>,
    pub(crate) other_landmarks: Vec<OrpLandmarkRow>,
    pub(crate) landmarks_count: i64,
    pub(crate) pools: Vec<OrpPoolRow>,
    pub(crate) photo: Option<MunicipalityPhotoInfo>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct OrpPoolRow {
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) is_aquapark: bool,
    #[allow(dead_code)]
    pub(crate) is_indoor: bool,
    pub(crate) is_outdoor: bool,
    pub(crate) is_natural: bool,
}

#[derive(sqlx::FromRow)]
pub(crate) struct OrpLandmarkRow {
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) type_name: String,
    pub(crate) municipality_name: String,
    pub(crate) municipality_slug: String,
    pub(crate) is_main: bool,
    pub(crate) npu_description: Option<String>,
}

#[derive(Template)]
#[template(path = "municipality.html")]
pub(crate) struct MunicipalityTemplate {
    pub(crate) img: String,
    pub(crate) region: RegionRow,
    pub(crate) orp: OrpRow,
    pub(crate) municipality: MunicipalityRow,
    pub(crate) landmarks: Vec<MunicipalityLandmarkRow>,
    pub(crate) photo: Option<MunicipalityPhotoInfo>,
    pub(crate) gallery_photos: Vec<MunicipalityPhotoInfo>,
}

pub(crate) struct MunicipalityPhotoInfo {
    pub(crate) url: String,
    pub(crate) thumb_url: String,
    pub(crate) description: String,
}

#[derive(sqlx::FromRow)]
struct MunicipalityPhotoRow {
    slug: String,
    description: Option<String>,
    object_name: Option<String>,
}

pub(crate) async fn fetch_municipality_photo(
    db: &sqlx::PgPool,
    municipality_code: &str,
    orp_slug: &str,
    municipality_slug: &str,
) -> Option<MunicipalityPhotoInfo> {
    let row = sqlx::query_as::<_, MunicipalityPhotoRow>(
        "SELECT slug, description, object_name FROM municipality_photos \
         WHERE municipality_code = $1 AND is_primary = true \
         ORDER BY photo_index LIMIT 1",
    )
    .bind(municipality_code)
    .fetch_optional(db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("fetch_municipality_photo query failed: {e}");
        None
    })?;

    let url = if orp_slug == municipality_slug {
        format!("/{}/{}.webp", orp_slug, row.slug)
    } else {
        format!("/{}/{}/{}.webp", orp_slug, municipality_slug, row.slug)
    };
    let thumb_url = format!("{}?w=360", &url);
    let description = row.description.or(row.object_name).unwrap_or_default();

    Some(MunicipalityPhotoInfo {
        url,
        thumb_url,
        description,
    })
}

pub(crate) async fn fetch_municipality_gallery(
    db: &sqlx::PgPool,
    municipality_code: &str,
    orp_slug: &str,
    municipality_slug: &str,
) -> Vec<MunicipalityPhotoInfo> {
    let rows = sqlx::query_as::<_, MunicipalityPhotoRow>(
        "SELECT slug, description, object_name FROM municipality_photos \
         WHERE municipality_code = $1 AND (is_primary = false OR photo_index > 1) \
         ORDER BY photo_index",
    )
    .bind(municipality_code)
    .fetch_all(db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("fetch_municipality_gallery query failed: {e}");
        Vec::new()
    });

    rows.into_iter()
        .map(|row| {
            let url = if orp_slug == municipality_slug {
                format!("/{}/{}.webp", orp_slug, row.slug)
            } else {
                format!("/{}/{}/{}.webp", orp_slug, municipality_slug, row.slug)
            };
            let thumb_url = format!("{}?w=360", &url);
            let description = row.description.or(row.object_name).unwrap_or_default();
            MunicipalityPhotoInfo {
                url,
                thumb_url,
                description,
            }
        })
        .collect()
}

#[derive(sqlx::FromRow)]
pub(crate) struct MunicipalityLandmarkRow {
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) type_name: String,
}

#[derive(Template)]
#[template(path = "404.html")]
struct NotFoundTemplate {
    img: String,
}

#[derive(Template)]
#[template(path = "landmarks_index.html")]
pub(crate) struct LandmarksIndexTemplate {
    pub(crate) img: String,
    pub(crate) types: Vec<LandmarkTypeCount>,
}

#[derive(Template)]
#[template(path = "landmarks_list.html")]
pub(crate) struct LandmarksListTemplate {
    pub(crate) img: String,
    pub(crate) type_name: String,
    pub(crate) type_slug: String,
    pub(crate) landmarks: Vec<LandmarkRow>,
    pub(crate) page: i64,
    pub(crate) total_pages: i64,
    pub(crate) total_count: i64,
}

#[derive(Template)]
#[template(path = "landmark_detail.html")]
pub(crate) struct LandmarkDetailTemplate {
    pub(crate) img: String,
    pub(crate) landmark: LandmarkRow,
    pub(crate) region: RegionRow,
    pub(crate) orp: OrpRow,
    pub(crate) photos: Vec<PhotoInfo>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct AudiobookRow {
    #[allow(dead_code)]
    pub(crate) id: i32,
    pub(crate) title: String,
    pub(crate) author: String,
    pub(crate) narrator: String,
    pub(crate) year: i16,
    pub(crate) duration: String,
    pub(crate) archive_id: String,
    pub(crate) cover_filename: String,
}

#[derive(Template)]
#[template(path = "audiobooks.html")]
pub(crate) struct AudiobooksTemplate {
    pub(crate) img: String,
    pub(crate) audiobooks: Vec<AudiobookRow>,
}

// --- Pool types ---

#[derive(sqlx::FromRow)]
pub(crate) struct PoolListRow {
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) description: Option<String>,
    pub(crate) municipality_name: Option<String>,
    pub(crate) orp_slug: Option<String>,
    pub(crate) region_slug: Option<String>,
    #[allow(dead_code)]
    pub(crate) photo_count: i16,
}

#[derive(sqlx::FromRow)]
pub(crate) struct PoolDetailRow {
    #[allow(dead_code)]
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) description: Option<String>,
    pub(crate) address: Option<String>,
    pub(crate) latitude: Option<f64>,
    pub(crate) longitude: Option<f64>,
    pub(crate) website: Option<String>,
    pub(crate) email: Option<String>,
    pub(crate) phone: Option<String>,
    pub(crate) facebook: Option<String>,
    pub(crate) facilities: Option<String>,
    pub(crate) pool_length_m: Option<i32>,
    pub(crate) is_aquapark: bool,
    pub(crate) is_indoor: bool,
    pub(crate) is_outdoor: bool,
    pub(crate) is_natural: bool,
    #[allow(dead_code)]
    pub(crate) photo_count: i16,
    pub(crate) municipality_name: Option<String>,
}

#[derive(Template)]
#[template(path = "pools_hub.html")]
pub(crate) struct PoolsHubTemplate {
    pub(crate) img: String,
    pub(crate) aquapark_count: i64,
    pub(crate) indoor_count: i64,
    pub(crate) outdoor_count: i64,
    pub(crate) natural_count: i64,
}

#[derive(Template)]
#[template(path = "pools_list.html")]
pub(crate) struct PoolsListTemplate {
    pub(crate) img: String,
    pub(crate) category_name: String,
    #[allow(dead_code)]
    pub(crate) category_slug: String,
    pub(crate) pools: Vec<PoolListRow>,
    pub(crate) page: i64,
    pub(crate) total_pages: i64,
    pub(crate) total_count: i64,
}

#[derive(Template)]
#[template(path = "pool_detail.html")]
pub(crate) struct PoolDetailTemplate {
    pub(crate) img: String,
    pub(crate) pool: PoolDetailRow,
    pub(crate) region: RegionRow,
    pub(crate) orp: OrpRow,
    pub(crate) photos: Vec<PhotoInfo>,
}

// --- Shared helpers ---

pub(crate) fn not_found(image_base_url: &str) -> (StatusCode, Html<String>) {
    let tmpl = NotFoundTemplate {
        img: image_base_url.to_string(),
    };
    (
        StatusCode::NOT_FOUND,
        Html(tmpl.render().unwrap_or_default()),
    )
}

/// Helper: find region slug for an ORP slug
pub(crate) async fn region_slug_for_orp(db: &sqlx::PgPool, orp_slug: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT r.slug FROM regions r \
         JOIN districts d ON d.region_id = r.id \
         JOIN orp o ON o.district_id = d.id \
         WHERE o.slug = $1",
    )
    .bind(orp_slug)
    .fetch_optional(db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("region_slug_for_orp query failed: {e}");
        None
    })
}

/// Map URL path segments to database type slugs
pub(crate) fn url_slug_to_type_slug(url_slug: &str) -> Option<&'static str> {
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
pub(crate) fn type_slug_to_url(type_slug: &str) -> &'static str {
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

// --- Public handlers that stay in mod.rs ---

pub async fn health() -> &'static str {
    "OK"
}

pub async fn homepage(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let regions: Vec<RegionRow> = state
        .region_repo
        .find_all()
        .await?
        .into_iter()
        .map(RegionRow::from)
        .collect();

    let tmpl = HomepageTemplate {
        img: state.image_base_url.clone(),
        regions,
    };
    Ok(Html(tmpl.render()?))
}

pub async fn resolve_path(State(state): State<AppState>, uri: Uri) -> WebResult<Response> {
    let path = uri.path().trim_matches('/');

    // Detect image requests by file extension — serve via img proxy
    if path.ends_with(".webp")
        || path.ends_with(".jpg")
        || path.ends_with(".jpeg")
        || path.ends_with(".png")
    {
        let width: Option<u32> = uri.query().unwrap_or("").split('&').find_map(|param| {
            let (k, v) = param.split_once('=')?;
            if k == "w" { v.parse().ok() } else { None }
        });
        return Ok(crate::img_proxy::serve_image(&state, path, width)
            .await
            .into_response());
    }

    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    match segments.len() {
        1 => {
            // Try landmark type first (e.g. /hrady, /zamky)
            if url_slug_to_type_slug(segments[0]).is_some() {
                let query = uri
                    .query()
                    .map(|q| {
                        q.split('&')
                            .filter_map(|p| {
                                let mut kv = p.splitn(2, '=');
                                Some((kv.next()?.to_string(), kv.next().unwrap_or("").to_string()))
                            })
                            .collect::<std::collections::HashMap<String, String>>()
                    })
                    .unwrap_or_default();
                return landmarks_by_url(
                    State(state.clone()),
                    Path(segments[0].to_string()),
                    axum::extract::Query(query),
                )
                .await;
            }
            // Single query: is this an ORP or region slug?
            let is_orp =
                sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM orp WHERE slug = $1)")
                    .bind(segments[0])
                    .fetch_one(&state.db)
                    .await?;

            if is_orp {
                return Ok(orp::render_orp_by_slug(&state, segments[0])
                    .await
                    .into_response());
            }
            Ok(regions::render_region(&state, segments[0])
                .await
                .into_response())
        }
        2 => {
            // Single query to determine what /{seg0}/{seg1}/ refers to
            #[derive(sqlx::FromRow)]
            struct SlugLookup {
                entity_type: String,
            }
            // Priority: municipality > landmark > pool > legacy region redirect
            let lookup = sqlx::query_as::<_, SlugLookup>(
                "SELECT entity_type FROM ( \
                 SELECT 'municipality' as entity_type, 1 as priority FROM municipalities m \
                 JOIN orp o ON m.orp_id = o.id WHERE o.slug = $1 AND m.slug = $2 \
                 UNION ALL \
                 SELECT 'landmark' as entity_type, 2 as priority FROM landmarks l \
                 JOIN municipalities m ON l.municipality_id = m.id \
                 JOIN orp o ON m.orp_id = o.id WHERE o.slug = $1 AND l.slug = $2 \
                 UNION ALL \
                 SELECT 'pool' as entity_type, 3 as priority FROM pools p \
                 JOIN orp o ON p.orp_id = o.id WHERE o.slug = $1 AND p.slug = $2 \
                 UNION ALL \
                 SELECT 'region_redirect' as entity_type, 4 as priority FROM regions r \
                 WHERE r.slug = $1 \
                 ORDER BY priority LIMIT 1) sub",
            )
            .bind(segments[0])
            .bind(segments[1])
            .fetch_optional(&state.db)
            .await?;

            match lookup.as_ref().map(|l| l.entity_type.as_str()) {
                Some("municipality") => {
                    Ok(
                        municipalities::render_municipality_short(&state, segments[0], segments[1])
                            .await
                            .into_response(),
                    )
                }
                Some("landmark") => {
                    Ok(
                        landmarks::render_landmark_short(&state, segments[0], segments[1])
                            .await
                            .into_response(),
                    )
                }
                Some("pool") => Ok(pools::render_pool_short(&state, segments[0], segments[1])
                    .await
                    .into_response()),
                Some("region_redirect") => {
                    let new_url = format!("/{}/", segments[1]);
                    Ok(axum::response::Redirect::permanent(&new_url).into_response())
                }
                _ => Ok(not_found(&state.image_base_url).into_response()),
            }
        }
        3 => {
            // First check: /{orp}/{municipality}/{entity}/ — landmark or pool in specific municipality
            let orp_id = sqlx::query_scalar::<_, i32>("SELECT id FROM orp WHERE slug = $1")
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
                    let landmark_result = landmarks::render_landmark_in_municipality(
                        &state,
                        segments[0],
                        segments[2],
                        oid,
                    )
                    .await;
                    if landmark_result.0 != StatusCode::NOT_FOUND {
                        return Ok(landmark_result.into_response());
                    }
                    return Ok(pools::render_pool_short(&state, segments[0], segments[2])
                        .await
                        .into_response());
                }
            }

            // Legacy fallback: /{region}/{orp}/{entity}/ → redirect to /{orp}/{entity}/
            let is_region = sqlx::query_scalar::<_, i32>("SELECT id FROM regions WHERE slug = $1")
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
