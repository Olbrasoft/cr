use cr_domain::id::RegionId;
use cr_domain::repository::{OrpRepository, RegionRepository};

use super::*;

impl From<cr_domain::repository::RegionRecord> for RegionRow {
    fn from(r: cr_domain::repository::RegionRecord) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            region_code: r.region_code,
            latitude: r.latitude,
            longitude: r.longitude,
            coat_of_arms_ext: r.coat_of_arms_ext,
            flag_ext: r.flag_ext,
            description: r.description,
            hero_photo_url: None,
        }
    }
}

impl From<cr_domain::repository::OrpRecord> for OrpRow {
    fn from(r: cr_domain::repository::OrpRecord) -> Self {
        Self {
            id: r.id,
            name: r.name,
            slug: r.slug,
            orp_code: r.orp_code,
            latitude: r.latitude,
            longitude: r.longitude,
        }
    }
}

pub(crate) async fn render_region(
    state: &AppState,
    region_slug: &str,
) -> (StatusCode, Html<String>) {
    let region = state
        .region_repo
        .find_by_slug(region_slug)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("render_region region query failed: {e}");
            None
        });

    let Some(region) = region else {
        return not_found(&state.image_base_url);
    };

    let region_id = region.id;
    let region_slug_owned = region.slug.clone();
    let mut region_row: RegionRow = region.into();

    // Resolve hero photo SEO URL — priority: landmark → municipality → direct
    // Landmark hero: /{orp}/{muni}/{landmark}/{photo-slug}.webp
    #[derive(sqlx::FromRow)]
    struct LandmarkHero {
        orp_slug: String,
        muni_slug: String,
        landmark_slug: String,
        photo_slug: String,
    }
    let landmark_hero = sqlx::query_as::<_, LandmarkHero>(
        "SELECT o.slug as orp_slug, m.slug as muni_slug, l.slug as landmark_slug, lp.slug as photo_slug \
         FROM landmark_photos lp \
         JOIN landmarks l ON l.npu_catalog_id = lp.npu_catalog_id \
         JOIN municipalities m ON l.municipality_id = m.id \
         JOIN orp o ON m.orp_id = o.id \
         JOIN regions r ON r.hero_landmark_id = l.id \
         WHERE r.id = $1 AND lp.photo_index = r.hero_photo_index",
    )
    .bind(region_id)
    .fetch_optional(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::error!("render_region hero landmark query failed: {e}");
        None
    });

    if let Some(h) = landmark_hero {
        let url = if h.orp_slug == h.muni_slug {
            format!("/{}/{}/{}.webp", h.orp_slug, h.landmark_slug, h.photo_slug)
        } else {
            format!(
                "/{}/{}/{}/{}.webp",
                h.orp_slug, h.muni_slug, h.landmark_slug, h.photo_slug
            )
        };
        region_row.hero_photo_url = Some(url);
    } else {
        // Municipality hero: /{orp}/{photo-slug}.webp or /{orp}/{muni}/{photo-slug}.webp
        #[derive(sqlx::FromRow)]
        struct MuniHero {
            orp_slug: String,
            muni_slug: String,
            photo_slug: String,
        }
        let muni_hero = sqlx::query_as::<_, MuniHero>(
            "SELECT o.slug as orp_slug, m.slug as muni_slug, mp.slug as photo_slug \
             FROM municipality_photos mp \
             JOIN municipalities m ON m.municipality_code = mp.municipality_code \
             JOIN orp o ON m.orp_id = o.id \
             JOIN regions r ON r.hero_municipality_code = mp.municipality_code \
             WHERE r.id = $1 AND mp.photo_index = r.hero_municipality_photo_index",
        )
        .bind(region_id)
        .fetch_optional(&state.db)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("render_region hero municipality query failed: {e}");
            None
        });

        if let Some(h) = muni_hero {
            let url = if h.orp_slug == h.muni_slug {
                format!("/{}/{}.webp", h.orp_slug, h.photo_slug)
            } else {
                format!("/{}/{}/{}.webp", h.orp_slug, h.muni_slug, h.photo_slug)
            };
            region_row.hero_photo_url = Some(url);
        } else {
            // Direct region photo: /{region-slug}.webp
            let has_direct = sqlx::query_scalar::<_, bool>(
                "SELECT hero_photo_r2_key IS NOT NULL FROM regions WHERE id = $1",
            )
            .bind(region_id)
            .fetch_one(&state.db)
            .await
            .unwrap_or(false);
            if has_direct {
                region_row.hero_photo_url = Some(format!("/{}.webp", region_slug_owned));
            }
        }
    }

    let orps: Vec<OrpRow> = state
        .orp_repo
        .find_by_region(RegionId::from(region_id))
        .await
        .unwrap_or_else(|e| {
            tracing::error!("render_region orps query failed: {e}");
            Vec::new()
        })
        .into_iter()
        .map(OrpRow::from)
        .collect();

    // Special case: region with single ORP (e.g. Praha) — render ORP page directly
    if orps.len() == 1 {
        return orp::render_orp(state, region_slug, &orps[0].slug).await;
    }

    let tmpl = RegionTemplate {
        img: state.image_base_url.clone(),
        region: region_row,
        orps,
    };
    match tmpl.render() {
        Ok(html) => (StatusCode::OK, Html(html)),
        Err(e) => {
            tracing::error!("template render failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, Html(String::new()))
        }
    }
}
