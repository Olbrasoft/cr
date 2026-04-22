//! TV pořady listing and detail pages at `/tv-porady/`.
//!
//! Separate catalog from scripted series — reality shows, talk shows,
//! cooking, telenovelas, etc. The data lives in `tv_shows` + `tv_episodes`
//! (migration 041). URL shape mirrors `/serialy-online/` but no genre
//! routes and no cast/crew sections for now — TV pořady typically don't
//! have rich TMDB credits.

use askama::Template;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::error::WebResult;
use crate::state::AppState;

const TV_SHOWS_PER_PAGE: i64 = 24;

#[derive(FromRow, Serialize)]
pub struct TvShowRow {
    id: i32,
    title: String,
    slug: String,
    first_air_year: Option<i16>,
    last_air_year: Option<i16>,
    description: Option<String>,
    original_title: Option<String>,
    imdb_rating: Option<f32>,
    csfd_rating: Option<i16>,
    #[allow(dead_code)]
    season_count: Option<i16>,
    #[allow(dead_code)]
    episode_count: Option<i16>,
    #[allow(dead_code)]
    added_at: Option<chrono::DateTime<chrono::Utc>>,
    /// TMDB poster_path (e.g. `/mqlg…uJ.jpg`), backfilled by
    /// `scripts/backfill-tmdb-poster-paths.py --table tv_shows`. When set,
    /// `large_url_ext()` flips the extension to `.jpg`/`.png`, which
    /// `tv_porad_cover_large_dynamic` proxies from TMDB. Otherwise falls
    /// back to the R2-backed `-large.webp` URL.
    tmdb_poster_path: Option<String>,
}

impl TvShowRow {
    /// Extension for the large-cover URL rendered in the detail template.
    /// Derived from `tmdb_poster_path` when the tv show has been backfilled;
    /// otherwise falls back to `webp` so the existing R2-backed route keeps
    /// serving until the backfill completes.
    ///
    /// Only `jpg` and `png` are whitelisted — `tv_porad_detail` dispatches
    /// exactly those two large-cover extensions to the dynamic proxy, and
    /// TMDB's in-practice storage is always JPG. Unknown/unexpected TMDB
    /// suffixes (e.g. a future `jpeg`) get normalized to `jpg` rather than
    /// falling through to the HTML handler with a mismatching URL.
    pub fn large_url_ext(&self) -> &str {
        match self.tmdb_poster_path.as_deref() {
            Some(p) => match p.rsplit_once('.').map(|(_, ext)| ext) {
                Some("png") => "png",
                Some(_) | None => "jpg",
            },
            None => "webp",
        }
    }
}

/// Episode card shown on list page — one latest episode per TV pořad.
#[derive(FromRow, Serialize)]
pub struct TvEpisodeCardRow {
    #[allow(dead_code)]
    pub id: i32,
    pub tv_show_slug: String,
    pub tv_show_title: String,
    pub tv_show_original_title: Option<String>,
    pub tv_show_first_air_year: Option<i16>,
    pub tv_show_imdb_rating: Option<f32>,
    pub tv_show_csfd_rating: Option<i16>,
    pub tv_show_description: Option<String>,
    pub season: i16,
    pub episode: i16,
    pub has_subtitles: Option<bool>,
    pub has_dub: Option<bool>,
    #[allow(dead_code)]
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub episode_slug: Option<String>,
    pub episode_name: Option<String>,
}

#[derive(FromRow, Serialize)]
pub struct TvEpisodeRow {
    pub id: i32,
    pub season: i16,
    pub episode: i16,
    pub title: Option<String>,
    pub sktorrent_video_id: Option<i32>,
    pub sktorrent_cdn: Option<i16>,
    pub sktorrent_qualities: Option<String>,
    pub episode_name: Option<String>,
    pub overview: Option<String>,
    pub air_date: Option<chrono::NaiveDate>,
    pub runtime: Option<i16>,
    pub still_filename: Option<String>,
    pub prehrajto_url: Option<String>,
    pub prehrajto_has_dub: bool,
    pub prehrajto_has_subs: bool,
    pub slug: Option<String>,
}

#[derive(FromRow)]
struct CountRow {
    count: Option<i64>,
}

#[derive(Deserialize)]
pub struct TvShowQuery {
    strana: Option<i64>,
    razeni: Option<String>,
    q: Option<String>,
}

impl TvShowQuery {
    fn page(&self) -> i64 {
        self.strana.unwrap_or(1).max(1)
    }

    fn order_clause(&self) -> &'static str {
        match self.razeni.as_deref() {
            Some("rok") => "s.first_air_year DESC NULLS LAST, s.title",
            Some("imdb") => "s.imdb_rating DESC NULLS LAST, s.title",
            Some("nazev") => "s.title ASC",
            _ => "s.added_at DESC NULLS LAST, s.title",
        }
    }

    fn sort_key(&self) -> &str {
        self.razeni.as_deref().unwrap_or("pridano")
    }
}

#[derive(Template)]
#[template(path = "tv_porady_list.html")]
struct TvPoradyListTemplate {
    img: String,
    episodes: Vec<TvEpisodeCardRow>,
    shows: Vec<TvShowRow>,
    page: i64,
    total_pages: i64,
    total_count: i64,
    #[allow(dead_code)]
    sort_key: String,
    query_string: String,
    search_query: Option<String>,
}

#[derive(Template)]
#[template(path = "tv_porad_detail.html")]
struct TvPoradDetailTemplate {
    img: String,
    show: TvShowRow,
    seasons: Vec<Season>,
}

#[derive(Template)]
#[template(path = "tv_epizoda_detail.html")]
struct TvEpizodaDetailTemplate {
    img: String,
    show: TvShowRow,
    episode: TvEpisodeRow,
    prev_episode: Option<EpisodeNav>,
    next_episode: Option<EpisodeNav>,
}

pub struct EpisodeNav {
    pub season: i16,
    pub episode: i16,
    pub episode_name: Option<String>,
    pub slug: Option<String>,
}

pub struct Season {
    pub number: i16,
    pub episodes: Vec<TvEpisodeRow>,
}

pub async fn tv_porady_list(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<TvShowQuery>,
) -> WebResult<Response> {
    let page = params.page();
    let offset = (page - 1) * TV_SHOWS_PER_PAGE;
    let order = params.order_clause();

    let search_q = params.q.as_ref().and_then(|q| {
        let t = q.trim();
        if t.len() >= 2 {
            Some(format!("%{t}%"))
        } else {
            None
        }
    });

    let (total_count, shows, episodes) = if let Some(ref pattern) = search_q {
        let count_row = sqlx::query_as::<_, CountRow>(
            "SELECT count(*) as count FROM tv_shows \
             WHERE title ILIKE $1 OR original_title ILIKE $1",
        )
        .bind(pattern)
        .fetch_one(&state.db)
        .await?;

        let query = format!(
            "SELECT s.id, s.title, s.slug, s.first_air_year, s.last_air_year, \
             s.description, s.original_title, s.imdb_rating, s.csfd_rating, \
             s.season_count, s.episode_count, s.added_at, \
             s.tmdb_poster_path \
             FROM tv_shows s \
             WHERE s.title ILIKE $1 OR s.original_title ILIKE $1 \
             ORDER BY {order} LIMIT $2 OFFSET $3"
        );
        let rows = sqlx::query_as::<_, TvShowRow>(&query)
            .bind(pattern)
            .bind(TV_SHOWS_PER_PAGE)
            .bind(offset)
            .fetch_all(&state.db)
            .await?;
        (count_row.count.unwrap_or(0), rows, Vec::new())
    } else {
        let count_row = sqlx::query_as::<_, CountRow>(
            "SELECT count(DISTINCT e.tv_show_id) as count FROM tv_episodes e",
        )
        .fetch_one(&state.db)
        .await?;

        let episodes = fetch_latest_episode_cards(&state, TV_SHOWS_PER_PAGE, offset).await?;
        (count_row.count.unwrap_or(0), Vec::new(), episodes)
    };

    let total_pages = (total_count as f64 / TV_SHOWS_PER_PAGE as f64).ceil() as i64;

    let query_string = build_query_string(&params);

    let search_query = params.q.clone().and_then(|q| {
        let t = q.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });

    let tmpl = TvPoradyListTemplate {
        img: state.image_base_url.clone(),
        episodes,
        shows,
        page,
        total_pages,
        total_count,
        sort_key: params.sort_key().to_string(),
        query_string,
        search_query,
    };
    Ok(Html(tmpl.render()?).into_response())
}

async fn fetch_latest_episode_cards(
    state: &AppState,
    limit: i64,
    offset: i64,
) -> WebResult<Vec<TvEpisodeCardRow>> {
    let sql = "WITH per_show AS ( \
        SELECT DISTINCT ON (e.tv_show_id) \
            e.id, e.tv_show_id, e.season, e.episode, e.has_subtitles, e.has_dub, e.created_at \
        FROM tv_episodes e \
        ORDER BY e.tv_show_id, e.created_at DESC \
     ) \
     SELECT ps.id, \
        s.slug AS tv_show_slug, \
        s.title AS tv_show_title, \
        s.original_title AS tv_show_original_title, \
        s.first_air_year AS tv_show_first_air_year, \
        s.imdb_rating AS tv_show_imdb_rating, \
        s.csfd_rating AS tv_show_csfd_rating, \
        s.description AS tv_show_description, \
        ps.season, ps.episode, ps.has_subtitles, ps.has_dub, ps.created_at, \
        (SELECT e2.slug FROM tv_episodes e2 WHERE e2.id = ps.id) AS episode_slug, \
        (SELECT e2.episode_name FROM tv_episodes e2 WHERE e2.id = ps.id) AS episode_name \
     FROM per_show ps \
     JOIN tv_shows s ON s.id = ps.tv_show_id \
     ORDER BY ps.created_at DESC NULLS LAST \
     LIMIT $1 OFFSET $2";

    let rows = sqlx::query_as::<_, TvEpisodeCardRow>(sql)
        .bind(limit)
        .bind(offset)
        .fetch_all(&state.db)
        .await?;
    Ok(rows)
}

#[derive(Serialize)]
struct TvPoradSearchResult {
    slug: String,
    title: String,
    year: Option<i16>,
    imdb_rating: Option<f32>,
    cover: bool,
}

#[derive(FromRow)]
struct TvPoradSearchRow {
    slug: String,
    title: String,
    first_air_year: Option<i16>,
    imdb_rating: Option<f32>,
}

/// GET /api/tv-porady/search?q=...
pub async fn tv_porady_search(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    let q = params.get("q").map(|s| s.trim()).unwrap_or("");
    if q.len() < 2 {
        return Ok(axum::Json(Vec::<TvPoradSearchResult>::new()).into_response());
    }
    let pattern = format!("%{q}%");
    let starts_pattern = format!("{q}%");
    let rows = sqlx::query_as::<_, TvPoradSearchRow>(
        "SELECT slug, title, first_air_year, imdb_rating \
         FROM tv_shows \
         WHERE title ILIKE $1 OR original_title ILIKE $1 \
         ORDER BY \
           CASE WHEN title ILIKE $2 THEN 0 \
                WHEN title ILIKE $1 THEN 1 \
                WHEN original_title ILIKE $2 THEN 2 \
                ELSE 3 END, \
           imdb_rating DESC NULLS LAST \
         LIMIT 10",
    )
    .bind(&pattern)
    .bind(&starts_pattern)
    .fetch_all(&state.db)
    .await?;

    let results: Vec<TvPoradSearchResult> = rows
        .into_iter()
        .map(|r| TvPoradSearchResult {
            slug: r.slug,
            title: r.title,
            year: r.first_air_year,
            imdb_rating: r.imdb_rating,
            cover: true,
        })
        .collect();

    Ok(axum::Json(results).into_response())
}

/// GET /tv-porady/{slug}/ — TV pořad detail with episode list.
pub async fn tv_porad_detail(
    State(state): State<AppState>,
    Path(slug_raw): Path<String>,
) -> WebResult<Response> {
    // Large cover dynamically proxied from TMDB — real extension in URL so
    // the response content type matches what the template rendered. See
    // `tv_porad_cover_large_dynamic` for the fallback chain.
    if slug_raw.ends_with("-large.jpg") || slug_raw.ends_with("-large.png") {
        return tv_porad_cover_large_dynamic(State(state), Path(slug_raw)).await;
    }
    // WebP cover variants routed here too (no genre routes on /tv-porady/)
    if slug_raw.ends_with(".webp") {
        return tv_porad_cover(State(state), Path(slug_raw)).await;
    }

    let show = sqlx::query_as::<_, TvShowRow>(
        "SELECT id, title, slug, first_air_year, last_air_year, description, \
         original_title, imdb_rating, csfd_rating, season_count, episode_count, \
         added_at, tmdb_poster_path FROM tv_shows WHERE slug = $1",
    )
    .bind(&slug_raw)
    .fetch_optional(&state.db)
    .await?;

    let show = match show {
        Some(s) => s,
        None => {
            let old_match = sqlx::query_as::<_, TvShowRow>(
                "SELECT id, title, slug, first_air_year, last_air_year, description, \
                 original_title, imdb_rating, csfd_rating, season_count, episode_count, \
                 added_at, tmdb_poster_path FROM tv_shows WHERE old_slug = $1",
            )
            .bind(&slug_raw)
            .fetch_optional(&state.db)
            .await?;
            match old_match {
                Some(s) => {
                    let new_url = format!("/tv-porady/{}/", s.slug);
                    return Ok(
                        (StatusCode::MOVED_PERMANENTLY, [(header::LOCATION, new_url)])
                            .into_response(),
                    );
                }
                None => return Ok((StatusCode::NOT_FOUND, "TV pořad nenalezen").into_response()),
            }
        }
    };

    let episodes = sqlx::query_as::<_, TvEpisodeRow>(
        "SELECT id, season, episode, title, sktorrent_video_id, sktorrent_cdn, \
         sktorrent_qualities, episode_name, overview, air_date, runtime, still_filename, \
         prehrajto_url, prehrajto_has_dub, prehrajto_has_subs, slug \
         FROM tv_episodes \
         WHERE tv_show_id = $1 \
           AND (sktorrent_video_id IS NOT NULL OR prehrajto_url IS NOT NULL) \
         ORDER BY season, episode, sktorrent_video_id",
    )
    .bind(show.id)
    .fetch_all(&state.db)
    .await?;

    let mut seasons: Vec<Season> = Vec::new();
    let mut current_season: Option<Season> = None;
    let mut seen_in_season: std::collections::HashSet<i16> = std::collections::HashSet::new();

    for ep in episodes {
        let boundary = current_season.as_ref().map(|s| s.number) != Some(ep.season);
        if boundary && let Some(finished) = current_season.take() {
            seasons.push(finished);
            seen_in_season.clear();
        }
        if current_season.is_none() {
            current_season = Some(Season {
                number: ep.season,
                episodes: Vec::new(),
            });
        }
        if seen_in_season.insert(ep.episode)
            && let Some(ref mut s) = current_season
        {
            s.episodes.push(ep);
        }
    }
    if let Some(s) = current_season {
        seasons.push(s);
    }

    let tmpl = TvPoradDetailTemplate {
        img: state.image_base_url.clone(),
        show,
        seasons,
    };
    Ok(Html(tmpl.render()?).into_response())
}

/// GET /tv-porady/{slug}/{ep_slug}/ — episode detail page with player.
pub async fn tv_epizoda_detail(
    State(state): State<AppState>,
    Path((slug, ep_path)): Path<(String, String)>,
) -> WebResult<Response> {
    let show = sqlx::query_as::<_, TvShowRow>(
        "SELECT id, title, slug, first_air_year, last_air_year, description, \
         original_title, imdb_rating, csfd_rating, season_count, episode_count, \
         added_at, tmdb_poster_path FROM tv_shows WHERE slug = $1",
    )
    .bind(&slug)
    .fetch_optional(&state.db)
    .await?;

    let show = match show {
        Some(s) => s,
        None => {
            let old_match = sqlx::query_as::<_, TvShowRow>(
                "SELECT id, title, slug, first_air_year, last_air_year, description, \
                 original_title, imdb_rating, csfd_rating, season_count, episode_count, \
                 added_at, tmdb_poster_path FROM tv_shows WHERE old_slug = $1",
            )
            .bind(&slug)
            .fetch_optional(&state.db)
            .await?;
            match old_match {
                Some(s) => {
                    let new_url = format!("/tv-porady/{}/{ep_path}/", s.slug);
                    return Ok(
                        (StatusCode::MOVED_PERMANENTLY, [(header::LOCATION, new_url)])
                            .into_response(),
                    );
                }
                None => return Ok((StatusCode::NOT_FOUND, "TV pořad nenalezen").into_response()),
            }
        }
    };

    let episode = sqlx::query_as::<_, TvEpisodeRow>(
        "SELECT id, season, episode, title, sktorrent_video_id, sktorrent_cdn, \
         sktorrent_qualities, episode_name, overview, air_date, runtime, still_filename, \
         prehrajto_url, prehrajto_has_dub, prehrajto_has_subs, slug \
         FROM tv_episodes \
         WHERE tv_show_id = $1 AND slug = $2 \
           AND (sktorrent_video_id IS NOT NULL OR prehrajto_url IS NOT NULL) \
         ORDER BY sktorrent_video_id LIMIT 1",
    )
    .bind(show.id)
    .bind(&ep_path)
    .fetch_optional(&state.db)
    .await?;

    let episode = match episode {
        Some(ep) => ep,
        None => {
            if let Some((s_str, e_str)) = ep_path.split_once('x') {
                if let (Ok(season_num), Ok(episode_num)) =
                    (s_str.parse::<i16>(), e_str.parse::<i16>())
                {
                    let found = sqlx::query_as::<_, TvEpisodeRow>(
                        "SELECT id, season, episode, title, sktorrent_video_id, sktorrent_cdn, \
                         sktorrent_qualities, episode_name, overview, air_date, runtime, \
                         still_filename, prehrajto_url, prehrajto_has_dub, prehrajto_has_subs, slug \
                         FROM tv_episodes \
                         WHERE tv_show_id = $1 AND season = $2 AND episode = $3 \
                           AND (sktorrent_video_id IS NOT NULL OR prehrajto_url IS NOT NULL) \
                         ORDER BY sktorrent_video_id LIMIT 1",
                    )
                    .bind(show.id)
                    .bind(season_num)
                    .bind(episode_num)
                    .fetch_optional(&state.db)
                    .await?;

                    if let Some(ep) = found {
                        if let Some(ref ep_slug) = ep.slug {
                            let new_url = format!("/tv-porady/{}/{ep_slug}/", show.slug);
                            return Ok((
                                StatusCode::MOVED_PERMANENTLY,
                                [(header::LOCATION, new_url)],
                            )
                                .into_response());
                        }
                        ep
                    } else {
                        return Ok((StatusCode::NOT_FOUND, "Epizoda nenalezena").into_response());
                    }
                } else {
                    return Ok((StatusCode::NOT_FOUND, "Neplatná URL").into_response());
                }
            } else {
                return Ok((StatusCode::NOT_FOUND, "Epizoda nenalezena").into_response());
            }
        }
    };

    let season_num = episode.season;
    let episode_num = episode.episode;

    let all_episodes = sqlx::query_as::<_, (i16, i16, Option<String>, Option<String>)>(
        "SELECT DISTINCT ON (season, episode) season, episode, episode_name, slug \
         FROM tv_episodes \
         WHERE tv_show_id = $1 \
           AND (sktorrent_video_id IS NOT NULL OR prehrajto_url IS NOT NULL) \
         ORDER BY season, episode",
    )
    .bind(show.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    let current_idx = all_episodes
        .iter()
        .position(|(s, e, _, _)| *s == season_num && *e == episode_num);
    let prev_episode = current_idx
        .and_then(|i| i.checked_sub(1).and_then(|j| all_episodes.get(j)))
        .map(|(s, e, n, sl)| EpisodeNav {
            season: *s,
            episode: *e,
            episode_name: n.clone(),
            slug: sl.clone(),
        });
    let next_episode = current_idx
        .and_then(|i| all_episodes.get(i + 1))
        .map(|(s, e, n, sl)| EpisodeNav {
            season: *s,
            episode: *e,
            episode_name: n.clone(),
            slug: sl.clone(),
        });

    let tmpl = TvEpizodaDetailTemplate {
        img: state.image_base_url.clone(),
        show,
        episode,
        prev_episode,
        next_episode,
    };
    Ok(Html(tmpl.render()?).into_response())
}

/// GET /tv-porady/{slug}.webp — cover (small) with TMDB fallback.
pub async fn tv_porad_cover(
    State(state): State<AppState>,
    Path(slug_webp): Path<String>,
) -> WebResult<Response> {
    use crate::handlers::cover_proxy::{
        fetch_cover, new_r2_key, parse_cover_slug, placeholder_webp,
    };

    if slug_webp.ends_with("-large.webp") {
        return Box::pin(tv_porad_cover_large(State(state), Path(slug_webp))).await;
    }
    let (slug, _is_large) = parse_cover_slug(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        id: i32,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT id FROM tv_shows WHERE slug = $1")
        .bind(&slug)
        .fetch_optional(&state.db)
        .await?;

    let Some(row) = row else {
        return Ok(placeholder_webp());
    };

    let new_key = new_r2_key("tv-shows", row.id, false);
    Ok(fetch_cover(&state, &new_key).await)
}

/// GET /tv-porady/{slug}-large.webp — large (780×1170) cover.
pub async fn tv_porad_cover_large(
    State(state): State<AppState>,
    Path(slug_webp): Path<String>,
) -> WebResult<Response> {
    use crate::handlers::cover_proxy::{
        immutable_webp, new_r2_key, parse_cover_slug, placeholder_webp, try_fetch_r2,
    };

    let (slug, _is_large) = parse_cover_slug(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        id: i32,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT id FROM tv_shows WHERE slug = $1")
        .bind(&slug)
        .fetch_optional(&state.db)
        .await?;

    let Some(row) = row else {
        return Ok(placeholder_webp());
    };

    use crate::handlers::cover_proxy::no_store_webp;
    // Pre-migration large covers were cached under the `series/large/`
    // prefix (tv_shows shared the series_covers_dir). (R2 key,
    // is_small_fallback) — small-variant fallbacks are served with
    // `no-store` so a later-imported large can take over (see
    // films_cover_large).
    let candidates: Vec<(String, bool)> = vec![
        (new_r2_key("tv-shows", row.id, true), false),
        (format!("series/large/{slug}.webp"), false),
        (new_r2_key("tv-shows", row.id, false), true),
    ];
    for (key, is_small_fallback) in &candidates {
        if let Some(bytes) = try_fetch_r2(&state, key).await {
            return Ok(if *is_small_fallback {
                no_store_webp(bytes)
            } else {
                immutable_webp(bytes)
            });
        }
    }
    Ok(placeholder_webp())
}

/// GET /tv-porady/{slug}-large.{jpg,png} — proxy TMDB poster on demand.
///
/// Mirrors `films_cover_large_dynamic` / `series_cover_large_dynamic`:
/// detail-page thumbnails get few hits, so we skip R2 storage and stream
/// the TMDB image through. Cloudflare caches the response for a year.
/// On any failure we serve a placeholder in the SAME format the URL
/// advertises so browsers and OG scrapers decode without a MIME mismatch.
pub async fn tv_porad_cover_large_dynamic(
    State(state): State<AppState>,
    Path(slug_ext): Path<String>,
) -> WebResult<Response> {
    use crate::handlers::cover_proxy::placeholder_for_ext;

    let (slug, ext) = if let Some(s) = slug_ext.strip_suffix("-large.jpg") {
        (s, "jpg")
    } else if let Some(s) = slug_ext.strip_suffix("-large.png") {
        (s, "png")
    } else {
        (slug_ext.as_str(), "jpg")
    };

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        tmdb_poster_path: Option<String>,
    }

    let row =
        sqlx::query_as::<_, CoverRow>("SELECT tmdb_poster_path FROM tv_shows WHERE slug = $1")
            .bind(slug)
            .fetch_optional(&state.db)
            .await?;

    let Some(path) = row.and_then(|r| r.tmdb_poster_path) else {
        return Ok(placeholder_for_ext(ext));
    };

    let url = format!("https://image.tmdb.org/t/p/w780{path}");
    let Ok(resp) = state
        .http_client
        .get(&url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    else {
        return Ok(placeholder_for_ext(ext));
    };
    if !resp.status().is_success() {
        return Ok(placeholder_for_ext(ext));
    }
    let ct = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "image/jpeg".to_string());
    let Ok(bytes) = resp.bytes().await else {
        return Ok(placeholder_for_ext(ext));
    };
    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_TYPE, ct),
            (
                axum::http::header::CACHE_CONTROL,
                "public, max-age=31536000, immutable".to_string(),
            ),
        ],
        bytes.to_vec(),
    )
        .into_response())
}

fn build_query_string(params: &TvShowQuery) -> String {
    let mut parts: Vec<(&str, String)> = Vec::new();
    if params.razeni.is_some() {
        parts.push(("razeni", params.sort_key().to_string()));
    }
    if let Some(ref q) = params.q {
        let t = q.trim();
        if !t.is_empty() {
            parts.push(("q", t.to_string()));
        }
    }
    super::build_pagination_qs(&parts)
}
