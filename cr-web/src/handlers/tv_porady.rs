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
    cover_filename: Option<String>,
    #[allow(dead_code)]
    added_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Episode card shown on list page — one latest episode per TV pořad.
#[derive(FromRow, Serialize)]
pub struct TvEpisodeCardRow {
    #[allow(dead_code)]
    pub id: i32,
    pub tv_show_slug: String,
    pub tv_show_title: String,
    pub tv_show_original_title: Option<String>,
    pub tv_show_cover_filename: Option<String>,
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
             s.season_count, s.episode_count, s.cover_filename, s.added_at \
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
        s.cover_filename AS tv_show_cover_filename, \
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

/// GET /tv-porady/{slug}/ — TV pořad detail with episode list.
pub async fn tv_porad_detail(
    State(state): State<AppState>,
    Path(slug_raw): Path<String>,
) -> WebResult<Response> {
    // WebP cover variants routed here too (no genre routes on /tv-porady/)
    if slug_raw.ends_with(".webp") {
        return tv_porad_cover(State(state), Path(slug_raw)).await;
    }

    let show = sqlx::query_as::<_, TvShowRow>(
        "SELECT id, title, slug, first_air_year, last_air_year, description, \
         original_title, imdb_rating, csfd_rating, season_count, episode_count, \
         cover_filename, added_at FROM tv_shows WHERE slug = $1",
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
                 cover_filename, added_at FROM tv_shows WHERE old_slug = $1",
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
         cover_filename, added_at FROM tv_shows WHERE slug = $1",
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
                 cover_filename, added_at FROM tv_shows WHERE old_slug = $1",
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
    if slug_webp.ends_with("-large.webp") {
        return tv_porad_cover_large(State(state), Path(slug_webp)).await;
    }
    let slug = slug_webp.strip_suffix(".webp").unwrap_or(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        cover_filename: Option<String>,
        tmdb_id: Option<i32>,
    }

    let row = sqlx::query_as::<_, CoverRow>(
        "SELECT cover_filename, tmdb_id FROM tv_shows WHERE slug = $1",
    )
    .bind(slug)
    .fetch_optional(&state.db)
    .await?;

    let (cover_filename, tmdb_id) = match row {
        Some(r) => (r.cover_filename, r.tmdb_id),
        None => (None, None),
    };
    // Reuse series_covers_dir for now (migration reuses files written for
    // these same slugs before they were moved).
    let covers_dir = state.config.series_covers_dir.clone();

    if let Some(ref filename) = cover_filename {
        let path = std::path::Path::new(&covers_dir).join(format!("{filename}.webp"));
        if path.exists()
            && let Ok(bytes) = tokio::fs::read(&path).await
        {
            return Ok((
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "image/webp"),
                    (header::CACHE_CONTROL, "public, max-age=31536000"),
                ],
                bytes,
            )
                .into_response());
        }
    }

    if let Some(tid) = tmdb_id {
        let tmdb_key = std::env::var("TMDB_API_KEY").unwrap_or_default();
        if !tmdb_key.is_empty() {
            let detail_url =
                format!("https://api.themoviedb.org/3/tv/{tid}?api_key={tmdb_key}&language=cs-CZ");
            if let Ok(resp) = state
                .http_client
                .get(&detail_url)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await
                && let Ok(data) = resp.json::<serde_json::Value>().await
                && let Some(poster_path) = data.get("poster_path").and_then(|v| v.as_str())
            {
                let img_url = format!("https://image.tmdb.org/t/p/w200{poster_path}");
                if let Ok(img_resp) = state
                    .http_client
                    .get(&img_url)
                    .timeout(std::time::Duration::from_secs(15))
                    .send()
                    .await
                    && img_resp.status().is_success()
                    && let Ok(raw_bytes) = img_resp.bytes().await
                {
                    // TMDB returns JPEG; re-encode to WebP off the async
                    // runtime (CPU-bound) and bail out if transcoding fails
                    // so we don't cache raw JPEG under a .webp filename.
                    let raw = raw_bytes.to_vec();
                    let output_bytes = tokio::task::spawn_blocking(move || -> Option<Vec<u8>> {
                        let img = image::load_from_memory(&raw).ok()?;
                        let mut buf = Vec::new();
                        let mut cursor = std::io::Cursor::new(&mut buf);
                        img.write_to(&mut cursor, image::ImageFormat::WebP).ok()?;
                        Some(buf)
                    })
                    .await
                    .ok()
                    .flatten();

                    if let Some(output_bytes) = output_bytes {
                        let cache_path =
                            std::path::Path::new(&covers_dir).join(format!("{slug}.webp"));
                        let _ = tokio::fs::create_dir_all(&covers_dir).await;
                        let _ = tokio::fs::write(&cache_path, &output_bytes).await;

                        return Ok((
                            StatusCode::OK,
                            [
                                (header::CONTENT_TYPE, "image/webp"),
                                (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
                            ],
                            output_bytes,
                        )
                            .into_response());
                    }
                }
            }
        }
    }

    static PLACEHOLDER: &[u8] = &[
        0x52, 0x49, 0x46, 0x46, 0x1a, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38,
        0x4c, 0x0d, 0x00, 0x00, 0x00, 0x2f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ];
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/webp"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        PLACEHOLDER.to_vec(),
    )
        .into_response())
}

/// GET /tv-porady/{slug}-large.webp — w780 poster from TMDB, cached.
pub async fn tv_porad_cover_large(
    State(state): State<AppState>,
    Path(slug_webp): Path<String>,
) -> WebResult<Response> {
    let slug = slug_webp.strip_suffix("-large.webp").unwrap_or(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        tmdb_id: Option<i32>,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT tmdb_id FROM tv_shows WHERE slug = $1")
        .bind(slug)
        .fetch_optional(&state.db)
        .await?;

    let tmdb_id = row.and_then(|r| r.tmdb_id);
    let covers_dir = state.config.series_covers_dir.clone();

    let cache_dir = std::path::Path::new(&covers_dir).join("large");
    let cache_path = cache_dir.join(format!("{slug}.webp"));

    if cache_path.exists()
        && let Ok(bytes) = tokio::fs::read(&cache_path).await
    {
        return Ok((
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "image/webp"),
                (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
            ],
            bytes,
        )
            .into_response());
    }

    if let Some(tid) = tmdb_id {
        let tmdb_key = std::env::var("TMDB_API_KEY").unwrap_or_default();
        if !tmdb_key.is_empty() {
            let detail_url =
                format!("https://api.themoviedb.org/3/tv/{tid}?api_key={tmdb_key}&language=cs-CZ");

            if let Ok(resp) = state
                .http_client
                .get(&detail_url)
                .timeout(std::time::Duration::from_secs(10))
                .send()
                .await
                && let Ok(data) = resp.json::<serde_json::Value>().await
                && let Some(poster_path) = data.get("poster_path").and_then(|v| v.as_str())
            {
                let poster_url = format!("https://image.tmdb.org/t/p/w780{poster_path}");
                if let Ok(img_resp) = state
                    .http_client
                    .get(&poster_url)
                    .timeout(std::time::Duration::from_secs(15))
                    .send()
                    .await
                    && img_resp.status().is_success()
                    && let Ok(bytes) = img_resp.bytes().await
                {
                    let raw = bytes.to_vec();
                    let output_bytes = tokio::task::spawn_blocking(move || -> Option<Vec<u8>> {
                        let img = image::load_from_memory(&raw).ok()?;
                        let mut buf = Vec::new();
                        let mut cursor = std::io::Cursor::new(&mut buf);
                        img.write_to(&mut cursor, image::ImageFormat::WebP).ok()?;
                        Some(buf)
                    })
                    .await
                    .ok()
                    .flatten();

                    if let Some(output_bytes) = output_bytes {
                        let _ = tokio::fs::create_dir_all(&cache_dir).await;
                        let _ = tokio::fs::write(&cache_path, &output_bytes).await;

                        return Ok((
                            StatusCode::OK,
                            [
                                (header::CONTENT_TYPE, "image/webp"),
                                (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
                            ],
                            output_bytes,
                        )
                            .into_response());
                    }
                }
            }
        }
    }

    static PLACEHOLDER: &[u8] = &[
        0x52, 0x49, 0x46, 0x46, 0x1a, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38,
        0x4c, 0x0d, 0x00, 0x00, 0x00, 0x2f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ];
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/webp"),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        PLACEHOLDER.to_vec(),
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
