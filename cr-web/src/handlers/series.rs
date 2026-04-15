//! TV series listing and detail pages at `/serialy-online/`.
//! Similar structure to films but with seasons + episodes.

use askama::Template;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::error::WebResult;
use crate::state::AppState;

const SERIES_PER_PAGE: i64 = 24;

#[derive(FromRow, Serialize)]
pub struct SeriesRow {
    id: i32,
    title: String,
    slug: String,
    first_air_year: Option<i16>,
    #[allow(dead_code)]
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

/// Episode card shown on list pages — one latest episode per series,
/// sorted by added_at DESC. Layout mirrors bombuj.si: series cover + title
/// + "Epizoda S×E" badge + CC (subtitles) badge.
#[derive(FromRow, Serialize)]
pub struct EpisodeCardRow {
    #[allow(dead_code)]
    pub id: i32,
    pub series_slug: String,
    pub series_title: String,
    pub series_original_title: Option<String>,
    pub series_cover_filename: Option<String>,
    pub series_first_air_year: Option<i16>,
    pub series_imdb_rating: Option<f32>,
    pub series_csfd_rating: Option<i16>,
    pub series_description: Option<String>,
    pub season: i16,
    pub episode: i16,
    pub has_subtitles: Option<bool>,
    pub has_dub: Option<bool>,
    #[allow(dead_code)]
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(FromRow, Serialize)]
pub struct EpisodeRow {
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
}

#[derive(FromRow)]
struct GenreRow {
    id: i32,
    slug: String,
    name_cs: String,
}

#[derive(FromRow)]
struct CountRow {
    count: Option<i64>,
}

#[derive(Deserialize)]
pub struct SeriesQuery {
    strana: Option<i64>,
    razeni: Option<String>,
    q: Option<String>,
}

impl SeriesQuery {
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
#[template(path = "series_list.html")]
struct SeriesListTemplate {
    img: String,
    /// Episode cards — one latest per series.
    episodes: Vec<EpisodeCardRow>,
    /// Full series results — shown only when user searches by title.
    series: Vec<SeriesRow>,
    genres: Vec<GenreRow>,
    page: i64,
    total_pages: i64,
    total_count: i64,
    current_genre: Option<GenreRow>,
    #[allow(dead_code)]
    sort_key: String,
    query_string: String,
    search_query: Option<String>,
}

#[derive(Template)]
#[template(path = "series_detail.html")]
struct SeriesDetailTemplate {
    img: String,
    series: SeriesRow,
    genres: Vec<GenreRow>,
    seasons: Vec<Season>,
    actors: Vec<PersonRow>,
    creators: Vec<PersonRow>,
}

#[derive(Template)]
#[template(path = "episode_detail.html")]
struct EpisodeDetailTemplate {
    img: String,
    series: SeriesRow,
    episode: EpisodeRow,
    prev_episode: Option<EpisodeNav>,
    next_episode: Option<EpisodeNav>,
    directors: Vec<PersonRow>,
    writers: Vec<PersonRow>,
}

pub struct EpisodeNav {
    pub season: i16,
    pub episode: i16,
    pub episode_name: Option<String>,
}

#[derive(FromRow, Clone)]
pub struct PersonRow {
    #[allow(dead_code)]
    pub id: i32,
    pub name: String,
    pub profile_filename: Option<String>,
    pub character_name: Option<String>,
}

pub struct Season {
    pub number: i16,
    pub episodes: Vec<EpisodeRow>,
}

pub async fn series_list(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<SeriesQuery>,
) -> WebResult<Response> {
    let page = params.page();
    let offset = (page - 1) * SERIES_PER_PAGE;
    let order = params.order_clause();

    let search_q = params.q.as_ref().and_then(|q| {
        let t = q.trim();
        if t.len() >= 2 {
            Some(format!("%{t}%"))
        } else {
            None
        }
    });

    // Search mode: show series results (by title). No search: show latest-
    // episode-per-series grid (bombuj.si style).
    let (total_count, series, episodes) = if let Some(ref pattern) = search_q {
        let count_row = sqlx::query_as::<_, CountRow>(
            "SELECT count(*) as count FROM series WHERE title ILIKE $1 OR original_title ILIKE $1",
        )
        .bind(pattern)
        .fetch_one(&state.db)
        .await?;

        let query = format!(
            "SELECT s.id, s.title, s.slug, s.first_air_year, s.last_air_year, \
             s.description, s.original_title, s.imdb_rating, s.csfd_rating, \
             s.season_count, s.episode_count, s.cover_filename, s.added_at \
             FROM series s \
             WHERE s.title ILIKE $1 OR s.original_title ILIKE $1 \
             ORDER BY {order} LIMIT $2 OFFSET $3"
        );
        let rows = sqlx::query_as::<_, SeriesRow>(&query)
            .bind(pattern)
            .bind(SERIES_PER_PAGE)
            .bind(offset)
            .fetch_all(&state.db)
            .await?;
        (count_row.count.unwrap_or(0), rows, Vec::new())
    } else {
        let count_row = sqlx::query_as::<_, CountRow>(
            "SELECT count(DISTINCT e.series_id) as count FROM episodes e",
        )
        .fetch_one(&state.db)
        .await?;

        let episodes = fetch_latest_episode_cards(&state, None, SERIES_PER_PAGE, offset).await?;
        (count_row.count.unwrap_or(0), Vec::new(), episodes)
    };

    let total_pages = (total_count as f64 / SERIES_PER_PAGE as f64).ceil() as i64;

    let genres = sqlx::query_as::<_, GenreRow>(
        "SELECT g.id, g.slug, g.name_cs FROM genres g \
         JOIN series_genres sg ON g.id = sg.genre_id \
         GROUP BY g.id, g.slug, g.name_cs ORDER BY g.name_cs",
    )
    .fetch_all(&state.db)
    .await?;

    let mut qs_parts = Vec::new();
    if params.razeni.is_some() {
        qs_parts.push(format!("razeni={}", params.sort_key()));
    }
    if let Some(ref q) = params.q {
        let t = q.trim();
        if !t.is_empty() {
            qs_parts.push(format!("q={}", urlencoding::encode(t)));
        }
    }
    let query_string = if qs_parts.is_empty() {
        String::new()
    } else {
        format!("&{}", qs_parts.join("&"))
    };

    let search_query = params.q.clone().and_then(|q| {
        let t = q.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });

    let tmpl = SeriesListTemplate {
        img: state.image_base_url.clone(),
        episodes,
        series,
        genres,
        page,
        total_pages,
        total_count,
        current_genre: None,
        sort_key: params.sort_key().to_string(),
        query_string,
        search_query,
    };
    Ok(Html(tmpl.render()?).into_response())
}

/// Latest-episode-per-series query. If `genre_id` is provided, only series
/// belonging to that genre are included. Result is already sorted so the
/// caller needn't reorder.
async fn fetch_latest_episode_cards(
    state: &AppState,
    genre_id: Option<i32>,
    limit: i64,
    offset: i64,
) -> WebResult<Vec<EpisodeCardRow>> {
    let base = "WITH per_series AS ( \
        SELECT DISTINCT ON (e.series_id) \
            e.id, e.series_id, e.season, e.episode, e.has_subtitles, e.has_dub, e.created_at \
        FROM episodes e \
        {GENRE_JOIN} \
        {GENRE_WHERE} \
        ORDER BY e.series_id, e.created_at DESC \
     ) \
     SELECT ps.id, \
        s.slug AS series_slug, \
        s.title AS series_title, \
        s.original_title AS series_original_title, \
        s.cover_filename AS series_cover_filename, \
        s.first_air_year AS series_first_air_year, \
        s.imdb_rating AS series_imdb_rating, \
        s.csfd_rating AS series_csfd_rating, \
        s.description AS series_description, \
        ps.season, ps.episode, ps.has_subtitles, ps.has_dub, ps.created_at \
     FROM per_series ps \
     JOIN series s ON s.id = ps.series_id \
     ORDER BY ps.created_at DESC NULLS LAST \
     LIMIT $1 OFFSET $2";

    let (genre_join, genre_where) = if genre_id.is_some() {
        (
            "JOIN series_genres sg ON sg.series_id = e.series_id",
            "WHERE sg.genre_id = $3",
        )
    } else {
        ("", "")
    };
    let sql = base
        .replace("{GENRE_JOIN}", genre_join)
        .replace("{GENRE_WHERE}", genre_where);

    let rows = if let Some(gid) = genre_id {
        sqlx::query_as::<_, EpisodeCardRow>(&sql)
            .bind(limit)
            .bind(offset)
            .bind(gid)
            .fetch_all(&state.db)
            .await?
    } else {
        sqlx::query_as::<_, EpisodeCardRow>(&sql)
            .bind(limit)
            .bind(offset)
            .fetch_all(&state.db)
            .await?
    };
    Ok(rows)
}

/// Resolve /serialy-online/{slug}/ — genre or series detail
pub async fn series_resolve(
    State(state): State<AppState>,
    Path(slug_raw): Path<String>,
    axum::extract::Query(params): axum::extract::Query<SeriesQuery>,
) -> WebResult<Response> {
    let state_clone = state.clone();
    // WebP cover
    if slug_raw.ends_with(".webp") {
        return series_cover(State(state), Path(slug_raw)).await;
    }

    // Genre page?
    let genre =
        sqlx::query_as::<_, GenreRow>("SELECT id, slug, name_cs FROM genres WHERE slug = $1")
            .bind(&slug_raw)
            .fetch_optional(&state.db)
            .await?;

    if let Some(genre) = genre {
        return series_by_genre(state, genre, params).await;
    }

    // Series detail
    let series = sqlx::query_as::<_, SeriesRow>(
        "SELECT id, title, slug, first_air_year, last_air_year, description, \
         original_title, imdb_rating, csfd_rating, season_count, episode_count, \
         cover_filename, added_at \
         FROM series WHERE slug = $1",
    )
    .bind(&slug_raw)
    .fetch_optional(&state.db)
    .await?;

    let Some(series) = series else {
        return Ok((StatusCode::NOT_FOUND, "Seriál nenalezen").into_response());
    };

    let genres = sqlx::query_as::<_, GenreRow>(
        "SELECT g.id, g.slug, g.name_cs FROM genres g \
         JOIN series_genres sg ON g.id = sg.genre_id \
         WHERE sg.series_id = $1 ORDER BY g.name_cs",
    )
    .bind(series.id)
    .fetch_all(&state.db)
    .await?;

    // Only list episodes that have a playable source — either SK Torrent or a
    // cached Přehraj.to URL. TMDB stubs without any source stay in the DB
    // (useful when enrichment picks them up later) but we don't show them.
    let episodes = sqlx::query_as::<_, EpisodeRow>(
        "SELECT id, season, episode, title, sktorrent_video_id, sktorrent_cdn, sktorrent_qualities, \
         episode_name, overview, air_date, runtime, still_filename, \
         prehrajto_url, prehrajto_has_dub, prehrajto_has_subs \
         FROM episodes \
         WHERE series_id = $1 \
           AND (sktorrent_video_id IS NOT NULL OR prehrajto_url IS NOT NULL) \
         ORDER BY season, episode, sktorrent_video_id",
    )
    .bind(series.id)
    .fetch_all(&state.db)
    .await?;

    // Group by season, dedupe episode numbers (prefer first quality version per season/episode)
    let mut seasons: Vec<Season> = Vec::new();
    let mut current_season: Option<Season> = None;
    let mut seen_in_season: std::collections::HashSet<i16> = std::collections::HashSet::new();

    for ep in episodes {
        // Close out the previous season block when we cross a boundary.
        // Copy the number before take() to avoid borrow overlap.
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

    // Top cast + creators
    let actors = sqlx::query_as::<_, PersonRow>(
        "SELECT p.id, p.name, p.profile_filename, sa.character_name \
         FROM people p JOIN series_actors sa ON sa.person_id = p.id \
         WHERE sa.series_id = $1 ORDER BY sa.order_index LIMIT 10",
    )
    .bind(series.id)
    .fetch_all(&state_clone.db)
    .await
    .unwrap_or_default();

    let creators = sqlx::query_as::<_, PersonRow>(
        "SELECT p.id, p.name, p.profile_filename, NULL::varchar as character_name \
         FROM people p JOIN series_directors sd ON sd.person_id = p.id \
         WHERE sd.series_id = $1 ORDER BY p.name",
    )
    .bind(series.id)
    .fetch_all(&state_clone.db)
    .await
    .unwrap_or_default();

    let tmpl = SeriesDetailTemplate {
        img: state_clone.image_base_url.clone(),
        series,
        genres,
        seasons,
        actors,
        creators,
    };
    Ok(Html(tmpl.render()?).into_response())
}

/// GET /serialy-online/{slug}/{NxM}/ — episode detail page with player
pub async fn episode_detail(
    State(state): State<AppState>,
    Path((slug, ep_path)): Path<(String, String)>,
) -> WebResult<Response> {
    // Parse "1x3" → (1, 3)
    let Some((s_str, e_str)) = ep_path.split_once('x') else {
        return Ok((StatusCode::NOT_FOUND, "Neplatná URL").into_response());
    };
    let Ok(season_num) = s_str.parse::<i16>() else {
        return Ok((StatusCode::NOT_FOUND, "Neplatná sezóna").into_response());
    };
    let Ok(episode_num) = e_str.parse::<i16>() else {
        return Ok((StatusCode::NOT_FOUND, "Neplatná epizoda").into_response());
    };

    let series = sqlx::query_as::<_, SeriesRow>(
        "SELECT id, title, slug, first_air_year, last_air_year, description, \
         original_title, imdb_rating, csfd_rating, season_count, episode_count, \
         cover_filename, added_at FROM series WHERE slug = $1",
    )
    .bind(&slug)
    .fetch_optional(&state.db)
    .await?;
    let Some(series) = series else {
        return Ok((StatusCode::NOT_FOUND, "Seriál nenalezen").into_response());
    };

    // Episode must have a playable source (SK Torrent or cached Přehraj.to URL).
    // TMDB-stub episodes without any source 404 — they exist only for future
    // enrichment and should never be reachable from the listing or via URL.
    let episode = sqlx::query_as::<_, EpisodeRow>(
        "SELECT id, season, episode, title, sktorrent_video_id, sktorrent_cdn, sktorrent_qualities, \
         episode_name, overview, air_date, runtime, still_filename, \
         prehrajto_url, prehrajto_has_dub, prehrajto_has_subs \
         FROM episodes \
         WHERE series_id = $1 AND season = $2 AND episode = $3 \
           AND (sktorrent_video_id IS NOT NULL OR prehrajto_url IS NOT NULL) \
         ORDER BY sktorrent_video_id LIMIT 1",
    )
    .bind(series.id)
    .bind(season_num)
    .bind(episode_num)
    .fetch_optional(&state.db)
    .await?;
    let Some(episode) = episode else {
        return Ok((StatusCode::NOT_FOUND, "Epizoda nenalezena").into_response());
    };

    // Navigation: previous and next episode — same source-available filter
    let all_episodes = sqlx::query_as::<_, (i16, i16, Option<String>)>(
        "SELECT DISTINCT ON (season, episode) season, episode, episode_name \
         FROM episodes \
         WHERE series_id = $1 \
           AND (sktorrent_video_id IS NOT NULL OR prehrajto_url IS NOT NULL) \
         ORDER BY season, episode",
    )
    .bind(series.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();
    let current_idx = all_episodes
        .iter()
        .position(|(s, e, _)| *s == season_num && *e == episode_num);
    let prev_episode = current_idx
        .and_then(|i| i.checked_sub(1).and_then(|j| all_episodes.get(j)))
        .map(|(s, e, n)| EpisodeNav {
            season: *s,
            episode: *e,
            episode_name: n.clone(),
        });
    let next_episode = current_idx
        .and_then(|i| all_episodes.get(i + 1))
        .map(|(s, e, n)| EpisodeNav {
            season: *s,
            episode: *e,
            episode_name: n.clone(),
        });

    let directors = sqlx::query_as::<_, PersonRow>(
        "SELECT p.id, p.name, p.profile_filename, NULL::varchar as character_name \
         FROM people p JOIN episode_directors ed ON ed.person_id = p.id \
         WHERE ed.episode_id = $1 ORDER BY p.name",
    )
    .bind(episode.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let writers = sqlx::query_as::<_, PersonRow>(
        "SELECT p.id, p.name, p.profile_filename, NULL::varchar as character_name \
         FROM people p JOIN episode_writers ew ON ew.person_id = p.id \
         WHERE ew.episode_id = $1 ORDER BY p.name",
    )
    .bind(episode.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let tmpl = EpisodeDetailTemplate {
        img: state.image_base_url.clone(),
        series,
        episode,
        prev_episode,
        next_episode,
        directors,
        writers,
    };
    Ok(Html(tmpl.render()?).into_response())
}

async fn series_by_genre(
    state: AppState,
    genre: GenreRow,
    params: SeriesQuery,
) -> WebResult<Response> {
    let page = params.page();
    let offset = (page - 1) * SERIES_PER_PAGE;

    let count_row = sqlx::query_as::<_, CountRow>(
        "SELECT count(DISTINCT e.series_id) as count FROM episodes e \
         JOIN series_genres sg ON sg.series_id = e.series_id WHERE sg.genre_id = $1",
    )
    .bind(genre.id)
    .fetch_one(&state.db)
    .await?;
    let total_count = count_row.count.unwrap_or(0);
    let total_pages = (total_count as f64 / SERIES_PER_PAGE as f64).ceil() as i64;

    let episodes =
        fetch_latest_episode_cards(&state, Some(genre.id), SERIES_PER_PAGE, offset).await?;

    let all_genres = sqlx::query_as::<_, GenreRow>(
        "SELECT g.id, g.slug, g.name_cs FROM genres g \
         JOIN series_genres sg ON g.id = sg.genre_id \
         GROUP BY g.id, g.slug, g.name_cs ORDER BY g.name_cs",
    )
    .fetch_all(&state.db)
    .await?;

    let tmpl = SeriesListTemplate {
        img: state.image_base_url.clone(),
        episodes,
        series: Vec::new(),
        genres: all_genres,
        page,
        total_pages,
        total_count,
        current_genre: Some(genre),
        sort_key: params.sort_key().to_string(),
        query_string: String::new(),
        search_query: None,
    };
    Ok(Html(tmpl.render()?).into_response())
}

#[derive(Serialize)]
struct SeriesSearchResult {
    slug: String,
    title: String,
    year: Option<i16>,
    imdb_rating: Option<f32>,
    cover: bool,
}

#[derive(FromRow)]
struct SeriesSearchRow {
    slug: String,
    title: String,
    first_air_year: Option<i16>,
    imdb_rating: Option<f32>,
    cover_filename: Option<String>,
}

/// GET /api/series/search?q=...
pub async fn series_search(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    let q = params.get("q").map(|s| s.trim()).unwrap_or("");
    if q.len() < 2 {
        return Ok(axum::Json(Vec::<SeriesSearchResult>::new()).into_response());
    }
    let pattern = format!("%{q}%");
    let starts_pattern = format!("{q}%");
    let rows = sqlx::query_as::<_, SeriesSearchRow>(
        "SELECT slug, title, first_air_year, imdb_rating, cover_filename \
         FROM series \
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

    let results: Vec<SeriesSearchResult> = rows
        .into_iter()
        .map(|r| SeriesSearchResult {
            slug: r.slug,
            title: r.title,
            year: r.first_air_year,
            imdb_rating: r.imdb_rating,
            cover: r.cover_filename.is_some(),
        })
        .collect();

    Ok(axum::Json(results).into_response())
}

pub async fn series_cover(
    State(state): State<AppState>,
    Path(slug_webp): Path<String>,
) -> WebResult<Response> {
    // Detect -large variant — fetches w780 poster from TMDB (cached to disk)
    if slug_webp.ends_with("-large.webp") {
        return series_cover_large(State(state), Path(slug_webp)).await;
    }
    let slug = slug_webp.strip_suffix(".webp").unwrap_or(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        cover_filename: Option<String>,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT cover_filename FROM series WHERE slug = $1")
        .bind(slug)
        .fetch_optional(&state.db)
        .await?;

    let cover_filename = row.and_then(|r| r.cover_filename);
    let covers_dir = std::env::var("SERIES_COVERS_DIR")
        .unwrap_or_else(|_| "data/series/covers-webp".to_string());

    if let Some(filename) = cover_filename {
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

    // Placeholder
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

/// GET /serialy-online/{slug}-large.webp — serve w780 poster from TMDB (cached).
/// Mirrors films_cover_large for feature parity on series detail pages.
pub async fn series_cover_large(
    State(state): State<AppState>,
    Path(slug_webp): Path<String>,
) -> WebResult<Response> {
    let slug = slug_webp.strip_suffix("-large.webp").unwrap_or(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        tmdb_id: Option<i32>,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT tmdb_id FROM series WHERE slug = $1")
        .bind(slug)
        .fetch_optional(&state.db)
        .await?;

    let tmdb_id = row.and_then(|r| r.tmdb_id);
    let covers_dir = std::env::var("SERIES_COVERS_DIR")
        .unwrap_or_else(|_| "data/series/covers-webp".to_string());

    // Cache path: {covers_dir}/large/{slug}.webp
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

    // Fetch from TMDB (TV endpoint)
    if let Some(tid) = tmdb_id {
        let tmdb_key = "0405855b8275307d3cf3284470fd9d28";
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
                let output_bytes = if let Ok(img) = image::load_from_memory(&bytes) {
                    let mut buf = Vec::new();
                    let mut cursor = std::io::Cursor::new(&mut buf);
                    if img.write_to(&mut cursor, image::ImageFormat::WebP).is_ok() {
                        buf
                    } else {
                        bytes.to_vec()
                    }
                } else {
                    bytes.to_vec()
                };

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

    // Fallback to small cover (inline to avoid async recursion)
    let row = sqlx::query_as::<_, CoverRow2>("SELECT cover_filename FROM series WHERE slug = $1")
        .bind(slug)
        .fetch_optional(&state.db)
        .await?;
    let covers_dir_small = std::env::var("SERIES_COVERS_DIR")
        .unwrap_or_else(|_| "data/series/covers-webp".to_string());
    if let Some(filename) = row.and_then(|r| r.cover_filename) {
        let path = std::path::Path::new(&covers_dir_small).join(format!("{filename}.webp"));
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
    // Tiny empty WebP placeholder
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

#[derive(sqlx::FromRow)]
struct CoverRow2 {
    cover_filename: Option<String>,
}

/// GET /serialy-online/person/{filename} — serve person profile image from disk.
pub async fn series_person_image(Path(filename): Path<String>) -> WebResult<Response> {
    if !filename.ends_with(".webp") || filename.contains('/') || filename.contains("..") {
        return Ok((StatusCode::NOT_FOUND, "Not found").into_response());
    }
    let dir =
        std::env::var("SERIES_PEOPLE_DIR").unwrap_or_else(|_| "data/series/people".to_string());
    let path = std::path::Path::new(&dir).join(&filename);
    if path.exists()
        && let Ok(bytes) = tokio::fs::read(&path).await
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
    Ok((StatusCode::NOT_FOUND, "Not found").into_response())
}

/// GET /serialy-online/still/{filename} — serve episode still from disk.
pub async fn series_episode_still(Path(filename): Path<String>) -> WebResult<Response> {
    // Sanity-check filename: WebP only, no path traversal
    if !filename.ends_with(".webp") || filename.contains('/') || filename.contains("..") {
        return Ok((StatusCode::NOT_FOUND, "Not found").into_response());
    }
    let stills_dir = std::env::var("SERIES_STILLS_DIR")
        .unwrap_or_else(|_| "data/series/episode-stills".to_string());
    let path = std::path::Path::new(&stills_dir).join(&filename);
    if path.exists()
        && let Ok(bytes) = tokio::fs::read(&path).await
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
    Ok((StatusCode::NOT_FOUND, "Not found").into_response())
}
