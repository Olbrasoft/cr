use super::*;
use serde::{Deserialize, Serialize};

const FILMS_PER_PAGE: i64 = 24;

// --- DB row types ---

#[derive(sqlx::FromRow)]
struct FilmRow {
    id: i32,
    title: String,
    slug: String,
    year: Option<i16>,
    description: Option<String>,
    original_title: Option<String>,
    imdb_rating: Option<f32>,
    csfd_rating: Option<i16>,
    runtime_min: Option<i16>,
    cover_filename: Option<String>,
    sktorrent_video_id: Option<i32>,
    #[allow(dead_code)]
    sktorrent_cdn: Option<i16>,
    #[allow(dead_code)]
    sktorrent_qualities: Option<String>,
}

#[derive(sqlx::FromRow)]
struct GenreRow {
    id: i32,
    slug: String,
    name_cs: String,
}

#[derive(sqlx::FromRow)]
struct FilmGenreNameRow {
    name_cs: String,
    slug: String,
}

#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct FilmSourceRow {
    provider: String,
    code: String,
    language: Option<String>,
    audio_type: Option<String>,
    stream_format: Option<String>,
}

#[derive(sqlx::FromRow)]
struct CountRow {
    count: Option<i64>,
}

// --- Query params ---

#[derive(Deserialize)]
pub struct FilmsQuery {
    strana: Option<i64>,
    razeni: Option<String>, // "rok", "imdb", "csfd", "nazev"
    #[allow(dead_code)]
    zanry: Option<String>, // comma-separated genre slugs to include (future)
    #[allow(dead_code)]
    bez: Option<String>, // comma-separated genre slugs to exclude (future)
    q: Option<String>,      // search query
}

impl FilmsQuery {
    fn page(&self) -> i64 {
        self.strana.unwrap_or(1).max(1)
    }

    fn order_clause(&self) -> &str {
        match self.razeni.as_deref() {
            Some("imdb") => "f.imdb_rating DESC NULLS LAST, f.title",
            Some("csfd") => "f.csfd_rating DESC NULLS LAST, f.title",
            Some("nazev") => "f.title, f.year DESC NULLS LAST",
            _ => "f.year DESC NULLS LAST, f.title",
        }
    }

    fn sort_key(&self) -> &str {
        self.razeni.as_deref().unwrap_or("rok")
    }
}

// --- Templates ---

#[derive(Template)]
#[template(path = "films_list.html")]
struct FilmsListTemplate {
    img: String,
    films: Vec<FilmRow>,
    genres: Vec<GenreRow>,
    page: i64,
    total_pages: i64,
    total_count: i64,
    current_genre: Option<GenreRow>,
    sort_key: String,
    query_string: String,
}

#[derive(Template)]
#[template(path = "film_detail.html")]
struct FilmDetailTemplate {
    img: String,
    film: FilmRow,
    genres: Vec<FilmGenreNameRow>,
    #[allow(dead_code)]
    sources: Vec<FilmSourceRow>,
}

// --- Search API types ---

#[derive(Serialize)]
struct SearchResult {
    slug: String,
    title: String,
    year: Option<i16>,
    imdb_rating: Option<f32>,
    cover: bool,
}

#[derive(sqlx::FromRow)]
struct SearchRow {
    slug: String,
    title: String,
    year: Option<i16>,
    imdb_rating: Option<f32>,
    cover_filename: Option<String>,
}

// --- Handlers ---

/// GET /filmy-online/ — film listing with pagination, sorting, filtering
pub async fn films_list(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<FilmsQuery>,
) -> WebResult<Response> {
    let page = params.page();
    let offset = (page - 1) * FILMS_PER_PAGE;
    let order = params.order_clause();

    // Search filter
    let search_q = params.q.as_ref().and_then(|q| {
        let t = q.trim();
        if t.len() >= 2 {
            Some(format!("%{t}%"))
        } else {
            None
        }
    });

    let (total_count, films) = if let Some(ref pattern) = search_q {
        let count_row = sqlx::query_as::<_, CountRow>(
            "SELECT count(*) as count FROM films WHERE title ILIKE $1 OR original_title ILIKE $1",
        )
        .bind(pattern)
        .fetch_one(&state.db)
        .await?;

        let query = format!(
            "SELECT f.id, f.title, f.slug, f.year, f.description, f.original_title, \
             f.imdb_rating, f.csfd_rating, f.runtime_min, f.cover_filename, \
             f.sktorrent_video_id, f.sktorrent_cdn, f.sktorrent_qualities \
             FROM films f \
             WHERE f.title ILIKE $1 OR f.original_title ILIKE $1 \
             ORDER BY {order} \
             LIMIT $2 OFFSET $3"
        );
        let films = sqlx::query_as::<_, FilmRow>(&query)
            .bind(pattern)
            .bind(FILMS_PER_PAGE)
            .bind(offset)
            .fetch_all(&state.db)
            .await?;

        (count_row.count.unwrap_or(0), films)
    } else {
        let count_row = sqlx::query_as::<_, CountRow>("SELECT count(*) as count FROM films")
            .fetch_one(&state.db)
            .await?;

        let query = format!(
            "SELECT f.id, f.title, f.slug, f.year, f.description, f.original_title, \
             f.imdb_rating, f.csfd_rating, f.runtime_min, f.cover_filename, \
             f.sktorrent_video_id, f.sktorrent_cdn, f.sktorrent_qualities \
             FROM films f \
             ORDER BY {order} \
             LIMIT $1 OFFSET $2"
        );
        let films = sqlx::query_as::<_, FilmRow>(&query)
            .bind(FILMS_PER_PAGE)
            .bind(offset)
            .fetch_all(&state.db)
            .await?;

        (count_row.count.unwrap_or(0), films)
    };
    let total_pages = (total_count as f64 / FILMS_PER_PAGE as f64).ceil() as i64;

    let genres = load_genres(&state.db).await?;

    // Build query string for pagination links (preserve sort + search)
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

    let tmpl = FilmsListTemplate {
        img: state.image_base_url.clone(),
        films,
        genres,
        page,
        total_pages,
        total_count,
        current_genre: None,
        sort_key: params.sort_key().to_string(),
        query_string,
    };
    Ok(Html(tmpl.render()?).into_response())
}

/// GET /filmy-online/{slug}/ — film detail, genre listing, or cover image
pub async fn films_detail(
    State(state): State<AppState>,
    Path(slug): Path<String>,
    axum::extract::Query(params): axum::extract::Query<FilmsQuery>,
) -> WebResult<Response> {
    // WebP cover request: /filmy-online/some-film.webp
    if slug.ends_with(".webp") {
        return films_cover(State(state), Path(slug)).await;
    }

    // First check: is this a genre slug?
    let genre =
        sqlx::query_as::<_, GenreRow>("SELECT id, slug, name_cs FROM genres WHERE slug = $1")
            .bind(&slug)
            .fetch_optional(&state.db)
            .await?;

    if let Some(genre) = genre {
        return films_by_genre(state, genre, params).await;
    }

    // Otherwise: film detail
    let film = sqlx::query_as::<_, FilmRow>(
        "SELECT id, title, slug, year, description, original_title, \
         imdb_rating, csfd_rating, runtime_min, cover_filename, \
         sktorrent_video_id, sktorrent_cdn, sktorrent_qualities \
         FROM films WHERE slug = $1",
    )
    .bind(&slug)
    .fetch_optional(&state.db)
    .await?;

    let Some(film) = film else {
        return Ok(not_found_response().into_response());
    };

    let genres = sqlx::query_as::<_, FilmGenreNameRow>(
        "SELECT g.name_cs, g.slug FROM genres g \
         JOIN film_genres fg ON g.id = fg.genre_id \
         WHERE fg.film_id = $1 \
         ORDER BY g.name_cs",
    )
    .bind(film.id)
    .fetch_all(&state.db)
    .await?;

    let sources = sqlx::query_as::<_, FilmSourceRow>(
        "SELECT provider, code, language, audio_type, stream_format \
         FROM film_sources WHERE film_id = $1 \
         ORDER BY CASE WHEN audio_type = 'dubbing' THEN 0 ELSE 1 END, provider",
    )
    .bind(film.id)
    .fetch_all(&state.db)
    .await?;

    let tmpl = FilmDetailTemplate {
        img: state.image_base_url.clone(),
        film,
        genres,
        sources,
    };
    Ok(Html(tmpl.render()?).into_response())
}

/// Genre sub-listing: /filmy-online/{genre-slug}/
async fn films_by_genre(
    state: AppState,
    genre: GenreRow,
    params: FilmsQuery,
) -> WebResult<Response> {
    let page = params.page();
    let offset = (page - 1) * FILMS_PER_PAGE;
    let order = params.order_clause();

    let count_row = sqlx::query_as::<_, CountRow>(
        "SELECT count(*) as count FROM films f \
         JOIN film_genres fg ON f.id = fg.film_id \
         WHERE fg.genre_id = $1",
    )
    .bind(genre.id)
    .fetch_one(&state.db)
    .await?;
    let total_count = count_row.count.unwrap_or(0);
    let total_pages = (total_count as f64 / FILMS_PER_PAGE as f64).ceil() as i64;

    let query = format!(
        "SELECT f.id, f.title, f.slug, f.year, f.description, f.original_title, \
         f.imdb_rating, f.csfd_rating, f.runtime_min, f.cover_filename, \
         f.sktorrent_video_id, f.sktorrent_cdn, f.sktorrent_qualities \
         FROM films f \
         JOIN film_genres fg ON f.id = fg.film_id \
         WHERE fg.genre_id = $1 \
         ORDER BY {order} \
         LIMIT $2 OFFSET $3"
    );
    let films = sqlx::query_as::<_, FilmRow>(&query)
        .bind(genre.id)
        .bind(FILMS_PER_PAGE)
        .bind(offset)
        .fetch_all(&state.db)
        .await?;

    let all_genres = load_genres(&state.db).await?;

    let mut qs_parts = Vec::new();
    if params.razeni.is_some() {
        qs_parts.push(format!("razeni={}", params.sort_key()));
    }
    let query_string = if qs_parts.is_empty() {
        String::new()
    } else {
        format!("&{}", qs_parts.join("&"))
    };

    let tmpl = FilmsListTemplate {
        img: state.image_base_url.clone(),
        films,
        genres: all_genres,
        page,
        total_pages,
        total_count,
        current_genre: Some(genre),
        sort_key: params.sort_key().to_string(),
        query_string,
    };
    Ok(Html(tmpl.render()?).into_response())
}

/// GET /api/films/search?q=matrix — search autocomplete
pub async fn films_search(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    let q = params.get("q").map(|s| s.trim()).unwrap_or("");
    if q.len() < 2 {
        return Ok(axum::Json(Vec::<SearchResult>::new()).into_response());
    }

    let pattern = format!("%{q}%");
    let starts_pattern = format!("{q}%");
    let rows = sqlx::query_as::<_, SearchRow>(
        "SELECT slug, title, year, imdb_rating, cover_filename \
         FROM films \
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

    let results: Vec<SearchResult> = rows
        .into_iter()
        .map(|r| SearchResult {
            slug: r.slug,
            title: r.title,
            year: r.year,
            imdb_rating: r.imdb_rating,
            cover: r.cover_filename.is_some(),
        })
        .collect();

    Ok(axum::Json(results).into_response())
}

/// GET /filmy-online/{slug}.webp — serve WebP cover image
pub async fn films_cover(
    State(state): State<AppState>,
    Path(slug_webp): Path<String>,
) -> WebResult<Response> {
    let slug = slug_webp.strip_suffix(".webp").unwrap_or(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        cover_filename: Option<String>,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT cover_filename FROM films WHERE slug = $1")
        .bind(slug)
        .fetch_optional(&state.db)
        .await?;

    let cover_filename = row.and_then(|r| r.cover_filename);

    let covers_dir =
        std::env::var("COVERS_DIR").unwrap_or_else(|_| "data/movies/covers-webp".to_string());

    if let Some(filename) = cover_filename {
        let path = std::path::Path::new(&covers_dir).join(format!("{filename}.webp"));
        if path.exists() {
            let bytes = tokio::fs::read(&path).await.map_err(|e| {
                tracing::error!("Failed to read cover {}: {}", path.display(), e);
                crate::error::WebError(anyhow::anyhow!("Failed to read cover: {e}"))
            })?;
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

    // Placeholder: 1x1 transparent WebP
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

// --- Sktorrent resolve API ---

#[derive(Deserialize)]
pub struct SktorrentResolveQuery {
    video_id: i32,
}

#[derive(Serialize, Clone)]
pub struct SktorrentSource {
    url: String,
    quality: String,
    res: i32,
}

#[derive(Serialize)]
pub struct SktorrentResolveResponse {
    video_id: i32,
    sources: Vec<SktorrentSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    cached: bool,
}

/// In-memory cache for resolved sktorrent sources. Key: video_id, Value: (sources, resolved_at).
type SktorrentCache =
    tokio::sync::Mutex<std::collections::HashMap<i32, (Vec<SktorrentSource>, std::time::Instant)>>;
static SKTORRENT_CACHE: std::sync::LazyLock<SktorrentCache> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(std::collections::HashMap::new()));

/// GET /api/films/sktorrent-resolve?video_id=99
/// Fetches current CDN URLs from sktorrent.eu video page.
pub async fn sktorrent_resolve(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<SktorrentResolveQuery>,
) -> axum::Json<SktorrentResolveResponse> {
    let video_id = params.video_id;
    let cache_ttl = std::time::Duration::from_secs(2 * 3600); // 2h cache

    // Check cache
    {
        let cache = SKTORRENT_CACHE.lock().await;
        if let Some((sources, resolved_at)) = cache.get(&video_id)
            && resolved_at.elapsed() < cache_ttl
        {
            return axum::Json(SktorrentResolveResponse {
                video_id,
                sources: sources
                    .iter()
                    .map(|s| SktorrentSource {
                        url: s.url.clone(),
                        quality: s.quality.clone(),
                        res: s.res,
                    })
                    .collect(),
                error: None,
                cached: true,
            });
        }
    }

    // Fetch sktorrent video page
    let page_url = format!("https://online.sktorrent.eu/video/{video_id}/");
    let result = state
        .http_client
        .get(&page_url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        )
        .header("Accept-Encoding", "identity")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await;

    let html = match result {
        Ok(resp) => match resp.text().await {
            Ok(text) => text,
            Err(e) => {
                return axum::Json(SktorrentResolveResponse {
                    video_id,
                    sources: vec![],
                    error: Some(format!("Failed to read response: {e}")),
                    cached: false,
                });
            }
        },
        Err(e) => {
            return axum::Json(SktorrentResolveResponse {
                video_id,
                sources: vec![],
                error: Some(format!("Failed to fetch sktorrent page: {e}")),
                cached: false,
            });
        }
    };

    // Extract <source> tags
    let re = regex::Regex::new(
        r#"<source\s+src="([^"]+)"\s+type='video/mp4'\s+label='(\d+p)'\s+res='(\d+)'"#,
    )
    .unwrap();

    let sources: Vec<SktorrentSource> = re
        .captures_iter(&html)
        .filter_map(|cap| {
            Some(SktorrentSource {
                url: cap[1].to_string(),
                quality: cap[2].to_string(),
                res: cap[3].parse().ok()?,
            })
        })
        .collect();

    if sources.is_empty() {
        return axum::Json(SktorrentResolveResponse {
            video_id,
            sources: vec![],
            error: Some("No video sources found on sktorrent page".to_string()),
            cached: false,
        });
    }

    // Cache
    {
        let mut cache = SKTORRENT_CACHE.lock().await;
        cache.insert(
            video_id,
            (
                sources
                    .iter()
                    .map(|s| SktorrentSource {
                        url: s.url.clone(),
                        quality: s.quality.clone(),
                        res: s.res,
                    })
                    .collect(),
                std::time::Instant::now(),
            ),
        );
    }

    axum::Json(SktorrentResolveResponse {
        video_id,
        sources,
        error: None,
        cached: false,
    })
}

// --- Helpers ---

async fn load_genres(db: &sqlx::PgPool) -> Result<Vec<GenreRow>, sqlx::Error> {
    sqlx::query_as::<_, GenreRow>(
        "SELECT g.id, g.slug, g.name_cs \
         FROM genres g \
         JOIN film_genres fg ON g.id = fg.genre_id \
         GROUP BY g.id, g.slug, g.name_cs \
         ORDER BY g.name_cs",
    )
    .fetch_all(db)
    .await
}

fn not_found_response() -> Response {
    (
        StatusCode::NOT_FOUND,
        Html("<h1>404 — Stránka nenalezena</h1>"),
    )
        .into_response()
}
