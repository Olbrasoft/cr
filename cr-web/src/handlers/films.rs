use super::*;
use serde::{Deserialize, Serialize};

const FILMS_PER_PAGE: i64 = 24;

/// SELECT column list for `FilmRow` queries. Kept as a const to avoid
/// duplication across `films_list`, `films_by_genre`, and `films_detail`.
const FILM_COLUMNS: &str = "f.id, f.title, f.slug, f.year, f.description, f.original_title, \
    f.imdb_rating, f.csfd_rating, f.runtime_min, f.cover_filename, \
    f.sktorrent_video_id, f.sktorrent_cdn, f.sktorrent_qualities, f.added_at, \
    f.prehrajto_url, f.prehrajto_has_dub, f.prehrajto_has_subs";

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
    // `sktorrent_cdn` is used as a first-try hint by `sktorrent_resolve`
    // (see docs/sktorrent-cdn-stability.md) — ~80 % of films still live on
    // the node stored here, so the fast path usually resolves after probing
    // 4 labels (720p/480p/HD/SD) instead of falling back to scanning up to
    // 25 nodes × 4 labels = 100 HEAD requests. FilmRow itself doesn't read
    // it; the resolver queries the column directly.
    #[allow(dead_code)]
    sktorrent_cdn: Option<i16>,
    #[allow(dead_code)]
    sktorrent_qualities: Option<String>,
    #[allow(dead_code)] // Needed in SELECT for ORDER BY; not rendered in templates
    added_at: Option<chrono::DateTime<chrono::Utc>>,
    prehrajto_url: Option<String>,
    #[allow(dead_code)] // Not rendered in current templates; kept for future dub/sub badges
    prehrajto_has_dub: bool,
    #[allow(dead_code)] // Not rendered in current templates; kept for future dub/sub badges
    prehrajto_has_subs: bool,
}

#[derive(sqlx::FromRow)]
struct GenreRow {
    id: i32,
    slug: String,
    name_cs: String,
}

impl GenreRow {
    /// Pretty Czech plural title for headings and SEO (e.g. "Horory", "Dramata", "Televizní filmy").
    fn pretty_plural(&self) -> String {
        let known: &str = match self.slug.as_str() {
            "akcni" => "Akční filmy",
            "animovany" => "Animované filmy",
            "dobrodruzny" => "Dobrodružné filmy",
            "dokumentarni" => "Dokumentární filmy",
            "drama" => "Dramata",
            "fantasy" => "Fantasy filmy",
            "historicky" => "Historické filmy",
            "horor" => "Horory",
            "hudebni" => "Hudební filmy",
            "komedie" => "Komedie",
            "krimi" => "Kriminální filmy",
            "mysteriozni" => "Mysteriózní filmy",
            "rodinny" => "Rodinné filmy",
            "romanticky" => "Romantické filmy",
            "sci-fi" => "Sci-Fi filmy",
            "thriller" => "Thrillery",
            "tv-film" => "Televizní filmy",
            "valecny" => "Válečné filmy",
            "western" => "Westerny",
            _ => "",
        };
        if known.is_empty() {
            format!("{} filmy", self.name_cs)
        } else {
            known.to_string()
        }
    }
}

#[derive(sqlx::FromRow)]
struct FilmGenreNameRow {
    name_cs: String,
    slug: String,
}

#[derive(sqlx::FromRow)]
#[allow(dead_code)] // Fetched in films_detail; template accesses via JS, not Askama
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
    zanry: Option<String>,  // comma-separated genre slugs to include
    bez: Option<String>,    // comma-separated genre slugs to exclude
    q: Option<String>,      // search query
    rok: Option<String>,    // year filter
    rezim: Option<String>,  // "and" (all genres) or "or" (any genre)
    smer: Option<String>,   // "asc" or "desc" (default)
    jazyk: Option<String>,  // comma-separated: dub, sub
}

impl FilmsQuery {
    fn genre_mode_and(&self) -> bool {
        self.rezim.as_deref() == Some("and")
    }

    fn sort_desc(&self) -> bool {
        self.smer.as_deref() != Some("asc")
    }

    fn audio_filter(&self) -> Option<&'static str> {
        let val = self.jazyk.as_deref().map(|s| s.trim()).unwrap_or("");
        if val.is_empty() || val == "vse" {
            return None;
        }
        let parts: Vec<&str> = val.split(',').map(|s| s.trim()).collect();
        let has_dub = parts.contains(&"dub") || parts.contains(&"cz") || parts.contains(&"sk");
        let has_sub = parts.contains(&"sub") || parts.contains(&"titulky");
        match (has_dub, has_sub) {
            (true, false) => Some("f.has_dub = true"),
            (false, true) => Some("f.has_subtitles = true"),
            (true, true) => Some("(f.has_dub = true OR f.has_subtitles = true)"),
            _ => None,
        }
    }
}

impl FilmsQuery {
    fn page(&self) -> i64 {
        self.strana.unwrap_or(1).max(1)
    }

    fn order_clause(&self) -> &'static str {
        let desc = self.sort_desc();
        match (self.razeni.as_deref(), desc) {
            (Some("rok"), true) => "f.year DESC NULLS LAST, f.title",
            (Some("rok"), false) => "f.year ASC NULLS LAST, f.title",
            (Some("imdb"), true) => "f.imdb_rating DESC NULLS LAST, f.title",
            (Some("imdb"), false) => "f.imdb_rating ASC NULLS LAST, f.title",
            (Some("csfd"), true) => "f.csfd_rating DESC NULLS LAST, f.title",
            (Some("csfd"), false) => "f.csfd_rating ASC NULLS LAST, f.title",
            (Some("nazev"), true) => "f.title DESC, f.year DESC NULLS LAST",
            (Some("nazev"), false) => "f.title ASC, f.year DESC NULLS LAST",
            // Default: "pridano" (added_at) — most recently added first
            (_, true) => "f.added_at DESC NULLS LAST, f.title",
            (_, false) => "f.added_at ASC NULLS LAST, f.title",
        }
    }

    fn sort_key(&self) -> &str {
        self.razeni.as_deref().unwrap_or("pridano")
    }

    fn parse_genre_slugs(input: Option<&String>) -> Vec<String> {
        // Dedup to keep AND-mode `HAVING COUNT(DISTINCT g.slug) = slugs.len()` correct.
        let mut slugs: Vec<String> = Vec::new();
        if let Some(input) = input {
            for s in input.split(',').map(|g| g.trim()).filter(|g| !g.is_empty()) {
                if !slugs.iter().any(|x| x == s) {
                    slugs.push(s.to_string());
                }
            }
        }
        slugs
    }

    fn include_genres(&self) -> Vec<String> {
        Self::parse_genre_slugs(self.zanry.as_ref())
    }

    fn exclude_genres(&self) -> Vec<String> {
        Self::parse_genre_slugs(self.bez.as_ref())
    }

    fn year_filter(&self) -> Option<i16> {
        self.rok.as_ref().and_then(|s| s.trim().parse().ok())
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
    #[allow(dead_code)] // TODO: verify usage — may be needed for sort UI active state
    sort_key: String,
    query_string: String,
    search_query: Option<String>,
    open_filter: bool,
    /// Genre slugs the user is filtering by right now (from `?zanry=` on main page).
    /// Empty on a category sub-page — that info is already in `current_genre`.
    selected_genre_slugs: Vec<String>,
    /// Genres per film, keyed by film id — rendered as chips in desktop list view.
    film_genres_map: std::collections::HashMap<i32, Vec<FilmGenreNameRow>>,
}

impl FilmsListTemplate {
    fn is_selected(&self, slug: &str) -> bool {
        self.selected_genre_slugs.iter().any(|s| s == slug)
    }
    fn is_multi_filter_mode(&self) -> bool {
        self.current_genre.is_some() || !self.selected_genre_slugs.is_empty()
    }
    fn is_current_genre(&self, g: &GenreRow) -> bool {
        self.current_genre.as_ref().is_some_and(|cg| cg.id == g.id)
    }
    // NOTE: Askama auto-refs arguments in template method calls — `{{ self.film_genres(f.id) }}`
    // generates a call with `&f.id`. Keep the signature as `&i32` to match; Copilot's
    // "accept i32 by value" suggestion would break Askama codegen here.
    fn film_genres(&self, film_id: &i32) -> &[FilmGenreNameRow] {
        static EMPTY: Vec<FilmGenreNameRow> = Vec::new();
        self.film_genres_map
            .get(film_id)
            .map(|v| v.as_slice())
            .unwrap_or(EMPTY.as_slice())
    }
}

#[derive(Template)]
#[template(path = "film_detail.html")]
struct FilmDetailTemplate {
    img: String,
    film: FilmRow,
    genres: Vec<FilmGenreNameRow>,
    #[allow(dead_code)] // Fetched from DB but not rendered via Askama; JS handles sources
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

    let include = params.include_genres();
    let exclude = params.exclude_genres();
    let year_f = params.year_filter();

    // Build dynamic WHERE parts
    let mut where_parts: Vec<String> = vec![];
    let mut bind_idx = 1;

    let has_search = search_q.is_some();
    if has_search {
        where_parts.push(format!(
            "(f.title ILIKE ${bind_idx} OR f.original_title ILIKE ${bind_idx})"
        ));
        bind_idx += 1;
    }
    if !include.is_empty() {
        if params.genre_mode_and() {
            // AND: film must have ALL selected genres
            where_parts.push(format!(
                "f.id IN (SELECT fg.film_id FROM film_genres fg \
                 JOIN genres g ON g.id = fg.genre_id \
                 WHERE g.slug = ANY(${bind_idx}) \
                 GROUP BY fg.film_id HAVING COUNT(DISTINCT g.slug) = {})",
                include.len()
            ));
        } else {
            // OR (default): film must have ANY selected genre
            where_parts.push(format!(
                "f.id IN (SELECT fg.film_id FROM film_genres fg \
                 JOIN genres g ON g.id = fg.genre_id \
                 WHERE g.slug = ANY(${bind_idx}))"
            ));
        }
        bind_idx += 1;
    }
    if !exclude.is_empty() {
        where_parts.push(format!(
            "f.id NOT IN (SELECT fg.film_id FROM film_genres fg \
             JOIN genres g ON g.id = fg.genre_id \
             WHERE g.slug = ANY(${bind_idx}))"
        ));
        bind_idx += 1;
    }
    if year_f.is_some() {
        where_parts.push(format!("f.year = ${bind_idx}"));
        bind_idx += 1;
    }
    if let Some(af) = params.audio_filter() {
        where_parts.push(af.to_string());
    }

    let where_clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_parts.join(" AND "))
    };

    // Count query
    let count_query = format!("SELECT count(*) as count FROM films f {where_clause}");
    let mut cq = sqlx::query_as::<_, CountRow>(&count_query);
    if let Some(ref p) = search_q {
        cq = cq.bind(p.clone());
    }
    if !include.is_empty() {
        cq = cq.bind(include.clone());
    }
    if !exclude.is_empty() {
        cq = cq.bind(exclude.clone());
    }
    if let Some(yr) = year_f {
        cq = cq.bind(yr);
    }
    let count_row = cq.fetch_one(&state.db).await?;

    // Films query
    let films_query = format!(
        "SELECT {FILM_COLUMNS} \
         FROM films f {where_clause} \
         ORDER BY {order} \
         LIMIT ${limit_idx} OFFSET ${offset_idx}",
        limit_idx = bind_idx,
        offset_idx = bind_idx + 1
    );
    let mut fq = sqlx::query_as::<_, FilmRow>(&films_query);
    if let Some(ref p) = search_q {
        fq = fq.bind(p.clone());
    }
    if !include.is_empty() {
        fq = fq.bind(include.clone());
    }
    if !exclude.is_empty() {
        fq = fq.bind(exclude.clone());
    }
    if let Some(yr) = year_f {
        fq = fq.bind(yr);
    }
    let films = fq
        .bind(FILMS_PER_PAGE)
        .bind(offset)
        .fetch_all(&state.db)
        .await?;

    let (total_count, films) = (count_row.count.unwrap_or(0), films);
    let total_pages = (total_count as f64 / FILMS_PER_PAGE as f64).ceil() as i64;

    let genres = load_genres(&state.db).await?;

    // Build query string for pagination links (preserve sort + filters)
    let query_string = build_films_query_string(&params);

    let search_query = params.q.as_ref().and_then(|q| {
        let t = q.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });

    let selected_genre_slugs: Vec<String> = params
        .zanry
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let open_filter = !selected_genre_slugs.is_empty();
    let film_genres_map = load_film_genres_map(&state.db, &films).await?;

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
        search_query,
        open_filter,
        selected_genre_slugs,
        film_genres_map,
    };
    Ok(Html(tmpl.render()?).into_response())
}

/// GET /filmy-online/{slug}/ — film detail, genre listing, or cover image
pub async fn films_detail(
    State(state): State<AppState>,
    Path(slug): Path<String>,
    axum::extract::Query(params): axum::extract::Query<FilmsQuery>,
    headers: axum::http::HeaderMap,
) -> WebResult<Response> {
    // WebP cover request: /filmy-online/some-film.webp (small) or -large.webp
    if slug.ends_with("-large.webp") {
        return films_cover_large(State(state), Path(slug)).await;
    }
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
        let from_films_home = headers
            .get(axum::http::header::REFERER)
            .and_then(|h| h.to_str().ok())
            .map(|r| {
                if let Some(path) = r.split_once("://").and_then(|(_, s)| s.split_once('/')) {
                    let p = format!("/{}", path.1);
                    let clean = p.split('?').next().unwrap_or(&p);
                    clean == "/filmy-online/" || clean == "/filmy-online"
                } else {
                    false
                }
            })
            .unwrap_or(false);
        return films_by_genre(state, genre, params, from_films_home).await;
    }

    // Otherwise: film detail
    let film = sqlx::query_as::<_, FilmRow>(&format!(
        "SELECT {FILM_COLUMNS} FROM films f WHERE f.slug = $1"
    ))
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
    open_filter: bool,
) -> WebResult<Response> {
    let page = params.page();
    let offset = (page - 1) * FILMS_PER_PAGE;
    let order = params.order_clause();
    let exclude = params.exclude_genres();
    let year_f = params.year_filter();
    let zanry_extras = params.include_genres();

    // Merge path genre with extras from ?zanry= into one include list
    let mut include_slugs: Vec<String> = vec![genre.slug.clone()];
    for s in zanry_extras.iter() {
        if !include_slugs.contains(s) {
            include_slugs.push(s.clone());
        }
    }
    let multi_include = include_slugs.len() > 1;

    // Build WHERE clauses
    let mut where_parts: Vec<String> = vec![];
    let mut bind_idx = 1;
    if !multi_include {
        where_parts.push(format!(
            "f.id IN (SELECT fg.film_id FROM film_genres fg WHERE fg.genre_id = ${bind_idx})"
        ));
        bind_idx += 1;
    } else if params.genre_mode_and() {
        where_parts.push(format!(
            "f.id IN (SELECT fg.film_id FROM film_genres fg \
             JOIN genres g ON g.id = fg.genre_id \
             WHERE g.slug = ANY(${bind_idx}) \
             GROUP BY fg.film_id HAVING COUNT(DISTINCT g.slug) = {})",
            include_slugs.len()
        ));
        bind_idx += 1;
    } else {
        where_parts.push(format!(
            "f.id IN (SELECT fg.film_id FROM film_genres fg \
             JOIN genres g ON g.id = fg.genre_id \
             WHERE g.slug = ANY(${bind_idx}))"
        ));
        bind_idx += 1;
    }
    if !exclude.is_empty() {
        where_parts.push(format!(
            "f.id NOT IN (SELECT fg2.film_id FROM film_genres fg2 \
             JOIN genres g2 ON g2.id = fg2.genre_id \
             WHERE g2.slug = ANY(${bind_idx}))"
        ));
        bind_idx += 1;
    }
    if year_f.is_some() {
        where_parts.push(format!("f.year = ${bind_idx}"));
        bind_idx += 1;
    }
    if let Some(af) = params.audio_filter() {
        where_parts.push(af.to_string());
    }
    let where_clause = where_parts.join(" AND ");

    // Count
    let count_query = format!("SELECT count(*) as count FROM films f WHERE {where_clause}");
    let mut count_q = sqlx::query_as::<_, CountRow>(&count_query);
    if multi_include {
        count_q = count_q.bind(include_slugs.clone());
    } else {
        count_q = count_q.bind(genre.id);
    }
    if !exclude.is_empty() {
        count_q = count_q.bind(exclude.clone());
    }
    if let Some(yr) = year_f {
        count_q = count_q.bind(yr);
    }
    let count_row = count_q.fetch_one(&state.db).await?;
    let total_count = count_row.count.unwrap_or(0);
    let total_pages = (total_count as f64 / FILMS_PER_PAGE as f64).ceil() as i64;

    // Films
    let films_query = format!(
        "SELECT {FILM_COLUMNS} FROM films f \
         WHERE {where_clause} \
         ORDER BY {order} \
         LIMIT ${limit_idx} OFFSET ${offset_idx}",
        limit_idx = bind_idx,
        offset_idx = bind_idx + 1
    );
    let mut q = sqlx::query_as::<_, FilmRow>(&films_query);
    if multi_include {
        q = q.bind(include_slugs.clone());
    } else {
        q = q.bind(genre.id);
    }
    if !exclude.is_empty() {
        q = q.bind(exclude.clone());
    }
    if let Some(yr) = year_f {
        q = q.bind(yr);
    }
    let films = q
        .bind(FILMS_PER_PAGE)
        .bind(offset)
        .fetch_all(&state.db)
        .await?;

    let all_genres = load_genres(&state.db).await?;

    let query_string = build_films_query_string(&params);

    let film_genres_map = load_film_genres_map(&state.db, &films).await?;
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
        search_query: None,
        open_filter: open_filter || !zanry_extras.is_empty(),
        selected_genre_slugs: zanry_extras.clone(),
        film_genres_map,
    };
    Ok(Html(tmpl.render()?).into_response())
}

/// GET /api/films/search?q=matrix — search autocomplete.
///
/// If the raw query returns nothing and it ends with a year pattern
/// (" (YYYY)" or " YYYY"), retry without the trailing year — users often
/// type "Mstitel (1989)" but the title column stores only "Mstitel".
pub async fn films_search(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    let q = params.get("q").map(|s| s.trim()).unwrap_or("");
    if q.len() < 2 {
        return Ok(axum::Json(Vec::<SearchResult>::new()).into_response());
    }

    let mut rows = search_films_by_title(&state.db, q).await?;
    if rows.is_empty()
        && let Some(stripped) = strip_trailing_year(q)
    {
        rows = search_films_by_title(&state.db, &stripped).await?;
    }

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

async fn search_films_by_title(db: &sqlx::PgPool, q: &str) -> Result<Vec<SearchRow>, sqlx::Error> {
    let pattern = format!("%{q}%");
    let starts_pattern = format!("{q}%");
    sqlx::query_as::<_, SearchRow>(
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
    .fetch_all(db)
    .await
}

/// Strip a trailing year pattern (" (YYYY)" or " YYYY") from a search query.
/// Requires leading whitespace and ≥2 chars remaining — avoids stripping a
/// standalone year like "1984" or short titles like "X 1989".
fn strip_trailing_year(q: &str) -> Option<String> {
    static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"\s+(?:\(\d{4}\)|\d{4})\s*$").expect("const regex literal compiles")
    });
    let m = RE.find(q)?;
    let before = q[..m.start()].trim_end();
    if before.chars().count() < 2 {
        return None;
    }
    Some(before.to_string())
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

    let covers_dir = state.config.film_covers_dir.clone();

    if let Some(filename) = cover_filename {
        let path = std::path::Path::new(&covers_dir).join(format!("{filename}.webp"));
        if path.exists() {
            let bytes = tokio::fs::read(&path).await.map_err(|e| {
                tracing::error!("Failed to read cover {}: {}", path.display(), e);
                crate::error::WebError::Internal(anyhow::anyhow!("Failed to read cover: {e}"))
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

    // Placeholder: 1x1 transparent WebP. `no-store` — a missing cover is a
    // transient state (import will fill it shortly); caching the placeholder
    // pins an empty card in the browser for hours after the real WebP lands.
    static PLACEHOLDER: &[u8] = &[
        0x52, 0x49, 0x46, 0x46, 0x1a, 0x00, 0x00, 0x00, 0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38,
        0x4c, 0x0d, 0x00, 0x00, 0x00, 0x2f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ];
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/webp"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        PLACEHOLDER.to_vec(),
    )
        .into_response())
}

/// GET /filmy-online/{slug}-large.webp — serve large WebP cover (w780 from TMDB, cached)
pub async fn films_cover_large(
    State(state): State<AppState>,
    Path(slug_webp): Path<String>,
) -> WebResult<Response> {
    let slug = slug_webp.strip_suffix("-large.webp").unwrap_or(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        tmdb_id: Option<i32>,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT tmdb_id FROM films WHERE slug = $1")
        .bind(slug)
        .fetch_optional(&state.db)
        .await?;

    let tmdb_id = row.and_then(|r| r.tmdb_id);
    let covers_dir = state.config.film_covers_dir.clone();

    // Cache path: {covers_dir}/large/{slug}.webp
    let cache_dir = std::path::Path::new(&covers_dir).join("large");
    let cache_path = cache_dir.join(format!("{slug}.webp"));

    // Serve from cache if exists
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

    // Fetch from TMDB
    if let Some(tid) = tmdb_id {
        // First get poster_path
        let tmdb_key = "0405855b8275307d3cf3284470fd9d28";
        let detail_url =
            format!("https://api.themoviedb.org/3/movie/{tid}?api_key={tmdb_key}&language=cs-CZ");

        if let Ok(resp) = state
            .http_client
            .get(&detail_url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            && let Ok(data) = resp.json::<serde_json::Value>().await
            && let Some(poster_path) = data.get("poster_path").and_then(|v| v.as_str())
        {
            // Download w780 poster
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
                // Try to convert to WebP for smaller size
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

                // Save to cache
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

    // Fallback to small cover
    films_cover(State(state), Path(format!("{slug}.webp"))).await
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

// SK Torrent resolution cache moved to `AppState.sktorrent_cache` (a
// `BoundedTtlCache`) to bound memory growth. Configured in main.rs with
// `max_entries=2000, ttl=6h`.

/// GET /api/films/sktorrent-resolve?video_id=99
///
/// Resolves the current CDN node that hosts a given SK Torrent video. The
/// main page `online.sktorrent.eu/video/{id}/` stealth-blocks datacentre
/// ASNs (HTTP 200, 0 bytes from Hetzner), so we never parse HTML here —
/// instead we HEAD-probe the edge CDN nodes `online{N}.sktorrent.eu` which
/// are not blocked.
///
/// Strategy (min load → max load):
///   1. In-memory cache hit (6 h TTL, `AppState.sktorrent_cache`).
///   2. DB hint: `sktorrent_cdn` stored at import time. Probe that single
///      node for {720p, 480p, HD, SD} (4 HEAD). Per the 2026-04 stability
///      test (docs/sktorrent-cdn-stability.md) this works for ~80 % of
///      films.
///   3. Full scan: `online1..online25` × {720p, 480p, HD, SD} = 100 HEAD.
///      Run only when the hint is missing or stale. On success we self-heal
///      the DB row so future plays go back to the 4-HEAD fast path.
pub async fn sktorrent_resolve(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<SktorrentResolveQuery>,
) -> axum::Json<SktorrentResolveResponse> {
    let video_id = params.video_id;

    if let Some(cached_sources) = state.sktorrent_cache.get(&video_id).await {
        return axum::Json(SktorrentResolveResponse {
            video_id,
            sources: cached_sources,
            error: None,
            cached: true,
        });
    }

    // DB hint — look up the last known CDN node across all three tables
    // that can carry an SK Torrent video (films / series episodes / tv show
    // episodes). If the same video_id ever appeared in more than one table,
    // the `priority` column fixes a deterministic order: films → series →
    // tv shows.
    let hint: Option<i16> = match sqlx::query_scalar(
        "SELECT sktorrent_cdn FROM ( \
             SELECT sktorrent_cdn, 1 AS priority FROM films \
              WHERE sktorrent_video_id = $1 AND sktorrent_cdn IS NOT NULL \
             UNION ALL \
             SELECT sktorrent_cdn, 2 AS priority FROM series_episodes \
              WHERE sktorrent_video_id = $1 AND sktorrent_cdn IS NOT NULL \
             UNION ALL \
             SELECT sktorrent_cdn, 3 AS priority FROM tv_episodes \
              WHERE sktorrent_video_id = $1 AND sktorrent_cdn IS NOT NULL \
         ) AS cdn_hints \
         ORDER BY priority \
         LIMIT 1",
    )
    .bind(video_id)
    .fetch_optional(&state.db)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            // Not fatal — we'll just fall through to the full scan. Logged
            // so operational issues (connection drops, schema drift) surface
            // in journalctl instead of silently inflating load.
            tracing::warn!("sktorrent_resolve hint lookup failed for video_id={video_id}: {e}");
            None
        }
    };

    let mut sources: Vec<SktorrentSource> = if let Some(cdn) = hint {
        probe_sktorrent_cdn(&state.http_client, video_id, cdn as i32).await
    } else {
        vec![]
    };

    // Fall back to full scan on miss / missing hint. When this finds the
    // current node, write it back so the next play is cheap again.
    if sources.is_empty() {
        sources = scan_sktorrent_cdns(&state.http_client, video_id).await;
        if let Some(new_cdn) = infer_cdn_from_sources(&sources) {
            update_sktorrent_cdn(&state.db, video_id, new_cdn as i16).await;
        }
    }

    if sources.is_empty() {
        return axum::Json(SktorrentResolveResponse {
            video_id,
            sources: vec![],
            error: Some("No video sources found on sktorrent".to_string()),
            cached: false,
        });
    }

    state
        .sktorrent_cache
        .insert(video_id, sources.clone())
        .await;

    axum::Json(SktorrentResolveResponse {
        video_id,
        sources,
        error: None,
        cached: false,
    })
}

/// Probe a single CDN node across all six supported quality labels in
/// parallel. Used as the fast path when we have a DB hint. Order doesn't
/// matter here because everything fans out in parallel and any hit counts
/// — older films may live only under the legacy `HD`/`SD` labels (e.g.
/// video_id 36411) and a small tail only has `360p`/`240p`. Six HEADs is
/// still cheap versus a 100-HEAD full scan, and returning multiple hits
/// lets the frontend render a quality switcher when the node has more
/// than one encode.
async fn probe_sktorrent_cdn(
    client: &reqwest::Client,
    video_id: i32,
    cdn: i32,
) -> Vec<SktorrentSource> {
    let qualities = [
        ("720p", 720),
        ("HD", 700),
        ("480p", 480),
        ("SD", 360),
        ("360p", 360),
        ("240p", 240),
    ];
    let mut tasks = Vec::new();
    for (q_label, q_res) in qualities.iter() {
        let url =
            format!("https://online{cdn}.sktorrent.eu/media/videos//h264/{video_id}_{q_label}.mp4");
        let client = client.clone();
        let q_label = q_label.to_string();
        let q_res = *q_res;
        let url_clone = url.clone();
        tasks.push(tokio::spawn(async move {
            let ok = client
                .head(&url_clone)
                .timeout(std::time::Duration::from_secs(3))
                .send()
                .await
                .map(|r| r.status().is_success() || r.status().as_u16() == 206)
                .unwrap_or(false);
            (ok, url, q_label, q_res)
        }));
    }
    let mut found = Vec::new();
    for t in tasks {
        if let Ok((ok, url, q, res)) = t.await
            && ok
        {
            found.push(SktorrentSource {
                url,
                quality: q,
                res,
            });
        }
    }
    found
}

/// Extract the CDN node number from a resolved source URL. URLs look like
/// `https://online11.sktorrent.eu/media/videos//h264/...` — we just parse
/// the digits between `online` and the next `.`.
fn infer_cdn_from_sources(sources: &[SktorrentSource]) -> Option<i32> {
    let url = sources.first()?.url.as_str();
    let after = url.strip_prefix("https://online")?;
    let dot = after.find('.')?;
    after[..dot].parse::<i32>().ok()
}

/// Write the freshly-discovered CDN node back to whichever of the three
/// sktorrent-bearing tables owns this `video_id`. Best-effort — failures
/// are logged but not surfaced; the playback already has its URL.
async fn update_sktorrent_cdn(db: &sqlx::PgPool, video_id: i32, cdn: i16) {
    for sql in [
        "UPDATE films SET sktorrent_cdn = $1 WHERE sktorrent_video_id = $2",
        "UPDATE series_episodes SET sktorrent_cdn = $1 WHERE sktorrent_video_id = $2",
        "UPDATE tv_episodes SET sktorrent_cdn = $1 WHERE sktorrent_video_id = $2",
    ] {
        if let Err(e) = sqlx::query(sql).bind(cdn).bind(video_id).execute(db).await {
            tracing::warn!("sktorrent_cdn self-heal failed ({sql}): {e}");
        }
    }
}

/// Full-scan fallback: called only when the DB hint is missing or stale.
///
/// Tiered probe — for each quality from best to worst, race all 30 CDN
/// nodes in parallel via `JoinSet` and return on the first HEAD 2xx. A
/// successful tier drops the set, which aborts the remaining probes;
/// empty tiers wait out the 3 s per-request timeout before moving on.
/// Order is 720p → HD → 480p → SD → 360p → 240p; HD covers 267 legacy-
/// label films and the two new low-res tiers catch the long tail that
/// SK Torrent only encodes in 360p / 240p (measured on 2026-04-18 over
/// 834 low-quality films — docs/sktorrent-cdn-stability.md).
///
/// Happy-path cost is ~30 HEAD (whichever CDN answers first). Worst case
/// (no source anywhere) is 6 × 30 = 180 HEAD, higher than the previous
/// 100, but we accept that to unlock the 360p/240p tier.
///
/// Returns a single source (the first hit in the highest reachable tier);
/// the frontend's `renderSktorrentSources` handles a 1-element list
/// fine — one quality button, no switcher — which is the trade-off for
/// keeping sktorrent.eu's request volume down.
async fn scan_sktorrent_cdns(client: &reqwest::Client, video_id: i32) -> Vec<SktorrentSource> {
    // (label, res). `res` is used for frontend sorting; HD/SD are rough.
    const QUALITIES: &[(&str, i32)] = &[
        ("720p", 720),
        ("HD", 700),
        ("480p", 480),
        ("SD", 360),
        ("360p", 360),
        ("240p", 240),
    ];

    for &(q_label, q_res) in QUALITIES {
        // JoinSet races all 30 CDN probes; we take the FIRST success and
        // drop the set to abort the rest — no waiting on slow/timing-out
        // peers once we already have an answer.
        let mut set = tokio::task::JoinSet::new();
        for cdn in 1..=30 {
            let url = format!(
                "https://online{cdn}.sktorrent.eu/media/videos//h264/{video_id}_{q_label}.mp4"
            );
            let client = client.clone();
            set.spawn(async move {
                let ok = client
                    .head(&url)
                    .timeout(std::time::Duration::from_secs(3))
                    .send()
                    .await
                    .map(|r| r.status().is_success() || r.status().as_u16() == 206)
                    .unwrap_or(false);
                (ok, url)
            });
        }

        let mut hit: Option<String> = None;
        while let Some(res) = set.join_next().await {
            if let Ok((true, url)) = res {
                hit = Some(url);
                break;
            }
        }
        // Dropping `set` here aborts any still-pending probes.

        if let Some(url) = hit {
            return vec![SktorrentSource {
                url,
                quality: q_label.to_string(),
                res: q_res,
            }];
        }
    }

    vec![]
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

#[derive(sqlx::FromRow)]
struct FilmGenreJoinRow {
    film_id: i32,
    name_cs: String,
    slug: String,
}

async fn load_film_genres_map(
    db: &sqlx::PgPool,
    films: &[FilmRow],
) -> Result<std::collections::HashMap<i32, Vec<FilmGenreNameRow>>, sqlx::Error> {
    if films.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let ids: Vec<i32> = films.iter().map(|f| f.id).collect();
    let rows = sqlx::query_as::<_, FilmGenreJoinRow>(
        "SELECT fg.film_id, g.name_cs, g.slug \
         FROM film_genres fg JOIN genres g ON g.id = fg.genre_id \
         WHERE fg.film_id = ANY($1) \
         ORDER BY fg.film_id, g.name_cs",
    )
    .bind(ids)
    .fetch_all(db)
    .await?;
    let mut map: std::collections::HashMap<i32, Vec<FilmGenreNameRow>> =
        std::collections::HashMap::new();
    for r in rows {
        map.entry(r.film_id).or_default().push(FilmGenreNameRow {
            name_cs: r.name_cs,
            slug: r.slug,
        });
    }
    Ok(map)
}

/// Build pagination query string for film list/genre views.
/// Consolidates the qs_parts logic that was duplicated in `films_list`
/// and `films_by_genre`.
fn build_films_query_string(params: &FilmsQuery) -> String {
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
    if let Some(ref z) = params.zanry
        && !z.is_empty()
    {
        parts.push(("zanry", z.clone()));
    }
    if params.rezim.as_deref() == Some("and") {
        parts.push(("rezim", "and".to_string()));
    }
    if params.smer.as_deref() == Some("asc") {
        parts.push(("smer", "asc".to_string()));
    }
    if let Some(ref j) = params.jazyk
        && !j.is_empty()
    {
        parts.push(("jazyk", j.clone()));
    }
    if let Some(ref b) = params.bez
        && !b.is_empty()
    {
        parts.push(("bez", b.clone()));
    }
    if let Some(ref r) = params.rok
        && !r.is_empty()
    {
        parts.push(("rok", r.clone()));
    }
    super::build_pagination_qs(&parts)
}

fn not_found_response() -> Response {
    (
        StatusCode::NOT_FOUND,
        Html("<h1>404 — Stránka nenalezena</h1>"),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::strip_trailing_year;

    #[test]
    fn strips_parenthesized_year() {
        assert_eq!(
            strip_trailing_year("Mstitel (1989)").as_deref(),
            Some("Mstitel")
        );
        assert_eq!(
            strip_trailing_year("The Matrix (1999)").as_deref(),
            Some("The Matrix")
        );
        assert_eq!(
            strip_trailing_year("Mstitel  (1989)  ").as_deref(),
            Some("Mstitel")
        );
    }

    #[test]
    fn strips_bare_year() {
        assert_eq!(
            strip_trailing_year("Mstitel 1989").as_deref(),
            Some("Mstitel")
        );
        assert_eq!(
            strip_trailing_year("Rambo II 1985").as_deref(),
            Some("Rambo II")
        );
    }

    #[test]
    fn does_not_strip_when_no_year_at_end() {
        assert_eq!(strip_trailing_year("2001: A Space Odyssey"), None);
        assert_eq!(strip_trailing_year("Matrix"), None);
        assert_eq!(strip_trailing_year("Terminator 2"), None);
    }

    #[test]
    fn does_not_strip_bare_year_alone() {
        assert_eq!(strip_trailing_year("1984"), None);
        assert_eq!(strip_trailing_year("2025"), None);
    }

    #[test]
    fn does_not_strip_when_remaining_too_short() {
        assert_eq!(strip_trailing_year("X 1989"), None);
    }
}
