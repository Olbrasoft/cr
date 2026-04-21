use super::*;
use serde::{Deserialize, Serialize};

const FILMS_PER_PAGE: i64 = 24;

/// SELECT column list for `FilmRow` queries. Kept as a const to avoid
/// duplication across `films_list`, `films_by_genre`, and `films_detail`.
const FILM_COLUMNS: &str = "f.id, f.title, f.slug, f.year, f.description, f.original_title, \
    f.imdb_rating, f.csfd_rating, f.runtime_min, f.cover_filename, \
    f.sktorrent_video_id, f.sktorrent_cdn, f.sktorrent_qualities, f.added_at, \
    f.prehrajto_url, f.prehrajto_has_dub, f.prehrajto_has_subs, f.tmdb_poster_path";

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
    /// TMDB poster_path (e.g. `/mqlg…uJ.jpg`), backfilled by
    /// `scripts/backfill-tmdb-poster-paths.py`. When set, the large-cover
    /// URL switches to the extension the path ends with (`.jpg`/`.png`) and
    /// `films_cover_large_dynamic` proxies the TMDB image. When None, the
    /// template keeps the legacy `-large.webp` URL served from R2.
    tmdb_poster_path: Option<String>,
}

impl FilmRow {
    /// Extension for the large-cover URL rendered in the detail template.
    /// Derived from `tmdb_poster_path` when the film has been backfilled;
    /// otherwise falls back to `webp` so the existing R2-backed route keeps
    /// serving until the backfill completes.
    pub fn large_url_ext(&self) -> &str {
        match self.tmdb_poster_path.as_deref() {
            Some(p) => {
                if let Some(dot) = p.rfind('.') {
                    // Strip leading dot from path suffix, e.g. "/x.jpg" -> "jpg".
                    &p[dot + 1..]
                } else {
                    "jpg"
                }
            }
            None => "webp",
        }
    }
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

    // CZ-audio / CZ-subs matching unions sktorrent (`f.has_dub`, `f.has_subtitles`)
    // with prehraj.to rollup flags (`f.prehrajto_has_dub` = CZ audio incl. CZ_NATIVE,
    // `f.prehrajto_has_subs` = CZ subs; see migration 20260508_048 note).
    fn audio_filter(&self) -> Option<&'static str> {
        let val = self.jazyk.as_deref().map(|s| s.trim()).unwrap_or("");
        if val.is_empty() || val == "vse" {
            return None;
        }
        let parts: Vec<&str> = val.split(',').map(|s| s.trim()).collect();
        let has_dub = parts.contains(&"dub") || parts.contains(&"cz") || parts.contains(&"sk");
        let has_sub = parts.contains(&"sub") || parts.contains(&"titulky");
        match (has_dub, has_sub) {
            (true, false) => Some("(f.has_dub = true OR f.prehrajto_has_dub = true)"),
            (false, true) => Some("(f.has_subtitles = true OR f.prehrajto_has_subs = true)"),
            (true, true) => Some(
                "(f.has_dub = true OR f.has_subtitles = true \
                  OR f.prehrajto_has_dub = true OR f.prehrajto_has_subs = true)",
            ),
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
    /// Gate the "Další zdroje" JS between the legacy live-scrape flow
    /// and the new DB-backed `/api/films/{id}/prehrajto-sources`
    /// endpoint (issue #521). Template renders this into a JS
    /// boolean — flipping `PREHRAJTO_SOURCES_FROM_DB` at the
    /// process env + restart is all it takes to roll back.
    prehrajto_sources_from_db: bool,
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

    // Search filter. Primary pattern uses ILIKE on title/original_title. If
    // that returns zero rows for a non-trivial query, fall back to matching
    // against CONCAT_WS(' ', title, year::text) with the paren-stripped
    // query — lets "Mstitel (1989)", "Marvinův pokoj 1996)", and partial
    // "Marvinův pokoj 19" resolve. Mirrors the autocomplete API behavior.
    let raw_q = params.q.as_deref().map(str::trim).filter(|t| t.len() >= 2);
    let raw_pattern = raw_q.map(|t| format!("%{t}%"));

    let include = params.include_genres();
    let exclude = params.exclude_genres();
    let year_f = params.year_filter();

    let (total_count, films) = run_films_query(
        &state.db,
        FilmsSearchMode::Primary,
        raw_pattern.as_deref(),
        &params,
        &include,
        &exclude,
        year_f,
        order,
        offset,
    )
    .await?;

    let (total_count, films) = match (total_count, raw_q) {
        (0, Some(q)) => {
            let normalized = normalize_query(q);
            // Same min-length guard as the autocomplete API — avoid running
            // a full-table scan with `%x%` when normalization collapsed the
            // query to something trivial like "1" (from "(1").
            if normalized.chars().count() < 2 {
                (total_count, films)
            } else {
                let fb_pattern = format!("%{normalized}%");
                run_films_query(
                    &state.db,
                    FilmsSearchMode::TitleYear,
                    Some(&fb_pattern),
                    &params,
                    &include,
                    &exclude,
                    year_f,
                    order,
                    offset,
                )
                .await?
            }
        }
        _ => (total_count, films),
    };
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
    // Large cover dynamically proxied from TMDB — real extension in URL
    // so the response content type matches what the template rendered.
    // See `films_cover_large_dynamic` for the fallback chain.
    if slug.ends_with("-large.jpg") || slug.ends_with("-large.png") {
        return films_cover_large_dynamic(State(state), Path(slug)).await;
    }
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
        prehrajto_sources_from_db: state.config.prehrajto_sources_from_db,
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
/// Two-stage search:
/// 1. Primary: ILIKE against `title` / `original_title`. Handles the common
///    case and titles that literally contain digits ("1984", "2001: A Space
///    Odyssey").
/// 2. Fallback (only when primary returns zero): strip parens and collapse
///    whitespace from the query, then ILIKE against the virtual expression
///    `CONCAT_WS(' ', title, year)`. Lets "Marvinův pokoj 1996", "Marvinův
///    pokoj (1996", "Marvinův pokoj 1996)", and even partial "Marvinův pokoj
///    19" resolve to the same film — no schema/view needed, Postgres
///    evaluates the expression at query time.
pub async fn films_search(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    let q = params.get("q").map(|s| s.trim()).unwrap_or("");
    if q.len() < 2 {
        return Ok(axum::Json(Vec::<SearchResult>::new()).into_response());
    }

    let mut rows = search_films_by_title(&state.db, q).await?;
    if rows.is_empty() {
        let normalized = normalize_query(q);
        // Skip fallback for collapsed queries like "(1" → "1" — `%1%` would
        // bloat results with everything containing a "1" in title or year.
        if normalized.chars().count() >= 2 {
            rows = search_films_by_title_year(&state.db, &normalized).await?;
        }
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

async fn search_films_by_title_year(
    db: &sqlx::PgPool,
    q: &str,
) -> Result<Vec<SearchRow>, sqlx::Error> {
    let pattern = format!("%{q}%");
    let starts_pattern = format!("{q}%");
    sqlx::query_as::<_, SearchRow>(
        "SELECT slug, title, year, imdb_rating, cover_filename \
         FROM films \
         WHERE CONCAT_WS(' ', title, year::text) ILIKE $1 \
            OR CONCAT_WS(' ', original_title, year::text) ILIKE $1 \
         ORDER BY \
           CASE WHEN CONCAT_WS(' ', title, year::text) ILIKE $2 THEN 0 \
                WHEN CONCAT_WS(' ', title, year::text) ILIKE $1 THEN 1 \
                WHEN CONCAT_WS(' ', original_title, year::text) ILIKE $2 THEN 2 \
                ELSE 3 END, \
           imdb_rating DESC NULLS LAST \
         LIMIT 10",
    )
    .bind(&pattern)
    .bind(&starts_pattern)
    .fetch_all(db)
    .await
}

/// Normalize a search query for the title+year fallback: strip parentheses
/// and collapse whitespace runs to single spaces. Handles malformed paren
/// pairs: "Title (1996" and "Title 1996)" both normalize to "Title 1996",
/// which then ILIKE-matches against `CONCAT_WS(' ', title, year)`.
fn normalize_query(q: &str) -> String {
    q.chars()
        .filter(|c| *c != '(' && *c != ')')
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Which predicate shape to use for the search filter in `films_list`.
#[derive(Clone, Copy)]
enum FilmsSearchMode {
    /// `title ILIKE $N OR original_title ILIKE $N` — primary path.
    Primary,
    /// `CONCAT_WS(' ', title, year::text) ILIKE $N OR CONCAT_WS(' ',
    /// original_title, year::text) ILIKE $N` — title+year fallback.
    TitleYear,
}

impl FilmsSearchMode {
    fn predicate(self, bind_idx: usize) -> String {
        match self {
            Self::Primary => {
                format!("(f.title ILIKE ${bind_idx} OR f.original_title ILIKE ${bind_idx})")
            }
            Self::TitleYear => format!(
                "(CONCAT_WS(' ', f.title, f.year::text) ILIKE ${bind_idx} \
                 OR CONCAT_WS(' ', f.original_title, f.year::text) ILIKE ${bind_idx})"
            ),
        }
    }
}

/// Run the count + paginated films query for `films_list`, with the chosen
/// search predicate shape (or no search filter when `search_pattern` is None).
#[allow(clippy::too_many_arguments)]
async fn run_films_query(
    db: &sqlx::PgPool,
    mode: FilmsSearchMode,
    search_pattern: Option<&str>,
    params: &FilmsQuery,
    include: &[String],
    exclude: &[String],
    year_f: Option<i16>,
    order: &str,
    offset: i64,
) -> Result<(i64, Vec<FilmRow>), sqlx::Error> {
    let mut where_parts: Vec<String> = vec![];
    let mut bind_idx = 1;

    if search_pattern.is_some() {
        where_parts.push(mode.predicate(bind_idx));
        bind_idx += 1;
    }
    if !include.is_empty() {
        if params.genre_mode_and() {
            where_parts.push(format!(
                "f.id IN (SELECT fg.film_id FROM film_genres fg \
                 JOIN genres g ON g.id = fg.genre_id \
                 WHERE g.slug = ANY(${bind_idx}) \
                 GROUP BY fg.film_id HAVING COUNT(DISTINCT g.slug) = {})",
                include.len()
            ));
        } else {
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

    let count_query = format!("SELECT count(*) as count FROM films f {where_clause}");
    let mut cq = sqlx::query_as::<_, CountRow>(&count_query);
    if let Some(p) = search_pattern {
        cq = cq.bind(p.to_string());
    }
    if !include.is_empty() {
        cq = cq.bind(include.to_vec());
    }
    if !exclude.is_empty() {
        cq = cq.bind(exclude.to_vec());
    }
    if let Some(yr) = year_f {
        cq = cq.bind(yr);
    }
    let count_row = cq.fetch_one(db).await?;

    let films_query = format!(
        "SELECT {FILM_COLUMNS} \
         FROM films f {where_clause} \
         ORDER BY {order} \
         LIMIT ${limit_idx} OFFSET ${offset_idx}",
        limit_idx = bind_idx,
        offset_idx = bind_idx + 1
    );
    let mut fq = sqlx::query_as::<_, FilmRow>(&films_query);
    if let Some(p) = search_pattern {
        fq = fq.bind(p.to_string());
    }
    if !include.is_empty() {
        fq = fq.bind(include.to_vec());
    }
    if !exclude.is_empty() {
        fq = fq.bind(exclude.to_vec());
    }
    if let Some(yr) = year_f {
        fq = fq.bind(yr);
    }
    let films = fq.bind(FILMS_PER_PAGE).bind(offset).fetch_all(db).await?;

    Ok((count_row.count.unwrap_or(0), films))
}

/// GET /filmy-online/{slug}.webp — serve WebP cover image
pub async fn films_cover(
    State(state): State<AppState>,
    Path(slug_webp): Path<String>,
) -> WebResult<Response> {
    use crate::handlers::cover_proxy::{
        fetch_cover, new_r2_key, old_r2_key, parse_cover_slug, placeholder_webp,
    };

    let (slug, _is_large) = parse_cover_slug(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        id: i32,
        cover_filename: Option<String>,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT id, cover_filename FROM films WHERE slug = $1")
        .bind(&slug)
        .fetch_optional(&state.db)
        .await?;

    let Some(row) = row else {
        return Ok(placeholder_webp());
    };

    // Try the new id-keyed layout first, then the pre-migration name-keyed
    // one. See handlers::cover_proxy for the rollout story.
    let new_key = new_r2_key("films", row.id, false);
    let old_key = row
        .cover_filename
        .as_deref()
        .map(|cf| old_r2_key("films", cf, false));
    Ok(fetch_cover(&state, &new_key, old_key.as_deref()).await)
}

/// GET /filmy-online/{slug}-large.webp — large (780×1170) cover.
///
/// Mirrors `films_cover` but targets the `{id}/cover-large.webp` key.
/// If neither the new nor the old R2 key exists we fall back to the
/// small variant — `images tmdb.org` fetching is now the import
/// pipeline's job, the handler stays stateless.
pub async fn films_cover_large(
    State(state): State<AppState>,
    Path(slug_webp): Path<String>,
) -> WebResult<Response> {
    use crate::handlers::cover_proxy::{
        new_r2_key, old_r2_key, parse_cover_slug, placeholder_webp,
    };

    let (slug, _is_large) = parse_cover_slug(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        id: i32,
        cover_filename: Option<String>,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT id, cover_filename FROM films WHERE slug = $1")
        .bind(&slug)
        .fetch_optional(&state.db)
        .await?;

    let Some(row) = row else {
        return Ok(placeholder_webp());
    };

    use crate::handlers::cover_proxy::{immutable_webp, no_store_webp, try_fetch_r2};
    // Tuple: (R2 key, is_small_fallback). Large-variant hits are cached
    // for a year (`immutable`); small-variant fallbacks are `no-store`
    // so a later-imported large cover can unseat them without a manual
    // CF purge.
    let mut candidates: Vec<(String, bool)> = vec![(new_r2_key("films", row.id, true), false)];
    candidates.push((format!("films/large/{slug}.webp"), false));
    if let Some(cf) = row.cover_filename.as_deref() {
        candidates.push((old_r2_key("films", cf, true), false));
    }
    // Small-variant fallbacks — inlined to avoid async recursion with
    // films_cover. Marked `is_small_fallback=true` so they get served
    // with `no-store`.
    candidates.push((new_r2_key("films", row.id, false), true));
    if let Some(cf) = row.cover_filename.as_deref() {
        candidates.push((old_r2_key("films", cf, false), true));
    }
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

/// GET /filmy-online/{slug}-large.{jpg,png} — proxy TMDB poster on demand.
///
/// Detail-page thumbnails get few hits, so we skip R2 storage and stream the
/// TMDB image through. Cloudflare caches the response for a year, so each
/// edge fetches TMDB at most once per (edge, poster) — the next visitor is
/// served from cache. Fallback chain when TMDB has no poster or fails:
/// small R2 cover (`no-store` so a later backfill can unseat it) → 1×1
/// placeholder.
pub async fn films_cover_large_dynamic(
    State(state): State<AppState>,
    Path(slug_ext): Path<String>,
) -> WebResult<Response> {
    use crate::handlers::cover_proxy::{
        new_r2_key, no_store_webp, old_r2_key, placeholder_webp, try_fetch_r2,
    };

    // Strip `-large.jpg` / `-large.png`, whichever the request carried.
    let slug = slug_ext
        .strip_suffix("-large.jpg")
        .or_else(|| slug_ext.strip_suffix("-large.png"))
        .unwrap_or(&slug_ext);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        id: i32,
        cover_filename: Option<String>,
        tmdb_poster_path: Option<String>,
    }

    let row = sqlx::query_as::<_, CoverRow>(
        "SELECT id, cover_filename, tmdb_poster_path FROM films WHERE slug = $1",
    )
    .bind(slug)
    .fetch_optional(&state.db)
    .await?;

    let Some(row) = row else {
        return Ok(placeholder_webp());
    };

    // 1) TMDB proxy via image.tmdb.org. Preserve whatever content type
    //    TMDB returns (jpg/png) so the client decodes correctly — the URL
    //    suffix we route on was chosen from the same poster_path.
    if let Some(path) = row.tmdb_poster_path.as_deref() {
        let url = format!("https://image.tmdb.org/t/p/w780{path}");
        if let Ok(resp) = state
            .http_client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            && resp.status().is_success()
        {
            let ct = resp
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "image/jpeg".to_string());
            if let Ok(bytes) = resp.bytes().await {
                return Ok((
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
                    .into_response());
            }
        }
    }

    // 2) Small R2 cover as a transient fallback — keeps something visible
    //    while TMDB is unreachable or the row has no poster_path yet.
    let small_candidates: Vec<String> = std::iter::once(new_r2_key("films", row.id, false))
        .chain(
            row.cover_filename
                .as_deref()
                .map(|cf| old_r2_key("films", cf, false)),
        )
        .collect();
    for key in &small_candidates {
        if let Some(bytes) = try_fetch_r2(&state, key).await {
            // `no-store` on purpose: TMDB is the canonical source once the
            // row is backfilled — we want a subsequent request to try TMDB
            // again instead of serving the small cover from cache for a year.
            return Ok(no_store_webp(bytes));
        }
    }

    Ok(placeholder_webp())
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
    use super::{FilmsQuery, normalize_query};

    fn query_with_jazyk(jazyk: Option<&str>) -> FilmsQuery {
        FilmsQuery {
            strana: None,
            razeni: None,
            zanry: None,
            bez: None,
            q: None,
            rok: None,
            rezim: None,
            smer: None,
            jazyk: jazyk.map(|s| s.to_string()),
        }
    }

    #[test]
    fn audio_filter_dub_unions_prehrajto() {
        let q = query_with_jazyk(Some("dub"));
        assert_eq!(
            q.audio_filter(),
            Some("(f.has_dub = true OR f.prehrajto_has_dub = true)")
        );
    }

    #[test]
    fn audio_filter_sub_unions_prehrajto() {
        let q = query_with_jazyk(Some("sub"));
        assert_eq!(
            q.audio_filter(),
            Some("(f.has_subtitles = true OR f.prehrajto_has_subs = true)")
        );
    }

    #[test]
    fn audio_filter_dub_and_sub_unions_both() {
        let q = query_with_jazyk(Some("dub,sub"));
        let sql = q.audio_filter().expect("expected filter for dub,sub");
        assert!(sql.contains("f.has_dub = true"), "sql = {sql}");
        assert!(sql.contains("f.has_subtitles = true"), "sql = {sql}");
        assert!(sql.contains("f.prehrajto_has_dub = true"), "sql = {sql}");
        assert!(sql.contains("f.prehrajto_has_subs = true"), "sql = {sql}");
    }

    #[test]
    fn audio_filter_vse_is_none() {
        assert!(query_with_jazyk(Some("vse")).audio_filter().is_none());
    }

    #[test]
    fn audio_filter_missing_or_empty_is_none() {
        assert!(query_with_jazyk(None).audio_filter().is_none());
        assert!(query_with_jazyk(Some("")).audio_filter().is_none());
        assert!(query_with_jazyk(Some("   ")).audio_filter().is_none());
    }

    #[test]
    fn strips_parens_complete_pair() {
        assert_eq!(normalize_query("Mstitel (1989)"), "Mstitel 1989");
        assert_eq!(normalize_query("The Matrix (1999)"), "The Matrix 1999");
    }

    #[test]
    fn strips_parens_malformed() {
        assert_eq!(
            normalize_query("Marvinův pokoj (1996"),
            "Marvinův pokoj 1996"
        );
        assert_eq!(
            normalize_query("Marvinův pokoj 1996)"),
            "Marvinův pokoj 1996"
        );
    }

    #[test]
    fn collapses_whitespace() {
        assert_eq!(normalize_query("Mstitel    1989"), "Mstitel 1989");
        assert_eq!(normalize_query("  Mstitel  (1989)  "), "Mstitel 1989");
    }

    #[test]
    fn preserves_partial_year() {
        assert_eq!(normalize_query("Marvinův pokoj 19"), "Marvinův pokoj 19");
        assert_eq!(normalize_query("Marvinův pokoj 1"), "Marvinův pokoj 1");
        assert_eq!(normalize_query("Marvinův pokoj (19"), "Marvinův pokoj 19");
    }

    #[test]
    fn preserves_ordinary_queries() {
        assert_eq!(normalize_query("Marvinův pokoj"), "Marvinův pokoj");
        assert_eq!(
            normalize_query("2001: A Space Odyssey"),
            "2001: A Space Odyssey"
        );
        assert_eq!(normalize_query("1984"), "1984");
    }

    #[test]
    fn empty_when_only_whitespace_or_parens() {
        assert_eq!(normalize_query(""), "");
        assert_eq!(normalize_query("   "), "");
        assert_eq!(normalize_query("()"), "");
        assert_eq!(normalize_query("(  )"), "");
    }
}
