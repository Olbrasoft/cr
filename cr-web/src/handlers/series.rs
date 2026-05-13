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

/// Column list for `EpisodeRow` queries. After the #611 reader switch this
/// projects the same field names as the legacy `episodes` columns but reads
/// each provider attribute from `video_sources`. The FilmRow counterpart
/// in `films.rs` uses the same pattern — see there for performance notes.
///
/// Scope: series episodes only (provider_id from `sktorrent` / `prehrajto`
/// joined to `video_sources.episode_id`). sledujteto has no series support
/// in legacy, so no sledujteto subquery here.
const EPISODE_COLUMNS: &str = "e.id, e.season, e.episode, e.title, \
    (SELECT vs.external_id::INTEGER \
       FROM video_sources vs \
       JOIN video_providers p ON p.id = vs.provider_id \
      WHERE vs.episode_id = e.id AND p.slug = 'sktorrent' \
        AND vs.is_primary AND vs.is_alive \
      LIMIT 1) AS sktorrent_video_id, \
    (SELECT vs.cdn::SMALLINT \
       FROM video_sources vs \
       JOIN video_providers p ON p.id = vs.provider_id \
      WHERE vs.episode_id = e.id AND p.slug = 'sktorrent' \
        AND vs.is_primary AND vs.is_alive \
      LIMIT 1) AS sktorrent_cdn, \
    (SELECT vs.metadata->>'qualities' \
       FROM video_sources vs \
       JOIN video_providers p ON p.id = vs.provider_id \
      WHERE vs.episode_id = e.id AND p.slug = 'sktorrent' \
        AND vs.is_primary AND vs.is_alive \
      LIMIT 1) AS sktorrent_qualities, \
    e.episode_name, e.overview, e.air_date, e.runtime, e.still_filename, \
    (SELECT REPLACE(COALESCE(vs.metadata->>'url', 'https://prehraj.to/' || vs.external_id), \
                    'https://prehrajto.cz/', 'https://prehraj.to/') \
       FROM video_sources vs \
       JOIN video_providers p ON p.id = vs.provider_id \
      WHERE vs.episode_id = e.id AND p.slug = 'prehrajto' AND vs.is_alive \
      ORDER BY vs.is_primary DESC, vs.updated_at DESC \
      LIMIT 1) AS prehrajto_url, \
    false AS prehrajto_has_dub, \
    false AS prehrajto_has_subs, \
    e.slug";

/// Predicate that gates whether an episode has any playable source at all.
/// Used instead of the legacy `(sktorrent_video_id IS NOT NULL OR prehrajto_url
/// IS NOT NULL)` OR-chain. Prevents zombie episodes (TMDB stubs without any
/// source) from rendering.
const EPISODE_HAS_SOURCE_PREDICATE: &str = "EXISTS (SELECT 1 FROM video_sources vs \
                                                    WHERE vs.episode_id = e.id AND vs.is_alive)";

#[derive(FromRow, Serialize)]
pub struct SeriesRow {
    id: i32,
    title: String,
    slug: String,
    first_air_year: Option<i16>,
    // Used by Askama template (series_detail.html) for year range display
    last_air_year: Option<i16>,
    description: Option<String>,
    original_title: Option<String>,
    tmdb_rating: Option<f32>,
    imdb_rating: Option<f32>,
    csfd_rating: Option<i16>,
    #[allow(dead_code)] // Not rendered in current templates; kept for future series stats
    season_count: Option<i16>,
    #[allow(dead_code)] // Not rendered in current templates; kept for future series stats
    episode_count: Option<i16>,
    #[allow(dead_code)] // Needed in SELECT for ORDER BY; not rendered in templates
    added_at: Option<chrono::DateTime<chrono::Utc>>,
    /// TMDB poster_path (e.g. `/mqlg…uJ.jpg`), backfilled by
    /// `scripts/backfill-tmdb-poster-paths.py --table series`. When set,
    /// `large_url_ext()` flips the extension to `.jpg`/`.png`, which
    /// `series_cover_large_dynamic` proxies from TMDB. Otherwise falls back
    /// to the R2-backed `-large.webp` URL.
    tmdb_poster_path: Option<String>,
}

impl SeriesRow {
    /// Extension for the large-cover URL rendered in the detail template.
    /// Derived from `tmdb_poster_path` when the series has been backfilled;
    /// otherwise falls back to `webp` so the existing R2-backed route keeps
    /// serving until the backfill completes.
    ///
    /// Only `jpg` and `png` are whitelisted — `series_resolve` dispatches
    /// exactly those two large-cover extensions to the dynamic proxy, and
    /// TMDB's in-practice storage is always JPG. Unknown/unexpected suffixes
    /// get normalized to `jpg` rather than falling through to the HTML handler.
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

/// Episode card shown on list pages — one latest episode per series,
/// sorted by added_at DESC. Layout mirrors bombuj.si: series cover + title
/// + "Epizoda S×E" badge + CC (subtitles) badge.
#[derive(FromRow, Serialize)]
pub struct EpisodeCardRow {
    #[allow(dead_code)] // Primary key; not rendered in series_list template
    pub id: i32,
    pub series_id: i32,
    pub series_slug: String,
    pub series_title: String,
    pub series_original_title: Option<String>,
    pub series_first_air_year: Option<i16>,
    pub series_tmdb_rating: Option<f32>,
    pub series_imdb_rating: Option<f32>,
    pub series_csfd_rating: Option<i16>,
    pub series_description: Option<String>,
    pub season: i16,
    pub episode: i16,
    pub has_subtitles: Option<bool>,
    pub has_dub: Option<bool>,
    #[allow(dead_code)] // Used in ORDER BY; not rendered in series_list template
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub episode_slug: Option<String>,
    pub episode_name: Option<String>,
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
    pub slug: Option<String>,
}

#[derive(FromRow)]
struct GenreRow {
    id: i32,
    slug: String,
    name_cs: String,
}

impl GenreRow {
    /// Pretty Czech plural title for headings and SEO
    /// (e.g. "Akční seriály", "Dramata", "Horory").
    fn pretty_plural(&self) -> String {
        let known: &str = match self.slug.as_str() {
            "akcni" => "Akční seriály",
            "animovany" => "Animované seriály",
            "dobrodruzny" => "Dobrodružné seriály",
            "dokumentarni" => "Dokumentární seriály",
            "drama" => "Dramata",
            "fantasy" => "Fantasy seriály",
            "historicky" => "Historické seriály",
            "horor" => "Horory",
            "hudebni" => "Hudební seriály",
            "komedie" => "Komedie",
            "krimi" => "Kriminální seriály",
            "mysteriozni" => "Mysteriózní seriály",
            "rodinny" => "Rodinné seriály",
            "romanticky" => "Romantické seriály",
            "sci-fi" => "Sci-Fi seriály",
            "thriller" => "Thrillery",
            "tv-film" => "Televizní seriály",
            "valecny" => "Válečné seriály",
            "western" => "Westerny",
            _ => "",
        };
        if known.is_empty() {
            format!("{} seriály", self.name_cs)
        } else {
            known.to_string()
        }
    }
}

#[derive(FromRow)]
struct SeriesGenreNameRow {
    name_cs: String,
    #[allow(dead_code)]
    slug: String,
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
    zanry: Option<String>, // comma-separated include
    bez: Option<String>,   // comma-separated exclude
    rok: Option<String>,   // year filter
    rezim: Option<String>, // "and" / "or"
    smer: Option<String>,  // "asc" / "desc"
    // #716 — audio-language filter. Same `audio=cs,sk,en` shape as
    // FilmsQuery, but with a coverage-mode switch unique to series:
    // language is a property of the video source on each episode, not
    // of the series itself, so a series can be partially or fully in a
    // language. `audio_mode=any` (default) means "at least one episode
    // has a source in every selected language"; `audio_mode=all` means
    // "every episode has a source in every selected language".
    audio: Option<String>,
    audio_mode: Option<String>,
}

impl SeriesQuery {
    fn page(&self) -> i64 {
        self.strana.unwrap_or(1).max(1)
    }

    fn sort_desc(&self) -> bool {
        self.smer.as_deref() != Some("asc")
    }

    fn genre_mode_and(&self) -> bool {
        self.rezim.as_deref() == Some("and")
    }

    fn order_clause(&self) -> &'static str {
        let desc = self.sort_desc();
        match (self.razeni.as_deref(), desc) {
            (Some("rok"), true) => "s.first_air_year DESC NULLS LAST, s.title",
            (Some("rok"), false) => "s.first_air_year ASC NULLS LAST, s.title",
            // `imdb` and `tmdb` are sibling URL keys, each sorting by its
            // own *_rating column (#701). Pre-#701 `razeni=imdb` was a
            // legacy alias for tmdb_rating; the imdb_rating backfill in
            // #690 retired that alias.
            (Some("imdb"), true) => "s.imdb_rating DESC NULLS LAST, s.title",
            (Some("imdb"), false) => "s.imdb_rating ASC NULLS LAST, s.title",
            (Some("tmdb"), true) => "s.tmdb_rating DESC NULLS LAST, s.title",
            (Some("tmdb"), false) => "s.tmdb_rating ASC NULLS LAST, s.title",
            (Some("nazev"), true) => "s.title DESC",
            (Some("nazev"), false) => "s.title ASC",
            (_, true) => "s.added_at DESC NULLS LAST, s.title",
            (_, false) => "s.added_at ASC NULLS LAST, s.title",
        }
    }

    fn sort_key(&self) -> &str {
        self.razeni.as_deref().unwrap_or("pridano")
    }

    /// Credibility threshold for rating-sort listings (#704). Mirrors
    /// FilmsQuery::votes_threshold_predicate — see comment there.
    fn votes_threshold_predicate(&self) -> Option<&'static str> {
        match self.razeni.as_deref() {
            Some("imdb") => Some("s.imdb_votes >= 500"),
            Some("tmdb") => Some("s.tmdb_vote_count >= 50"),
            _ => None,
        }
    }

    /// Whether the user picked a non-default sort that the latest-episode
    /// listing can't honour (it's hard-ordered by `created_at`). When true
    /// and no genre/year filters are active, the handler switches to a
    /// series-by-`order_clause` query so the page actually reorders rather
    /// than silently ignoring `razeni=` (parity with tv_porady #702 review).
    fn wants_shows_mode(&self) -> bool {
        matches!(
            self.razeni.as_deref(),
            Some("rok") | Some("imdb") | Some("tmdb") | Some("nazev")
        )
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

    /// Parse `audio=cs,sk,en` into validated ISO-639 codes. Mirrors
    /// FilmsQuery::audio_langs_filter — the regex-shape guard avoids
    /// dirty values in the GIN-array binding even though the binding
    /// itself is parameterised.
    pub(crate) fn audio_langs_filter(&self) -> Option<Vec<String>> {
        let raw = self.audio.as_deref()?.trim();
        if raw.is_empty() {
            return None;
        }
        let langs: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty() && s.len() <= 3 && s.chars().all(|c| c.is_ascii_lowercase()))
            .collect();
        if langs.is_empty() { None } else { Some(langs) }
    }

    /// `true` when the user picked the strict "every episode has it"
    /// mode. Default (None or anything else) is the permissive `any`
    /// mode that hits `series.audio_langs_any`.
    pub(crate) fn audio_mode_is_all(&self) -> bool {
        self.audio_mode.as_deref() == Some("all")
    }

    /// Column name on `series` to apply `@>` against. Centralised so
    /// every WHERE-clause builder picks the same one.
    pub(crate) fn audio_rollup_column(&self) -> &'static str {
        if self.audio_mode_is_all() {
            "s.audio_langs_all"
        } else {
            "s.audio_langs_any"
        }
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
    #[allow(dead_code)] // TODO: verify usage — may be needed for sort UI active state
    sort_key: String,
    query_string: String,
    search_query: Option<String>,
    open_filter: bool,
    /// Genre slugs the user is filtering by right now (from `?zanry=` on main page).
    selected_genre_slugs: Vec<String>,
    /// Genres per series, keyed by series id — rendered as chips in desktop list view.
    series_genres_map: std::collections::HashMap<i32, Vec<SeriesGenreNameRow>>,
    /// Audio-language filter (#716). Comma-separated ISO codes selected by user.
    selected_audio_langs: Vec<String>,
    /// `true` when `audio_mode=all` is active (strict "every episode has it").
    /// `false` is the permissive "any episode" default.
    audio_mode_all: bool,
}

impl SeriesListTemplate {
    fn is_selected(&self, slug: &str) -> bool {
        self.selected_genre_slugs.iter().any(|s| s == slug)
    }
    fn is_multi_filter_mode(&self) -> bool {
        self.current_genre.is_some() || !self.selected_genre_slugs.is_empty()
    }
    fn is_current_genre(&self, g: &GenreRow) -> bool {
        self.current_genre.as_ref().is_some_and(|cg| cg.id == g.id)
    }
    fn is_audio_selected(&self, lang: &str) -> bool {
        self.selected_audio_langs.iter().any(|l| l == lang)
    }
    fn has_audio_filter(&self) -> bool {
        !self.selected_audio_langs.is_empty()
    }
    // NOTE: Askama auto-refs arguments in template method calls — `{{ self.series_genres(s.id) }}`
    // generates a call with `&s.id`. Keep the signature as `&i32` to match; Copilot's
    // "accept i32 by value" suggestion would break Askama codegen here.
    fn series_genres(&self, series_id: &i32) -> &[SeriesGenreNameRow] {
        static EMPTY: Vec<SeriesGenreNameRow> = Vec::new();
        self.series_genres_map
            .get(series_id)
            .map(|v| v.as_slice())
            .unwrap_or(EMPTY.as_slice())
    }
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
    pub slug: Option<String>,
}

#[derive(FromRow, Clone)]
pub struct PersonRow {
    #[allow(dead_code)] // Primary key; not rendered in templates
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

    let include = params.include_genres();
    let exclude = params.exclude_genres();
    let year_f = params.year_filter();
    let audio_langs = params.audio_langs_filter();
    let audio_col = params.audio_rollup_column();

    // Search mode: show series results (by title). No search: show latest-
    // episode-per-series grid (bombuj.si style).
    //
    // Search uses `unaccent()` so "laska nebeska" matches "Láska nebeská"
    // (#673). Diacritic-exact ILIKE hits are pushed in front of unaccent-
    // only hits via the leading CASE in ORDER BY — when the user typed
    // diacritics, rows whose title literally contains them rank first.
    let (total_count, series, episodes) = if let Some(ref pattern) = search_q {
        // Apply the #704 vote-count threshold here too — Copilot review on
        // PR #710 caught that a `razeni=imdb` search would otherwise still
        // surface low-vote outliers at the top of the result list.
        let votes_clause = params
            .votes_threshold_predicate()
            .map(|p| format!(" AND {p}"))
            .unwrap_or_default();
        // #716 audio filter. $1 is the search pattern, $2 the audio array
        // (when present), then LIMIT/OFFSET trail. We thread the audio
        // bind index dynamically so the same SQL string serves both
        // "filter present" and "no filter" cases.
        let (audio_clause, limit_idx, offset_idx) = if audio_langs.is_some() {
            (format!(" AND {audio_col} @> $2::TEXT[]"), 3, 4)
        } else {
            (String::new(), 2, 3)
        };
        let count_query = format!(
            "SELECT count(*) as count FROM series s \
             WHERE (unaccent(s.title) ILIKE unaccent($1) \
                OR unaccent(s.original_title) ILIKE unaccent($1)){votes_clause}{audio_clause}"
        );
        let mut cq = sqlx::query_as::<_, CountRow>(&count_query).bind(pattern);
        if let Some(ref langs) = audio_langs {
            cq = cq.bind(langs.clone());
        }
        let count_row = cq.fetch_one(&state.db).await?;

        let query = format!(
            "SELECT s.id, s.title, s.slug, s.first_air_year, s.last_air_year, \
             s.description, s.original_title, s.tmdb_rating, s.imdb_rating, s.csfd_rating, \
             s.season_count, s.episode_count, s.added_at, \
             s.tmdb_poster_path \
             FROM series s \
             WHERE (unaccent(s.title) ILIKE unaccent($1) \
                OR unaccent(s.original_title) ILIKE unaccent($1)){votes_clause}{audio_clause} \
             ORDER BY \
               (CASE WHEN s.title ILIKE $1 OR s.original_title ILIKE $1 THEN 0 ELSE 1 END), \
               {order} LIMIT ${limit_idx} OFFSET ${offset_idx}"
        );
        let mut rq = sqlx::query_as::<_, SeriesRow>(&query).bind(pattern);
        if let Some(ref langs) = audio_langs {
            rq = rq.bind(langs.clone());
        }
        let rows = rq
            .bind(SERIES_PER_PAGE)
            .bind(offset)
            .fetch_all(&state.db)
            .await?;
        (count_row.count.unwrap_or(0), rows, Vec::new())
    } else if params.wants_shows_mode() {
        // Non-default sort (rok / imdb / tmdb / nazev): drop the latest-
        // episode listing (which is hard-ordered by `created_at` and would
        // silently ignore razeni) and run a series-by-`order_clause` query
        // instead. Pre-fix this only kicked in when no genre/year filters
        // were active; filters used to fall through to the latest-episode
        // branch which also ignored razeni. Now filters are pushed down
        // into the shows query so /serialy-online/sci-fi/?razeni=tmdb
        // actually reorders.
        let (rows, total) = run_shows_mode_query(
            &state.db,
            order,
            &include,
            params.genre_mode_and(),
            &exclude,
            year_f,
            audio_langs.as_deref(),
            audio_col,
            params.votes_threshold_predicate(),
            offset,
        )
        .await?;
        (total, rows, Vec::new())
    } else if include.is_empty() && exclude.is_empty() && year_f.is_none() && audio_langs.is_none()
    {
        // Default home listing (no filters): latest episode per series
        let count_row = sqlx::query_as::<_, CountRow>(
            "SELECT count(DISTINCT e.series_id) as count FROM episodes e",
        )
        .fetch_one(&state.db)
        .await?;

        let episodes = fetch_latest_episode_cards(
            &state,
            &[],
            false,
            &[],
            None,
            None,
            audio_col,
            params.sort_desc(),
            SERIES_PER_PAGE,
            offset,
        )
        .await?;
        (count_row.count.unwrap_or(0), Vec::new(), episodes)
    } else {
        // Filters active on the all-series page
        let count_row = count_filtered_series(
            &state,
            &include,
            params.genre_mode_and(),
            &exclude,
            year_f,
            audio_langs.as_deref(),
            audio_col,
        )
        .await?;
        let episodes = fetch_latest_episode_cards(
            &state,
            &include,
            params.genre_mode_and(),
            &exclude,
            year_f,
            audio_langs.as_deref(),
            audio_col,
            params.sort_desc(),
            SERIES_PER_PAGE,
            offset,
        )
        .await?;
        (count_row, Vec::new(), episodes)
    };

    let total_pages = (total_count as f64 / SERIES_PER_PAGE as f64).ceil() as i64;

    let genres = sqlx::query_as::<_, GenreRow>(
        "SELECT g.id, g.slug, g.name_cs FROM genres g \
         JOIN series_genres sg ON g.id = sg.genre_id \
         GROUP BY g.id, g.slug, g.name_cs ORDER BY g.name_cs",
    )
    .fetch_all(&state.db)
    .await?;

    let query_string = build_series_query_string(&params);

    let search_query = params.q.clone().and_then(|q| {
        let t = q.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });

    let selected_genre_slugs: Vec<String> = include.clone();
    let selected_audio_langs: Vec<String> = audio_langs.clone().unwrap_or_default();
    let open_filter = !selected_genre_slugs.is_empty() || !selected_audio_langs.is_empty();
    let series_genres_map = load_series_genres_map(&state.db, &series, &episodes).await?;

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
        open_filter,
        selected_genre_slugs,
        series_genres_map,
        selected_audio_langs,
        audio_mode_all: params.audio_mode_is_all(),
    };
    let html = tmpl.render()?;
    // Search-result HTML is `?q=`-derived: tag it `private,
    // max-age=60` so the browser may reuse it for a minute on
    // back-button / repeat-search, but no shared cache stores
    // it. Gate matches the actual search predicate above
    // (`search_q` filters `len() >= 2`) — `?q=a` still gets
    // the default cacheable listing.
    if is_active_search(params.q.as_deref()) {
        Ok(super::search_cached_html(html))
    } else {
        Ok(Html(html).into_response())
    }
}

/// True when `?q=…` is a real search query — same trim+length gate
/// the search predicate uses (`search_q` filters `t.len() >= 2`,
/// byte length). Single source of truth so the search-cache branch
/// and the predicate gate can't drift apart on multibyte chars.
fn is_active_search(q: Option<&str>) -> bool {
    q.map(str::trim).is_some_and(|t| t.len() >= 2)
}

/// Series-by-`order_clause` query with optional include/exclude/year
/// filters plus the #704 vote-count gate. Runs whenever
/// `params.wants_shows_mode()` is true so /serialy-online/sci-fi/
/// ?razeni=tmdb actually reorders by rating rather than silently falling
/// back to the latest-episode listing's `created_at DESC`.
#[allow(clippy::too_many_arguments)]
async fn run_shows_mode_query(
    db: &sqlx::PgPool,
    order: &str,
    include_slugs: &[String],
    include_mode_and: bool,
    exclude_slugs: &[String],
    year_f: Option<i16>,
    audio_langs: Option<&[String]>,
    audio_col: &str,
    votes_predicate: Option<&str>,
    offset: i64,
) -> WebResult<(Vec<SeriesRow>, i64)> {
    let mut where_parts: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;
    if !include_slugs.is_empty() {
        if include_mode_and {
            where_parts.push(format!(
                "s.id IN (SELECT sg.series_id FROM series_genres sg \
                 JOIN genres g ON g.id = sg.genre_id \
                 WHERE g.slug = ANY(${bind_idx}) \
                 GROUP BY sg.series_id HAVING COUNT(DISTINCT g.slug) = {})",
                include_slugs.len()
            ));
        } else {
            where_parts.push(format!(
                "s.id IN (SELECT sg.series_id FROM series_genres sg \
                 JOIN genres g ON g.id = sg.genre_id \
                 WHERE g.slug = ANY(${bind_idx}))"
            ));
        }
        bind_idx += 1;
    }
    if !exclude_slugs.is_empty() {
        where_parts.push(format!(
            "s.id NOT IN (SELECT sg2.series_id FROM series_genres sg2 \
             JOIN genres g2 ON g2.id = sg2.genre_id \
             WHERE g2.slug = ANY(${bind_idx}))"
        ));
        bind_idx += 1;
    }
    if year_f.is_some() {
        where_parts.push(format!("s.first_air_year = ${bind_idx}"));
        bind_idx += 1;
    }
    if audio_langs.is_some() {
        where_parts.push(format!("{audio_col} @> ${bind_idx}::TEXT[]"));
        bind_idx += 1;
    }
    if let Some(p) = votes_predicate {
        where_parts.push(p.to_string());
    }

    let where_clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_parts.join(" AND "))
    };

    let count_sql = format!("SELECT count(*) as count FROM series s {where_clause}");
    let mut cq = sqlx::query_as::<_, CountRow>(&count_sql);
    if !include_slugs.is_empty() {
        cq = cq.bind(include_slugs.to_vec());
    }
    if !exclude_slugs.is_empty() {
        cq = cq.bind(exclude_slugs.to_vec());
    }
    if let Some(yr) = year_f {
        cq = cq.bind(yr);
    }
    if let Some(langs) = audio_langs {
        cq = cq.bind(langs.to_vec());
    }
    let count_row = cq.fetch_one(db).await?;

    let rows_sql = format!(
        "SELECT s.id, s.title, s.slug, s.first_air_year, s.last_air_year, \
         s.description, s.original_title, s.tmdb_rating, s.imdb_rating, s.csfd_rating, \
         s.season_count, s.episode_count, s.added_at, \
         s.tmdb_poster_path \
         FROM series s {where_clause} \
         ORDER BY {order} LIMIT ${limit_idx} OFFSET ${offset_idx}",
        limit_idx = bind_idx,
        offset_idx = bind_idx + 1,
    );
    let mut rq = sqlx::query_as::<_, SeriesRow>(&rows_sql);
    if !include_slugs.is_empty() {
        rq = rq.bind(include_slugs.to_vec());
    }
    if !exclude_slugs.is_empty() {
        rq = rq.bind(exclude_slugs.to_vec());
    }
    if let Some(yr) = year_f {
        rq = rq.bind(yr);
    }
    if let Some(langs) = audio_langs {
        rq = rq.bind(langs.to_vec());
    }
    let rows = rq.bind(SERIES_PER_PAGE).bind(offset).fetch_all(db).await?;
    Ok((rows, count_row.count.unwrap_or(0)))
}

/// Latest-episode-per-series query. Supports include/exclude genre slug lists
/// (OR / AND mode) + optional year filter. `series.id` is carried through so
/// list-view can look up genre chips per series.
#[allow(clippy::too_many_arguments)]
async fn fetch_latest_episode_cards(
    state: &AppState,
    include_slugs: &[String],
    include_mode_and: bool,
    exclude_slugs: &[String],
    year_f: Option<i16>,
    audio_langs: Option<&[String]>,
    audio_col: &str,
    desc: bool,
    limit: i64,
    offset: i64,
) -> WebResult<Vec<EpisodeCardRow>> {
    let mut where_parts: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 3; // $1 = limit, $2 = offset
    if !include_slugs.is_empty() {
        if include_mode_and {
            where_parts.push(format!(
                "s.id IN (SELECT sg.series_id FROM series_genres sg \
                 JOIN genres g ON g.id = sg.genre_id \
                 WHERE g.slug = ANY(${bind_idx}) \
                 GROUP BY sg.series_id HAVING COUNT(DISTINCT g.slug) = {})",
                include_slugs.len()
            ));
        } else {
            where_parts.push(format!(
                "s.id IN (SELECT sg.series_id FROM series_genres sg \
                 JOIN genres g ON g.id = sg.genre_id \
                 WHERE g.slug = ANY(${bind_idx}))"
            ));
        }
        bind_idx += 1;
    }
    if !exclude_slugs.is_empty() {
        where_parts.push(format!(
            "s.id NOT IN (SELECT sg2.series_id FROM series_genres sg2 \
             JOIN genres g2 ON g2.id = sg2.genre_id \
             WHERE g2.slug = ANY(${bind_idx}))"
        ));
        bind_idx += 1;
    }
    if year_f.is_some() {
        where_parts.push(format!("s.first_air_year = ${bind_idx}"));
        bind_idx += 1;
    }
    if audio_langs.is_some() {
        where_parts.push(format!("{audio_col} @> ${bind_idx}::TEXT[]"));
    }
    let series_filter = if where_parts.is_empty() {
        String::new()
    } else {
        format!("AND {}", where_parts.join(" AND "))
    };

    // razeni=pridano honors smer (#serialy-razeni fix). Pre-fix the outer
    // ORDER BY was hardcoded DESC, so `?smer=asc` toggled the chip but the
    // grid stayed the same.
    let direction = if desc { "DESC" } else { "ASC" };
    let sql = format!(
        "WITH per_series AS ( \
            SELECT DISTINCT ON (e.series_id) \
                e.id, e.series_id, e.season, e.episode, e.has_subtitles, e.has_dub, e.created_at \
            FROM episodes e \
            JOIN series s ON s.id = e.series_id \
            WHERE 1=1 {series_filter} \
            ORDER BY e.series_id, e.created_at DESC \
         ) \
         SELECT ps.id, \
            s.id AS series_id, \
            s.slug AS series_slug, \
            s.title AS series_title, \
            s.original_title AS series_original_title, \
            s.first_air_year AS series_first_air_year, \
            s.tmdb_rating AS series_tmdb_rating, \
            s.imdb_rating AS series_imdb_rating, \
            s.csfd_rating AS series_csfd_rating, \
            s.description AS series_description, \
            ps.season, ps.episode, ps.has_subtitles, ps.has_dub, ps.created_at, \
            (SELECT e2.slug FROM episodes e2 WHERE e2.id = ps.id) AS episode_slug, \
            (SELECT e2.episode_name FROM episodes e2 WHERE e2.id = ps.id) AS episode_name \
         FROM per_series ps \
         JOIN series s ON s.id = ps.series_id \
         ORDER BY ps.created_at {direction} NULLS LAST \
         LIMIT $1 OFFSET $2"
    );

    let mut q = sqlx::query_as::<_, EpisodeCardRow>(&sql)
        .bind(limit)
        .bind(offset);
    if !include_slugs.is_empty() {
        q = q.bind(include_slugs.to_vec());
    }
    if !exclude_slugs.is_empty() {
        q = q.bind(exclude_slugs.to_vec());
    }
    if let Some(yr) = year_f {
        q = q.bind(yr);
    }
    if let Some(langs) = audio_langs {
        q = q.bind(langs.to_vec());
    }
    Ok(q.fetch_all(&state.db).await?)
}

/// Count series matching include/exclude/year/audio filters (for pagination).
#[allow(clippy::too_many_arguments)]
async fn count_filtered_series(
    state: &AppState,
    include_slugs: &[String],
    include_mode_and: bool,
    exclude_slugs: &[String],
    year_f: Option<i16>,
    audio_langs: Option<&[String]>,
    audio_col: &str,
) -> WebResult<i64> {
    let mut where_parts: Vec<String> =
        vec!["EXISTS (SELECT 1 FROM episodes e WHERE e.series_id = s.id)".to_string()];
    let mut bind_idx: i32 = 1;
    if !include_slugs.is_empty() {
        if include_mode_and {
            where_parts.push(format!(
                "s.id IN (SELECT sg.series_id FROM series_genres sg \
                 JOIN genres g ON g.id = sg.genre_id \
                 WHERE g.slug = ANY(${bind_idx}) \
                 GROUP BY sg.series_id HAVING COUNT(DISTINCT g.slug) = {})",
                include_slugs.len()
            ));
        } else {
            where_parts.push(format!(
                "s.id IN (SELECT sg.series_id FROM series_genres sg \
                 JOIN genres g ON g.id = sg.genre_id \
                 WHERE g.slug = ANY(${bind_idx}))"
            ));
        }
        bind_idx += 1;
    }
    if !exclude_slugs.is_empty() {
        where_parts.push(format!(
            "s.id NOT IN (SELECT sg2.series_id FROM series_genres sg2 \
             JOIN genres g2 ON g2.id = sg2.genre_id \
             WHERE g2.slug = ANY(${bind_idx}))"
        ));
        bind_idx += 1;
    }
    if year_f.is_some() {
        where_parts.push(format!("s.first_air_year = ${bind_idx}"));
        bind_idx += 1;
    }
    if audio_langs.is_some() {
        where_parts.push(format!("{audio_col} @> ${bind_idx}::TEXT[]"));
    }
    let sql = format!(
        "SELECT count(*) as count FROM series s WHERE {}",
        where_parts.join(" AND ")
    );
    let mut q = sqlx::query_as::<_, CountRow>(&sql);
    if !include_slugs.is_empty() {
        q = q.bind(include_slugs.to_vec());
    }
    if !exclude_slugs.is_empty() {
        q = q.bind(exclude_slugs.to_vec());
    }
    if let Some(yr) = year_f {
        q = q.bind(yr);
    }
    if let Some(langs) = audio_langs {
        q = q.bind(langs.to_vec());
    }
    let row = q.fetch_one(&state.db).await?;
    Ok(row.count.unwrap_or(0))
}

#[derive(FromRow)]
struct SeriesGenreJoinRow {
    series_id: i32,
    name_cs: String,
    slug: String,
}

/// Load genres for all displayed series (from search results + episode cards).
async fn load_series_genres_map(
    db: &sqlx::PgPool,
    series: &[SeriesRow],
    episodes: &[EpisodeCardRow],
) -> WebResult<std::collections::HashMap<i32, Vec<SeriesGenreNameRow>>> {
    let mut ids: Vec<i32> = series.iter().map(|s| s.id).collect();
    ids.extend(episodes.iter().map(|e| e.series_id));
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let rows = sqlx::query_as::<_, SeriesGenreJoinRow>(
        "SELECT sg.series_id, g.name_cs, g.slug \
         FROM series_genres sg JOIN genres g ON g.id = sg.genre_id \
         WHERE sg.series_id = ANY($1) \
         ORDER BY sg.series_id, g.name_cs",
    )
    .bind(ids)
    .fetch_all(db)
    .await?;
    let mut map: std::collections::HashMap<i32, Vec<SeriesGenreNameRow>> =
        std::collections::HashMap::new();
    for r in rows {
        map.entry(r.series_id)
            .or_default()
            .push(SeriesGenreNameRow {
                name_cs: r.name_cs,
                slug: r.slug,
            });
    }
    Ok(map)
}

/// Resolve /serialy-online/{slug}/ — genre or series detail
pub async fn series_resolve(
    State(state): State<AppState>,
    Path(slug_raw): Path<String>,
    axum::extract::Query(params): axum::extract::Query<SeriesQuery>,
    headers: axum::http::HeaderMap,
) -> WebResult<Response> {
    // Cover requests short-circuit before the `state.clone()` below so that
    // hot image endpoints don't pay for a clone they never read.
    // Large cover dynamically proxied from TMDB — real extension in URL so
    // the response content type matches what the template rendered. See
    // `series_cover_large_dynamic` for the fallback chain.
    if slug_raw.ends_with("-large.jpg") || slug_raw.ends_with("-large.png") {
        return series_cover_large_dynamic(State(state), Path(slug_raw)).await;
    }
    // WebP cover
    if slug_raw.ends_with(".webp") {
        return series_cover(State(state), Path(slug_raw)).await;
    }
    let state_clone = state.clone();

    // Genre page?
    let genre =
        sqlx::query_as::<_, GenreRow>("SELECT id, slug, name_cs FROM genres WHERE slug = $1")
            .bind(&slug_raw)
            .fetch_optional(&state.db)
            .await?;

    if let Some(genre) = genre {
        let from_series_home = headers
            .get(axum::http::header::REFERER)
            .and_then(|h| h.to_str().ok())
            .map(|r| {
                if let Some(path) = r.split_once("://").and_then(|(_, s)| s.split_once('/')) {
                    let p = format!("/{}", path.1);
                    let clean = p.split('?').next().unwrap_or(&p);
                    clean == "/serialy-online/" || clean == "/serialy-online"
                } else {
                    false
                }
            })
            .unwrap_or(false);
        return series_by_genre(state, genre, params, from_series_home).await;
    }

    // Series detail
    let series = sqlx::query_as::<_, SeriesRow>(
        "SELECT id, title, slug, first_air_year, last_air_year, description, \
         original_title, tmdb_rating, imdb_rating, csfd_rating, season_count, episode_count, \
         added_at, tmdb_poster_path \
         FROM series WHERE slug = $1",
    )
    .bind(&slug_raw)
    .fetch_optional(&state.db)
    .await?;

    let series = match series {
        Some(s) => s,
        None => {
            // Check old_slug for 301 redirect (series slug changed, e.g. year removed)
            let old_match = sqlx::query_as::<_, SeriesRow>(
                "SELECT id, title, slug, first_air_year, last_air_year, description, \
                 original_title, tmdb_rating, imdb_rating, csfd_rating, season_count, episode_count, \
                 added_at, tmdb_poster_path FROM series WHERE old_slug = $1",
            )
            .bind(&slug_raw)
            .fetch_optional(&state.db)
            .await?;
            if let Some(s) = old_match {
                let new_url = format!("/serialy-online/{}/", s.slug);
                return Ok(
                    (StatusCode::MOVED_PERMANENTLY, [(header::LOCATION, new_url)]).into_response(),
                );
            }
            // Moved to tv_shows? Redirect to /tv-porady/
            let tv_slug = sqlx::query_scalar::<_, String>(
                "SELECT slug FROM tv_shows WHERE slug = $1 OR old_slug = $1 LIMIT 1",
            )
            .bind(&slug_raw)
            .fetch_optional(&state.db)
            .await?;
            if let Some(s) = tv_slug {
                let new_url = format!("/tv-porady/{s}/");
                return Ok(
                    (StatusCode::MOVED_PERMANENTLY, [(header::LOCATION, new_url)]).into_response(),
                );
            }
            return Ok((StatusCode::NOT_FOUND, "Seriál nenalezen").into_response());
        }
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
    let episodes = sqlx::query_as::<_, EpisodeRow>(&format!(
        "SELECT {EPISODE_COLUMNS} \
           FROM episodes e \
          WHERE e.series_id = $1 \
            AND {EPISODE_HAS_SOURCE_PREDICATE} \
          ORDER BY e.season, e.episode, e.id",
    ))
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

/// GET /serialy-online/{slug}/{ep_slug}/ — episode detail page with player.
///
/// Supports two URL formats:
/// - **New (SEO):** `/serialy-online/teorie-velkeho-tresku/hamburgerovy-postulat-s01e05/`
///   Matched via `episodes.slug` column.
/// - **Old (legacy):** `/serialy-online/teorie-velkeho-tresku/1x5/`
///   Parsed as season×episode, then 301 redirected to the new slug URL.
pub async fn episode_detail(
    State(state): State<AppState>,
    Path((slug, ep_path)): Path<(String, String)>,
) -> WebResult<Response> {
    // --- Resolve series (support old slugs with year via redirect) ---
    let series = sqlx::query_as::<_, SeriesRow>(
        "SELECT id, title, slug, first_air_year, last_air_year, description, \
         original_title, tmdb_rating, imdb_rating, csfd_rating, season_count, episode_count, \
         added_at, tmdb_poster_path FROM series WHERE slug = $1",
    )
    .bind(&slug)
    .fetch_optional(&state.db)
    .await?;

    // If not found by current slug, check old_slug for redirect
    let series = match series {
        Some(s) => s,
        None => {
            let old_match = sqlx::query_as::<_, SeriesRow>(
                "SELECT id, title, slug, first_air_year, last_air_year, description, \
                 original_title, tmdb_rating, imdb_rating, csfd_rating, season_count, episode_count, \
                 added_at, tmdb_poster_path FROM series WHERE old_slug = $1",
            )
            .bind(&slug)
            .fetch_optional(&state.db)
            .await?;
            if let Some(s) = old_match {
                // 301 redirect to new series slug URL
                let new_url = format!("/serialy-online/{}/{ep_path}/", s.slug);
                return Ok(
                    (StatusCode::MOVED_PERMANENTLY, [(header::LOCATION, new_url)]).into_response(),
                );
            }
            // Moved to tv_shows? Redirect episode URL to /tv-porady/
            let tv_slug = sqlx::query_scalar::<_, String>(
                "SELECT slug FROM tv_shows WHERE slug = $1 OR old_slug = $1 LIMIT 1",
            )
            .bind(&slug)
            .fetch_optional(&state.db)
            .await?;
            if let Some(s) = tv_slug {
                let new_url = format!("/tv-porady/{s}/{ep_path}/");
                return Ok(
                    (StatusCode::MOVED_PERMANENTLY, [(header::LOCATION, new_url)]).into_response(),
                );
            }
            return Ok((StatusCode::NOT_FOUND, "Seriál nenalezen").into_response());
        }
    };

    // --- Resolve episode: try slug first, then parse old NxM format ---
    let episode = sqlx::query_as::<_, EpisodeRow>(&format!(
        "SELECT {EPISODE_COLUMNS} \
           FROM episodes e \
          WHERE e.series_id = $1 AND e.slug = $2 \
            AND {EPISODE_HAS_SOURCE_PREDICATE} \
          ORDER BY e.id LIMIT 1",
    ))
    .bind(series.id)
    .bind(&ep_path)
    .fetch_optional(&state.db)
    .await?;

    let episode = match episode {
        Some(ep) => ep,
        None => {
            // Try old "NxM" format → parse and redirect to new slug
            if let Some((s_str, e_str)) = ep_path.split_once('x') {
                if let (Ok(season_num), Ok(episode_num)) =
                    (s_str.parse::<i16>(), e_str.parse::<i16>())
                {
                    // Find the episode by season+episode to get its slug
                    let found = sqlx::query_as::<_, EpisodeRow>(&format!(
                        "SELECT {EPISODE_COLUMNS} \
                           FROM episodes e \
                          WHERE e.series_id = $1 AND e.season = $2 AND e.episode = $3 \
                            AND {EPISODE_HAS_SOURCE_PREDICATE} \
                          ORDER BY e.id LIMIT 1",
                    ))
                    .bind(series.id)
                    .bind(season_num)
                    .bind(episode_num)
                    .fetch_optional(&state.db)
                    .await?;

                    if let Some(ep) = found {
                        if let Some(ref ep_slug) = ep.slug {
                            // 301 redirect old NxM → new slug
                            let new_url = format!("/serialy-online/{}/{ep_slug}/", series.slug);
                            return Ok((
                                StatusCode::MOVED_PERMANENTLY,
                                [(header::LOCATION, new_url)],
                            )
                                .into_response());
                        }
                        // Slug not set yet — serve episode directly (fallback)
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

    // Navigation: previous and next episode — same source-available filter
    let all_episodes = sqlx::query_as::<_, (i16, i16, Option<String>, Option<String>)>(&format!(
        "SELECT DISTINCT ON (e.season, e.episode) e.season, e.episode, e.episode_name, e.slug \
           FROM episodes e \
          WHERE e.series_id = $1 \
            AND {EPISODE_HAS_SOURCE_PREDICATE} \
          ORDER BY e.season, e.episode",
    ))
    .bind(series.id)
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
    open_filter: bool,
) -> WebResult<Response> {
    let page = params.page();
    let offset = (page - 1) * SERIES_PER_PAGE;
    let exclude = params.exclude_genres();
    let year_f = params.year_filter();
    let zanry_extras = params.include_genres();

    // Merge path genre with ?zanry= extras into one include list
    let mut include_slugs: Vec<String> = vec![genre.slug.clone()];
    for s in zanry_extras.iter() {
        if !include_slugs.contains(s) {
            include_slugs.push(s.clone());
        }
    }

    // razeni=imdb/tmdb/nazev/rok on a /serialy-online/{genre}/ page now
    // honors the requested order — pre-fix this branch always rendered
    // the latest-episode grid which is hard-coded `created_at DESC`, so
    // the user's "Podle hodnocení TMDB" pick was silently ignored.
    let audio_langs = params.audio_langs_filter();
    let audio_col = params.audio_rollup_column();
    let (series_rows, episodes, total_count) = if params.wants_shows_mode() {
        let (rows, total) = run_shows_mode_query(
            &state.db,
            params.order_clause(),
            &include_slugs,
            params.genre_mode_and(),
            &exclude,
            year_f,
            audio_langs.as_deref(),
            audio_col,
            params.votes_threshold_predicate(),
            offset,
        )
        .await?;
        (rows, Vec::new(), total)
    } else {
        let total = count_filtered_series(
            &state,
            &include_slugs,
            params.genre_mode_and(),
            &exclude,
            year_f,
            audio_langs.as_deref(),
            audio_col,
        )
        .await?;
        let eps = fetch_latest_episode_cards(
            &state,
            &include_slugs,
            params.genre_mode_and(),
            &exclude,
            year_f,
            audio_langs.as_deref(),
            audio_col,
            params.sort_desc(),
            SERIES_PER_PAGE,
            offset,
        )
        .await?;
        (Vec::new(), eps, total)
    };
    let total_pages = (total_count as f64 / SERIES_PER_PAGE as f64).ceil() as i64;

    let all_genres = sqlx::query_as::<_, GenreRow>(
        "SELECT g.id, g.slug, g.name_cs FROM genres g \
         JOIN series_genres sg ON g.id = sg.genre_id \
         GROUP BY g.id, g.slug, g.name_cs ORDER BY g.name_cs",
    )
    .fetch_all(&state.db)
    .await?;

    let query_string = build_series_query_string(&params);
    let series_genres_map = load_series_genres_map(&state.db, &series_rows, &episodes).await?;

    let selected_audio_langs: Vec<String> = audio_langs.clone().unwrap_or_default();
    let tmpl = SeriesListTemplate {
        img: state.image_base_url.clone(),
        episodes,
        series: series_rows,
        genres: all_genres,
        page,
        total_pages,
        total_count,
        current_genre: Some(genre),
        sort_key: params.sort_key().to_string(),
        query_string,
        search_query: None,
        open_filter: open_filter || !zanry_extras.is_empty() || !selected_audio_langs.is_empty(),
        selected_genre_slugs: zanry_extras,
        series_genres_map,
        selected_audio_langs,
        audio_mode_all: params.audio_mode_is_all(),
    };
    Ok(Html(tmpl.render()?).into_response())
}

#[derive(Serialize)]
struct SeriesSearchResult {
    slug: String,
    title: String,
    year: Option<i16>,
    tmdb_rating: Option<f32>,
    imdb_rating: Option<f32>,
    cover: bool,
}

#[derive(FromRow)]
struct SeriesSearchRow {
    slug: String,
    title: String,
    first_air_year: Option<i16>,
    tmdb_rating: Option<f32>,
    imdb_rating: Option<f32>,
}

/// GET /api/series/search?q=...
pub async fn series_search(
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> WebResult<Response> {
    let q = params.get("q").map(|s| s.trim()).unwrap_or("");
    if q.len() < 2 {
        return Ok(super::search_cached_json(Vec::<SeriesSearchResult>::new()));
    }
    let pattern = format!("%{q}%");
    let starts_pattern = format!("{q}%");
    // WHERE matches via `unaccent()` so "laska nebeska" finds "Láska
    // nebeská" (#673). The CASE in ORDER BY uses raw ILIKE: rows whose
    // title literally contains the user's diacritics rank in buckets
    // 0–2, unaccent-only matches drop to bucket 3.
    let rows = sqlx::query_as::<_, SeriesSearchRow>(
        "SELECT slug, title, first_air_year, tmdb_rating, imdb_rating \
         FROM series \
         WHERE unaccent(title) ILIKE unaccent($1) \
            OR unaccent(original_title) ILIKE unaccent($1) \
         ORDER BY \
           CASE WHEN title ILIKE $2 THEN 0 \
                WHEN title ILIKE $1 THEN 1 \
                WHEN original_title ILIKE $2 THEN 2 \
                ELSE 3 END, \
           tmdb_rating DESC NULLS LAST \
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
            tmdb_rating: r.tmdb_rating,
            imdb_rating: r.imdb_rating,
            cover: true,
        })
        .collect();

    Ok(super::search_cached_json(results))
}

pub async fn series_cover(
    State(state): State<AppState>,
    Path(slug_webp): Path<String>,
) -> WebResult<Response> {
    use crate::handlers::cover_proxy::{
        fetch_cover, new_r2_key, parse_cover_slug, placeholder_webp,
    };

    if slug_webp.ends_with("-large.webp") {
        // Box::pin to break the mutual-recursion future size cycle
        // (series_cover → series_cover_large → … small fallback paths).
        return Box::pin(series_cover_large(State(state), Path(slug_webp))).await;
    }
    let (slug, _is_large) = parse_cover_slug(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        id: i32,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT id FROM series WHERE slug = $1")
        .bind(&slug)
        .fetch_optional(&state.db)
        .await?;

    let Some(row) = row else {
        return Ok(placeholder_webp());
    };

    let new_key = new_r2_key("series", row.id, false);
    Ok(fetch_cover(&state, &new_key).await)
}

/// GET /serialy-online/{slug}-large.webp — large (780×1170) cover.
/// See handlers::cover_proxy for the R2 key schema + fallback rationale.
pub async fn series_cover_large(
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

    let row = sqlx::query_as::<_, CoverRow>("SELECT id FROM series WHERE slug = $1")
        .bind(&slug)
        .fetch_optional(&state.db)
        .await?;

    let Some(row) = row else {
        return Ok(placeholder_webp());
    };

    use crate::handlers::cover_proxy::no_store_webp;
    // (R2 key, is_small_fallback) — small variants served under the
    // `-large.webp` URL must be `no-store` (see films_cover_large).
    let candidates: Vec<(String, bool)> = vec![
        (new_r2_key("series", row.id, true), false),
        (format!("series/large/{slug}.webp"), false),
        (new_r2_key("series", row.id, false), true),
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

/// GET /serialy-online/{slug}-large.{jpg,png} — proxy TMDB poster on demand.
///
/// Mirrors `films_cover_large_dynamic` (see `handlers::films`): detail-page
/// thumbnails get few hits, so we skip R2 storage and stream the TMDB image
/// through. Cloudflare caches the response for a year. On any failure we
/// serve a placeholder in the SAME format the URL advertises so browsers
/// and OG scrapers decode without a MIME mismatch.
pub async fn series_cover_large_dynamic(
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

    let row = sqlx::query_as::<_, CoverRow>("SELECT tmdb_poster_path FROM series WHERE slug = $1")
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

/// Build pagination query string for series list views.
fn build_series_query_string(params: &SeriesQuery) -> String {
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
    if let Some(ref a) = params.audio
        && !a.is_empty()
    {
        parts.push(("audio", a.clone()));
    }
    if params.audio_mode_is_all() {
        parts.push(("audio_mode", "all".to_string()));
    }
    super::build_pagination_qs(&parts)
}

/// GET /serialy-online/person/{filename} — serve person profile image from disk.
pub async fn series_person_image(
    State(state): State<AppState>,
    Path(filename): Path<String>,
) -> WebResult<Response> {
    if !filename.ends_with(".webp") || filename.contains('/') || filename.contains("..") {
        return Ok((StatusCode::NOT_FOUND, "Not found").into_response());
    }
    let dir = state.config.series_people_dir.clone();
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
pub async fn series_episode_still(
    State(state): State<AppState>,
    Path(filename): Path<String>,
) -> WebResult<Response> {
    // Sanity-check filename: WebP only, no path traversal
    if !filename.ends_with(".webp") || filename.contains('/') || filename.contains("..") {
        return Ok((StatusCode::NOT_FOUND, "Not found").into_response());
    }
    let stills_dir = state.config.series_stills_dir.clone();
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
