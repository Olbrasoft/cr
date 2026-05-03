use super::*;
use serde::{Deserialize, Serialize};

const FILMS_PER_PAGE: i64 = 24;

/// SELECT column list for `FilmRow` queries. Kept as a const to avoid
/// duplication across `films_list`, `films_by_genre`, and `films_detail`.
///
/// Each provider column is sourced from the unified `video_sources` table
/// (#607 / #611 reader switch). The output shape is byte-for-byte identical
/// to the pre-refactor legacy reads, so templates and downstream handlers
/// don't need any change.
///
/// Correlated subqueries drive each of the four provider fields. They're
/// cheap in practice: `idx_vs_film_alive` is a partial index keyed on
/// `film_id`, and every subquery filters by `is_primary` which the partial
/// unique indexes (`uq_vs_primary_film`) keep at ≤ 1 row per provider. The
/// listing query hits 4 subqueries × 24 rows = ≤ 96 index lookups per page,
/// which EXPLAIN ANALYZE puts in the low-single-digit millisecond range.
///
/// Notes per field:
/// - `sktorrent_video_id` / `sktorrent_cdn`: both INT in legacy; the new
///   `external_id` is TEXT and `cdn` is VARCHAR(32). Casts to INTEGER /
///   SMALLINT reproduce the old types for FilmRow. sktorrent CDN is always
///   a numeric string ("22"), so the cast is safe.
/// - `sktorrent_qualities`: pulled from JSONB `metadata->>'qualities'`.
///   Backfill + dual-write both write `{"qualities": "720p,480p"}`.
/// - `prehrajto_url`: legacy `films.prehrajto_url` is a single cached URL;
///   new schema stores N uploads per film. We pick the primary alive row
///   (fallback: most recently updated alive row). URL is reconstructed from
///   `metadata->>'url'` (dual-write writes this) or synthesised from
///   `external_id` as a last resort.
/// - `sledujteto_primary_file_id`: legacy column had a CDN-gate (only set
///   when the primary upload's CDN is `www`, because data{N} is blocked
///   from datacenter ASNs). Preserved here via `cdn = 'www'` predicate.
const FILM_COLUMNS: &str = "f.id, f.title, f.slug, f.year, f.description, f.original_title, \
    f.imdb_rating, f.csfd_rating, NULLIF(f.runtime_min, 0) AS runtime_min, \
    f.added_at, f.tmdb_poster_path, \
    (SELECT vs.external_id::INTEGER \
       FROM video_sources vs \
       JOIN video_providers p ON p.id = vs.provider_id \
      WHERE vs.film_id = f.id AND p.slug = 'sktorrent' \
        AND vs.is_primary AND vs.is_alive \
      LIMIT 1) AS sktorrent_video_id, \
    (SELECT vs.cdn::SMALLINT \
       FROM video_sources vs \
       JOIN video_providers p ON p.id = vs.provider_id \
      WHERE vs.film_id = f.id AND p.slug = 'sktorrent' \
        AND vs.is_primary AND vs.is_alive \
      LIMIT 1) AS sktorrent_cdn, \
    (SELECT vs.metadata->>'qualities' \
       FROM video_sources vs \
       JOIN video_providers p ON p.id = vs.provider_id \
      WHERE vs.film_id = f.id AND p.slug = 'sktorrent' \
        AND vs.is_primary AND vs.is_alive \
      LIMIT 1) AS sktorrent_qualities, \
    (SELECT COALESCE(vs.metadata->>'url', 'https://prehraj.to/' || vs.external_id) \
       FROM video_sources vs \
       JOIN video_providers p ON p.id = vs.provider_id \
      WHERE vs.film_id = f.id AND p.slug = 'prehrajto' AND vs.is_alive \
      ORDER BY vs.is_primary DESC, vs.updated_at DESC \
      LIMIT 1) AS prehrajto_url, \
    false AS prehrajto_has_dub, \
    false AS prehrajto_has_subs, \
    (SELECT vs.external_id::INTEGER \
       FROM video_sources vs \
       JOIN video_providers p ON p.id = vs.provider_id \
      WHERE vs.film_id = f.id AND p.slug = 'sledujteto' \
        AND vs.is_primary AND vs.is_alive AND vs.cdn = 'www' \
      LIMIT 1) AS sledujteto_primary_file_id";

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
    #[allow(dead_code)]
    // Template reads sources via `video_sources_for_badges`; column stays for legacy scripts
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
    /// Best sledujteto upload for this film — written by the sledujteto
    /// import script. Template reads sources via `video_sources_for_badges`;
    /// the column stays for legacy scripts that still SELECT it.
    #[allow(dead_code)]
    pub sledujteto_primary_file_id: Option<i32>,
}

impl FilmRow {
    /// Extension for the large-cover URL rendered in the detail template.
    /// Derived from `tmdb_poster_path` when the film has been backfilled;
    /// otherwise falls back to `webp` so the existing R2-backed route keeps
    /// serving until the backfill completes.
    ///
    /// Only `jpg` and `png` are whitelisted — `films_detail` dispatches exactly
    /// those two large-cover extensions to the dynamic proxy, and TMDB's
    /// in-practice storage is always JPG. Unknown/unexpected suffixes get
    /// normalized to `jpg` rather than falling through to the HTML handler.
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

/// Per-source badge row rendered on the film detail page (#613). One row
/// per alive `video_sources` entry for the film, with subtitle languages
/// aggregated from `video_source_subtitles` in the same query. Ordered by
/// provider `sort_priority` so badge tabs follow the same visual order
/// as the provider switcher at the top of the player.
#[derive(sqlx::FromRow, Serialize)]
pub(crate) struct VideoSourceBadgeRow {
    pub(crate) provider_slug: String,
    pub(crate) provider_host: String,
    pub(crate) provider_display_name: String,
    #[allow(dead_code)] // used only for ORDER BY; template reads by slug
    pub(crate) sort_priority: i16,
    pub(crate) external_id: String,
    /// What the frontend passes back to the provider-specific playback
    /// endpoint. For prehraj.to this is the full upload URL (stored in
    /// `metadata->>'url'`), not the short external_id suffix — the
    /// `/api/movies/video-url` endpoint expects the URL. For other
    /// providers `external_id` is already the playback handle, so the
    /// SQL `COALESCE` falls back to it.
    pub(crate) playback_id: String,
    pub(crate) title: Option<String>,
    pub(crate) lang_class: String,
    pub(crate) audio_lang: Option<String>,
    pub(crate) audio_confidence: Option<f32>,
    pub(crate) audio_detected_by: Option<String>,
    pub(crate) resolution_hint: Option<String>,
    #[allow(dead_code)] // rendered indirectly via `cdn_label`; kept for ordering decisions
    pub(crate) cdn: Option<String>,
    pub(crate) is_primary: bool,
    pub(crate) subtitle_langs: Vec<String>,
}

impl VideoSourceBadgeRow {
    /// Human-readable label for the audio language (or "—" when unknown).
    pub(crate) fn audio_label(&self) -> String {
        match self.audio_lang.as_deref() {
            Some("cs") => "Čeština".to_string(),
            Some("sk") => "Slovenština".to_string(),
            Some("en") => "Angličtina".to_string(),
            Some("de") => "Němčina".to_string(),
            Some(other) => other.to_ascii_uppercase(),
            None => "—".to_string(),
        }
    }

    /// Formatted confidence badge. Hidden when the detector was Whisper at
    /// ≥ 0.8 confidence (per #613 — that's noise for the user; only show
    /// when detection was lower-quality so it's visible which sources are
    /// reliable and which are regex-guessed).
    pub(crate) fn confidence_display(&self) -> Option<String> {
        match (self.audio_detected_by.as_deref(), self.audio_confidence) {
            (Some("whisper"), Some(c)) if c >= 0.8 => None,
            (_, Some(c)) => Some(format!("{:.0} %", (c * 100.0).round())),
            _ => None,
        }
    }

    /// Comma-separated display of subtitle languages, uppercased. Empty
    /// when the source has no subtitle tracks.
    pub(crate) fn subtitle_display(&self) -> String {
        if self.subtitle_langs.is_empty() {
            String::new()
        } else {
            self.subtitle_langs
                .iter()
                .map(|s| s.to_ascii_uppercase())
                .collect::<Vec<_>>()
                .join(", ")
        }
    }
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
    jazyk: Option<String>,  // legacy: "dub" / "sub" — kept for shareable links
    /// Multi-value audio-language filter (#612). Comma-separated ISO 639
    /// codes like `cs,sk,en`. AND semantics: the film must have EVERY listed
    /// language present in `audio_langs`. Empty / absent = no filter.
    /// Repeated-key form like `?audio=cs&audio=en` isn't supported by the
    /// default `serde_urlencoded` extractor; going comma-separated matches
    /// the existing `zanry` / `jazyk` params and keeps the query string
    /// short + shareable.
    audio: Option<String>,
    /// Subtitle filter (#612). `titulky=1` or `titulky=any` requires the
    /// film to have at least one subtitle track. Comma-separated ISO codes
    /// like `titulky=cs,en` require EVERY listed language as a subtitle.
    titulky: Option<String>,
}

impl FilmsQuery {
    fn genre_mode_and(&self) -> bool {
        self.rezim.as_deref() == Some("and")
    }

    fn sort_desc(&self) -> bool {
        self.smer.as_deref() != Some("asc")
    }

    // Audio / subtitle filtering queries the `audio_langs` / `subtitle_langs`
    // rollup arrays (#607 / #611 reader switch). The arrays are maintained
    // by a TRIGGER on `video_sources` so they always reflect the current
    // per-source data without a join at query time. GIN indexes on both
    // arrays keep the filter fast.
    //
    // "dub" means "film has Czech OR Slovak audio" (i.e. user hears a local
    // language, not the original foreign track). "sub" means "film has at
    // least one subtitle track" (not gated by language because a viewer
    // with English audio + any CZ/SK subs is the canonical use case).
    fn audio_filter(&self) -> Option<&'static str> {
        let val = self.jazyk.as_deref().map(|s| s.trim()).unwrap_or("");
        if val.is_empty() || val == "vse" {
            return None;
        }
        let parts: Vec<&str> = val.split(',').map(|s| s.trim()).collect();
        let has_dub = parts.contains(&"dub") || parts.contains(&"cz") || parts.contains(&"sk");
        let has_sub = parts.contains(&"sub") || parts.contains(&"titulky");
        match (has_dub, has_sub) {
            (true, false) => Some("f.audio_langs && ARRAY['cs','sk']::TEXT[]"),
            (false, true) => Some("cardinality(f.subtitle_langs) > 0"),
            (true, true) => Some(
                "(f.audio_langs && ARRAY['cs','sk']::TEXT[] \
                  OR cardinality(f.subtitle_langs) > 0)",
            ),
            _ => None,
        }
    }

    /// Parse the `audio=cs,sk,en` param into a Vec of validated ISO codes.
    /// Returns None when absent/empty; Some(vec) with only `^[a-z]{2,3}$`
    /// entries otherwise. The regex guard avoids SQL-binding garbage strings
    /// (the binding itself is parameterised, but keeping the array contents
    /// canonical lets the GIN index fire and simplifies chip rendering).
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

    /// Parse the `titulky=...` param into a subtitle filter mode.
    /// Returns:
    ///   - None              — no filter (absent or empty)
    ///   - Some((None, _))   — `titulky=1` / `titulky=any` → any subtitles present
    ///   - Some((Some(vec))) — `titulky=cs,en` → film must have every listed lang
    pub(crate) fn subs_filter(&self) -> Option<Option<Vec<String>>> {
        let raw = self.titulky.as_deref()?.trim();
        if raw.is_empty() || raw == "0" {
            return None;
        }
        if raw == "1" || raw == "any" || raw == "ano" {
            return Some(None);
        }
        let langs: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| !s.is_empty() && s.len() <= 3 && s.chars().all(|c| c.is_ascii_lowercase()))
            .collect();
        if langs.is_empty() {
            Some(None)
        } else {
            Some(Some(langs))
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
    /// Currently-applied audio-language filter (#612). Empty → no filter.
    selected_audio_langs: Vec<String>,
    /// Currently-applied subtitle-language filter (#612). `Some(vec)` means
    /// "require these specific subs"; `Some(empty)` means `titulky=any`
    /// (any subs). `None` means "no subs filter applied".
    selected_subs: Option<Vec<String>>,
    /// Full raw query string including `strana=`, `zanry=`, etc. Used to
    /// build "remove this chip" URLs that strip a single audio/subs param
    /// without disturbing others.
    full_query: std::collections::BTreeMap<String, Vec<String>>,
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

    // --- Audio / subs filter helpers (#612) ---

    fn is_audio_selected(&self, lang: &str) -> bool {
        self.selected_audio_langs.iter().any(|l| l == lang)
    }

    fn is_subs_selected(&self, lang: &str) -> bool {
        match &self.selected_subs {
            Some(list) if !list.is_empty() => list.iter().any(|l| l == lang),
            _ => false,
        }
    }

    /// True iff the user checked "Má titulky" without specifying a language.
    fn is_subs_any(&self) -> bool {
        matches!(&self.selected_subs, Some(list) if list.is_empty())
    }

    /// Narrower form used by the active-chip row: show the generic
    /// "Titulky" chip ONLY when no language-specific chip is already
    /// rendered (otherwise the UI would duplicate the signal).
    fn is_subs_any_without_lang(&self) -> bool {
        matches!(&self.selected_subs, Some(list) if list.is_empty())
    }

    fn has_lang_filters(&self) -> bool {
        !self.selected_audio_langs.is_empty() || self.selected_subs.is_some()
    }

    fn audio_chips(&self) -> &[String] {
        &self.selected_audio_langs
    }

    fn subs_chips(&self) -> &[String] {
        match &self.selected_subs {
            Some(list) => list.as_slice(),
            None => &[],
        }
    }

    /// Map an ISO code to the Czech label used in active-filter chips.
    /// Falls back to uppercase of the code for languages we don't list
    /// explicitly (e.g. "de" → "DE"). Keeps chips short in the UI.
    fn lang_display(&self, lang: &str) -> String {
        match lang {
            "cs" => "Čeština".to_string(),
            "sk" => "Slovenština".to_string(),
            "en" => "Angličtina".to_string(),
            "de" => "Němčina".to_string(),
            "fr" => "Francouzština".to_string(),
            other => other.to_ascii_uppercase(),
        }
    }

    /// Build a URL back to `/filmy-online/` with the current filters minus
    /// one audio language. Keeps the rest of the query (zanry, razeni, rok,
    /// strana=1 reset so removing a filter returns to page 1).
    fn remove_audio_url(&self, lang: &str) -> String {
        let mut params = self.full_query.clone();
        if let Some(vs) = params.get_mut("audio") {
            // Rebuild each CSV value with the requested lang removed; drop
            // empties. Using iter_mut + assignment would hit E0594 because
            // Vec::retain yields `&T` not `&mut T`; reconstructing the Vec
            // is the cleanest way across Rust 1.x versions.
            let rebuilt: Vec<String> = vs
                .iter()
                .filter_map(|existing_csv| {
                    let filtered: Vec<&str> = existing_csv
                        .split(',')
                        .map(|s| s.trim())
                        .filter(|s| *s != lang)
                        .collect();
                    if filtered.is_empty() {
                        None
                    } else {
                        Some(filtered.join(","))
                    }
                })
                .collect();
            if rebuilt.is_empty() {
                params.remove("audio");
            } else {
                *vs = rebuilt;
            }
        }
        params.remove("strana");
        build_filter_url(&params)
    }

    fn remove_subs_url(&self) -> String {
        let mut params = self.full_query.clone();
        params.remove("titulky");
        params.remove("strana");
        build_filter_url(&params)
    }

    fn remove_subs_lang_url(&self, lang: &str) -> String {
        let mut params = self.full_query.clone();
        if let Some(vs) = params.get_mut("titulky") {
            let rebuilt: Vec<String> = vs
                .iter()
                .filter_map(|existing_csv| {
                    let filtered: Vec<&str> = existing_csv
                        .split(',')
                        .map(|s| s.trim())
                        .filter(|s| *s != lang)
                        .collect();
                    if filtered.is_empty() {
                        None
                    } else {
                        Some(filtered.join(","))
                    }
                })
                .collect();
            if rebuilt.is_empty() {
                params.remove("titulky");
            } else {
                *vs = rebuilt;
            }
        }
        params.remove("strana");
        build_filter_url(&params)
    }
}

/// Re-serialize `params` into a `/filmy-online/?a=b&c=d` URL. Values with
/// commas are emitted as-is (the caller already canonicalised them), and
/// repeated keys each get their own `&key=` entry so the URL shape stays
/// predictable.
fn build_filter_url(params: &std::collections::BTreeMap<String, Vec<String>>) -> String {
    let mut parts = Vec::new();
    for (key, values) in params {
        for v in values {
            parts.push(format!("{}={}", key, urlencoding::encode(v)));
        }
    }
    if parts.is_empty() {
        "/filmy-online/".to_string()
    } else {
        format!("/filmy-online/?{}", parts.join("&"))
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
    /// Per-source badge rows (#613). Already sorted by provider
    /// `sort_priority` + primary-first. Rendered under the provider
    /// tabs on the detail page.
    video_sources_for_badges: Vec<VideoSourceBadgeRow>,
    /// Which providers have at least one row in `video_sources_for_badges`,
    /// driving whether the template renders that provider's numbered tab.
    /// Keeps the tab row in sync with the source list, unlike the legacy
    /// `film.prehrajto_url` / `film.sktorrent_video_id` fields which can
    /// drift from the unified schema (PR #623 Copilot review).
    has_source_sktorrent: bool,
    has_source_prehrajto: bool,
    has_source_sledujteto: bool,
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
    let selected_audio_langs = params.audio_langs_filter().unwrap_or_default();
    let selected_subs = match params.subs_filter() {
        Some(None) => Some(Vec::new()),
        Some(Some(v)) => Some(v),
        None => None,
    };
    let full_query = build_full_query_map(&params);
    let open_filter = !selected_genre_slugs.is_empty()
        || !selected_audio_langs.is_empty()
        || selected_subs.is_some();
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
        selected_audio_langs,
        selected_subs,
        full_query,
    };
    let html = tmpl.render()?;
    // Search-result HTML is `?q=`-derived: tag it `private,
    // max-age=60` so the browser can reuse it for back-button /
    // repeat-search but no shared cache hangs on to it. Gate
    // matches the actual search predicate (`raw_q` filters
    // `len() >= 2`), so `?q=a` still gets the default listing.
    if is_active_search(params.q.as_deref()) {
        Ok(super::search_cached_html(html))
    } else {
        Ok(Html(html).into_response())
    }
}

/// True when `?q=…` is a real search query — same trim+length gate
/// the search predicate uses. Single source of truth so the
/// search-cache branch and the predicate gate can't drift apart
/// (Copilot review on #674).
fn is_active_search(q: Option<&str>) -> bool {
    q.map(str::trim).is_some_and(|t| t.chars().count() >= 2)
}

/// Serialize the current filter params into a BTreeMap keyed by query
/// parameter name. Used by the active-chip "remove" URL helpers so each
/// chip can rebuild the URL with one of its own filters stripped out.
/// BTreeMap gives a deterministic ordering for the rendered URLs — stable
/// across renders so the browser cache isn't constantly invalidated.
fn build_full_query_map(params: &FilmsQuery) -> std::collections::BTreeMap<String, Vec<String>> {
    let mut map: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    if let Some(z) = params.zanry.as_ref()
        && !z.trim().is_empty()
    {
        map.insert("zanry".into(), vec![z.clone()]);
    }
    if let Some(b) = params.bez.as_ref()
        && !b.trim().is_empty()
    {
        map.insert("bez".into(), vec![b.clone()]);
    }
    if let Some(q) = params.q.as_ref()
        && !q.trim().is_empty()
    {
        map.insert("q".into(), vec![q.clone()]);
    }
    if let Some(r) = params.rok.as_ref()
        && !r.trim().is_empty()
    {
        map.insert("rok".into(), vec![r.clone()]);
    }
    if let Some(r) = params.razeni.as_ref()
        && !r.trim().is_empty()
    {
        map.insert("razeni".into(), vec![r.clone()]);
    }
    if let Some(s) = params.smer.as_ref()
        && !s.trim().is_empty()
    {
        map.insert("smer".into(), vec![s.clone()]);
    }
    if let Some(rz) = params.rezim.as_ref()
        && !rz.trim().is_empty()
    {
        map.insert("rezim".into(), vec![rz.clone()]);
    }
    if let Some(j) = params.jazyk.as_ref()
        && !j.trim().is_empty()
    {
        map.insert("jazyk".into(), vec![j.clone()]);
    }
    if let Some(a) = params.audio.as_ref()
        && !a.trim().is_empty()
    {
        map.insert("audio".into(), vec![a.clone()]);
    }
    if let Some(t) = params.titulky.as_ref()
        && !t.trim().is_empty()
    {
        map.insert("titulky".into(), vec![t.clone()]);
    }
    map
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

    // Per-source badge data (#613). One row per alive video source for this
    // film, with an array-aggregated list of subtitle languages. Ordered by
    // `video_providers.sort_priority` so the tab order matches the badge
    // order — the frontend scroll-anchor logic relies on that.
    let video_sources_for_badges = sqlx::query_as::<_, VideoSourceBadgeRow>(
        "SELECT p.slug AS provider_slug, \
                    p.host AS provider_host, \
                    p.display_name AS provider_display_name, \
                    p.sort_priority, \
                    vs.external_id, \
                    -- prehrajto needs a full URL for /api/movies/video-url to
                    -- accept the playback_id; fall back to synthesizing the
                    -- canonical detail URL when metadata.url is missing
                    -- (#647 Copilot review).
                    COALESCE( \
                        vs.metadata->>'url', \
                        CASE WHEN p.slug = 'prehrajto' THEN 'https://prehraj.to/' || vs.external_id ELSE vs.external_id END \
                    ) AS playback_id, \
                    vs.title, \
                    vs.lang_class, \
                    vs.audio_lang, \
                    vs.audio_confidence, \
                    vs.audio_detected_by, \
                    vs.resolution_hint, \
                    vs.cdn, \
                    vs.is_primary, \
                    COALESCE( \
                        (SELECT array_agg(DISTINCT vss.lang::TEXT ORDER BY vss.lang::TEXT) \
                         FROM video_source_subtitles vss \
                         WHERE vss.source_id = vs.id), \
                        '{}'::TEXT[] \
                    ) AS subtitle_langs \
             FROM video_sources vs \
             JOIN video_providers p ON p.id = vs.provider_id \
             WHERE vs.film_id = $1 AND vs.is_alive \
             ORDER BY p.sort_priority, vs.is_primary DESC, vs.updated_at DESC",
    )
    .bind(film.id)
    .fetch_all(&state.db)
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(film_id = film.id, error = ?e,
                "video_sources badge query failed; detail page renders without badges");
        Vec::new()
    });

    let has_source_sktorrent = video_sources_for_badges
        .iter()
        .any(|r| r.provider_slug == "sktorrent");
    let has_source_prehrajto = video_sources_for_badges
        .iter()
        .any(|r| r.provider_slug == "prehrajto");
    let has_source_sledujteto = video_sources_for_badges
        .iter()
        .any(|r| r.provider_slug == "sledujteto");

    let tmpl = FilmDetailTemplate {
        img: state.image_base_url.clone(),
        film,
        genres,
        sources,
        video_sources_for_badges,
        has_source_sktorrent,
        has_source_prehrajto,
        has_source_sledujteto,
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
        selected_audio_langs: params.audio_langs_filter().unwrap_or_default(),
        selected_subs: match params.subs_filter() {
            Some(None) => Some(Vec::new()),
            Some(Some(v)) => Some(v),
            None => None,
        },
        full_query: build_full_query_map(&params),
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
        return Ok(super::search_cached_json(Vec::<SearchResult>::new()));
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
            cover: true,
        })
        .collect();

    Ok(super::search_cached_json(results))
}

async fn search_films_by_title(db: &sqlx::PgPool, q: &str) -> Result<Vec<SearchRow>, sqlx::Error> {
    let pattern = format!("%{q}%");
    let starts_pattern = format!("{q}%");
    // WHERE matches via `unaccent()` so "laska nebeska" finds "Láska
    // nebeská" (#673). ORDER BY keeps the raw ILIKE in the CASE
    // arms — rows whose title literally contains the query (with the
    // user's diacritics) win bucket 0/1/2; rows that only match after
    // unaccent fall to bucket 3, behind the diacritic-exact hits.
    sqlx::query_as::<_, SearchRow>(
        "SELECT slug, title, year, imdb_rating \
         FROM films \
         WHERE unaccent(title) ILIKE unaccent($1) \
            OR unaccent(original_title) ILIKE unaccent($1) \
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
        "SELECT slug, title, year, imdb_rating \
         FROM films \
         WHERE unaccent(CONCAT_WS(' ', title, year::text)) ILIKE unaccent($1) \
            OR unaccent(CONCAT_WS(' ', original_title, year::text)) ILIKE unaccent($1) \
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
                format!(
                    "(unaccent(f.title) ILIKE unaccent(${bind_idx}) \
                     OR unaccent(f.original_title) ILIKE unaccent(${bind_idx}))"
                )
            }
            Self::TitleYear => format!(
                "(unaccent(CONCAT_WS(' ', f.title, f.year::text)) ILIKE unaccent(${bind_idx}) \
                 OR unaccent(CONCAT_WS(' ', f.original_title, f.year::text)) ILIKE unaccent(${bind_idx}))"
            ),
        }
    }

    /// Same shape as `predicate` but without the `unaccent()` wrapper —
    /// matches only when the column literally contains the user's
    /// diacritics. Used in ORDER BY to push diacritic-exact hits in
    /// front of unaccent-only hits.
    fn raw_predicate(self, bind_idx: usize) -> String {
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
    // Multi-value audio-lang filter (#612). `f.audio_langs @> $N` asks "is
    // every user-selected lang present in the film's rollup array" — AND
    // semantics per the issue. GIN on `audio_langs` makes this a cheap
    // bitmap intersection.
    let audio_langs_param = params.audio_langs_filter();
    if audio_langs_param.is_some() {
        where_parts.push(format!("f.audio_langs @> ${bind_idx}::TEXT[]"));
        bind_idx += 1;
    }
    // Subtitle filter (#612).
    //   titulky=any → require at least one subtitle track (any language)
    //   titulky=cs,en → require every listed language as a subtitle track
    let subs_param = params.subs_filter();
    let mut subs_bind_idx = None;
    match &subs_param {
        Some(None) => {
            where_parts.push("cardinality(f.subtitle_langs) > 0".to_string());
        }
        Some(Some(_)) => {
            where_parts.push(format!("f.subtitle_langs @> ${bind_idx}::TEXT[]"));
            subs_bind_idx = Some(bind_idx);
            bind_idx += 1;
        }
        None => {}
    }
    let _ = subs_bind_idx; // reserved for future chip rendering; kept to document order

    // Anti-zombie filter: only list films that actually have at least one
    // alive video source. Before the #611 reader switch, this condition was
    // implicit — the pre-refactor SELECT read `sktorrent_video_id` /
    // `prehrajto_url` / `sledujteto_primary_file_id` columns directly, and
    // films without any source had all three columns NULL but were still
    // rendered (clicking through led to an empty detail page). The new
    // schema puts source existence in a separate table, so we have to gate
    // it explicitly. The partial index `idx_vs_film_alive` makes this EXISTS
    // a cheap hash-semi-join.
    where_parts.push(
        "EXISTS (SELECT 1 FROM video_sources vs \
                 WHERE vs.film_id = f.id AND vs.is_alive)"
            .to_string(),
    );

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
    if let Some(langs) = audio_langs_param.as_ref() {
        cq = cq.bind(langs.clone());
    }
    if let Some(Some(langs)) = subs_param.as_ref() {
        cq = cq.bind(langs.clone());
    }
    let count_row = cq.fetch_one(db).await?;

    // When a search is active, push rows whose title literally contains
    // the user's query (with diacritics) ahead of rows that match only
    // after `unaccent()` — see #673. The search pattern is bound at $1.
    let order_clause = if search_pattern.is_some() {
        format!(
            "(CASE WHEN {raw} THEN 0 ELSE 1 END), {order}",
            raw = mode.raw_predicate(1)
        )
    } else {
        order.to_string()
    };

    let films_query = format!(
        "SELECT {FILM_COLUMNS} \
         FROM films f {where_clause} \
         ORDER BY {order_clause} \
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
    if let Some(langs) = audio_langs_param.as_ref() {
        fq = fq.bind(langs.clone());
    }
    if let Some(Some(langs)) = subs_param.as_ref() {
        fq = fq.bind(langs.clone());
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
        fetch_cover, new_r2_key, parse_cover_slug, placeholder_webp,
    };

    let (slug, _is_large) = parse_cover_slug(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        id: i32,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT id FROM films WHERE slug = $1")
        .bind(&slug)
        .fetch_optional(&state.db)
        .await?;

    let Some(row) = row else {
        return Ok(placeholder_webp());
    };

    let new_key = new_r2_key("films", row.id, false);
    Ok(fetch_cover(&state, &new_key).await)
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
    use crate::handlers::cover_proxy::{new_r2_key, parse_cover_slug, placeholder_webp};

    let (slug, _is_large) = parse_cover_slug(&slug_webp);

    #[derive(sqlx::FromRow)]
    struct CoverRow {
        id: i32,
    }

    let row = sqlx::query_as::<_, CoverRow>("SELECT id FROM films WHERE slug = $1")
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
    let candidates: Vec<(String, bool)> = vec![
        (new_r2_key("films", row.id, true), false),
        (format!("films/large/{slug}.webp"), false),
        (new_r2_key("films", row.id, false), true),
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

/// GET /filmy-online/{slug}-large.{jpg,png} — proxy TMDB poster on demand.
///
/// Detail-page thumbnails get few hits, so we skip R2 storage and stream the
/// TMDB image through. Cloudflare caches the response for a year, so each
/// edge fetches TMDB at most once per (edge, poster) — the next visitor is
/// served from cache. On failure we serve a placeholder in the SAME format
/// the URL advertises (jpeg for `-large.jpg`, png for `-large.png`) so
/// browsers and OG scrapers decode without a MIME mismatch — the template's
/// `og:image:type` is derived from the same URL extension.
pub async fn films_cover_large_dynamic(
    State(state): State<AppState>,
    Path(slug_ext): Path<String>,
) -> WebResult<Response> {
    use crate::handlers::cover_proxy::placeholder_for_ext;

    // Strip `-large.jpg` / `-large.png`, whichever the request carried, and
    // remember the extension so fallbacks can emit bytes of the same type.
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

    let row = sqlx::query_as::<_, CoverRow>("SELECT tmdb_poster_path FROM films WHERE slug = $1")
        .bind(slug)
        .fetch_optional(&state.db)
        .await?;

    // Row missing or poster_path not backfilled → placeholder in requested
    // format. The small-cover (`/filmy-online/{slug}.webp`) route still
    // serves the list-view thumbnail from R2, so nothing user-visible
    // depends on this handler synthesising something from R2.
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

    // DB hint — look up the last known CDN node. After the #611 reader
    // switch this is a single-row lookup against `video_sources` (the
    // UNIQUE constraint on `(provider_id, external_id)` means each
    // sktorrent video_id maps to exactly one source row across films,
    // series episodes, and tv_episodes). Pre-refactor this was a 3-way
    // UNION across legacy tables; the new query is both simpler and
    // faster because the unique index directly lands on the row.
    let hint: Option<i16> = match sqlx::query_scalar(
        "SELECT vs.cdn::SMALLINT \
           FROM video_sources vs \
           JOIN video_providers p ON p.id = vs.provider_id \
          WHERE p.slug = 'sktorrent' \
            AND vs.external_id = $1::TEXT \
            AND vs.cdn IS NOT NULL \
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

/// Write the freshly-discovered CDN node back to the `video_sources` row
/// for this sktorrent video. Best-effort — failures are logged but not
/// surfaced; the playback already has its URL.
///
/// After the #611 reader switch there is exactly one `video_sources` row
/// per sktorrent `video_id` (UNIQUE `(provider_id, external_id)`), so the
/// 3-way UPDATE across legacy tables collapses to a single statement.
async fn update_sktorrent_cdn(db: &sqlx::PgPool, video_id: i32, cdn: i16) {
    let sql = "UPDATE video_sources SET cdn = $1, updated_at = NOW() \
               WHERE provider_id = (SELECT id FROM video_providers WHERE slug = 'sktorrent') \
                 AND external_id = $2::TEXT";
    if let Err(e) = sqlx::query(sql)
        .bind(cdn.to_string())
        .bind(video_id)
        .execute(db)
        .await
    {
        tracing::warn!("sktorrent_cdn self-heal failed: {e}");
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
    use super::{FilmsQuery, FilmsSearchMode, is_active_search, normalize_query};

    #[test]
    fn is_active_search_gate_matches_predicate_threshold() {
        // Must mirror `raw_q`'s `len() >= 2` filter so the
        // search-cache branch in `films_list` and the search
        // predicate gate stay in sync — Copilot review on #674
        // caught the original looser check letting `?q=a` mark
        // non-search pages with the search cache header.
        assert!(!is_active_search(None));
        assert!(!is_active_search(Some("")));
        assert!(!is_active_search(Some("   ")));
        assert!(!is_active_search(Some("a")));
        assert!(!is_active_search(Some("  a  ")));
        assert!(is_active_search(Some("ab")));
        assert!(is_active_search(Some("  ab  ")));
        // Diacritic-only counts as a real character — `chars().count()`
        // on "á" is 1 (single grapheme), but "áb" is 2.
        assert!(!is_active_search(Some("á")));
        assert!(is_active_search(Some("áb")));
    }

    #[test]
    fn search_cached_json_helper_uses_private_short_max_age() {
        // Lock the contract: every body wrapped by
        // `search_cached_json` must come back with the standard
        // `private, max-age=60` policy. Two failure modes to guard:
        //   - regression to bare `no-store` (wasteful, what we
        //     replaced),
        //   - missing `private` (would let CF / ISP caches store
        //     query-derived responses).
        let resp = super::super::search_cached_json(Vec::<u8>::new());
        let cc = resp
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .expect("Cache-Control header must be set")
            .to_str()
            .unwrap();
        assert!(cc.contains("private"), "missing `private`: {cc}");
        assert!(cc.contains("max-age=60"), "missing `max-age=60`: {cc}");
        assert!(!cc.contains("public"), "must not be `public`: {cc}");
        assert!(!cc.contains("no-store"), "must not be `no-store`: {cc}");
    }

    #[test]
    fn search_cached_html_helper_uses_private_short_max_age() {
        // Same contract for the HTML helper used by
        // `films_list` / `series_list` / `tv_porady_list` on the
        // search-active branch.
        let resp = super::super::search_cached_html(String::from("<p>hi</p>"));
        let cc = resp
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .expect("Cache-Control header must be set")
            .to_str()
            .unwrap();
        assert!(cc.contains("private"), "missing `private`: {cc}");
        assert!(cc.contains("max-age=60"), "missing `max-age=60`: {cc}");
        assert!(!cc.contains("public"), "must not be `public`: {cc}");
    }

    #[test]
    fn predicate_wraps_columns_in_unaccent() {
        // Both modes must run unaccent() on the column AND on the bound
        // pattern — that's how diacritics-insensitive matching works
        // (#673). Lock the shape so we don't accidentally drop the
        // wrapper from one side of an ILIKE in a future refactor.
        let primary = FilmsSearchMode::Primary.predicate(1);
        assert!(
            primary.contains("unaccent(f.title) ILIKE unaccent($1)"),
            "{primary}"
        );
        assert!(
            primary.contains("unaccent(f.original_title) ILIKE unaccent($1)"),
            "{primary}"
        );

        let title_year = FilmsSearchMode::TitleYear.predicate(1);
        assert!(
            title_year
                .contains("unaccent(CONCAT_WS(' ', f.title, f.year::text)) ILIKE unaccent($1)"),
            "{title_year}"
        );
        assert!(
            title_year.contains(
                "unaccent(CONCAT_WS(' ', f.original_title, f.year::text)) ILIKE unaccent($1)"
            ),
            "{title_year}"
        );
    }

    #[test]
    fn raw_predicate_does_not_use_unaccent() {
        // raw_predicate is the ORDER BY tiebreaker that keeps
        // diacritic-exact hits in front. If unaccent() leaks in here,
        // every row becomes "exact" and the priority bucket collapses.
        let primary = FilmsSearchMode::Primary.raw_predicate(1);
        assert!(!primary.contains("unaccent"), "{primary}");
        assert!(primary.contains("f.title ILIKE $1"), "{primary}");
        assert!(primary.contains("f.original_title ILIKE $1"), "{primary}");

        let title_year = FilmsSearchMode::TitleYear.raw_predicate(1);
        assert!(!title_year.contains("unaccent"), "{title_year}");
        assert!(
            title_year.contains("CONCAT_WS(' ', f.title, f.year::text) ILIKE $1"),
            "{title_year}"
        );
    }

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
            audio: None,
            titulky: None,
        }
    }

    #[test]
    fn audio_filter_dub_matches_audio_langs_rollup() {
        let q = query_with_jazyk(Some("dub"));
        let sql = q.audio_filter().expect("expected filter for dub");
        assert!(
            sql.contains("f.audio_langs && ARRAY['cs','sk']"),
            "sql = {sql}"
        );
        assert!(!sql.contains("subtitle_langs"), "sql = {sql}");
    }

    #[test]
    fn audio_filter_sub_checks_subtitle_langs_rollup() {
        let q = query_with_jazyk(Some("sub"));
        let sql = q.audio_filter().expect("expected filter for sub");
        assert!(sql.contains("cardinality(f.subtitle_langs)"), "sql = {sql}");
        assert!(!sql.contains("audio_langs"), "sql = {sql}");
    }

    #[test]
    fn audio_filter_dub_and_sub_unions_both_rollups() {
        let q = query_with_jazyk(Some("dub,sub"));
        let sql = q.audio_filter().expect("expected filter for dub,sub");
        assert!(
            sql.contains("f.audio_langs && ARRAY['cs','sk']"),
            "sql = {sql}"
        );
        assert!(sql.contains("cardinality(f.subtitle_langs)"), "sql = {sql}");
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
