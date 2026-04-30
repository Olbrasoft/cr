//! Read-side access for `prehrajto_search_hints` (issue #632, parent #631).
//!
//! Stores stable search inputs — `(film_id|episode_id, search_query, variant)`
//! — that the resolver in #633 will use to discover live prehraj.to IDs at
//! request time. Caching live IDs is unsafe because prehraj.to rotates them
//! on every re-upload; the hints table holds only the inputs we control.
//!
//! Wiring is intentionally minimal here — #633 introduces the resolver that
//! will call these functions. Marked `#[allow(dead_code)]` until then.

use chrono::{DateTime, Utc};
use sqlx::FromRow;

/// One search hint per `(owner, variant)` pair. Owner is XOR-polymorphic over
/// `film_id` / `episode_id`, enforced by the `prsh_owner_xor` CHECK constraint
/// on the table — code can rely on exactly one of the two being `Some`.
#[allow(dead_code)] // Used by #633 (resolver).
#[derive(Debug, Clone, FromRow)]
pub struct PrehrajtoSearchHint {
    pub id: i32,
    pub film_id: Option<i32>,
    pub episode_id: Option<i32>,
    pub search_query: String,
    pub variant: String,
    pub title_filter_regex: Option<String>,
    pub last_resolved_id: Option<String>,
    pub last_resolved_at: Option<DateTime<Utc>>,
}

#[allow(dead_code)] // Used by #633 (resolver).
pub async fn find_for_film(
    pool: &sqlx::PgPool,
    film_id: i32,
) -> Result<Vec<PrehrajtoSearchHint>, sqlx::Error> {
    sqlx::query_as::<_, PrehrajtoSearchHint>(
        "SELECT id, film_id, episode_id, search_query, variant, \
                title_filter_regex, last_resolved_id, last_resolved_at \
           FROM prehrajto_search_hints \
          WHERE film_id = $1 \
          ORDER BY variant",
    )
    .bind(film_id)
    .fetch_all(pool)
    .await
}

#[allow(dead_code)] // Used by #633 (resolver).
pub async fn find_for_episode(
    pool: &sqlx::PgPool,
    episode_id: i32,
) -> Result<Vec<PrehrajtoSearchHint>, sqlx::Error> {
    sqlx::query_as::<_, PrehrajtoSearchHint>(
        "SELECT id, film_id, episode_id, search_query, variant, \
                title_filter_regex, last_resolved_id, last_resolved_at \
           FROM prehrajto_search_hints \
          WHERE episode_id = $1 \
          ORDER BY variant",
    )
    .bind(episode_id)
    .fetch_all(pool)
    .await
}
