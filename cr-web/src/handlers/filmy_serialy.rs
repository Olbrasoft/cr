use super::*;
use axum::extract::Query;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct FilmySerialyQuery {
    q: Option<String>,
}

pub async fn filmy_serialy(
    State(state): State<AppState>,
    Query(params): Query<FilmySerialyQuery>,
) -> WebResult<impl IntoResponse> {
    // When ?q=<query> is present, do a server-side search so the first result's
    // thumbnail can be injected into the og:image meta tag. This gives WhatsApp,
    // Facebook and other link-preview crawlers a rich thumbnail instead of the
    // static logo — without blocking the client-side search that re-runs below.
    let query_str: Option<String> = params.q.as_ref().and_then(|q| {
        let t = q.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    });

    let mut og_image: Option<String> = None;
    let mut og_title: Option<String> = None;

    if let Some(q) = query_str.as_ref() {
        og_title = Some(format!("Filmy a seriály — {q} | ceskarepublika.wiki"));
        if let Some(thumb) = fetch_first_thumbnail(&state, q).await {
            og_image = Some(proxy_thumb_url(&thumb));
        }
    }

    let tmpl = FilmySerialyTemplate {
        img: state.image_base_url.clone(),
        og_image,
        og_title,
    };
    Ok(Html(tmpl.render()?))
}

/// Fetch the first movie thumbnail URL for a query via the CzProxy search.
/// Returns None on any error (network, parse, no results) — the page still
/// renders with the default static og:image.
async fn fetch_first_thumbnail(state: &AppState, query: &str) -> Option<String> {
    let cz = state.config.cz_proxy.as_ref()?;
    let proxy_url = cz.url.clone();
    let proxy_key = cz.key.clone();

    let url = format!(
        "{}?action=search&q={}&key={}",
        proxy_url,
        urlencoding::encode(query),
        proxy_key
    );

    let resp = state
        .http_client
        .get(&url)
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await
        .ok()?;

    #[derive(serde::Deserialize)]
    struct Resp {
        movies: Option<Vec<Movie>>,
    }
    #[derive(serde::Deserialize)]
    struct Movie {
        thumbnail: Option<String>,
    }

    let data: Resp = resp.json().await.ok()?;
    data.movies?
        .into_iter()
        .find_map(|m| m.thumbnail.filter(|t| !t.trim().is_empty()))
}

/// Wrap an upstream thumbnail URL in our proxy endpoint so link-preview crawlers
/// hit ceskarepublika.wiki, not thumb.prehrajto.cz.
fn proxy_thumb_url(upstream: &str) -> String {
    format!(
        "https://ceskarepublika.wiki/api/movies/thumb?url={}",
        urlencoding::encode(upstream)
    )
}
