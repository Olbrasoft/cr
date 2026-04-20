//! Experimental mirror of `/filmy-a-serialy/` that uses sledujteto.cz as the
//! upstream source instead of prehraj.to.
//!
//! Test page exposed at `/filmy-a-serialy-1/`. User types a query → backend
//! hits sledujteto's search API → frontend renders result cards → click →
//! backend resolves `files_id` to a fresh `data{N}.sledujteto.cz` stream URL
//! → browser plays directly (HTML5 `<video>`). No server-side proxy: the
//! stream CDN is geo-blocked to CZ, but our visitors already sit on CZ IPs,
//! so they reach it directly.

use askama::Template;
use axum::extract::State;
use axum::response::{Html, IntoResponse};

use crate::error::WebResult;
use crate::state::AppState;

#[derive(Template)]
#[template(path = "filmy_serialy_sledujteto.html")]
struct FilmySerialySledujtetoTemplate {
    img: String,
}

pub async fn filmy_serialy_sledujteto(
    State(state): State<AppState>,
) -> WebResult<impl IntoResponse> {
    let tmpl = FilmySerialySledujtetoTemplate {
        img: state.image_base_url.clone(),
    };
    Ok(Html(tmpl.render()?))
}
