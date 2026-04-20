//! `/admin/test-sledujteto/` — internal diagnostic page for sledujteto.cz.
//!
//! Renders a search UI + hardcoded "Rychlé testy" tiles that exercise the
//! live hash-gen and CDN streaming path. Useful when sledujteto ships a
//! change that breaks our integration — one click tells us whether it's
//! an IP block, an API schema change, or upstream is down.
//!
//! Served under `/admin/` with `X-Robots-Tag: noindex, nofollow` so the
//! page never leaks into search. The rest of `/admin/` is also currently
//! auth-less (see comment in `admin_dashboard.rs`); once admin auth lands,
//! this page inherits it automatically.

use askama::Template;
use axum::extract::State;
use axum::response::{Html, IntoResponse, Response};

use crate::error::WebResult;
use crate::state::AppState;

#[derive(Template)]
#[template(path = "admin_test_sledujteto.html")]
struct AdminTestSledujtetoTemplate {
    img: String,
}

pub async fn admin_test_sledujteto(State(state): State<AppState>) -> WebResult<Response> {
    let tmpl = AdminTestSledujtetoTemplate {
        img: state.image_base_url.clone(),
    };
    let mut resp = Html(tmpl.render()?).into_response();
    resp.headers_mut().insert(
        "X-Robots-Tag",
        axum::http::HeaderValue::from_static("noindex, nofollow"),
    );
    Ok(resp)
}
