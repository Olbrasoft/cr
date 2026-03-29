//! Web error type with proper logging and 500 response.

use askama::Template;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};

/// Web-layer error that logs and returns a 500 error page.
pub struct WebError(pub anyhow::Error);

impl<E: Into<anyhow::Error>> From<E> for WebError {
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[derive(Template)]
#[template(path = "500.html")]
struct ErrorTemplate {
    img: String,
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        tracing::error!("Internal error: {:#}", self.0);
        let tmpl = ErrorTemplate { img: String::new() };
        let body = tmpl.render().unwrap_or_else(|e| {
            tracing::error!("Failed to render 500 error template: {}", e);
            "Internal Server Error".to_string()
        });
        (StatusCode::INTERNAL_SERVER_ERROR, Html(body)).into_response()
    }
}

/// Result type for web handlers.
pub type WebResult<T> = Result<T, WebError>;
