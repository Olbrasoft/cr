//! Web error type with proper logging and status-code mapping.
//!
//! Two variants:
//! - `Internal` — unexpected error; renders the 500 page and logs a full trace.
//! - `Status` — deliberate non-2xx (400, 403, 404, 502, …); renders a short
//!   plain-text body, no 500 page.
//!
//! Handlers return `WebResult<Response>`; use `?` on any `anyhow::Error`-
//! convertible type, or `WebError::bad_request("...")` / `.status(code, msg)`
//! for intentional client errors.

use askama::Template;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};

pub enum WebError {
    /// Unexpected — renders /500 and logs the backtrace.
    Internal(anyhow::Error),
    /// Intentional non-2xx — short plain-text body, short log line.
    Status(StatusCode, String),
}

impl WebError {
    pub fn status(code: StatusCode, message: impl Into<String>) -> Self {
        Self::Status(code, message.into())
    }
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::Status(StatusCode::BAD_REQUEST, message.into())
    }
    pub fn bad_gateway(message: impl Into<String>) -> Self {
        Self::Status(StatusCode::BAD_GATEWAY, message.into())
    }
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Status(StatusCode::FORBIDDEN, message.into())
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::Status(StatusCode::NOT_FOUND, message.into())
    }
}

// Blanket conversion for any `anyhow::Error`-convertible type (reqwest, sqlx,
// serde_json, askama, io, …) funnels into the Internal variant so `?` works
// in every handler.
impl<E: Into<anyhow::Error>> From<E> for WebError {
    fn from(err: E) -> Self {
        Self::Internal(err.into())
    }
}

#[derive(Template)]
#[template(path = "500.html")]
struct ErrorTemplate {
    img: String,
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        match self {
            WebError::Status(code, message) => {
                // Client-visible failures are not noise — log at info so
                // ops can still aggregate them without paging.
                tracing::info!(status = %code, "handler returned {code}: {message}");
                (code, message).into_response()
            }
            WebError::Internal(err) => {
                tracing::error!("Internal error: {:#}", err);
                let tmpl = ErrorTemplate { img: String::new() };
                let body = tmpl.render().unwrap_or_else(|e| {
                    tracing::error!("Failed to render 500 error template: {}", e);
                    "Internal Server Error".to_string()
                });
                (StatusCode::INTERNAL_SERVER_ERROR, Html(body)).into_response()
            }
        }
    }
}

pub type WebResult<T> = Result<T, WebError>;
