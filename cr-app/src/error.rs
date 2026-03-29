//! Application-layer error type.

/// Errors that can occur in the application layer.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("not found")]
    NotFound,

    #[error("repository error: {0}")]
    Repository(String),

    #[error("domain error: {0}")]
    Domain(#[from] cr_domain::DomainError),
}
