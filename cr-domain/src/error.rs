//! Domain-specific error types.

/// Errors that can occur when constructing or validating domain objects.
#[derive(Debug, Clone, PartialEq)]
pub enum DomainError {
    /// Latitude must be between -90 and 90.
    InvalidLatitude(f64),
    /// Longitude must be between -180 and 180.
    InvalidLongitude(f64),
    /// Name must not be empty.
    EmptyName,
}

impl std::fmt::Display for DomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLatitude(v) => write!(f, "invalid latitude {v}: must be -90..=90"),
            Self::InvalidLongitude(v) => write!(f, "invalid longitude {v}: must be -180..=180"),
            Self::EmptyName => write!(f, "name must not be empty"),
        }
    }
}

impl std::error::Error for DomainError {}
