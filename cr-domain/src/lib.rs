//! # cr-domain
//!
//! Domain layer for the Czech Republic portal.
//!
//! Contains pure entities (structs), enums, and traits
//! with zero external framework dependencies.
//!
//! ## Modules
//!
//! - `entities` - Region, District, Orp, Municipality structs
//! - `coordinates` - Coordinates value object with validation
//! - `error` - Domain-specific error types
//! - `slug` - URL slug generation from Czech names

pub mod coordinates;
pub mod entities;
pub mod error;
pub mod id;
pub mod slug;

pub use coordinates::Coordinates;
pub use error::DomainError;
pub use id::*;
pub use slug::slug_from_name;
