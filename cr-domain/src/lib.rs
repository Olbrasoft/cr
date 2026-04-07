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
//! - `id` - Strongly-typed ID wrappers
//! - `repository` - Repository trait definitions (ports)
//! - `slug` - URL slug generation from Czech names

pub mod coordinates;
pub mod entities;
pub mod error;
pub mod id;
pub mod intentional_negative_test_for_ci_wake;
pub mod repository;
pub mod slug;

pub use coordinates::Coordinates;
pub use error::DomainError;
pub use id::*;
pub use slug::slug_from_name;
