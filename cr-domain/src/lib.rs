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
//! - `dto` - Plain row-shaped records returned by repositories
//! - `error` - Domain-specific error types
//! - `id` - Strongly-typed ID wrappers
//! - `repository` - Repository trait definitions (ports)
//! - `slug` - URL slug generation from Czech names

pub mod coordinates;
pub mod dto;
pub mod entities;
pub mod error;
pub mod id;
pub mod repository;
pub mod slug;

pub use coordinates::Coordinates;
pub use error::DomainError;
pub use id::*;
pub use slug::slug_from_name;
