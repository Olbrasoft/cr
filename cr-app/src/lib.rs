//! # cr-app
//!
//! Application layer for the Czech Republic portal.
//!
//! Contains use-cases, query functions, and DTOs.
//! Orchestrates between domain logic and infrastructure.
//!
//! ## Modules
//!
//! - `error` - Application-layer error types
//! - `queries` - Read operations (homepage, region detail, etc.)

pub mod error;
pub mod queries;
