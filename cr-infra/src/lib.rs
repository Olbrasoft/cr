//! # cr-infra
//!
//! Infrastructure layer for the Czech Republic portal.
//!
//! Implements persistence (SQLx/PostgreSQL), CSV data import,
//! and external service integrations.
//!
//! ## Modules
//!
//! - `repositories` - SQLx-based repository implementations
//! - `db` (planned) - SQLx queries, migrations
//! - `import` (planned) - CSV importer for ČSÚ territorial data
//! - `github` (planned) - GitHub integration (Octocrab)

pub mod repositories;
