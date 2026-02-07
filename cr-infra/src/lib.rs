//! # cr-infra
//!
//! Infrastructure layer for the Czech Republic portal.
//!
//! Implements persistence (SQLx/PostgreSQL), CSV data import,
//! and external service integrations.
//!
//! ## Modules (planned)
//!
//! - `db` - SQLx queries, migrations
//! - `import` - CSV importer for ČSÚ territorial data
//! - `github` - GitHub integration (Octocrab)
