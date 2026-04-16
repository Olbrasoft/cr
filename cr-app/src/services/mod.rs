//! Use-case services — the orchestration layer between handlers and infra.
//!
//! Each service takes trait-object dependencies (repositories, external
//! clients) so tests can inject fakes. Handlers delegate to services
//! instead of calling repositories or infra crates directly.
//!
//! Before this module existed (#445), `cr-app/src/queries.rs` held only
//! pass-through wrappers with zero orchestration, and real business
//! logic lived in `cr-infra::video_library` (bypassing the use-case
//! layer). These services are the landing zone for pulling that logic
//! up over time.

pub mod video_publishing;
