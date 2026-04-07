//! SQLx-based repository implementations for PostgreSQL.
//!
//! Each struct wraps a `sqlx::PgPool` and implements the corresponding
//! trait from `cr_domain::repository`.

mod landmark;
mod municipality;
mod orp;
mod photo;
mod pool;
mod region;
mod video_library;

pub use landmark::PgLandmarkRepository;
pub use municipality::PgMunicipalityRepository;
pub use orp::PgOrpRepository;
pub use photo::PgPhotoRepository;
pub use pool::PgPoolRepository;
pub use region::PgRegionRepository;
pub use video_library::PgVideoRepository;
