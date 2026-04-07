//! Streamtape API client.
//!
//! Wraps the streamtape.com REST API endpoints we need for the video library:
//! upload, dlticket+dl (resolve direct CDN URL for inline playback), getsplash
//! (thumbnail fallback), delete, info. All endpoints take `login` and `key` as
//! query parameters.
//!
//! Reference: <https://streamtape.com/api>

use serde::{Deserialize, Serialize};

/// Streamtape API credentials.
///
/// Loaded from env at startup (`STREAMTAPE_LOGIN`, `STREAMTAPE_KEY`).
#[derive(Clone, Debug)]
pub struct StreamtapeConfig {
    pub login: String,
    pub key: String,
}

impl StreamtapeConfig {
    /// Load from environment variables. Returns `None` (instead of failing
    /// startup) when either variable is missing or empty so the rest of the
    /// app keeps working until the credentials are provisioned. Modules that
    /// actually need Streamtape can refuse to start when this is `None`.
    pub fn from_env() -> Option<Self> {
        let login = std::env::var("STREAMTAPE_LOGIN").ok()?;
        let key = std::env::var("STREAMTAPE_KEY").ok()?;
        if login.trim().is_empty() || key.trim().is_empty() {
            return None;
        }
        Some(Self { login, key })
    }
}

/// File metadata returned after a successful upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadedFile {
    /// Stable Streamtape file id (~14-15 chars).
    pub file_id: String,
    /// Public Streamtape URL: `https://streamtape.com/v/{file_id}/{name}`.
    pub url: String,
    /// Original file name supplied at upload time.
    pub name: String,
    /// Size in bytes as reported by Streamtape after the upload finished.
    pub size_bytes: u64,
    /// SHA-256 of the uploaded bytes (computed by Streamtape).
    pub sha256: String,
    /// MIME content-type detected by Streamtape (e.g. `video/mp4`).
    pub content_type: String,
}
