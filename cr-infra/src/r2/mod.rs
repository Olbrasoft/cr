//! Cloudflare R2 client.
//!
//! R2 speaks the S3 API on `https://<account_id>.r2.cloudflarestorage.com`.
//! This module currently only carries the typed configuration loaded at
//! startup; the actual upload/delete client (built on `aws-sdk-s3` with a
//! custom endpoint) lands in a follow-up sub-issue.

/// Cloudflare R2 credentials and bucket settings.
///
/// Loaded from env at startup (`R2_*`). The bucket here is the same one
/// used by the existing image proxy (`cr-images`); video thumbnails live
/// under the `videos/thumbs/` prefix.
#[derive(Clone, Debug)]
pub struct R2Config {
    pub account_id: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
    /// Public CDN base URL where bucket objects are served from
    /// (e.g. `https://ceskarepublika.wiki`). The full thumbnail URL is
    /// `{public_base_url}/{key}`.
    pub public_base_url: String,
}

impl R2Config {
    /// Load from environment variables. Returns `None` if any required
    /// variable is missing or empty — modules that need R2 can refuse to
    /// start, while the rest of the app keeps running.
    pub fn from_env() -> Option<Self> {
        let get = |name: &str| -> Option<String> {
            let v = std::env::var(name).ok()?;
            if v.trim().is_empty() { None } else { Some(v) }
        };
        Some(Self {
            account_id: get("R2_ACCOUNT_ID")?,
            access_key_id: get("R2_ACCESS_KEY_ID")?,
            secret_access_key: get("R2_SECRET_ACCESS_KEY")?,
            bucket: get("R2_BUCKET")?,
            public_base_url: get("R2_PUBLIC_BASE_URL")?,
        })
    }
}
