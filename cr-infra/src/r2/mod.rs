//! Cloudflare R2 client.
//!
//! R2 speaks the S3 API on `https://<account_id>.r2.cloudflarestorage.com`.
//! We use `aws-sdk-s3` with the custom endpoint set to that URL, virtual-
//! hosted style disabled (path-style is what R2 supports), and the region
//! pinned to `auto`.
//!
//! Currently used to upload **video thumbnails** for the hosted video
//! library — they live in the existing `cr-images` bucket under
//! `videos/thumbs/`. The full public URL is
//! `{R2_PUBLIC_BASE_URL}/{key}` (the public URL is fronted by a Cloudflare
//! Worker that translates `/img/...` paths to bucket keys).

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;

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

    fn endpoint_url(&self) -> String {
        format!("https://{}.r2.cloudflarestorage.com", self.account_id)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum R2Error {
    #[error("S3 PutObject failed: {0}")]
    Put(String),
    #[error("S3 DeleteObject failed: {0}")]
    Delete(String),
    #[error("aws-sdk-s3 build error: {0}")]
    Build(String),
}

/// Thin wrapper around `aws-sdk-s3` configured for Cloudflare R2.
#[derive(Clone)]
pub struct R2Client {
    inner: Client,
    config: R2Config,
}

impl R2Client {
    pub fn new(config: R2Config) -> Self {
        let creds = Credentials::new(
            &config.access_key_id,
            &config.secret_access_key,
            None,
            None,
            "static",
        );
        let s3_config = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(Region::new("auto"))
            .endpoint_url(config.endpoint_url())
            .credentials_provider(creds)
            // R2 supports path-style; force it to be safe across SDK versions.
            .force_path_style(true)
            .build();
        let inner = Client::from_conf(s3_config);
        Self { inner, config }
    }

    /// Upload bytes to `key` and return the public CDN URL where the
    /// object will be served from (`{public_base_url}/{key}`).
    pub async fn upload_thumbnail(
        &self,
        key: &str,
        bytes: Bytes,
        content_type: &str,
    ) -> Result<String, R2Error> {
        self.inner
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .content_type(content_type)
            .cache_control("public, max-age=31536000, immutable")
            .send()
            .await
            .map_err(|e| R2Error::Put(format!("{e:?}")))?;
        Ok(format!(
            "{}/{}",
            self.config.public_base_url.trim_end_matches('/'),
            key
        ))
    }

    /// Remove an object from R2.
    pub async fn delete_object(&self, key: &str) -> Result<(), R2Error> {
        self.inner
            .delete_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| R2Error::Delete(format!("{e:?}")))?;
        Ok(())
    }
}
