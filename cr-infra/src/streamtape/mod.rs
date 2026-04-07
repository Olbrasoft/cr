//! Streamtape API client.
//!
//! Wraps the streamtape.com REST API endpoints we need for the video library:
//! upload, dlticket+dl (resolve direct CDN URL for inline playback), getsplash
//! (thumbnail fallback), delete, info. All endpoints take `login` and `key` as
//! query parameters.
//!
//! Reference: <https://streamtape.com/api>

use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

const API_BASE: &str = "https://api.streamtape.com";

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

/// Metadata returned by `/file/info`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub file_id: String,
    pub name: String,
    pub size_bytes: u64,
    pub content_type: String,
    /// Streamtape returns the file's converted/streamable status here.
    pub status: i64,
}

/// Errors raised by the Streamtape client.
#[derive(Debug, thiserror::Error)]
pub enum StreamtapeError {
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("io error reading local file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Streamtape API returned status {status}: {msg}")]
    Api { status: i64, msg: String },
    #[error("file not found on Streamtape (file_id={0})")]
    NotFound(String),
    #[error("response missing expected field: {0}")]
    MissingField(&'static str),
    #[error("invalid response from upload server: {0}")]
    InvalidUploadResponse(String),
}

/// Streamtape envelope every API endpoint wraps its response in:
/// `{ status: 200, msg: "OK", result: <T> }`.
#[derive(Debug, Deserialize)]
struct Envelope<T> {
    status: i64,
    msg: String,
    result: Option<T>,
}

impl<T> Envelope<T> {
    fn into_result(self, file_id: Option<&str>) -> Result<T, StreamtapeError> {
        if self.status == 200 {
            self.result.ok_or(StreamtapeError::MissingField("result"))
        } else if self.status == 404
            && let Some(id) = file_id
        {
            Err(StreamtapeError::NotFound(id.to_string()))
        } else {
            Err(StreamtapeError::Api {
                status: self.status,
                msg: self.msg,
            })
        }
    }
}

// --- Upload response shapes ---

#[derive(Debug, Deserialize)]
struct UlResult {
    url: String,
    #[allow(dead_code)]
    valid_until: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UploadResponse {
    status: i64,
    msg: String,
    result: Option<UploadResult>,
}

#[derive(Debug, Deserialize)]
struct UploadResult {
    url: String,
    id: String,
    name: String,
    /// Streamtape's upload endpoint sometimes returns size as a JSON
    /// string ("13704925") rather than a number — accept either via the
    /// helper below.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    size: u64,
    sha256: String,
    content_type: String,
}

fn deserialize_u64_or_string<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let v = serde_json::Value::deserialize(d)?;
    match v {
        serde_json::Value::Number(n) => n.as_u64().ok_or_else(|| Error::custom("not u64")),
        serde_json::Value::String(s) => s.parse().map_err(Error::custom),
        _ => Err(Error::custom("expected u64 or string")),
    }
}

// --- dlticket / dl response shapes ---

#[derive(Debug, Deserialize)]
struct DlTicketResult {
    ticket: String,
    wait_time: u64,
}

#[derive(Debug, Deserialize)]
struct DlResult {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    size: u64,
    url: String,
}

// --- getsplash / info / file/info response shapes ---

#[derive(Debug, Deserialize)]
struct SplashResult {
    splash_url: String,
}

/// `/file/info` returns a map keyed by file_id; we only ever ask for one id.
type InfoResult = std::collections::HashMap<String, Option<InfoEntry>>;

#[derive(Debug, Deserialize)]
struct InfoEntry {
    id: String,
    name: String,
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    size: u64,
    content_type: String,
    status: i64,
}

/// Streamtape API client.
#[derive(Clone)]
pub struct StreamtapeClient {
    http: reqwest::Client,
    config: StreamtapeConfig,
}

impl StreamtapeClient {
    pub fn new(http: reqwest::Client, config: StreamtapeConfig) -> Self {
        Self { http, config }
    }

    fn auth_pair(&self) -> [(&str, &str); 2] {
        [("login", &self.config.login), ("key", &self.config.key)]
    }

    /// Upload a local file to Streamtape and return its stable id + URL.
    ///
    /// This is a two-step API: first GET `/file/ul` to obtain a one-shot
    /// upload URL on the cluster, then POST the file to that URL as
    /// multipart/form-data with the field name `file1`.
    ///
    /// Retries up to 3 times on transient upload errors (HTTP 5xx, network
    /// failures). Each retry fetches a fresh upload URL because Streamtape
    /// upload nodes are sticky (the URL has a server hash baked in) — a
    /// flaky cluster will keep failing until we move to a different node.
    pub async fn upload(
        &self,
        path: &Path,
        display_name: &str,
    ) -> Result<UploadedFile, StreamtapeError> {
        const MAX_ATTEMPTS: usize = 3;
        for attempt in 1..=MAX_ATTEMPTS {
            match self.upload_once(path, display_name).await {
                Ok(uploaded) => {
                    if attempt > 1 {
                        tracing::info!(
                            "streamtape upload succeeded on attempt {attempt}/{MAX_ATTEMPTS}"
                        );
                    }
                    return Ok(uploaded);
                }
                Err(e) => {
                    if attempt >= MAX_ATTEMPTS || !is_retryable_upload_error(&e) {
                        return Err(e);
                    }
                    let backoff = Duration::from_millis(500 * (1 << (attempt - 1)));
                    tracing::warn!(
                        "streamtape upload attempt {attempt}/{MAX_ATTEMPTS} failed: {e} — retrying after {backoff:?}"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
        Err(StreamtapeError::MissingField("upload.retries.exhausted"))
    }

    async fn upload_once(
        &self,
        path: &Path,
        display_name: &str,
    ) -> Result<UploadedFile, StreamtapeError> {
        // 1) Get a fresh upload URL — must be re-fetched per attempt because
        //    each is one-shot and bound to a specific Streamtape cluster.
        let url_resp: Envelope<UlResult> = self
            .http
            .get(format!("{API_BASE}/file/ul"))
            .query(&self.auth_pair())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let upload_url = url_resp.into_result(None)?.url;

        // 2) Stream the file as multipart. The filename is truncated to a
        //    short ASCII slug — long filenames have caused HTTP 500s on
        //    Streamtape's upload nodes (observed in production with the
        //    "Galaktická rada lhala…" video, where the 80-char title made
        //    the upload reject). curl's basename-style upload always works
        //    so we mimic that here.
        let short_name = shorten_upload_filename(display_name);
        let bytes = tokio::fs::read(path).await?;
        let part = reqwest::multipart::Part::bytes(bytes)
            .file_name(short_name)
            .mime_str("video/mp4")
            .unwrap_or_else(|_| reqwest::multipart::Part::bytes(Vec::new()).file_name("video.mp4"));
        let form = reqwest::multipart::Form::new().part("file1", part);

        let raw = self
            .http
            .post(&upload_url)
            .multipart(form)
            // Uploads can be large; let the caller's outer timeout govern total time.
            .timeout(Duration::from_secs(30 * 60))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        let resp: UploadResponse = serde_json::from_str(&raw)
            .map_err(|e| StreamtapeError::InvalidUploadResponse(format!("{e}: {raw}")))?;
        if resp.status != 200 {
            return Err(StreamtapeError::Api {
                status: resp.status,
                msg: resp.msg,
            });
        }
        let result = resp
            .result
            .ok_or(StreamtapeError::MissingField("upload.result"))?;
        Ok(UploadedFile {
            file_id: result.id,
            url: result.url,
            name: result.name,
            size_bytes: result.size,
            sha256: result.sha256,
            content_type: result.content_type,
        })
    }

    /// Resolve a fresh direct MP4 URL on `tapecontent.net` for inline playback.
    ///
    /// Streamtape requires a 5-second wait between dlticket and dl — that
    /// delay is built in. Cache the returned URL upstream (~45 min token
    /// validity) to avoid the wait on every play.
    pub async fn get_stream_url(&self, file_id: &str) -> Result<String, StreamtapeError> {
        // 1) dlticket
        let ticket_resp: Envelope<DlTicketResult> = self
            .http
            .get(format!("{API_BASE}/file/dlticket"))
            .query(&self.auth_pair())
            .query(&[("file", file_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let ticket_data = ticket_resp.into_result(Some(file_id))?;

        // 2) Wait the requested time (typically 5s)
        tokio::time::sleep(Duration::from_secs(ticket_data.wait_time)).await;

        // 3) dl — note: NO login/key here, only file + ticket
        let dl_resp: Envelope<DlResult> = self
            .http
            .get(format!("{API_BASE}/file/dl"))
            .query(&[("file", file_id), ("ticket", &ticket_data.ticket)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(dl_resp.into_result(Some(file_id))?.url)
    }

    /// Generated splash/thumbnail image URL — used as fallback when the
    /// upstream yt-dlp thumbnail is unavailable.
    pub async fn get_splash_url(&self, file_id: &str) -> Result<String, StreamtapeError> {
        let resp: Envelope<SplashResult> = self
            .http
            .get(format!("{API_BASE}/file/getsplash"))
            .query(&self.auth_pair())
            .query(&[("file", file_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp.into_result(Some(file_id))?.splash_url)
    }

    /// Delete a file from our Streamtape account.
    pub async fn delete(&self, file_id: &str) -> Result<(), StreamtapeError> {
        let resp: Envelope<serde_json::Value> = self
            .http
            .get(format!("{API_BASE}/file/delete"))
            .query(&self.auth_pair())
            .query(&[("file", file_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        // delete returns no useful payload; we just need the status check.
        let _ = resp.into_result(Some(file_id))?;
        Ok(())
    }

    /// Look up metadata for a single file id.
    pub async fn get_info(&self, file_id: &str) -> Result<FileInfo, StreamtapeError> {
        let resp: Envelope<InfoResult> = self
            .http
            .get(format!("{API_BASE}/file/info"))
            .query(&self.auth_pair())
            .query(&[("file", file_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let map = resp.into_result(Some(file_id))?;
        let entry = map
            .get(file_id)
            .and_then(|e| e.as_ref())
            .ok_or_else(|| StreamtapeError::NotFound(file_id.to_string()))?;
        Ok(FileInfo {
            file_id: entry.id.clone(),
            name: entry.name.clone(),
            size_bytes: entry.size,
            content_type: entry.content_type.clone(),
            status: entry.status,
        })
    }
}

/// Should the upload retry after this error?
///
/// Network failures, malformed responses (often a partial body from a
/// dying upload node) and 5xx API errors are transient. Auth/4xx errors
/// and not-found are not.
fn is_retryable_upload_error(e: &StreamtapeError) -> bool {
    match e {
        StreamtapeError::Network(_) => true,
        StreamtapeError::InvalidUploadResponse(_) => true,
        StreamtapeError::Api { status, .. } => *status >= 500,
        _ => false,
    }
}

/// Shorten a filename for the multipart upload field. Streamtape's upload
/// nodes appear to reject long filenames with HTTP 500 — we observed this
/// in production with the 80-char "Galaktická rada lhala…" title where
/// our reqwest upload failed but a hand-curl with `galakt-test.mp4`
/// succeeded against the exact same upload URL. Truncate to ~32 chars.
fn shorten_upload_filename(input: &str) -> String {
    // Split off any existing extension and preserve it.
    let (stem, ext) = match input.rsplit_once('.') {
        Some((s, e)) if !e.is_empty() && e.len() <= 6 => (s, e),
        _ => (input, "mp4"),
    };
    let mut shortened: String = stem.chars().take(32).collect::<String>().trim().to_string();
    if shortened.is_empty() {
        shortened = "video".to_string();
    }
    format!("{shortened}.{ext}")
}

#[cfg(test)]
mod tests {
    use super::shorten_upload_filename;

    #[test]
    fn long_title_gets_truncated() {
        // The actual title that broke the upload in production.
        let long = "Galaktick rada lhala Zem ude ila okam it a zanechala galaxii v oku.mp4";
        let short = shorten_upload_filename(long);
        assert!(short.len() <= 40, "got {short:?}");
        assert!(short.ends_with(".mp4"));
    }

    #[test]
    fn short_title_kept() {
        assert_eq!(shorten_upload_filename("test.mp4"), "test.mp4");
    }

    #[test]
    fn empty_falls_back() {
        assert_eq!(shorten_upload_filename(".mp4"), "video.mp4");
        assert_eq!(shorten_upload_filename(""), "video.mp4");
    }

    #[test]
    fn no_extension_defaults_to_mp4() {
        assert_eq!(shorten_upload_filename("video"), "video.mp4");
    }
}
