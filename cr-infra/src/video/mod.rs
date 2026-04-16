use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

mod extractors;
mod transcode;

pub use transcode::{
    WhatsAppPart, WhatsAppResult, convert_for_whatsapp, ensure_container, estimate_whatsapp_parts,
    extract_audio,
};

/// Information about a video extracted from a URL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoInfo {
    pub title: String,
    pub thumbnail: Option<String>,
    pub duration: Option<f64>,
    pub uploader: Option<String>,
    pub formats: Vec<VideoFormat>,
}

/// A single downloadable format/quality of a video.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoFormat {
    pub format_id: String,
    pub resolution: String,
    pub ext: String,
    pub url: String,
    pub filesize_approx: Option<u64>,
}

/// Check if URL is an Instagram reel/video.
fn is_instagram_url(url: &str) -> bool {
    url.contains("instagram.com")
}

/// Check if URL is Nova.cz (validates host, not substring).
fn is_nova_url(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_ascii_lowercase()))
        .map(|host| host == "nova.cz" || host.ends_with(".nova.cz"))
        .unwrap_or(false)
}

// ─── Public API ─────────────────────────────────────────────────────

/// Extract video info. Tries our extractors first, falls back to yt-dlp.
pub async fn extract_video_info(client: &reqwest::Client, url: &str) -> Result<VideoInfo> {
    if extractors::seznam::is_seznam_url(url) {
        extractors::seznam::seznam_extract_info(client, url).await
    } else if is_instagram_url(url) {
        extractors::instagram::instagram_extract_info(client, url).await
    } else {
        let result = extractors::ytdlp::ytdlp_extract_info(url).await;
        // If yt-dlp fails for Nova.cz, try Czech proxy to bypass geo-block
        if result.is_err() && is_nova_url(url) {
            tracing::warn!("yt-dlp failed for Nova.cz URL, trying Czech proxy fallback");
            if let Ok(info) = extractors::nova::nova_proxy_extract_info(client, url).await {
                return Ok(info);
            }
        }
        result
    }
}

/// Download a video file. Uses direct HTTP for Seznam/Instagram, yt-dlp for others.
/// After the download, always enforces the requested container format
/// via [`ensure_container`] so the caller gets exactly `.{container}`.
pub async fn download_video(
    client: &reqwest::Client,
    url: &str,
    format_id: &str,
    resolution: &str,
    container: &str,
    output_path: &std::path::Path,
) -> Result<u64> {
    download_video_with_progress(
        client,
        url,
        format_id,
        resolution,
        container,
        output_path,
        None,
    )
    .await
}

/// Download a video file with optional progress tracking, guaranteeing
/// the output is in the requested container (MP4 / WebM).
///
/// The `output_path` must already carry the `.{container}` extension
/// set by the caller. After the per-extractor download step finishes,
/// the file goes through [`ensure_container`] which ffprobes the real
/// on-disk container and, if it doesn't match, transcodes via ffmpeg
/// (see #366).
pub async fn download_video_with_progress(
    client: &reqwest::Client,
    url: &str,
    format_id: &str,
    resolution: &str,
    container: &str,
    output_path: &std::path::Path,
    progress: Option<std::sync::Arc<std::sync::atomic::AtomicU8>>,
) -> Result<u64> {
    // Raw-bytes extractors (Seznam / Instagram) and the Nova proxy
    // hand us arbitrary containers — yt-dlp's container-level flags
    // don't apply here, so the post-download `ensure_container` step
    // is the single source of truth for the final file format.
    if extractors::seznam::is_seznam_url(url) {
        let info = extractors::seznam::seznam_extract_info(client, url).await?;
        let fmt = info
            .formats
            .iter()
            .find(|f| f.format_id == format_id)
            .or(info.formats.last())
            .context("No format available")?;
        extractors::seznam::download_direct(client, &fmt.url, output_path).await?;
    } else if is_instagram_url(url) {
        let info = extractors::instagram::instagram_extract_info(client, url).await?;
        let fmt = info
            .formats
            .iter()
            .find(|f| f.format_id == format_id)
            .or(info.formats.last())
            .context("No format available")?;
        extractors::seznam::download_direct(client, &fmt.url, output_path).await?;
    } else if format_id == "proxy-hls" {
        // Nova.cz proxy fallback — `format_id` is a sentinel, not the direct m3u8 URL.
        // Re-extract to obtain a fresh tokenized manifest URL from `info.formats[0].url`.
        let info = extractors::nova::nova_proxy_extract_info(client, url).await?;
        let m3u8 = &info.formats[0].url;
        extractors::ytdlp::ytdlp_download(m3u8, "best", container, output_path, progress.clone())
            .await?;
    } else {
        extractors::ytdlp::ytdlp_download(
            url,
            resolution,
            container,
            output_path,
            progress.clone(),
        )
        .await?;
    }

    // Single source of truth for the final container (#366).
    ensure_container(output_path, container, progress.clone()).await?;

    let meta = tokio::fs::metadata(output_path)
        .await
        .context("Output file missing after ensure_container")?;
    Ok(meta.len())
}
