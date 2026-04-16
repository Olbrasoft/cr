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

fn is_seznam_url(url: &str) -> bool {
    extractors::seznam::is_seznam_url(url)
}

fn is_instagram_url(url: &str) -> bool {
    let lower = url.to_lowercase();
    lower.contains("instagram.com") || lower.contains("cdninstagram.com")
}

fn is_nova_url(url: &str) -> bool {
    let lower = url.to_lowercase();
    lower.contains("nova.cz") || lower.contains("voyo.nova.cz") || lower.contains("tn.cz")
}

/// Extract video metadata from any supported URL.
pub async fn extract_video_info(client: &reqwest::Client, url: &str) -> Result<VideoInfo> {
    if is_seznam_url(url) {
        extractors::seznam::seznam_extract_info(client, url).await
    } else if is_instagram_url(url) {
        extractors::instagram::instagram_extract_info(client, url).await
    } else {
        let result = extractors::ytdlp::ytdlp_extract_info(url).await;
        if result.is_err() && is_nova_url(url) {
            tracing::warn!("yt-dlp failed for Nova.cz URL, trying Czech proxy fallback");
            if let Ok(info) = extractors::nova::nova_proxy_extract_info(client, url).await {
                return Ok(info);
            }
        }
        result
    }
}

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

pub async fn download_video_with_progress(
    client: &reqwest::Client,
    url: &str,
    format_id: &str,
    resolution: &str,
    container: &str,
    output_path: &std::path::Path,
    progress: Option<std::sync::Arc<std::sync::atomic::AtomicU8>>,
) -> Result<u64> {
    if is_seznam_url(url) {
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

    ensure_container(output_path, container, progress.clone()).await?;

    let meta = tokio::fs::metadata(output_path)
        .await
        .context("Output file missing after ensure_container")?;
    Ok(meta.len())
}
