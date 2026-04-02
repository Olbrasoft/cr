use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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

/// Extract video info by calling the Python extraction script as a subprocess.
pub async fn extract_video_info(url: &str) -> Result<VideoInfo> {
    let script_path = find_script_path();

    let output = tokio::process::Command::new("python3")
        .arg(&script_path)
        .arg(url)
        .output()
        .await
        .context("Failed to run extract_video.py — is python3 + playwright installed?")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Video extraction failed: {stderr}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let info: VideoInfo =
        serde_json::from_str(&stdout).context("Failed to parse extraction script output")?;

    if info.formats.is_empty() {
        anyhow::bail!("No downloadable video formats found for this URL");
    }

    Ok(info)
}

/// Download a video file to a specified path.
pub async fn download_video_file(
    client: &reqwest::Client,
    video_url: &str,
    output_path: &std::path::Path,
) -> Result<u64> {
    let resp = client
        .get(video_url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
        .context("Failed to download video")?;

    if !resp.status().is_success() {
        anyhow::bail!("Video download returned HTTP {}", resp.status());
    }

    let bytes = resp
        .bytes()
        .await
        .context("Failed to read video response body")?;

    let size = bytes.len() as u64;
    tokio::fs::write(output_path, &bytes)
        .await
        .context("Failed to write video file")?;

    Ok(size)
}

/// Check if a URL is supported by our extractors.
pub fn is_supported_url(url: &str) -> bool {
    let supported = ["novinky.cz", "seznamzpravy.cz", "stream.cz"];
    supported.iter().any(|domain| url.contains(domain))
}

/// Find the extraction script path.
fn find_script_path() -> PathBuf {
    // Try relative to working directory first (Docker)
    let candidates = [
        PathBuf::from("scripts/extract_video.py"),
        PathBuf::from("/app/scripts/extract_video.py"),
    ];

    for path in &candidates {
        if path.exists() {
            return path.clone();
        }
    }

    // Default
    candidates[0].clone()
}
