use anyhow::{Context, Result};
use serde::Deserialize;

use super::super::{VideoFormat, VideoInfo};

// Consent cookie value for bypassing Seznam CMP consent wall
const CONSENT_COOKIE: &str = "euconsent-v2=CPzqWAAPzqWAAAGABCCSC5CgAP_gAEPgACiQKZNB9G7WTXFneXp2YPskOYUX0VBJ4CUAAwgBwAIAIBoBKBECAAAAAKAAEIIAAAABBAAICIAAgBIBAAMBAgMNAEAMgAYCASgBIAKIEACEAAOECAAAJAgCBDAQIJCgBMATEACAAJAQEBBQBUCgAAAACAAAAAmAUYmAgAILAAiKAGAAQAAoACAAAABIAAAAAIgAAAAYAAAAYiAAAAAAAAAAAAAABAAAAAAAAAAAAgAAAAAQAAAIAAAAAAAIAAAAAAAAAAAAAAAAIAGAgAAAAABDQAEBAAIABgIAAAAAAAAAAAAAAAAAAAAAABAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAIAIAAAAAIAAAAYgAAAAAAAAAAAAAAEAAAAKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ";

/// Domains handled by our own Rust extractor (Seznam ecosystem).
const SEZNAM_DOMAINS: &[&str] = &["novinky.cz", "seznamzpravy.cz"];

/// Check if a URL is handled by our own Seznam extractor.
pub(crate) fn is_seznam_url(url: &str) -> bool {
    SEZNAM_DOMAINS.iter().any(|d| url.contains(d))
}

/// SDN manifest response structure.
#[derive(Deserialize)]
struct SdnManifest {
    data: SdnData,
}

#[derive(Deserialize)]
struct SdnData {
    mp4: std::collections::HashMap<String, SdnMp4Format>,
}

#[derive(Deserialize)]
struct SdnMp4Format {
    url: String,
    bandwidth: Option<u64>,
    #[allow(dead_code)]
    resolution: Option<Vec<u32>>,
    #[allow(dead_code)]
    duration: Option<u64>,
}

/// Extract video info from Seznam ecosystem (Novinky.cz, Seznam Zpravy).
pub(crate) async fn seznam_extract_info(client: &reqwest::Client, url: &str) -> Result<VideoInfo> {
    let html = client
        .get(url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        .header("Cookie", CONSENT_COOKIE)
        .send()
        .await
        .context("Failed to fetch page")?
        .text()
        .await
        .context("Failed to read page body")?;

    let sdn_url = extract_sdn_url(&html)
        .context("No video found on this page — could not find SDN video URL")?;

    let title = extract_title(&html);
    let thumbnail = extract_thumbnail(&html);
    let duration = extract_duration(&html);
    let uploader = extract_domain(url);

    let manifest_url = format!("{sdn_url}spl2,2,VOD");
    let manifest: SdnManifest = client
        .get(&manifest_url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
        .context("Failed to fetch video manifest")?
        .json()
        .await
        .context("Failed to parse video manifest JSON")?;

    let base_url = sdn_url
        .rfind("/vmd")
        .map(|i| &sdn_url[..i])
        .unwrap_or(&sdn_url);

    let mut formats: Vec<VideoFormat> = manifest
        .data
        .mp4
        .iter()
        .map(|(quality, fmt): (&String, &SdnMp4Format)| {
            let relative = &fmt.url;
            let absolute = if let Some(stripped) = relative.strip_prefix("../") {
                format!("{base_url}/{stripped}")
            } else if relative.starts_with('/') {
                let host = sdn_url
                    .find("//")
                    .and_then(|i| sdn_url[i + 2..].find('/').map(|j| &sdn_url[..i + 2 + j]))
                    .unwrap_or("");
                format!("{host}{relative}")
            } else {
                relative.to_string()
            };

            VideoFormat {
                format_id: quality.clone(),
                resolution: quality.clone(),
                ext: "mp4".to_string(),
                url: absolute,
                filesize_approx: fmt.bandwidth.map(|bw| {
                    let dur_ms = fmt.duration.unwrap_or(0);
                    bw * dur_ms / 8 / 1000
                }),
            }
        })
        .collect();

    formats.sort_by_key(|f| {
        f.resolution
            .trim_end_matches('p')
            .parse::<u32>()
            .unwrap_or(0)
    });

    if formats.is_empty() {
        anyhow::bail!("No MP4 formats found in video manifest");
    }

    Ok(VideoInfo {
        title,
        thumbnail,
        duration,
        uploader: Some(uploader),
        formats,
    })
}

/// Simple base64 decode helper.
pub(crate) fn base64_decode(input: &str) -> Result<String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(input)
        .context("base64 decode failed")?;
    String::from_utf8(bytes).context("UTF-8 decode failed")
}

/// Download a file directly via HTTP.
pub(crate) async fn download_direct(
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

// ─── Helper functions ───────────────────────────────────────────────

fn extract_sdn_url(html: &str) -> Option<String> {
    let marker = "\"sdn\":\"";
    let start = html.find(marker)? + marker.len();
    let end = html[start..].find('"')? + start;
    let url = &html[start..end];

    if url.contains("vmd") {
        Some(strip_sec1_prefix(url))
    } else {
        None
    }
}

fn strip_sec1_prefix(url: &str) -> String {
    if let Some(sec1_start) = url.find("/~SEC1~") {
        let after_sec1 = &url[sec1_start + 1..];
        if let Some(slash_pos) = after_sec1.find('/') {
            let host = &url[..sec1_start];
            let path = &after_sec1[slash_pos..];
            return format!("{host}{path}");
        }
    }
    url.to_string()
}

fn extract_title(html: &str) -> String {
    if let Some(title) = extract_json_string(html, "captionTitle")
        && !title.is_empty()
    {
        return title;
    }
    if let Some(start) = html.find("<title>") {
        let s = start + 7;
        if let Some(end) = html[s..].find("</title>") {
            let title = &html[s..s + end];
            return title.split('|').next().unwrap_or(title).trim().to_string();
        }
    }
    "Untitled".to_string()
}

fn extract_thumbnail(html: &str) -> Option<String> {
    if let Some(thumb) = extract_json_string(html, "thumbnailUrl") {
        let url = if thumb.starts_with("//") {
            format!("https:{thumb}")
        } else {
            thumb
        };
        return Some(url);
    }
    let marker = "property=\"og:image\" content=\"";
    if let Some(start) = html.find(marker) {
        let s = start + marker.len();
        if let Some(end) = html[s..].find('"') {
            return Some(html[s..s + end].to_string());
        }
    }
    None
}

fn extract_duration(html: &str) -> Option<f64> {
    let marker = "\"durationS\":";
    let start = html.find(marker)? + marker.len();
    let end = html[start..]
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(html[start..].len());
    html[start..start + end].parse::<f64>().ok()
}

fn extract_domain(url: &str) -> String {
    url.split("//")
        .nth(1)
        .and_then(|s| s.split('/').next())
        .unwrap_or("")
        .replace("www.", "")
        .to_string()
}

fn extract_json_string(html: &str, key: &str) -> Option<String> {
    let pattern = format!("\"{key}\":\"");
    let start = html.find(&pattern)? + pattern.len();
    let end = html[start..].find('"')? + start;
    let value = html[start..end].to_string();
    let value = value
        .replace("\\\"", "\"")
        .replace("\\/", "/")
        .replace("\\n", "\n");
    Some(value)
}
