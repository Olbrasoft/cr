use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

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

// Consent cookie value for bypassing Seznam CMP consent wall
const CONSENT_COOKIE: &str = "euconsent-v2=CPzqWAAPzqWAAAGABCCSC5CgAP_gAEPgACiQKZNB9G7WTXFneXp2YPskOYUX0VBJ4CUAAwgBwAIAIBoBKBECAAAAAKAAEIIAAAABBAAICIAAgBIBAAMBAgMNAEAMgAYCASgBIAKIEACEAAOECAAAJAgCBDAQIJCgBMATEACAAJAQEBBQBUCgAAAACAAAAAmAUYmAgAILAAiKAGAAQAAoACAAAABIAAAAAIgAAAAYAAAAYiAAAAAAAAAAAAAABAAAAAAAAAAAAgAAAAAQAAAIAAAAAAAIAAAAAAAAAAAAAAAAIAGAgAAAAABDQAEBAAIABgIAAAAAAAAAAAAAAAAAAAAAABAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAIAIAAAAAIAAAAYgAAAAAAAAAAAAAAEAAAAKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ";

/// Domains handled by our own Rust extractor (Seznam ecosystem).
const SEZNAM_DOMAINS: &[&str] = &["novinky.cz", "seznamzpravy.cz"];

/// Check if a URL is handled by our own Seznam extractor.
fn is_seznam_url(url: &str) -> bool {
    SEZNAM_DOMAINS.iter().any(|d| url.contains(d))
}

/// Check if URL is an Instagram reel/video.
fn is_instagram_url(url: &str) -> bool {
    url.contains("instagram.com")
}

// ─── Public API ─────────────────────────────────────────────────────

/// Extract video info. Tries our extractors first, falls back to yt-dlp.
pub async fn extract_video_info(client: &reqwest::Client, url: &str) -> Result<VideoInfo> {
    if is_seznam_url(url) {
        seznam_extract_info(client, url).await
    } else if is_instagram_url(url) {
        instagram_extract_info(client, url).await
    } else {
        ytdlp_extract_info(url).await
    }
}

/// Download a video file. Uses direct HTTP for Seznam/Instagram, yt-dlp for others.
pub async fn download_video(
    client: &reqwest::Client,
    url: &str,
    format_id: &str,
    resolution: &str,
    output_path: &std::path::Path,
) -> Result<u64> {
    download_video_with_progress(client, url, format_id, resolution, output_path, None).await
}

/// Download a video file with optional progress tracking.
pub async fn download_video_with_progress(
    client: &reqwest::Client,
    url: &str,
    format_id: &str,
    resolution: &str,
    output_path: &std::path::Path,
    progress: Option<std::sync::Arc<std::sync::atomic::AtomicU8>>,
) -> Result<u64> {
    if is_seznam_url(url) {
        let info = seznam_extract_info(client, url).await?;
        let fmt = info
            .formats
            .iter()
            .find(|f| f.format_id == format_id)
            .or(info.formats.last())
            .context("No format available")?;
        download_direct(client, &fmt.url, output_path).await
    } else if is_instagram_url(url) {
        let info = instagram_extract_info(client, url).await?;
        let fmt = info
            .formats
            .iter()
            .find(|f| f.format_id == format_id)
            .or(info.formats.last())
            .context("No format available")?;
        download_direct(client, &fmt.url, output_path).await
    } else {
        ytdlp_download(url, resolution, output_path, progress).await
    }
}

/// Extract audio from a downloaded video file using yt-dlp + ffmpeg.
pub async fn extract_audio(
    input_path: &std::path::Path,
    output_path: &std::path::Path,
) -> Result<u64> {
    let input_str = input_path.to_str().context("Invalid input path")?;
    let output_str = output_path.to_str().context("Invalid output path")?;

    let output = ytdlp_command()
        .args(["-x", "--audio-format", "mp3", "-o", output_str, input_str])
        .output()
        .await
        .context("Failed to run yt-dlp for audio extraction")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Audio extraction failed: {stderr}");
    }

    let metadata = tokio::fs::metadata(output_str)
        .await
        .context("Audio output file not found")?;
    Ok(metadata.len())
}

// ─── yt-dlp subprocess ──────────────────────────────────────────────

/// yt-dlp JSON output structure (subset of fields we need).
#[derive(Deserialize)]
struct YtDlpInfo {
    title: Option<String>,
    thumbnail: Option<String>,
    duration: Option<f64>,
    uploader: Option<String>,
    formats: Option<Vec<YtDlpFormat>>,
    url: Option<String>,
    ext: Option<String>,
    height: Option<u32>,
}

#[derive(Deserialize)]
struct YtDlpFormat {
    format_id: Option<String>,
    ext: Option<String>,
    url: Option<String>,
    height: Option<u32>,
    filesize: Option<u64>,
    filesize_approx: Option<u64>,
    #[serde(default)]
    vcodec: Option<String>,
}

/// Build yt-dlp command with optional proxy from YTDLP_PROXY env var.
fn ytdlp_command() -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new("yt-dlp");
    if let Ok(proxy) = std::env::var("YTDLP_PROXY") {
        let proxy = proxy.trim();
        if !proxy.is_empty() {
            cmd.arg("--proxy").arg(proxy);
        }
    }
    if let Ok(cookies) = std::env::var("YTDLP_COOKIES") {
        let cookies = cookies.trim();
        if !cookies.is_empty() {
            cmd.arg("--cookies").arg(cookies);
        }
    }
    cmd
}

/// Extract video info using yt-dlp subprocess.
async fn ytdlp_extract_info(url: &str) -> Result<VideoInfo> {
    let output = ytdlp_command()
        .args(["--dump-json", "--no-download", "--no-warnings", url])
        .output()
        .await
        .context("Failed to run yt-dlp — is it installed?")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("yt-dlp failed: {stderr}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let raw: YtDlpInfo =
        serde_json::from_str(&stdout).context("Failed to parse yt-dlp JSON output")?;

    let formats = if let Some(fmts) = &raw.formats {
        let all_fmts: Vec<_> = fmts
            .iter()
            .filter(|f| {
                // Must have a URL and contain video (not audio-only)
                f.url.is_some() && f.vcodec.as_deref() != Some("none")
            })
            .map(|f| {
                let height = f.height.unwrap_or(0);
                let resolution = if height > 0 {
                    format!("{height}p")
                } else {
                    f.format_id.clone().unwrap_or_else(|| "unknown".to_string())
                };
                VideoFormat {
                    format_id: f.format_id.clone().unwrap_or_default(),
                    resolution,
                    ext: f.ext.clone().unwrap_or_else(|| "mp4".to_string()),
                    url: f.url.clone().unwrap_or_default(),
                    filesize_approx: f.filesize.or(f.filesize_approx),
                }
            })
            .collect();

        // Deduplicate: keep only the best format per resolution (largest filesize)
        let mut seen = std::collections::HashMap::new();
        for fmt in &all_fmts {
            let entry = seen.entry(fmt.resolution.clone()).or_insert(fmt.clone());
            if fmt.filesize_approx > entry.filesize_approx {
                *entry = fmt.clone();
            }
        }
        seen.into_values().collect()
    } else if let Some(url) = &raw.url {
        vec![VideoFormat {
            format_id: "default".to_string(),
            resolution: raw
                .height
                .map(|h| format!("{h}p"))
                .unwrap_or_else(|| "unknown".to_string()),
            ext: raw.ext.clone().unwrap_or_else(|| "mp4".to_string()),
            url: url.clone(),
            filesize_approx: None,
        }]
    } else {
        Vec::new()
    };

    if formats.is_empty() {
        anyhow::bail!("No downloadable video formats found");
    }

    Ok(VideoInfo {
        title: raw.title.unwrap_or_else(|| "Untitled".to_string()),
        thumbnail: raw.thumbnail,
        duration: raw.duration,
        uploader: raw.uploader,
        formats,
    })
}

/// Download a video using yt-dlp subprocess.
/// Uses format selector that merges video+audio (YouTube serves them separately).
async fn ytdlp_download(
    url: &str,
    resolution: &str,
    output_path: &std::path::Path,
    progress: Option<std::sync::Arc<std::sync::atomic::AtomicU8>>,
) -> Result<u64> {
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncBufReadExt, BufReader};

    let output_str = output_path.to_str().context("Invalid output path")?;

    let height: String = resolution
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();

    let format_selector = if !height.is_empty() {
        format!("bestvideo[height<={height}]+bestaudio/best[height<={height}]/best")
    } else {
        "bestvideo+bestaudio/best".to_string()
    };

    let mut child = ytdlp_command()
        .args([
            "-f",
            &format_selector,
            "--merge-output-format",
            "mp4",
            "--newline",
            "-o",
            output_str,
            "--no-warnings",
            url,
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("Failed to spawn yt-dlp")?;

    // yt-dlp with --newline writes progress to stdout, errors to stderr
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let progress_clone = progress.clone();

    // Parse progress from stdout (fragment count and percentage)
    let stdout_handle = tokio::spawn(async move {
        if let Some(stdout) = stdout {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(progress) = &progress_clone
                    && let Some(pct) = parse_ytdlp_progress(&line)
                    && pct > progress.load(Ordering::Relaxed)
                {
                    progress.store(pct, Ordering::Relaxed);
                }
            }
        }
    });

    // Capture stderr for error reporting (last 20 lines)
    let stderr_handle = tokio::spawn(async move {
        let mut tail: std::collections::VecDeque<String> = std::collections::VecDeque::new();
        if let Some(stderr) = stderr {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tail.push_back(line);
                if tail.len() > 20 {
                    tail.pop_front();
                }
            }
        }
        tail.into_iter().collect::<Vec<_>>().join("\n")
    });

    let status = child.wait().await.context("yt-dlp process failed")?;
    let _ = stdout_handle.await;
    let stderr_output = stderr_handle.await.unwrap_or_default();

    if !status.success() {
        anyhow::bail!("yt-dlp download failed: {stderr_output}");
    }

    if let Some(progress) = &progress {
        progress.store(100, Ordering::Relaxed);
    }

    let metadata = tokio::fs::metadata(output_str)
        .await
        .context("Downloaded file not found")?;
    Ok(metadata.len())
}

/// Parse yt-dlp progress line. Returns percentage (0-99).
/// Supports fragment-based "(frag 120/1068)" and percentage-based "45.2%".
fn parse_ytdlp_progress(line: &str) -> Option<u8> {
    if !line.contains("[download]") {
        return None;
    }
    // Fragment-based: "(frag 120/1068)"
    if let Some(frag_part) = line.split("(frag ").nth(1) {
        let parts: Vec<&str> = frag_part.trim_end_matches(')').split('/').collect();
        if parts.len() == 2
            && let Ok(done) = parts[0].parse::<u32>()
            && let Ok(total) = parts[1].parse::<u32>()
            && total > 0
        {
            return Some(((done as f32 / total as f32) * 99.0) as u8);
        }
    }
    // Percentage-based fallback: "45.2%"
    if let Some(pct_str) = line.split_whitespace().find(|s| s.ends_with('%'))
        && let Ok(pct) = pct_str.trim_end_matches('%').parse::<f32>()
    {
        return Some(pct.min(99.0) as u8);
    }
    None
}

// ─── Seznam Rust extractor ──────────────────────────────────────────

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

/// Extract video info from Seznam ecosystem (Novinky.cz, Seznam Zprávy).
async fn seznam_extract_info(client: &reqwest::Client, url: &str) -> Result<VideoInfo> {
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

// ─── Instagram extractor ────────────────────────────────────────────

/// Extract Instagram video using external API to get CDN URL.
async fn instagram_extract_info(client: &reqwest::Client, url: &str) -> Result<VideoInfo> {
    // Use savegram API to extract video CDN URL
    let resp = client
        .post("https://savegram.app/api/ajaxSearch")
        .header("User-Agent", "Mozilla/5.0")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(format!("q={url}&t=media&lang=en"))
        .send()
        .await
        .context("Failed to call Instagram extraction API")?;

    let body: serde_json::Value = resp.json().await.context("Failed to parse API response")?;

    if body.get("status").and_then(|s| s.as_str()) != Some("ok") {
        anyhow::bail!("Instagram extraction failed — video may be private or unavailable");
    }

    let html = body
        .get("data")
        .and_then(|d| d.as_str())
        .unwrap_or_default();

    // Extract download links and thumbnail from HTML response
    let mut video_url = None;
    let mut thumbnail_url = None;

    // Find all JWT tokens — extract CDN URLs for video and thumbnail
    for cap in html.match_indices("token=") {
        let start = cap.0 + 6;
        let end = html[start..]
            .find('"')
            .or_else(|| html[start..].find('&'))
            .map(|e| start + e)
            .unwrap_or(html.len());
        let jwt = &html[start..end];

        if let Some(payload) = jwt.split('.').nth(1) {
            let mut padded = payload.to_string();
            while padded.len() % 4 != 0 {
                padded.push('=');
            }
            if let Ok(decoded) = base64_decode(&padded)
                && let Ok(claims) = serde_json::from_str::<serde_json::Value>(&decoded)
                && let Some(cdn_url) = claims.get("url").and_then(|u| u.as_str())
            {
                if cdn_url.contains(".mp4") && video_url.is_none() {
                    video_url = Some(cdn_url.to_string());
                } else if (cdn_url.contains(".jpg") || cdn_url.contains(".png"))
                    && thumbnail_url.is_none()
                {
                    thumbnail_url = Some(cdn_url.to_string());
                }
            }
        }
    }

    let video_url = video_url.context("No video URL found in Instagram response")?;

    // Extract title from HTML
    let title = body
        .get("meta")
        .and_then(|m| m.get("title"))
        .and_then(|t| t.as_str())
        .unwrap_or("Instagram Video")
        .to_string();

    Ok(VideoInfo {
        title,
        thumbnail: thumbnail_url,
        duration: None,
        uploader: Some("instagram.com".to_string()),
        formats: vec![VideoFormat {
            format_id: "best".to_string(),
            resolution: "original".to_string(),
            ext: "mp4".to_string(),
            url: video_url,
            filesize_approx: None,
        }],
    })
}

/// Simple base64 decode helper.
fn base64_decode(input: &str) -> Result<String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(input)
        .context("base64 decode failed")?;
    String::from_utf8(bytes).context("UTF-8 decode failed")
}

/// Download a file directly via HTTP.
async fn download_direct(
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
