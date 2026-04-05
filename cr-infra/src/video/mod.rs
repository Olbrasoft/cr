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

/// Check if URL is Nova.cz (validates host, not substring).
fn is_nova_url(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_ascii_lowercase()))
        .map(|host| host == "nova.cz" || host.ends_with(".nova.cz"))
        .unwrap_or(false)
}

/// Czech proxy URL and key for geo-blocked Nova.cz embeds (from env vars).
fn cz_proxy_config() -> Option<(String, String)> {
    let url = std::env::var("CZ_PROXY_URL").ok()?;
    let key = std::env::var("CZ_PROXY_KEY").ok()?;
    if url.is_empty() || key.is_empty() {
        return None;
    }
    Some((url, key))
}

// ─── Public API ─────────────────────────────────────────────────────

/// Extract video info. Tries our extractors first, falls back to yt-dlp.
pub async fn extract_video_info(client: &reqwest::Client, url: &str) -> Result<VideoInfo> {
    if is_seznam_url(url) {
        seznam_extract_info(client, url).await
    } else if is_instagram_url(url) {
        instagram_extract_info(client, url).await
    } else {
        let result = ytdlp_extract_info(url).await;
        // If yt-dlp fails for Nova.cz, try Czech proxy to bypass geo-block
        if result.is_err() && is_nova_url(url) {
            tracing::warn!("yt-dlp failed for Nova.cz URL, trying Czech proxy fallback");
            if let Ok(info) = nova_proxy_extract_info(client, url).await {
                return Ok(info);
            }
        }
        result
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
    } else if format_id == "proxy-hls" {
        // Nova.cz proxy fallback — `format_id` is a sentinel, not the direct m3u8 URL.
        // Re-extract to obtain a fresh tokenized manifest URL from `info.formats[0].url`.
        let info = nova_proxy_extract_info(client, url).await?;
        let m3u8 = &info.formats[0].url;
        ytdlp_download(m3u8, "best", output_path, progress).await
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

// ─── WhatsApp conversion ──────────────────────────────────────────

/// WhatsApp video limits.
const WHATSAPP_MAX_SIZE: u64 = 16 * 1024 * 1024; // 16 MB
const WHATSAPP_SEGMENT_SECS: u32 = 180; // 3 minutes default

/// Result of WhatsApp conversion — single file or multiple parts.
#[derive(Debug)]
pub enum WhatsAppResult {
    Single { path: std::path::PathBuf, size: u64 },
    Parts(Vec<WhatsAppPart>),
}

#[derive(Debug)]
pub struct WhatsAppPart {
    pub path: std::path::PathBuf,
    pub size: u64,
    pub index: usize,
}

/// Convert a downloaded video to WhatsApp-compatible format.
/// Strategy: convert to H.264/AAC MP4 with ultrafast preset, then split if > 16 MB.
pub async fn convert_for_whatsapp(
    input_path: &std::path::Path,
    output_dir: &std::path::Path,
    base_name: &str,
    progress: Option<std::sync::Arc<std::sync::atomic::AtomicU8>>,
) -> Result<WhatsAppResult> {
    use std::sync::atomic::Ordering;

    let input_str = input_path.to_str().context("Invalid input path")?;
    let converted_path = output_dir.join(format!("{base_name}-wa.mp4"));
    let converted_str = converted_path.to_str().context("Invalid output path")?;

    if let Some(p) = &progress {
        p.store(40, Ordering::Relaxed);
    }

    // Remux to MP4 container with H.264+AAC (re-encode only if needed).
    // Source is already ≤480p from yt-dlp, so just ensure MP4 compatibility.
    let output = tokio::process::Command::new("ffmpeg")
        .args([
            "-i",
            input_str,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-pix_fmt",
            "yuv420p",
            "-crf",
            "30",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-ac",
            "2",
            "-movflags",
            "+faststart",
            "-y",
            converted_str,
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .context("Failed to run ffmpeg")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("ffmpeg conversion failed: {stderr}");
    }

    if let Some(p) = &progress {
        p.store(80, Ordering::Relaxed);
    }

    let meta = tokio::fs::metadata(&converted_path).await?;
    let size = meta.len();

    if size <= WHATSAPP_MAX_SIZE {
        if let Some(p) = &progress {
            p.store(99, Ordering::Relaxed);
        }
        return Ok(WhatsAppResult::Single {
            path: converted_path,
            size,
        });
    }

    // File too large — split into segments
    tracing::info!(
        "WhatsApp: {:.1} MB exceeds 16 MB, splitting",
        size as f64 / (1024.0 * 1024.0)
    );

    let duration = ffprobe_duration(&converted_path).await.unwrap_or(0.0);
    // Calculate segment duration to produce ~14 MB chunks
    let target_segment_bytes = 12.0 * 1024.0 * 1024.0;
    let segment_secs = if duration > 0.0 && size > 0 {
        (target_segment_bytes / (size as f64 / duration)) as u32
    } else {
        WHATSAPP_SEGMENT_SECS
    };
    let segment_secs = segment_secs.max(30);

    let segment_pattern = output_dir.join(format!("{base_name}-wa-part%03d.mp4"));
    let pattern_str = segment_pattern.to_str().context("Invalid segment path")?;
    let seg_time = format!("{segment_secs}");

    let split_output = tokio::process::Command::new("ffmpeg")
        .args([
            "-i",
            converted_str,
            "-c",
            "copy",
            "-f",
            "segment",
            "-segment_time",
            &seg_time,
            "-reset_timestamps",
            "1",
            "-movflags",
            "+faststart",
            "-y",
            pattern_str,
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .context("Failed to run ffmpeg segment")?;

    if !split_output.status.success() {
        let stderr = String::from_utf8_lossy(&split_output.stderr);
        anyhow::bail!("ffmpeg split failed: {stderr}");
    }

    // Clean up the single converted file
    let _ = tokio::fs::remove_file(&converted_path).await;

    // Collect parts
    let mut parts = Vec::new();
    for idx in 0..100 {
        let part_path = output_dir.join(format!("{base_name}-wa-part{idx:03}.mp4"));
        match tokio::fs::metadata(&part_path).await {
            Ok(m) => parts.push(WhatsAppPart {
                path: part_path,
                size: m.len(),
                index: idx,
            }),
            Err(_) => break,
        }
    }

    if parts.is_empty() {
        anyhow::bail!("No split parts were created");
    }

    if let Some(p) = &progress {
        p.store(99, Ordering::Relaxed);
    }

    tracing::info!("WhatsApp split: {} parts created", parts.len());
    Ok(WhatsAppResult::Parts(parts))
}

/// Estimate number of WhatsApp parts based on video duration.
pub fn estimate_whatsapp_parts(duration_secs: f64) -> u32 {
    if duration_secs <= 0.0 {
        return 1;
    }
    // At ~700kbps total, 16MB lasts about 180s
    let parts = (duration_secs / 180.0).ceil() as u32;
    parts.max(1)
}

/// Get video duration using ffprobe.
async fn ffprobe_duration(path: &std::path::Path) -> Option<f64> {
    let path_str = path.to_str()?;
    let output = tokio::process::Command::new("ffprobe")
        .args([
            "-v",
            "quiet",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            path_str,
        ])
        .output()
        .await
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.trim().parse::<f64>().ok()
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
    // Enable EJS challenge solver for YouTube (requires deno)
    cmd.arg("--remote-components").arg("ejs:github");
    if let Ok(proxy) = std::env::var("YTDLP_PROXY") {
        let proxy = proxy.trim();
        if !proxy.is_empty() {
            cmd.arg("--proxy").arg(proxy);
        }
    }
    if let Ok(cookies) = std::env::var("YTDLP_COOKIES") {
        let cookies = cookies.trim();
        if !cookies.is_empty() {
            let path = std::path::Path::new(cookies);
            match path.metadata() {
                Ok(meta) if meta.is_file() && meta.len() > 0 => {
                    cmd.arg("--cookies").arg(cookies);
                }
                Ok(_) => {
                    tracing::warn!(
                        "YTDLP_COOKIES path is not a valid cookies file, skipping: {cookies}"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        "YTDLP_COOKIES path metadata could not be read, skipping: {cookies}: {err}"
                    );
                }
            }
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

    // yt-dlp may create file with different extension (e.g., token.webm.mp4)
    // Find the actual file by prefix match
    if tokio::fs::metadata(output_str).await.is_ok() {
        let metadata = tokio::fs::metadata(output_str).await?;
        return Ok(metadata.len());
    }

    // Fallback: find file matching token prefix in the output directory
    let parent = output_path.parent().context("No parent directory")?;
    let stem = output_path
        .file_stem()
        .and_then(|s| s.to_str())
        .context("No file stem")?;

    let mut entries = tokio::fs::read_dir(parent).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with(stem)
            && !name_str.ends_with(".part")
            && !name_str.ends_with(".ytdl")
        {
            let actual_path = entry.path();
            // Rename to expected path
            tokio::fs::rename(&actual_path, output_path).await?;
            let metadata = tokio::fs::metadata(output_str).await?;
            return Ok(metadata.len());
        }
    }

    anyhow::bail!("Downloaded file not found")
}

// ─── Nova.cz proxy fallback ────────────────────────────────────────

/// Extract video info from Nova.cz via Czech proxy (for geo-blocked embeds).
/// 1. Fetch tv.nova.cz page to get embed ID
/// 2. Fetch embed page via Czech proxy
/// 3. Extract m3u8/mpd manifest URLs + metadata
async fn nova_proxy_extract_info(client: &reqwest::Client, url: &str) -> Result<VideoInfo> {
    // Step 1: Get embed ID from the Nova.cz page
    let page_html = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await?
        .text()
        .await?;

    let embed_id = page_html
        .split("media.cms.nova.cz/embed/")
        .nth(1)
        .and_then(|s| s.split(['?', '"', '\'', ' '].as_ref()).next())
        .context("Could not find Nova embed ID")?
        .to_string();

    // Step 2: Fetch embed page via Czech proxy
    let (proxy_base, proxy_key) =
        cz_proxy_config().context("CZ_PROXY_URL and CZ_PROXY_KEY env vars required")?;
    let embed_url = format!("https://media.cms.nova.cz/embed/{embed_id}");
    let proxy_url = format!(
        "{}?url={}&key={}",
        proxy_base,
        urlencoding::encode(&embed_url),
        proxy_key
    );

    let embed_html = client
        .get(&proxy_url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await?
        .text()
        .await?;

    // Step 3: Extract title from og:title
    let title = embed_html
        .split("og:title")
        .nth(1)
        .and_then(|s| s.split("content=\"").nth(1))
        .and_then(|s| s.split('"').next())
        .unwrap_or("Nova.cz Video")
        .to_string();

    // Extract thumbnail from og:image
    let thumbnail = embed_html
        .split("og:image")
        .nth(1)
        .and_then(|s| s.split("content=\"").nth(1))
        .and_then(|s| s.split('"').next())
        .map(|s| s.to_string());

    // Extract duration from programDuration
    let duration = embed_html
        .split("\"duration\":")
        .nth(1)
        .and_then(|s| s.split([',', '}'].as_ref()).next())
        .and_then(|s| s.trim().parse::<f64>().ok());

    // Extract m3u8 manifest URL
    let m3u8_url = embed_html
        .split("\"src\":\"")
        .filter_map(|s| {
            let url = s.split('"').next()?;
            if url.contains(".m3u8") {
                Some(url.to_string())
            } else {
                None
            }
        })
        .next()
        .context("No m3u8 manifest found in Nova.cz embed")?;

    tracing::info!("Nova.cz proxy: extracted manifest for {title}");

    // Build formats — we pass the m3u8 URL as a single format.
    // yt-dlp can download directly from this manifest URL.
    Ok(VideoInfo {
        title,
        thumbnail,
        duration,
        uploader: Some("Nova.cz".to_string()),
        formats: vec![VideoFormat {
            format_id: "proxy-hls".to_string(),
            resolution: "best".to_string(),
            ext: "mp4".to_string(),
            url: m3u8_url,
            filesize_approx: None,
        }],
    })
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
