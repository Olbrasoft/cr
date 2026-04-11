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
    if is_seznam_url(url) {
        let info = seznam_extract_info(client, url).await?;
        let fmt = info
            .formats
            .iter()
            .find(|f| f.format_id == format_id)
            .or(info.formats.last())
            .context("No format available")?;
        download_direct(client, &fmt.url, output_path).await?;
    } else if is_instagram_url(url) {
        let info = instagram_extract_info(client, url).await?;
        let fmt = info
            .formats
            .iter()
            .find(|f| f.format_id == format_id)
            .or(info.formats.last())
            .context("No format available")?;
        download_direct(client, &fmt.url, output_path).await?;
    } else if format_id == "proxy-hls" {
        // Nova.cz proxy fallback — `format_id` is a sentinel, not the direct m3u8 URL.
        // Re-extract to obtain a fresh tokenized manifest URL from `info.formats[0].url`.
        let info = nova_proxy_extract_info(client, url).await?;
        let m3u8 = &info.formats[0].url;
        ytdlp_download(m3u8, "best", container, output_path, progress.clone()).await?;
    } else {
        ytdlp_download(url, resolution, container, output_path, progress.clone()).await?;
    }

    // Single source of truth for the final container (#366).
    ensure_container(output_path, container, progress.clone()).await?;

    let meta = tokio::fs::metadata(output_path)
        .await
        .context("Output file missing after ensure_container")?;
    Ok(meta.len())
}

/// Force `path` to be in `container` format (`"mp4"` or `"webm"`). If
/// ffprobe reports the file is already in that container, returns
/// without doing any work. Otherwise runs an ffmpeg transcode in
/// place — first a remux attempt with `-c copy` (fast, works when
/// codecs are compatible), then a full codec re-encode fallback when
/// the remux fails due to codec incompatibility.
///
/// Bumps the shared progress atomic during transcode so the UI's
/// existing progress bar keeps moving instead of looking frozen (#366).
pub async fn ensure_container(
    path: &std::path::Path,
    container: &str,
    progress: Option<std::sync::Arc<std::sync::atomic::AtomicU8>>,
) -> Result<()> {
    use std::sync::atomic::Ordering;

    let actual = probe_container(path).await.unwrap_or_default();
    if container_matches(&actual, container) {
        return Ok(());
    }

    tracing::info!("ensure_container: {path:?} is {actual:?}, transcoding to {container}");
    if let Some(p) = &progress {
        p.store(40, Ordering::Relaxed);
    }

    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .context("Input path has no stem")?;
    let tmp = parent.join(format!("{stem}.reencoded.{container}"));

    // 1) Fast path — remux with -c copy. Works whenever the existing
    //    codecs can live in the target container (e.g. H.264/AAC → mp4,
    //    VP9/Opus → webm). Fails fast on mismatched codecs.
    let remux = run_ffmpeg_transcode(path, &tmp, container, /*recode*/ false).await;
    let transcoded = match remux {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!("ensure_container: remux failed ({e}), falling back to full re-encode");
            if let Some(p) = &progress {
                p.store(50, Ordering::Relaxed);
            }
            run_ffmpeg_transcode(path, &tmp, container, /*recode*/ true)
                .await
                .context("ffmpeg full re-encode failed")?;
            true
        }
    };

    if transcoded {
        tokio::fs::rename(&tmp, path)
            .await
            .context("Failed to swap transcoded file into place")?;
    }

    if let Some(p) = &progress {
        p.store(95, Ordering::Relaxed);
    }
    Ok(())
}

/// Run ffmpeg to produce `output` in `container` from `input`. When
/// `recode` is false, codecs are copied (`-c copy`) — fast remux path
/// that fails when the source codecs can't live in the target
/// container. When `recode` is true, video and audio are re-encoded
/// with container-appropriate codecs: H.264/AAC for MP4, VP9/Opus
/// for WebM.
async fn run_ffmpeg_transcode(
    input: &std::path::Path,
    output: &std::path::Path,
    container: &str,
    recode: bool,
) -> Result<()> {
    let input_str = input.to_str().context("Invalid input path")?;
    let output_str = output.to_str().context("Invalid output path")?;

    let mut args: Vec<&str> = vec!["-y", "-i", input_str];

    if recode {
        match container {
            "mp4" | "mkv" => {
                // H.264/AAC is compatible with both MP4 and MKV and
                // plays everywhere; MKV just wraps it in the Matroska
                // container instead of ISO/BMFF. The `+faststart`
                // flag is MP4-only — skip it for MKV.
                args.extend([
                    "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-pix_fmt", "yuv420p",
                    "-c:a", "aac", "-b:a", "128k",
                ]);
                if container == "mp4" {
                    args.extend(["-movflags", "+faststart"]);
                }
            }
            "webm" => {
                args.extend([
                    "-c:v",
                    "libvpx-vp9",
                    "-b:v",
                    "0",
                    "-crf",
                    "32",
                    "-deadline",
                    "good",
                    "-cpu-used",
                    "4",
                    "-c:a",
                    "libopus",
                    "-b:a",
                    "128k",
                ]);
            }
            other => anyhow::bail!("Unsupported container for re-encode: {other}"),
        }
    } else {
        // Fast-path remux — copy streams, let ffmpeg error out if the
        // target container can't hold them. MKV accepts essentially
        // any codec so this path almost always succeeds for MKV.
        args.extend(["-c", "copy"]);
        if container == "mp4" {
            args.extend(["-movflags", "+faststart"]);
        }
    }

    args.push(output_str);

    let out = tokio::process::Command::new("ffmpeg")
        .args(&args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .output()
        .await
        .context("Failed to spawn ffmpeg")?;

    if !out.status.success() {
        // Clean up any partial output so the fallback re-encode starts clean.
        let _ = tokio::fs::remove_file(output).await;
        let stderr = String::from_utf8_lossy(&out.stderr);
        let tail: String = stderr.lines().rev().take(4).collect::<Vec<_>>().join(" | ");
        anyhow::bail!("ffmpeg exited with {}: {}", out.status, tail);
    }
    Ok(())
}

/// Probe the real container of a video file using `ffprobe`. Returns
/// the first entry from the comma-separated `format_name` list
/// (e.g. `"mov,mp4,m4a,3gp,3g2,mj2"` → `"mov"`). Returns an empty
/// string on any error so the caller can fall through to transcoding.
async fn probe_container(path: &std::path::Path) -> Result<String> {
    let path_str = path.to_str().context("Invalid path for ffprobe")?;
    let out = tokio::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=format_name",
            "-of",
            "default=nokey=1:noprint_wrappers=1",
            path_str,
        ])
        .output()
        .await
        .context("Failed to spawn ffprobe")?;
    if !out.status.success() {
        anyhow::bail!("ffprobe failed for {path:?}");
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Decide whether a `format_name` string reported by ffprobe matches
/// the requested container tag. ffprobe returns comma-separated lists
/// like `mov,mp4,m4a,3gp,3g2,mj2` for MP4 and `matroska,webm` for
/// both WebM and MKV (they share the Matroska family) — we accept
/// any member that represents the target container.
///
/// Note the MKV ↔ WebM overlap: ffprobe can't distinguish them from
/// the format-name list alone, so both match `matroska`. The file
/// extension we renamed to post-download is what actually ties the
/// file to one or the other — and since both containers hold the
/// same codecs, a WebM-encoded file renamed to `.mkv` is a perfectly
/// valid MKV (just with web-friendly codecs inside).
///
/// For a requested `webm` output we must require an explicit `webm`
/// token — we can't accept a generic Matroska file here because MKV
/// can carry non-WebM codecs (H.264/AAC) that wouldn't play in a
/// `.webm` container. For `mkv` we stay permissive because WebM is
/// Matroska-based and plays fine in an `.mkv` container.
fn container_matches(ffprobe_format: &str, container: &str) -> bool {
    let parts: Vec<&str> = ffprobe_format.split(',').map(|s| s.trim()).collect();
    match container {
        "mp4" => parts.iter().any(|p| matches!(*p, "mp4" | "mov" | "m4a")),
        "webm" => parts.contains(&"webm"),
        "mkv" => parts.iter().any(|p| matches!(*p, "matroska" | "webm")),
        other => parts.contains(&other),
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
/// Strategy: transcode to H.264/AAC MP4 with veryfast preset, then split if > 16 MB.
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

    // Transcode to a WhatsApp-compatible MP4 with H.264 video and AAC audio.
    // The input is always re-encoded here to normalize codec/container compatibility.
    let output = tokio::process::Command::new("ffmpeg")
        .args([
            "-i",
            input_str,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-profile:v",
            "main",
            "-level",
            "3.1",
            "-pix_fmt",
            "yuv420p",
            "-crf",
            "30",
            "-vf",
            "scale=-2:'trunc(min(480,ih)/2)*2'",
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
    // Calculate segment duration to produce ~12 MB chunks
    let target_segment_bytes = 12.0 * 1024.0 * 1024.0;
    let segment_secs = if duration > 0.0 && size > 0 {
        (target_segment_bytes / (size as f64 / duration)) as u32
    } else {
        WHATSAPP_SEGMENT_SECS
    };
    let segment_secs = segment_secs.max(10); // minimum 10s to avoid degenerate splits

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
    for idx in 0usize.. {
        let part_path = output_dir.join(format!("{base_name}-wa-part{idx:03}.mp4"));
        match tokio::fs::metadata(&part_path).await {
            Ok(m) => parts.push(WhatsAppPart {
                path: part_path,
                size: m.len(),
                index: idx,
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
            Err(e) => {
                return Err(e).context(format!(
                    "Failed to read metadata for split part {}",
                    part_path.display()
                ));
            }
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
/// With CRF 30 at 480p, typical bitrate is ~1.1 Mbps.
/// 12 MB target segment / 1.1 Mbps ≈ 87 seconds per segment.
pub fn estimate_whatsapp_parts(duration_secs: f64) -> u32 {
    if duration_secs <= 0.0 {
        return 1;
    }
    // Estimate total size: ~1.1 Mbps = 137.5 KB/s for 480p CRF 30
    let estimated_bytes = duration_secs * 137.5 * 1024.0;
    if estimated_bytes <= WHATSAPP_MAX_SIZE as f64 {
        return 1;
    }
    let target_segment_bytes = 12.0 * 1024.0 * 1024.0;
    (estimated_bytes / target_segment_bytes).ceil() as u32
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

/// Build a yt-dlp `-f` selector that prefers the requested container
/// and then falls back to the generic `bestvideo+bestaudio` chain if
/// no container-native stream is available.
fn build_format_selector(height: &str, container: &str) -> String {
    // Pairs the container with its best audio codec partner so that
    // yt-dlp picks streams that can be merged directly into the
    // requested output container without re-encoding. For MKV there
    // is no native source extension — Matroska happily wraps MP4
    // streams, so we bias toward MP4 sources which are the most
    // common and remux without re-encoding (`-c copy`).
    let (video_ext, audio_ext) = match container {
        "mp4" | "mkv" => ("mp4", "m4a"),
        "webm" => ("webm", "webm"),
        _ => ("mp4", "m4a"),
    };
    if height.is_empty() {
        format!(
            "bestvideo[ext={video_ext}]+bestaudio[ext={audio_ext}]/\
             best[ext={video_ext}]/\
             bestvideo+bestaudio/best"
        )
    } else {
        format!(
            "bestvideo[ext={video_ext}][height<={height}]+bestaudio[ext={audio_ext}]/\
             best[ext={video_ext}][height<={height}]/\
             bestvideo[height<={height}]+bestaudio/\
             best[height<={height}]/best"
        )
    }
}

/// Download a video using yt-dlp subprocess.
///
/// Uses a container-aware format selector that prefers streams that
/// can be muxed directly into the requested container — MP4 streams
/// when `container = "mp4"`, WebM streams when `container = "webm"`.
/// Falls back to the generic `bestvideo+bestaudio` selector when no
/// container-native stream is available.
///
/// Deliberately does **not** pass `--merge-output-format` or
/// `--remux-video` — those flags hard-fail yt-dlp when the selected
/// codecs can't live in the requested container (e.g. asking for
/// WebM on an H.264/AAC TikTok source), and the caller's
/// [`ensure_container`] pass already does the right thing with
/// ffmpeg remux + re-encode fallback (#366). Instead we ask yt-dlp
/// to write `{stem}.%(ext)s` so its natural extension choice wins,
/// then rename the result to `{output_path}` so the caller sees a
/// stable filename regardless of the real on-disk container.
async fn ytdlp_download(
    url: &str,
    resolution: &str,
    container: &str,
    output_path: &std::path::Path,
    progress: Option<std::sync::Arc<std::sync::atomic::AtomicU8>>,
) -> Result<u64> {
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncBufReadExt, BufReader};

    let parent = output_path.parent().context("No parent directory")?;
    let stem = output_path
        .file_stem()
        .and_then(|s| s.to_str())
        .context("No file stem")?;
    let output_template = parent.join(format!("{stem}.%(ext)s"));
    let output_template_str = output_template
        .to_str()
        .context("Invalid output template path")?;

    let height: String = resolution
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();

    // Container-preferred format selector, then generic fallback.
    // Example for mp4 @ 720p:
    //   bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/
    //   best[ext=mp4][height<=720]/
    //   bestvideo[height<=720]+bestaudio/best[height<=720]/best
    let format_selector = build_format_selector(&height, container);

    let mut child = ytdlp_command()
        .args([
            "-f",
            &format_selector,
            "--newline",
            "-o",
            output_template_str,
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

    // yt-dlp wrote `{stem}.{whatever}` — find it, rename to the
    // caller's canonical `{stem}.{container}` path regardless of
    // actual container. The post-download `ensure_container` pass
    // will ffprobe the real format and transcode if it doesn't
    // match the extension, so the extension-to-content mismatch
    // window is handled one level up.
    let mut entries = tokio::fs::read_dir(parent).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with(&format!("{stem}."))
            && !name_str.ends_with(".part")
            && !name_str.ends_with(".ytdl")
        {
            let actual_path = entry.path();
            if actual_path != output_path {
                // If a stale target exists (e.g. from an earlier
                // failed attempt), remove it so `rename` doesn't
                // leave two files behind.
                let _ = tokio::fs::remove_file(output_path).await;
                tokio::fs::rename(&actual_path, output_path).await?;
            }
            let metadata = tokio::fs::metadata(output_path).await?;
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
