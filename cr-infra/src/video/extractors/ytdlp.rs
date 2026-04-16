use anyhow::{Context, Result};
use serde::Deserialize;

use super::super::{VideoFormat, VideoInfo};

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
pub(crate) fn ytdlp_command() -> tokio::process::Command {
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
pub(crate) async fn ytdlp_extract_info(url: &str) -> Result<VideoInfo> {
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
pub(crate) async fn ytdlp_download(
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

/// Parse yt-dlp progress line. Returns percentage (0-99).
/// Supports fragment-based "(frag 120/1068)" and percentage-based "45.2%".
pub(crate) fn parse_ytdlp_progress(line: &str) -> Option<u8> {
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
