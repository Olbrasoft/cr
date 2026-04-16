use anyhow::{Context, Result};

use super::extractors::ytdlp::ytdlp_command;

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
/// Note the MKV <-> WebM overlap: ffprobe can't distinguish them from
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
/// 12 MB target segment / 1.1 Mbps = 87 seconds per segment.
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
