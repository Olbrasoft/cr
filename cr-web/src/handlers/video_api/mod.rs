//! Video download & library API handlers.
//!
//! Split into submodules by concern:
//! - `download` — yt-dlp download flow (info, prepare, status, file, recent, cleanup)
//! - `library`  — Streamtape/R2 hosted library CRUD + streaming proxy
//! - `cleanup`  — periodic background reaper for temp files + tests
//! - `thumbnail` — CDN thumbnail proxy

mod cleanup;
mod download;
mod library;
mod thumbnail;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use serde::Serialize;
use tokio::sync::{Mutex, Semaphore};

// --- Re-exports consumed by main.rs, state.rs, and handlers/mod.rs ---

pub use cleanup::spawn_temp_video_cleanup_loop;
pub use download::{
    video_cleanup, video_file, video_file_part, video_info, video_prepare, video_recent,
    video_status,
};
pub use library::{library_delete, library_file, library_list, library_play, library_stream};
pub use thumbnail::video_thumb;

// --- Shared constants ---

/// Maximum concurrent video downloads.
pub static VIDEO_DOWNLOAD_SEMAPHORE: Semaphore = Semaphore::const_new(3);

/// On-disk directory where yt-dlp drops freshly downloaded videos before
/// they are either served to the user via `/api/video/file/{token}` or
/// published to the library pipeline.
pub(crate) const TMP_VIDEO_DIR: &str = "/tmp/cr-videos";

/// Maximum age a temp video may sit on disk before the periodic reaper
/// removes it. See issue #192 — the VPS has limited disk and we can't
/// keep videos around forever after the user has finished downloading
/// them (the library copy on Streamtape + R2 is the canonical long-term
/// store).
pub(crate) const TMP_VIDEO_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// How often the periodic reaper wakes up to scan the temp dir.
pub(crate) const TMP_VIDEO_CLEANUP_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);

// --- Shared types ---

/// Shared state for tracking video download tasks (async).
pub type VideoDownloads = Arc<Mutex<HashMap<String, VideoTask>>>;

pub struct VideoTask {
    pub status: DownloadStatus,
    pub progress: Arc<AtomicU8>,
    pub file_path: std::path::PathBuf,
    pub filename: String,
    pub parts: Vec<PartInfo>,
    #[allow(dead_code)]
    pub created_at: std::time::Instant,
    /// Set for tasks that hit the library dedup path — the client's
    /// ready-link delegates to `/api/video/library/{id}/file` via a
    /// 303 See Other from `video_file` (`Redirect::to(...)`) because
    /// there is no local temp file for deduped downloads (the content
    /// lives only on Streamtape/R2). `None` for normal downloads
    /// where `file_path` carries the bytes.
    pub library_id: Option<i32>,
}

#[derive(Clone, Serialize)]
pub struct PartInfo {
    pub index: usize,
    pub filename: String,
    pub size_mb: f64,
    pub file_path: std::path::PathBuf,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum DownloadStatus {
    Downloading { progress_percent: u8 },
    Converting { progress_percent: u8 },
    Ready { size_mb: f64, filename: String },
    ReadyParts { parts: Vec<PartResponse> },
    Failed { error: String },
}

#[derive(Clone, Serialize)]
pub struct PartResponse {
    pub index: usize,
    pub filename: String,
    pub size_mb: f64,
}

// --- Shared request/response types ---

#[derive(Serialize)]
pub(crate) struct VideoErrorResponse {
    pub(crate) error: String,
}

// --- Shared helper functions ---

/// ASCII-only filename sanitiser used as the `filename="…"` fallback in
/// `Content-Disposition` headers (and as the on-disk filename for the
/// local-download flow).
///
/// Allowlist: ASCII alphanumerics + space + dash + underscore. Whitespace
/// is collapsed, the result is truncated to `max` characters, and an
/// empty result falls back to `"video"` (so emoji-only / Cyrillic-only /
/// CJK-only titles never produce a nameless `.mp4`).
pub(crate) fn sanitize_filename_ascii(input: &str, max: usize) -> String {
    let cleaned: String = input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == ' ' || c == '-' || c == '_' {
                c
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(max)
        .collect();
    if cleaned.is_empty() {
        "video".to_string()
    } else {
        cleaned
    }
}

/// Unicode-friendly filename sanitiser used as the `filename*=UTF-8''…`
/// value in `Content-Disposition` headers — keeps Czech, Cyrillic, CJK
/// and any other letters/digits intact while still stripping path
/// separators, control characters, quotes and the like.
///
/// Browsers prefer `filename*` over the ASCII `filename` fallback, so a
/// Czech title like `I když se vše vyřeší, KRIZE ZŮSTANE!` ends up
/// saved as `I když se vše vyřeší KRIZE ZŮSTANE.mp4` instead of the
/// mangled ASCII transliteration.
pub(crate) fn sanitize_filename_unicode(input: &str, max: usize) -> String {
    let cleaned: String = input
        .chars()
        .map(|c| {
            // Keep any letter/number from any script. Replace anything
            // else (punctuation, control chars, separators) with a space
            // and collapse whitespace afterwards.
            if c.is_alphanumeric() || c == ' ' || c == '-' || c == '_' {
                c
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(max)
        .collect();
    if cleaned.is_empty() {
        "video".to_string()
    } else {
        cleaned
    }
}

/// Sanitize yt-dlp error messages into user-friendly Czech text.
pub(crate) fn sanitize_error(raw: &str) -> String {
    if raw.contains("Sign in to confirm")
        || raw.contains("not a bot")
        || raw.contains("login required")
        || raw.contains("rate-limit reached")
    {
        return "Tento server vyžaduje přihlášení a momentálně není podporován. Zkuste jiný odkaz."
            .to_string();
    }
    if raw.contains("Unsupported URL") {
        return "Nepodporovaná URL — tento server zatím neumíme zpracovat.".to_string();
    }
    if raw.contains("Video unavailable") || raw.contains("not available") {
        return "Video není dostupné — mohlo být smazáno nebo je omezené.".to_string();
    }
    if raw.contains("Private video") {
        return "Toto video je soukromé a nelze ho stáhnout.".to_string();
    }
    if raw.contains("No video found") || raw.contains("could not find SDN") {
        return "Na této stránce nebylo nalezeno žádné video.".to_string();
    }
    if raw.contains("Failed to parse video manifest") {
        return "Nepodařilo se načíst video — server nevrátil platná data.".to_string();
    }
    if raw.contains("ensure_container")
        || raw.contains("ffmpeg")
        || raw.contains("full re-encode failed")
    {
        return "Požadovaný formát není dostupný a konverze se nezdařila — zkuste jiný formát."
            .to_string();
    }
    // Generic fallback — don't expose raw yt-dlp output
    "Nepodařilo se získat informace o videu. Zkuste jiný odkaz.".to_string()
}

/// Map a file extension to a Content-Type mime string, defaulting to
/// `video/mp4` for anything we don't recognise so browsers at least
/// treat the response as some kind of video.
pub(crate) fn content_type_for_filename(name: &str) -> &'static str {
    let ext = std::path::Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("webm") => "video/webm",
        Some("mkv") => "video/x-matroska",
        _ => "video/mp4",
    }
}

// --- Tests for shared helpers ---

#[cfg(test)]
mod sanitize_tests {
    use super::{sanitize_filename_ascii, sanitize_filename_unicode};

    #[test]
    fn ascii_title_kept() {
        assert_eq!(sanitize_filename_ascii("Matrix (1999)", 60), "Matrix 1999");
    }

    #[test]
    fn cyrillic_only_falls_back() {
        assert_eq!(sanitize_filename_ascii("Москва", 60), "video");
    }

    #[test]
    fn emoji_only_falls_back() {
        assert_eq!(sanitize_filename_ascii("😭😭😭", 60), "video");
    }

    #[test]
    fn collapses_whitespace_and_truncates() {
        let long = "a".repeat(200);
        assert_eq!(sanitize_filename_ascii(&long, 80).len(), 80);
        assert_eq!(
            sanitize_filename_ascii("  hello   world  ", 60),
            "hello world"
        );
    }

    #[test]
    fn unicode_keeps_czech_diacritics() {
        assert_eq!(
            sanitize_filename_unicode("I když se vše vyřeší, KRIZE ZŮSTANE!", 80),
            "I když se vše vyřeší KRIZE ZŮSTANE"
        );
    }

    #[test]
    fn unicode_keeps_cyrillic_and_cjk() {
        assert_eq!(sanitize_filename_unicode("Москва", 80), "Москва");
        assert_eq!(sanitize_filename_unicode("世界", 80), "世界");
    }

    #[test]
    fn unicode_emoji_only_falls_back() {
        assert_eq!(sanitize_filename_unicode("😭😭😭", 80), "video");
    }
}
