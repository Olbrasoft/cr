//! Periodic background reaper for temp video files (#192).

use super::{TMP_VIDEO_CLEANUP_INTERVAL, TMP_VIDEO_DIR, TMP_VIDEO_MAX_AGE, VideoDownloads};

/// Scan `dir` once and delete any regular file whose last-modified
/// timestamp is older than `max_age`. Returns `(deleted_count,
/// bytes_freed)`.
///
/// Errors (failing to open the dir, failing to read an entry, failing
/// to delete a single file) are logged and skipped — the reaper runs
/// every few minutes so any transient issue will be retried on the
/// next tick.
pub(crate) async fn purge_stale_temp_videos(
    dir: &std::path::Path,
    max_age: std::time::Duration,
) -> (usize, u64) {
    let mut deleted = 0usize;
    let mut freed = 0u64;

    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (0, 0),
        Err(e) => {
            tracing::warn!("temp cleanup: cannot open {dir:?}: {e}");
            return (0, 0);
        }
    };

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => {
                tracing::warn!("temp cleanup: read_dir error: {e}");
                break;
            }
        };
        let meta = match entry.metadata().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("temp cleanup: stat {:?}: {e}", entry.path());
                continue;
            }
        };
        if !meta.is_file() {
            continue;
        }
        let age = meta
            .modified()
            .ok()
            .and_then(|m| m.elapsed().ok())
            .unwrap_or_default();
        if age < max_age {
            continue;
        }
        let path = entry.path();
        let size = meta.len();
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                deleted += 1;
                freed += size;
                tracing::debug!(
                    "temp cleanup: removed {path:?} (age={}s, size={} bytes)",
                    age.as_secs(),
                    size
                );
            }
            Err(e) => {
                tracing::warn!("temp cleanup: remove {path:?}: {e}");
            }
        }
    }

    (deleted, freed)
}

/// Prune in-memory `VideoDownloads` entries whose `created_at` is older
/// than `max_age`. Returns the number of tokens removed.
///
/// Runs alongside the on-disk reaper so that once a temp file is
/// deleted, the matching `/api/video/status/{token}` entry goes with
/// it — otherwise the handler would keep reporting `Ready` while
/// `/api/video/file/{token}` returns 500 for a missing file.
pub(crate) async fn prune_stale_video_downloads(
    downloads: &VideoDownloads,
    max_age: std::time::Duration,
) -> usize {
    let mut map = downloads.lock().await;
    let before = map.len();
    map.retain(|_, task| task.created_at.elapsed() < max_age);
    before - map.len()
}

/// Spawn the long-running periodic reaper. Call once at startup from
/// `main.rs`; the returned handle is detached (the task ends only when
/// the process exits).
///
/// Every `TMP_VIDEO_CLEANUP_INTERVAL` it scans [`TMP_VIDEO_DIR`] and
/// deletes any file older than `TMP_VIDEO_MAX_AGE`, then prunes the
/// corresponding in-memory `VideoDownloads` entries so the `status`
/// endpoint stops reporting `Ready` for tokens whose file is gone.
///
/// The first tick fires on the interval boundary — not immediately on
/// startup — which deliberately gives in-flight downloads time to
/// complete before the reaper runs the first time. The ticker uses
/// `MissedTickBehavior::Skip` so a slow sweep (lots of files / slow
/// I/O) can't trigger a back-to-back burst of catch-up sweeps.
pub fn spawn_temp_video_cleanup_loop(downloads: VideoDownloads) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let dir = std::path::PathBuf::from(TMP_VIDEO_DIR);
        let mut ticker = tokio::time::interval(TMP_VIDEO_CLEANUP_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate tick `interval` fires at t=0 — we want the
        // first scan to land `TMP_VIDEO_CLEANUP_INTERVAL` after startup
        // so any download racing the boot doesn't get swept mid-write.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let (deleted_files, freed) = purge_stale_temp_videos(&dir, TMP_VIDEO_MAX_AGE).await;
            let pruned_tokens = prune_stale_video_downloads(&downloads, TMP_VIDEO_MAX_AGE).await;
            if deleted_files > 0 || pruned_tokens > 0 {
                let freed_mb = freed as f64 / (1024.0 * 1024.0);
                tracing::info!(
                    "periodic temp cleanup: deleted {deleted_files} files ({freed_mb:.1} MB), pruned {pruned_tokens} stale tokens — age threshold {}m",
                    TMP_VIDEO_MAX_AGE.as_secs() / 60
                );
            }
        }
    })
}

#[cfg(test)]
mod temp_cleanup_tests {
    use super::purge_stale_temp_videos;
    use std::time::Duration;

    /// Create a file in `dir` whose mtime is `age_secs` in the past and
    /// whose body is `size` bytes of zeros.
    async fn write_aged_file(dir: &std::path::Path, name: &str, size: usize, age_secs: u64) {
        let path = dir.join(name);
        tokio::fs::write(&path, vec![0u8; size]).await.unwrap();
        // Back-date the mtime so the reaper thinks the file is stale.
        let past = std::time::SystemTime::now() - Duration::from_secs(age_secs);
        let ft = filetime::FileTime::from_system_time(past);
        filetime::set_file_mtime(&path, ft).unwrap();
    }

    #[tokio::test]
    async fn deletes_only_files_older_than_max_age() {
        let tmp = tempfile::tempdir().unwrap();
        // 2 stale files, 1 fresh file.
        write_aged_file(tmp.path(), "old1.mp4", 1024, 3600).await;
        write_aged_file(tmp.path(), "old2.mp4", 2048, 3600).await;
        write_aged_file(tmp.path(), "fresh.mp4", 512, 10).await;

        let (deleted, freed) =
            purge_stale_temp_videos(tmp.path(), Duration::from_secs(30 * 60)).await;

        assert_eq!(deleted, 2, "should delete the two old files");
        assert_eq!(freed, 1024 + 2048, "should free the exact byte count");
        assert!(
            tmp.path().join("fresh.mp4").exists(),
            "fresh file must survive"
        );
        assert!(!tmp.path().join("old1.mp4").exists());
        assert!(!tmp.path().join("old2.mp4").exists());
    }

    #[tokio::test]
    async fn missing_dir_is_a_no_op() {
        let missing = std::path::PathBuf::from("/tmp/cr-videos-periodic-cleanup-does-not-exist");
        let (deleted, freed) =
            purge_stale_temp_videos(&missing, Duration::from_secs(30 * 60)).await;
        assert_eq!(deleted, 0);
        assert_eq!(freed, 0);
    }

    #[tokio::test]
    async fn empty_dir_deletes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let (deleted, freed) =
            purge_stale_temp_videos(tmp.path(), Duration::from_secs(30 * 60)).await;
        assert_eq!(deleted, 0);
        assert_eq!(freed, 0);
    }

    #[tokio::test]
    async fn skips_subdirectories() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::create_dir(tmp.path().join("subdir"))
            .await
            .unwrap();
        write_aged_file(tmp.path(), "old.mp4", 100, 3600).await;

        let (deleted, _) = purge_stale_temp_videos(tmp.path(), Duration::from_secs(30 * 60)).await;
        assert_eq!(deleted, 1, "only the file should be deleted, not the dir");
        assert!(tmp.path().join("subdir").exists());
    }

    #[tokio::test]
    async fn prunes_video_downloads_older_than_max_age() {
        use super::{VideoDownloads, prune_stale_video_downloads};
        use crate::handlers::video_api::{DownloadStatus, VideoTask};
        use std::sync::{
            Arc,
            atomic::{AtomicU8, Ordering},
        };

        let downloads: VideoDownloads =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));

        fn task_with_age(age: Duration) -> VideoTask {
            VideoTask {
                status: DownloadStatus::Ready {
                    size_mb: 1.0,
                    filename: "x.mp4".to_string(),
                },
                progress: Arc::new(AtomicU8::new(100)),
                file_path: std::path::PathBuf::new(),
                filename: "x.mp4".to_string(),
                parts: Vec::new(),
                // Back-date created_at by subtracting from Instant::now().
                created_at: std::time::Instant::now() - age,
                library_id: None,
            }
        }

        {
            let mut map = downloads.lock().await;
            map.insert("stale1".into(), task_with_age(Duration::from_secs(3600)));
            map.insert("stale2".into(), task_with_age(Duration::from_secs(3600)));
            map.insert("fresh".into(), task_with_age(Duration::from_secs(10)));
        }

        let pruned = prune_stale_video_downloads(&downloads, Duration::from_secs(30 * 60)).await;
        assert_eq!(pruned, 2, "both stale tokens should be pruned");

        let map = downloads.lock().await;
        assert!(map.contains_key("fresh"), "fresh token must survive");
        assert!(!map.contains_key("stale1"));
        assert!(!map.contains_key("stale2"));
        // sanity: progress atomic is unused in this test but Clippy might
        // complain about it being dead if we drop it silently.
        assert_eq!(
            map["fresh"].progress.load(Ordering::Relaxed),
            100,
            "progress atomic survives unchanged"
        );
    }
}
