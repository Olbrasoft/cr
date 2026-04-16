use anyhow::{Context, Result};

use super::super::{VideoFormat, VideoInfo};

/// Czech proxy URL and key for geo-blocked Nova.cz embeds (from env vars).
pub(crate) fn cz_proxy_config() -> Option<(String, String)> {
    let url = std::env::var("CZ_PROXY_URL").ok()?;
    let key = std::env::var("CZ_PROXY_KEY").ok()?;
    if url.is_empty() || key.is_empty() {
        return None;
    }
    Some((url, key))
}

/// Extract video info from Nova.cz via Czech proxy (for geo-blocked embeds).
/// 1. Fetch tv.nova.cz page to get embed ID
/// 2. Fetch embed page via Czech proxy
/// 3. Extract m3u8/mpd manifest URLs + metadata
pub(crate) async fn nova_proxy_extract_info(
    client: &reqwest::Client,
    url: &str,
) -> Result<VideoInfo> {
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
