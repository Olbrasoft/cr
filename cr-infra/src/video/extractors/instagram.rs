use anyhow::{Context, Result};

use super::super::{VideoFormat, VideoInfo};
use super::seznam::base64_decode;

/// Extract Instagram video using external API to get CDN URL.
pub(crate) async fn instagram_extract_info(
    client: &reqwest::Client,
    url: &str,
) -> Result<VideoInfo> {
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
