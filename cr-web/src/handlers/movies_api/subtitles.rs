use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

// --- Types ---

#[derive(Serialize, Clone)]
pub struct SubtitleTrack {
    pub url: String,
    pub lang: String,
    pub label: String,
}

#[derive(Deserialize)]
pub struct SubtitleQuery {
    pub url: String,
}

// --- Handler ---

/// Proxy VTT subtitle files from premiumcdn.net through our domain.
/// HTML5 <track> elements require CORS headers that CDN doesn't provide.
pub async fn movies_subtitle(
    State(state): State<AppState>,
    Query(query): Query<SubtitleQuery>,
) -> impl IntoResponse {
    let parsed = match reqwest::Url::parse(&query.url) {
        Ok(u) => u,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid URL").into_response(),
    };
    // Only allow premiumcdn.net VTT files
    let host = parsed.host_str().unwrap_or("");
    if !host.ends_with("premiumcdn.net") || !parsed.path().ends_with(".vtt") {
        return (StatusCode::FORBIDDEN, "URL not allowed").into_response();
    }

    let resp = state
        .http_client
        .get(parsed.as_str())
        .header("User-Agent", "Mozilla/5.0")
        .header("Referer", "https://prehraj.to/")
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => match r.bytes().await {
            Ok(bytes) => (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "text/vtt; charset=utf-8".to_string()),
                    (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".to_string()),
                    (header::CACHE_CONTROL, "public, max-age=3600".to_string()),
                ],
                bytes,
            )
                .into_response(),
            Err(_) => (StatusCode::BAD_GATEWAY, "Failed to read subtitle").into_response(),
        },
        _ => (StatusCode::NOT_FOUND, "Subtitle not available").into_response(),
    }
}

// --- Subtitle extraction helpers ---

/// Extract VTT subtitle tracks from Prehraj.to page HTML.
/// Matches JWPlayer track config: { file: "...vtt", label: "CZE - 123 - cze", kind: "captions" }
pub(crate) fn extract_subtitles_from_html(html: &str) -> Vec<SubtitleTrack> {
    let re = regex::Regex::new(
        r#"\{\s*file\s*:\s*"([^"]+\.vtt[^"]*)"\s*,\s*(?:"default"\s*:\s*true\s*,\s*)?label\s*:\s*"([^"]+)"\s*,\s*kind\s*:\s*"captions"\s*\}"#,
    )
    .expect("const regex literal compiles");

    let lang_re = regex::Regex::new(r"(\w{2,3})\s*$").expect("const regex literal compiles");

    re.captures_iter(html)
        .map(|cap| {
            let vtt_url = cap[1].replace("\\u0026", "&").replace("&amp;", "&");
            let label_raw = &cap[2];

            // Extract language code from label like "CZE - 8929014 - cze"
            let lang = lang_re
                .captures(label_raw)
                .map(|m| m[1].to_lowercase())
                .unwrap_or_default();

            // Clean label: "CZE - 8929014 - cze" -> "CZE"
            let label = regex::Regex::new(r"\s*-\s*\d+\s*-\s*\w+$")
                .expect("const regex literal compiles")
                .replace(label_raw, "")
                .trim()
                .to_string();

            SubtitleTrack {
                url: vtt_url,
                lang,
                label,
            }
        })
        .collect()
}
