use std::collections::HashMap;
use std::sync::Arc;

use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub geojson_index: Arc<GeoJsonIndex>,
    /// Base URL prefix for images. Empty string in production (served via Cloudflare Worker),
    /// "https://ceskarepublika.wiki" in dev (images fetched from production).
    pub image_base_url: String,
}

/// In-memory index of GeoJSON features for fast API lookups.
/// Loaded at startup from simplified GeoJSON files.
pub struct GeoJsonIndex {
    /// Municipality code -> GeoJSON Feature (as raw JSON string)
    pub municipalities: HashMap<String, String>,
    /// ORP code -> GeoJSON Feature (as raw JSON string)
    pub orp: HashMap<String, String>,
}

impl GeoJsonIndex {
    pub fn load(data_dir: &str) -> anyhow::Result<Self> {
        let mut municipalities = HashMap::new();
        let mut orp = HashMap::new();

        // Load municipalities
        let muni_path = format!("{data_dir}/obce_simple.geojson");
        if let Ok(content) = std::fs::read_to_string(&muni_path) {
            let data: serde_json::Value = serde_json::from_str(&content)?;
            if let Some(features) = data["features"].as_array() {
                for feat in features {
                    if let Some(code) = feat["properties"]["kod_obec_p"].as_str() {
                        municipalities.insert(code.to_string(), feat.to_string());
                    }
                }
            }
            tracing::info!("Loaded {} municipality polygons", municipalities.len());
        } else {
            tracing::warn!("Municipality GeoJSON not found at {muni_path}");
        }

        // Load ORP
        let orp_path = format!("{data_dir}/orp_simple.geojson");
        if let Ok(content) = std::fs::read_to_string(&orp_path) {
            let data: serde_json::Value = serde_json::from_str(&content)?;
            if let Some(features) = data["features"].as_array() {
                for feat in features {
                    if let Some(code) = feat["properties"]["kod_orp_p"].as_str() {
                        orp.insert(code.to_string(), feat.to_string());
                    }
                }
            }
            tracing::info!("Loaded {} ORP polygons", orp.len());
        } else {
            tracing::warn!("ORP GeoJSON not found at {orp_path}");
        }

        Ok(Self { municipalities, orp })
    }
}
