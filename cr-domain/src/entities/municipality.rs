use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Municipality {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub municipality_code: String,
    pub pou_code: String,
    pub orp_id: i32,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub wikipedia_url: Option<String>,
    pub official_website: Option<String>,
    pub coat_of_arms_url: Option<String>,
    pub population: Option<i32>,
    pub elevation: Option<f64>,
    pub created_by: i32,
    pub created_at: DateTime<Utc>,
}
