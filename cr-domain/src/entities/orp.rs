use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Orp {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub orp_code: String,
    pub district_id: i32,
    pub created_by: i32,
    pub created_at: DateTime<Utc>,
}
