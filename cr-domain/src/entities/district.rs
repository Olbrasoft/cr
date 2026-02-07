use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct District {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub district_code: String,
    pub region_id: i32,
    pub created_by: i32,
    pub created_at: DateTime<Utc>,
}
