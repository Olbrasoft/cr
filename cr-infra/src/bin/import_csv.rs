use std::collections::HashMap;

use anyhow::{Context, Result};
use cr_domain::slug_from_name;
use sqlx::PgPool;

#[derive(Debug, serde::Deserialize)]
struct CsvRow {
    #[serde(rename = "obec_text")]
    municipality_name: String,
    #[serde(rename = "obec_kod")]
    municipality_code: String,
    #[serde(rename = "pou_csu_cis61_kod")]
    pou_code: String,
    #[serde(rename = "orp_text")]
    orp_name: String,
    #[serde(rename = "orp_csu_cis65_kod")]
    orp_code: String,
    #[serde(rename = "okres_text")]
    district_name: String,
    #[serde(rename = "okres_csu_cis101_lau_kod")]
    district_code: String,
    #[serde(rename = "kraj_text")]
    region_name: String,
    #[serde(rename = "kraj_csu_cis100_kod")]
    region_code: String,
    #[serde(rename = "kraj_csu_cis108_nuts_kod")]
    nuts_code: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    dotenvy::dotenv().ok();
    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL must be set in .env")?;

    let pool = PgPool::connect(&database_url)
        .await
        .context("Failed to connect to database")?;

    let csv_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "data/csu/struktura_uzemi_cr_2025.csv".to_string());

    tracing::info!("Reading CSV from {csv_path}");

    let mut rdr = csv::Reader::from_path(&csv_path)
        .with_context(|| format!("Failed to open {csv_path}"))?;

    let rows: Vec<CsvRow> = rdr
        .deserialize()
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to parse CSV")?;

    tracing::info!("Parsed {} municipality rows", rows.len());

    let mut tx = pool.begin().await.context("Failed to start transaction")?;

    // --- Regions ---
    let mut region_ids: HashMap<String, i32> = HashMap::new();
    let mut seen_regions: Vec<(&str, &str, &str)> = Vec::new();
    for row in &rows {
        if region_ids.contains_key(&row.region_code) {
            continue;
        }
        seen_regions.push((&row.region_name, &row.region_code, &row.nuts_code));
        // Insert with a placeholder; we'll get the real ID
        let slug = slug_from_name(&row.region_name);
        let rec = sqlx::query_scalar::<_, i32>(
            "INSERT INTO regions (name, slug, region_code, nuts_code) VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(&row.region_name)
        .bind(&slug)
        .bind(&row.region_code)
        .bind(&row.nuts_code)
        .fetch_one(&mut *tx)
        .await
        .with_context(|| format!("Failed to insert region: {}", row.region_name))?;

        region_ids.insert(row.region_code.clone(), rec);
    }
    tracing::info!("Inserted {} regions", region_ids.len());

    // --- Districts ---
    let mut district_ids: HashMap<String, i32> = HashMap::new();
    for row in &rows {
        if district_ids.contains_key(&row.district_code) {
            continue;
        }
        let region_id = region_ids[&row.region_code];
        let slug = slug_from_name(&row.district_name);
        let rec = sqlx::query_scalar::<_, i32>(
            "INSERT INTO districts (name, slug, district_code, region_id) VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(&row.district_name)
        .bind(&slug)
        .bind(&row.district_code)
        .bind(region_id)
        .fetch_one(&mut *tx)
        .await
        .with_context(|| format!("Failed to insert district: {}", row.district_name))?;

        district_ids.insert(row.district_code.clone(), rec);
    }
    tracing::info!("Inserted {} districts", district_ids.len());

    // --- ORP ---
    let mut orp_ids: HashMap<String, i32> = HashMap::new();
    for row in &rows {
        if orp_ids.contains_key(&row.orp_code) {
            continue;
        }
        let district_id = district_ids[&row.district_code];
        let slug = slug_from_name(&row.orp_name);
        let rec = sqlx::query_scalar::<_, i32>(
            "INSERT INTO orp (name, slug, orp_code, district_id) VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(&row.orp_name)
        .bind(&slug)
        .bind(&row.orp_code)
        .bind(district_id)
        .fetch_one(&mut *tx)
        .await
        .with_context(|| format!("Failed to insert ORP: {}", row.orp_name))?;

        orp_ids.insert(row.orp_code.clone(), rec);
    }
    tracing::info!("Inserted {} ORP", orp_ids.len());

    // --- Municipalities ---
    let mut municipality_count = 0;
    for row in &rows {
        let orp_id = orp_ids[&row.orp_code];
        let slug = slug_from_name(&row.municipality_name);
        sqlx::query(
            "INSERT INTO municipalities (name, slug, municipality_code, pou_code, orp_id) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&row.municipality_name)
        .bind(&slug)
        .bind(&row.municipality_code)
        .bind(&row.pou_code)
        .bind(orp_id)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("Failed to insert municipality: {}", row.municipality_name))?;

        municipality_count += 1;
    }
    tracing::info!("Inserted {} municipalities", municipality_count);

    tx.commit().await.context("Failed to commit transaction")?;

    tracing::info!("Import complete!");
    Ok(())
}
