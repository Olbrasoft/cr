use super::*;

pub async fn geojson_municipality(
    State(state): State<AppState>,
    Path(code): Path<String>,
) -> Response {
    match state.geojson_index.municipalities.get(&code) {
        Some(geojson) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/geo+json"),
             (header::CACHE_CONTROL, "public, max-age=86400")],
            geojson.clone(),
        ).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub async fn geojson_orp(
    State(state): State<AppState>,
    Path(code): Path<String>,
) -> Response {
    match state.geojson_index.orp.get(&code) {
        Some(geojson) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/geo+json"),
             (header::CACHE_CONTROL, "public, max-age=86400")],
            geojson.clone(),
        ).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
