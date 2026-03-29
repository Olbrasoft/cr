use super::*;

pub async fn audiobooks(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let audiobooks = sqlx::query_as::<_, AudiobookRow>(
        "SELECT id, title, author, narrator, year, duration, archive_id, cover_filename \
         FROM audiobooks ORDER BY year, title",
    )
    .fetch_all(&state.db)
    .await?;

    let tmpl = AudiobooksTemplate { img: state.image_base_url.clone(), audiobooks };
    Ok(Html(tmpl.render()?))
}
