use super::*;

pub async fn download_video(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let tmpl = DownloadVideoTemplate {
        img: state.image_base_url.clone(),
    };
    Ok(Html(tmpl.render()?))
}
