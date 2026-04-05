use super::*;

pub async fn filmy_serialy(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let tmpl = FilmySerialyTemplate {
        img: state.image_base_url.clone(),
    };
    Ok(Html(tmpl.render()?))
}
