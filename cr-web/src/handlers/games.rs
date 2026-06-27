use super::*;

#[derive(Template)]
#[template(path = "tetris.html")]
pub(crate) struct TetrisTemplate {
    pub(crate) img: String,
}

pub async fn tetris(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let tmpl = TetrisTemplate {
        img: state.image_base_url.clone(),
    };
    Ok(Html(tmpl.render()?))
}
