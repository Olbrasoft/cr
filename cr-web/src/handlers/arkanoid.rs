use super::*;

#[derive(Template)]
#[template(path = "arkanoid.html")]
pub(crate) struct ArkanoidTemplate {
    pub(crate) img: String,
}

pub async fn arkanoid(State(state): State<AppState>) -> WebResult<impl IntoResponse> {
    let tmpl = ArkanoidTemplate {
        img: state.image_base_url.clone(),
    };
    Ok(Html(tmpl.render()?))
}
