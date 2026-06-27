//! Admin web UI — serves the vendored `centrifugal/web` SPA bundle (the same
//! prebuilt assets centrifugo v2.8.6 embeds) at the admin root, or from
//! `admin_web_path` when set (matching Go's WebPath override). The bundle
//! authenticates via `POST /admin/auth` and drives the server through
//! `POST /admin/api`.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;

use crate::admin::AdminConfig;

// The vendored bundle (extracted from centrifugo v2.8.6's embedded webui).
const INDEX_HTML: &[u8] = include_bytes!("../web/index.html");
const BUNDLE_JS: &[u8] = include_bytes!("../web/bundle.js");
const STYLES_CSS: &[u8] = include_bytes!("../web/styles.css");
const FAVICON_PNG: &[u8] = include_bytes!("../web/favicon.png");

fn content_type(name: &str) -> &'static str {
    match name.rsplit('.').next() {
        Some("html") => "text/html; charset=UTF-8",
        Some("js") => "application/javascript; charset=UTF-8",
        Some("css") => "text/css; charset=UTF-8",
        Some("png") => "image/png",
        _ => "application/octet-stream",
    }
}

fn embedded(name: &str) -> Option<&'static [u8]> {
    match name {
        "index.html" => Some(INDEX_HTML),
        "bundle.js" => Some(BUNDLE_JS),
        "styles.css" => Some(STYLES_CSS),
        "favicon.png" => Some(FAVICON_PNG),
        _ => None,
    }
}

/// Serve one admin-UI asset (`name`), from `admin_web_path` if configured, else
/// the embedded bundle. 404 when admin is disabled or the asset is unknown.
async fn serve(admin: &AdminConfig, name: &str) -> Response {
    if !admin.enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    let ct = content_type(name);
    if !admin.web_path.is_empty() {
        let path = std::path::Path::new(&admin.web_path).join(name);
        return match std::fs::read(&path) {
            Ok(bytes) => ([(header::CONTENT_TYPE, ct)], bytes).into_response(),
            Err(_) => StatusCode::NOT_FOUND.into_response(),
        };
    }
    match embedded(name) {
        Some(bytes) => ([(header::CONTENT_TYPE, ct)], bytes).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub async fn index(Extension(admin): Extension<AdminConfig>) -> Response {
    serve(&admin, "index.html").await
}
pub async fn bundle_js(Extension(admin): Extension<AdminConfig>) -> Response {
    serve(&admin, "bundle.js").await
}
pub async fn styles_css(Extension(admin): Extension<AdminConfig>) -> Response {
    serve(&admin, "styles.css").await
}
pub async fn favicon_png(Extension(admin): Extension<AdminConfig>) -> Response {
    serve(&admin, "favicon.png").await
}
