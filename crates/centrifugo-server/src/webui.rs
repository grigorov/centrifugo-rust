//! Admin web UI — serves the vendored `centrifugal/web` SPA bundle (the same
//! prebuilt assets centrifugo v2.8.6 embeds) at the admin root, or an arbitrary
//! file tree from `admin_web_path` when set (Go's `http.FileServer(http.Dir)`
//! WebPath override). Mounted as the router fallback when admin is enabled, so it
//! never conflicts with the API/WS routes. The bundle authenticates via
//! `POST /admin/auth` and drives the server through `POST /admin/api`.

use axum::http::{header, StatusCode, Uri};
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
        Some("svg") => "image/svg+xml",
        Some("json") => "application/json",
        Some("ico") => "image/x-icon",
        Some("map") => "application/json",
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

/// Reject path-traversal / absolute paths before touching the filesystem.
fn is_safe(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('/')
        && !name.contains('\\')
        && name.split('/').all(|seg| !seg.is_empty() && seg != "..")
}

/// Serve one admin-UI asset (`name`), from `admin_web_path` (full tree) if set,
/// else the embedded bundle. 404 when admin is disabled, the path is unsafe, or
/// the asset is unknown/missing.
async fn serve_asset(admin: &AdminConfig, name: &str) -> Response {
    if !admin.enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    if !is_safe(name) {
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

/// Router fallback: maps the request path to an admin-UI asset (`/` → index.html).
/// Mounted only when admin is enabled.
pub async fn asset_fallback(Extension(admin): Extension<AdminConfig>, uri: Uri) -> Response {
    let raw = uri.path().trim_start_matches('/');
    let name = if raw.is_empty() { "index.html" } else { raw };
    serve_asset(&admin, name).await
}

#[cfg(test)]
mod tests {
    use super::is_safe;

    #[test]
    fn is_safe_rejects_traversal() {
        assert!(is_safe("bundle.js"));
        assert!(is_safe("sub/dir/app.js"));
        assert!(!is_safe(".."));
        assert!(!is_safe("../etc/passwd"));
        assert!(!is_safe("a/../../b"));
        assert!(!is_safe("/etc/passwd"));
        assert!(!is_safe("a\\b"));
        assert!(!is_safe(""));
    }
}
