//! Admin auth endpoint. `POST /admin/auth` exchanges the configured
//! `admin_password` for a session token (HS256 over `admin_secret`) that
//! authorizes the server API with the `token` scheme. Mirrors Go's authHandler:
//! the password is read via FormValue (query string or x-www-form-urlencoded
//! body — what the vendored SPA posts), `admin_insecure` skips the check, and a
//! missing config or a mismatch is `400 Bad Request`.

use axum::extract::RawQuery;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use centrifugo_auth::gen_admin_token;

/// Admin settings, carried as an axum Extension.
#[derive(Clone)]
pub struct AdminConfig {
    pub enabled: bool,
    /// Skip the password check and accept any admin request (Go `admin_insecure`).
    pub insecure: bool,
    pub password: String,
    pub secret: String,
    /// Filesystem path to serve the admin UI from (Go `admin_web_path`); empty =
    /// use the embedded bundle.
    pub web_path: String,
}

/// `POST /admin/auth` — `password` (form/query) → `{token}` when admin is enabled
/// and the password matches; `{"token":"insecure"}` in insecure mode; 400 on a
/// missing config or mismatch; 404 when admin is disabled (matches Go authHandler).
pub async fn admin_auth(
    Extension(cfg): Extension<AdminConfig>,
    RawQuery(query): RawQuery,
    body: String,
) -> Response {
    if !cfg.enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    if cfg.insecure {
        return Json(serde_json::json!({ "token": "insecure" })).into_response();
    }
    // Go requires both admin_password and admin_secret to be configured.
    if cfg.password.is_empty() || cfg.secret.is_empty() {
        return (StatusCode::BAD_REQUEST, "Bad Request").into_response();
    }
    // FormValue: the urlencoded body first, then the query string.
    let form_password = form_value(&body, "password")
        .or_else(|| query.as_deref().and_then(|q| form_value(q, "password")))
        .unwrap_or_default();
    if form_password != cfg.password {
        return (StatusCode::BAD_REQUEST, "Bad Request").into_response();
    }
    match gen_admin_token(&cfg.secret) {
        Ok(token) => Json(serde_json::json!({ "token": token })).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Find `key` in a urlencoded `key=value&...` string, decoding `+`/`%XX`.
fn form_value(body: &str, key: &str) -> Option<String> {
    body.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (urldecode(k) == key).then(|| urldecode(v))
    })
}

/// Minimal application/x-www-form-urlencoded value decode (`+` → space, `%XX`).
fn urldecode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 3 <= bytes.len() => {
                if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    out.push(b);
                    i += 3;
                    continue;
                }
                out.push(b'%');
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
