//! Admin auth endpoint. `POST /admin/auth` exchanges the configured
//! `admin_password` for a session token (HS256 over `admin_secret`) that
//! authorizes the server API as a `Bearer` credential. The admin web UI itself
//! is a prebuilt SPA shipped with the Go distribution and is out of scope here.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use centrifugo_auth::gen_admin_token;
use serde::Deserialize;

/// Admin settings, carried as an axum Extension.
#[derive(Clone)]
pub struct AdminConfig {
    pub enabled: bool,
    pub password: String,
    pub secret: String,
}

#[derive(Deserialize, Default)]
struct AuthRequest {
    #[serde(default)]
    password: String,
}

/// `POST /admin/auth` — `{password}` → `{token}` when admin is enabled and the
/// password matches; otherwise 401 (404 when admin is disabled).
pub async fn admin_auth(Extension(cfg): Extension<AdminConfig>, body: String) -> Response {
    if !cfg.enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    let req: AuthRequest = serde_json::from_str(&body).unwrap_or_default();
    // An empty configured password never authorizes.
    if cfg.password.is_empty() || req.password != cfg.password {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match gen_admin_token(&cfg.secret) {
        Ok(token) => Json(serde_json::json!({ "token": token })).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
