//! HTTP surface: the axum router and the serve loop. M1 exposes `/health` and
//! the WebSocket endpoint `/connection/websocket`.

use std::sync::Arc;

use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use centrifugo_core::Node;
use serde_json::json;

use crate::api::{self, ApiAuth};
use crate::ws;

pub fn router(node: Arc<Node>, api_auth: ApiAuth) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/connection/websocket", get(ws::ws_handler))
        .route("/api", post(api::api_handler))
        .layer(Extension(api_auth))
        .with_state(node)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({}))
}

pub async fn serve(addr: std::net::SocketAddr, app: Router) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
