//! HTTP surface: the axum router and the serve loop. M1 exposes `/health` and
//! the WebSocket endpoint `/connection/websocket`.

use std::sync::Arc;

use axum::routing::get;
use axum::{Json, Router};
use centrifugo_core::Node;
use serde_json::json;

use crate::ws;

pub fn router(node: Arc<Node>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/connection/websocket", get(ws::ws_handler))
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
