//! HTTP surface: the axum router and the serve loop. M1 exposes `/health` and
//! the WebSocket endpoint `/connection/websocket`.

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use centrifugo_core::Node;
use serde_json::json;

use crate::api::{self, ApiAuth};
use crate::sockjs::{self, Sessions};
use crate::ws;

pub fn router(node: Arc<Node>, api_auth: ApiAuth) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/connection/websocket", get(ws::ws_handler))
        .route("/api", post(api::api_handler))
        // SockJS fallback transport (xhr-polling + /info).
        .route(
            "/connection/sockjs/info",
            get(sockjs::info).options(sockjs::options),
        )
        .route(
            "/connection/sockjs/:server/:session/xhr",
            post(sockjs::xhr).options(sockjs::options),
        )
        .route(
            "/connection/sockjs/:server/:session/xhr_send",
            post(sockjs::xhr_send).options(sockjs::options),
        )
        .layer(Extension(api_auth))
        .layer(Extension(Sessions::default()))
        .with_state(node)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({}))
}

/// Prometheus text-format metrics. Exposes node gauges from the hub plus build
/// info. Full per-command counters are a future refinement.
async fn metrics(State(node): State<Arc<Node>>) -> Response {
    let hub = node.hub();
    let body = format!(
        "# HELP centrifugo_build_info Build information.\n\
         # TYPE centrifugo_build_info gauge\n\
         centrifugo_build_info{{version=\"{version}\"}} 1\n\
         # HELP centrifugo_node_num_clients Number of connected clients.\n\
         # TYPE centrifugo_node_num_clients gauge\n\
         centrifugo_node_num_clients {clients}\n\
         # HELP centrifugo_node_num_users Number of unique connected users.\n\
         # TYPE centrifugo_node_num_users gauge\n\
         centrifugo_node_num_users {users}\n\
         # HELP centrifugo_node_num_channels Number of active channels.\n\
         # TYPE centrifugo_node_num_channels gauge\n\
         centrifugo_node_num_channels {channels}\n",
        version = crate::VERSION,
        clients = hub.num_clients(),
        users = hub.num_users(),
        channels = hub.num_channels(),
    );
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

pub async fn serve(addr: std::net::SocketAddr, app: Router) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
