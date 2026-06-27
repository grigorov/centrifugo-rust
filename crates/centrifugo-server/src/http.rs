//! HTTP surface: the axum router and the serve loop. M1 exposes `/health` and
//! the WebSocket endpoint `/connection/websocket`.

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use centrifugo_core::Node;
use serde_json::json;

use crate::admin::{self, AdminConfig};
use crate::api::{self, ApiAuth};
use crate::sockjs::{self, Sessions};
use crate::webui;
use crate::ws;

pub fn router(node: Arc<Node>, api_auth: ApiAuth, admin_config: AdminConfig) -> Router {
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/admin/auth", post(admin::admin_auth))
        .route("/admin/api", post(api::admin_api_handler))
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
        );

    // Admin web UI served at the root when admin is enabled. A fallback serves
    // the whole asset tree (index.html for `/`, any file under admin_web_path or
    // the embedded bundle) without conflicting with the API/WS routes above.
    if admin_config.enabled {
        router = router.fallback(webui::asset_fallback);
    }

    router
        .layer(Extension(api_auth))
        .layer(Extension(admin_config))
        .layer(Extension(Sessions::default()))
        .with_state(node)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({}))
}

/// Prometheus text-format metrics. Exposes node gauges from the hub plus build
/// info. Full per-command counters are a future refinement.
async fn metrics(State(node): State<Arc<Node>>) -> Response {
    use centrifugo_core::metrics::{MESSAGE_KINDS, METHOD_NAMES, TRANSPORTS};
    let hub = node.hub();
    let m = node.metrics();
    let mut body = format!(
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

    // Messages fanned out, by kind (Go centrifugo_node_messages_sent_count).
    let sent = m.messages_sent();
    body.push_str(
        "# HELP centrifugo_node_messages_sent_count Number of messages sent.\n\
         # TYPE centrifugo_node_messages_sent_count counter\n",
    );
    for (i, kind) in MESSAGE_KINDS.iter().enumerate() {
        body.push_str(&format!(
            "centrifugo_node_messages_sent_count{{type=\"{kind}\"}} {}\n",
            sent[i]
        ));
    }

    // Client commands processed, by method.
    let cmds = m.commands();
    body.push_str(
        "# HELP centrifugo_client_command_count Number of client commands processed.\n\
         # TYPE centrifugo_client_command_count counter\n",
    );
    for (i, name) in METHOD_NAMES.iter().enumerate() {
        body.push_str(&format!(
            "centrifugo_client_command_count{{method=\"{name}\"}} {}\n",
            cmds[i]
        ));
    }

    // Connections accepted, by transport (Go centrifugo_transport_connect_count).
    let conns = m.connects();
    body.push_str(
        "# HELP centrifugo_transport_connect_count Number of connections accepted.\n\
         # TYPE centrifugo_transport_connect_count counter\n",
    );
    for (i, t) in TRANSPORTS.iter().enumerate() {
        body.push_str(&format!(
            "centrifugo_transport_connect_count{{transport=\"{t}\"}} {}\n",
            conns[i]
        ));
    }

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
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
