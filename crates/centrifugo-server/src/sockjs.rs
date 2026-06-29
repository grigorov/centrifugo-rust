//! SockJS fallback transport at `/connection/sockjs`. Implements the universal
//! **xhr-polling** transport plus the `/info` endpoint, which is what a SockJS
//! client falls back to whenever a raw WebSocket is unavailable.
//!
//! Protocol recap (xhr-polling): the client first opens a *receiving* channel
//! with `POST .../xhr` — a brand-new session replies with the open frame `o\n`.
//! It pushes commands with `POST .../xhr_send` carrying a JSON array of strings
//! (each string is a centrifuge NDJSON command). Replies + pushes are delivered
//! on the next `POST .../xhr` poll as a message frame `a["...","..."]\n`, or a
//! heartbeat `h\n` after ~25s idle, or a close frame `c[code,"reason"]\n`.
//!
//! Each SockJS session is driven by exactly the same [`Client`] state machine as
//! the native WebSocket transport (JSON protocol). Deferred SockJS transports:
//! xhr_streaming, eventsource, htmlfile, jsonp, and SockJS-over-WebSocket (the
//! native `/connection/websocket` already covers the WS case).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use centrifugo_core::{Node, Out, Signal};
use centrifugo_protocol::codec::{decode_commands, encode_replies, ProtocolType};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

const HEARTBEAT: Duration = Duration::from_secs(25);
const INCOMING_QUEUE: usize = 64;
const WRITE_QUEUE: usize = 256;

/// Per-session state: a queue feeding the client task and a queue draining to
/// polls. Only one poll runs at a time per session (SockJS guarantees this), so
/// the outgoing receiver is behind a simple async mutex.
struct Session {
    incoming: mpsc::Sender<String>,
    outgoing: AsyncMutex<mpsc::Receiver<Out>>,
}

/// Session registry, shared via an axum Extension.
#[derive(Clone, Default)]
pub struct Sessions {
    map: Arc<Mutex<HashMap<String, Arc<Session>>>>,
}

static ENTROPY: AtomicU32 = AtomicU32::new(2_154_001_001);

/// `GET /connection/sockjs/info` — transport capabilities. `websocket:true`
/// matches Go; clients that pick SockJS-WS and find it absent fall back to xhr.
pub async fn info() -> Response {
    let entropy = ENTROPY.fetch_add(2_654_435_761, Ordering::Relaxed);
    let body = serde_json::json!({
        "websocket": true,
        "origins": ["*:*"],
        "cookie_needed": false,
        "entropy": entropy,
    });
    (
        cors_headers(),
        [(header::CONTENT_TYPE, "application/json; charset=UTF-8")],
        body.to_string(),
    )
        .into_response()
}

/// CORS preflight for any SockJS endpoint.
pub async fn options() -> Response {
    (StatusCode::NO_CONTENT, cors_headers()).into_response()
}

/// `POST /connection/sockjs/:server/:session/xhr` — the receiving poll.
pub async fn xhr(
    State(node): State<Arc<Node>>,
    Extension(sessions): Extension<Sessions>,
    Path((_server, session_id)): Path<(String, String)>,
) -> Response {
    let existing = sessions.map.lock().get(&session_id).cloned();
    let session = match existing {
        // A brand-new session: open it and return the SockJS open frame.
        None => {
            create_session(&node, &sessions, &session_id);
            return sockjs_body("o\n");
        }
        Some(s) => s,
    };

    let mut rx = session.outgoing.lock().await;
    match tokio::time::timeout(HEARTBEAT, rx.recv()).await {
        Ok(Some(Out::Frame(bytes))) => {
            let mut msgs = vec![String::from_utf8_lossy(&bytes).into_owned()];
            while let Ok(Out::Frame(more)) = rx.try_recv() {
                msgs.push(String::from_utf8_lossy(&more).into_owned());
            }
            sockjs_body(&message_frame(&msgs))
        }
        Ok(Some(Out::Close(d))) => {
            drop(rx);
            sessions.map.lock().remove(&session_id);
            sockjs_body(&close_frame(d.code, d.close_text()))
        }
        // Writer side gone — session closed.
        Ok(None) => {
            drop(rx);
            sessions.map.lock().remove(&session_id);
            sockjs_body(&close_frame(3000, "Go away!".into()))
        }
        // Idle: heartbeat.
        Err(_) => sockjs_body("h\n"),
    }
}

/// `POST /connection/sockjs/:server/:session/xhr_send` — push commands.
pub async fn xhr_send(
    Extension(sessions): Extension<Sessions>,
    Path((_server, session_id)): Path<(String, String)>,
    body: String,
) -> Response {
    let Some(session) = sessions.map.lock().get(&session_id).cloned() else {
        return (StatusCode::NOT_FOUND, cors_headers()).into_response();
    };
    let messages = match parse_send_body(&body) {
        Some(m) => m,
        None => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "Broken JSON encoding.").into_response()
        }
    };
    for m in messages {
        if session.incoming.send(m).await.is_err() {
            break;
        }
    }
    (StatusCode::NO_CONTENT, cors_headers()).into_response()
}

/// Register a session and spawn its client task.
fn create_session(node: &Arc<Node>, sessions: &Sessions, session_id: &str) {
    let (in_tx, in_rx) = mpsc::channel::<String>(INCOMING_QUEUE);
    let (out_tx, out_rx) = mpsc::channel::<Out>(WRITE_QUEUE);
    sessions.map.lock().insert(
        session_id.to_string(),
        Arc::new(Session {
            incoming: in_tx,
            outgoing: AsyncMutex::new(out_rx),
        }),
    );
    tokio::spawn(run_session(node.clone(), in_rx, out_tx));
}

/// Drive one SockJS session through the shared `Client` state machine. Replies
/// are enqueued on the outgoing channel (drained by polls); pushes are delivered
/// to the same channel by the Node's fan-out.
async fn run_session(
    node: Arc<Node>,
    mut incoming: mpsc::Receiver<String>,
    out_tx: mpsc::Sender<Out>,
) {
    let reply_tx = out_tx.clone();
    let (ctrl_tx, mut ctrl_rx) = mpsc::channel::<Signal>(crate::ws::CTRL_QUEUE);
    let mut client = node.new_client(out_tx, ProtocolType::Json);
    client.set_transport("sockjs");
    client.set_ctrl(ctrl_tx);
    let mut presence = tokio::time::interval(node.presence_ping_interval());
    presence.tick().await; // consume the immediate first tick
    let refresh_lookahead = node.presence_ping_interval().as_secs() as i64;
    let mut expiry = tokio::time::interval(crate::ws::EXPIRY_CHECK_INTERVAL);
    expiry.tick().await; // consume the immediate first tick
    loop {
        let raw = tokio::select! {
            maybe = incoming.recv() => match maybe {
                Some(r) => r,
                None => break,
            },
            _ = presence.tick() => {
                client.refresh_presence().await;
                continue;
            }
            _ = expiry.tick() => {
                // Server-side proactive refresh (refresh proxy) before expiry.
                if let Some(d) = client.proactive_refresh(refresh_lookahead).await {
                    let _ = reply_tx.send(Out::Close(d)).await;
                    break;
                }
                if let Some(d) = client.check_expired() {
                    let _ = reply_tx.send(Out::Close(d)).await;
                    break;
                }
                continue;
            }
            sig = ctrl_rx.recv() => {
                match sig {
                    Some(Signal::Unsubscribe(ch)) if ch.is_empty() => {
                        for c in client.subscribed_channels() {
                            client.server_unsubscribe(&c).await;
                        }
                    }
                    Some(Signal::Unsubscribe(ch)) => client.server_unsubscribe(&ch).await,
                    Some(Signal::Disconnect(d)) => {
                        let _ = reply_tx.send(Out::Close(d)).await;
                        break;
                    }
                    None => {}
                }
                continue;
            }
        };
        // Go Client.Handle closes 3003 on a zero-length frame before decoding.
        if raw.is_empty() {
            let _ = reply_tx
                .send(Out::Close(centrifugo_protocol::Disconnect::bad_request()))
                .await;
            break;
        }
        let cmds = match decode_commands(ProtocolType::Json, raw.as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                let _ = reply_tx
                    .send(Out::Close(centrifugo_protocol::Disconnect::bad_request()))
                    .await;
                break;
            }
        };
        let mut replies = Vec::new();
        let mut disconnect = None;
        for c in &cmds {
            // Go Client.Handle: a reply-expecting command (any method except Send)
            // with id==0 -> close 3003 ("command ID required"). Applies to CONNECT.
            if c.id == 0 && c.method != centrifugo_protocol::MethodType::Send {
                disconnect = Some(centrifugo_protocol::Disconnect::bad_request());
                break;
            }
            let outcome = client.handle_command(c).await;
            replies.extend(outcome.replies);
            if let Some(d) = outcome.disconnect {
                disconnect = Some(d);
                break;
            }
        }
        if !replies.is_empty() {
            if let Ok(buf) = encode_replies(ProtocolType::Json, &replies) {
                if reply_tx.send(Out::Frame(buf)).await.is_err() {
                    break;
                }
            }
        }
        // Server-side-subscription Joins follow the (connect) reply frame.
        client.flush_pending_joins().await;
        if let Some(d) = disconnect {
            let _ = reply_tx.send(Out::Close(d)).await;
            break;
        }
    }
    client.on_disconnect().await;
}

/// Parse an xhr_send body: a JSON array of strings, or a single JSON string.
fn parse_send_body(body: &str) -> Option<Vec<String>> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return None;
    }
    match serde_json::from_str::<serde_json::Value>(trimmed).ok()? {
        serde_json::Value::Array(a) => a
            .into_iter()
            .map(|v| v.as_str().map(str::to_string))
            .collect(),
        serde_json::Value::String(s) => Some(vec![s]),
        _ => None,
    }
}

/// Build a SockJS message frame `a[json-encoded strings]\n`.
fn message_frame(msgs: &[String]) -> String {
    // Each centrifuge reply line is wrapped as a JSON string element.
    let mut framed: Vec<String> = Vec::with_capacity(msgs.len());
    for m in msgs {
        for line in m.lines().filter(|l| !l.is_empty()) {
            framed.push(line.to_string());
        }
    }
    format!(
        "a{}\n",
        serde_json::to_string(&framed).unwrap_or_else(|_| "[]".into())
    )
}

/// Build a SockJS close frame `c[code,"reason"]\n`.
fn close_frame(code: u32, reason: String) -> String {
    format!(
        "c{}\n",
        serde_json::to_string(&(code, reason)).unwrap_or_else(|_| "[3000,\"Go away!\"]".into())
    )
}

/// SockJS frame response (JS content-type + CORS + no-cache).
fn sockjs_body(frame: &str) -> Response {
    (
        cors_headers(),
        [
            (
                header::CONTENT_TYPE,
                "application/javascript; charset=UTF-8",
            ),
            (
                header::CACHE_CONTROL,
                "no-store, no-cache, must-revalidate, max-age=0",
            ),
        ],
        frame.to_string(),
    )
        .into_response()
}

fn cors_headers() -> [(header::HeaderName, &'static str); 3] {
    [
        (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        (header::ACCESS_CONTROL_ALLOW_METHODS, "OPTIONS, POST, GET"),
        (header::ACCESS_CONTROL_ALLOW_HEADERS, "content-type"),
    ]
}
