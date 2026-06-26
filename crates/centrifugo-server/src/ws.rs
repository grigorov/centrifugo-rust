//! WebSocket transport at `/connection/websocket`. The protocol is chosen by the
//! `?format=protobuf` / `?protocol=protobuf` query param (else JSON) — matching
//! centrifuge v0.14.2. JSON uses Text frames + NDJSON; protobuf uses Binary
//! frames + uvarint-length-prefixed messages. A native WS Ping control frame is
//! sent every 25s. On a protocol violation the connection is closed with the
//! centrifuge disconnect code (WS close code 3xxx) and JSON reason text.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{RawQuery, State};
use axum::response::Response;
use centrifugo_core::{Node, Out};
use centrifugo_protocol::codec::{decode_commands, encode_replies, ProtocolType};
use centrifugo_protocol::Disconnect;
use futures_util::{SinkExt, StreamExt};

const WRITE_QUEUE: usize = 256;
const PING_INTERVAL: Duration = Duration::from_secs(25);
const MAX_MESSAGE_SIZE: usize = 65536; // 64KB, matching centrifuge default

pub async fn ws_handler(
    State(node): State<Arc<Node>>,
    RawQuery(query): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    let proto = proto_from_query(query.as_deref());
    ws.max_message_size(MAX_MESSAGE_SIZE)
        .max_frame_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| handle_socket(socket, node, proto))
}

fn proto_from_query(query: Option<&str>) -> ProtocolType {
    if let Some(q) = query {
        for pair in q.split('&') {
            let mut it = pair.splitn(2, '=');
            let key = it.next().unwrap_or("");
            let val = it.next().unwrap_or("");
            if (key == "format" || key == "protocol") && val == "protobuf" {
                return ProtocolType::Protobuf;
            }
        }
    }
    ProtocolType::Json
}

async fn handle_socket(socket: WebSocket, node: Arc<Node>, proto: ProtocolType) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Out>(WRITE_QUEUE);
    let mut client = node.new_client(tx.clone(), proto);

    // Writer task: drain the queue (frames + close) and emit a 25s native ping.
    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.tick().await; // consume the immediate first tick
        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(Out::Frame(bytes)) => {
                        let msg = match proto {
                            ProtocolType::Json => Message::Text(String::from_utf8_lossy(&bytes).into_owned()),
                            ProtocolType::Protobuf => Message::Binary(bytes),
                        };
                        if sink.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Some(Out::Close(disconnect)) => {
                        let _ = sink
                            .send(Message::Close(Some(CloseFrame {
                                code: disconnect.code as u16,
                                reason: disconnect.close_text().into(),
                            })))
                            .await;
                        break;
                    }
                    None => break,
                },
                _ = ping.tick() => {
                    if sink.send(Message::Ping(Vec::new())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Read loop.
    'read: while let Some(Ok(msg)) = stream.next().await {
        let frame: Vec<u8> = match msg {
            Message::Text(t) => t.into_bytes(),
            Message::Binary(b) => b,
            Message::Close(_) => break,
            _ => continue, // pong/ping handled by tungstenite
        };
        let cmds = match decode_commands(proto, &frame) {
            Ok(cmds) => cmds,
            Err(_) => {
                let _ = tx.send(Out::Close(Disconnect::bad_request())).await;
                break;
            }
        };

        let mut replies = Vec::new();
        let mut disconnect = None;
        for c in &cmds {
            let outcome = client.handle_command(c);
            replies.extend(outcome.replies);
            if let Some(d) = outcome.disconnect {
                disconnect = Some(d);
                break;
            }
        }
        if !replies.is_empty() {
            match encode_replies(proto, &replies) {
                Ok(buf) => {
                    if tx.send(Out::Frame(buf)).await.is_err() {
                        break;
                    }
                }
                Err(_) => {
                    let _ = tx.send(Out::Close(Disconnect::server_error())).await;
                    break;
                }
            }
        }
        if let Some(d) = disconnect {
            let _ = tx.send(Out::Close(d)).await;
            break 'read;
        }
    }

    // Unregister and drop every sender so the writer drains, flushes any queued
    // close frame, and finishes on its own.
    node.remove(&client.id);
    drop(client);
    drop(tx);
    let _ = writer.await;
}
