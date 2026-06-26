//! WebSocket transport at `/connection/websocket`. The protocol is chosen by the
//! `?format=protobuf` / `?protocol=protobuf` query param (else JSON) — matching
//! centrifuge v0.14.2. JSON uses Text frames + NDJSON; protobuf uses Binary
//! frames + uvarint-length-prefixed messages. A native WS Ping control frame is
//! sent every 25s.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{RawQuery, State};
use axum::response::Response;
use centrifugo_core::Node;
use centrifugo_protocol::codec::{decode_commands, encode_replies, ProtocolType};
use futures_util::{SinkExt, StreamExt};

const WRITE_QUEUE: usize = 256;
const PING_INTERVAL: Duration = Duration::from_secs(25);

pub async fn ws_handler(
    State(node): State<Arc<Node>>,
    RawQuery(query): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    let proto = proto_from_query(query.as_deref());
    ws.on_upgrade(move |socket| handle_socket(socket, node, proto))
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
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(WRITE_QUEUE);
    let mut client = node.new_client(tx.clone(), proto);

    // Writer task: drain the queue (frames already encoded) + emit a 25s native ping.
    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.tick().await; // consume the immediate first tick
        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(bytes) => {
                        let msg = match proto {
                            ProtocolType::Json => Message::Text(String::from_utf8_lossy(&bytes).into_owned()),
                            ProtocolType::Protobuf => Message::Binary(bytes),
                        };
                        if sink.send(msg).await.is_err() {
                            break;
                        }
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
    while let Some(Ok(msg)) = stream.next().await {
        let frame: Vec<u8> = match msg {
            Message::Text(t) => t.into_bytes(),
            Message::Binary(b) => b,
            Message::Close(_) => break,
            _ => continue, // pong/ping handled by tungstenite
        };
        match decode_commands(proto, &frame) {
            Ok(cmds) => {
                let mut replies = Vec::new();
                for c in &cmds {
                    replies.extend(client.handle_command(c));
                }
                if !replies.is_empty() {
                    match encode_replies(proto, &replies) {
                        Ok(buf) => {
                            if tx.send(buf).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(_) => break, // malformed frame: close (disconnect codes in M2.5)
        }
    }

    node.remove(&client.id);
    writer.abort();
}
