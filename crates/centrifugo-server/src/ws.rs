//! WebSocket transport at `/connection/websocket` (JSON for M1; protobuf in M2).
//!
//! Each connection gets a bounded mpsc queue and a dedicated writer task. The
//! read loop decodes commands, dispatches them through the per-connection
//! `Client`, and funnels replies back through the same queue so command-replies
//! and async pushes stay ordered. A native WS Ping control frame is sent every
//! 25s (matching centrifuge v0.14.2's `WebsocketPingInterval`).

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use centrifugo_core::Node;
use centrifugo_protocol::json::{decode_commands, encode_replies};
use futures_util::{SinkExt, StreamExt};

const WRITE_QUEUE: usize = 256;
const PING_INTERVAL: Duration = Duration::from_secs(25);

pub async fn ws_handler(State(node): State<Arc<Node>>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, node))
}

async fn handle_socket(socket: WebSocket, node: Arc<Node>) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(WRITE_QUEUE);
    let mut client = node.new_client(tx.clone());

    // Writer task: drain the queue to the socket and emit a 25s native ping.
    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.tick().await; // consume the immediate first tick
        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(bytes) => {
                        let text = String::from_utf8_lossy(&bytes).into_owned();
                        if sink.send(Message::Text(text)).await.is_err() {
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
        match msg {
            Message::Text(t) => {
                match decode_commands(t.as_bytes()) {
                    Ok(cmds) => {
                        let mut replies = Vec::new();
                        for c in &cmds {
                            replies.extend(client.handle_command(c));
                        }
                        if !replies.is_empty() {
                            if let Ok(buf) = encode_replies(&replies) {
                                if tx.send(buf).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => break, // malformed frame: close (disconnect codes in M2)
                }
            }
            Message::Binary(_) => {
                // Protobuf path arrives in M2; for now ignore binary frames.
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    node.remove(&client.id);
    writer.abort();
}
