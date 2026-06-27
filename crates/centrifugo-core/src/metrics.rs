//! Lightweight Prometheus-style metrics: lock-free atomic counters incremented on
//! the hot paths (command dispatch, message fan-out, connect) and rendered by the
//! server's `/metrics` endpoint. Counter names mirror Go centrifuge where they
//! line up (`messages_sent_count{type}`, `connect_count{transport}`).

use std::sync::atomic::{AtomicU64, Ordering};

/// Client command method names, indexed by `MethodType` discriminant (0..=11).
pub const METHOD_NAMES: [&str; 12] = [
    "connect",
    "subscribe",
    "unsubscribe",
    "publish",
    "presence",
    "presence_stats",
    "history",
    "ping",
    "send",
    "rpc",
    "refresh",
    "sub_refresh",
];

/// Message (push) kinds, indexed for `messages_sent`.
pub const MESSAGE_KINDS: [&str; 3] = ["publication", "join", "leave"];

/// Transport names, indexed for `connect_count`.
pub const TRANSPORTS: [&str; 2] = ["websocket", "sockjs"];

#[derive(Default)]
pub struct Metrics {
    /// Messages fanned out, by kind (publication/join/leave).
    messages_sent: [AtomicU64; 3],
    /// Client commands processed, by method.
    commands: [AtomicU64; 12],
    /// Connections accepted, by transport.
    connects: [AtomicU64; 2],
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a fanned-out message of `kind` (0=publication, 1=join, 2=leave).
    pub fn inc_message_sent(&self, kind: usize) {
        if let Some(c) = self.messages_sent.get(kind) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a client command by its `MethodType` discriminant.
    pub fn inc_command(&self, method: usize) {
        if let Some(c) = self.commands.get(method) {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record an accepted connection by transport name.
    pub fn inc_connect(&self, transport: &str) {
        let i = if transport == "sockjs" { 1 } else { 0 };
        self.connects[i].fetch_add(1, Ordering::Relaxed);
    }

    pub fn messages_sent(&self) -> [u64; 3] {
        std::array::from_fn(|i| self.messages_sent[i].load(Ordering::Relaxed))
    }
    pub fn commands(&self) -> [u64; 12] {
        std::array::from_fn(|i| self.commands[i].load(Ordering::Relaxed))
    }
    pub fn connects(&self) -> [u64; 2] {
        std::array::from_fn(|i| self.connects[i].load(Ordering::Relaxed))
    }
}
