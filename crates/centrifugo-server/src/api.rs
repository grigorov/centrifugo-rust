//! Server-side HTTP API at `POST /api` (apikey auth). JSON commands
//! `{id?, method, params}` (NDJSON-pipelined); replies `{id?, error?, result?}`
//! at HTTP 200. Void commands (publish/broadcast/unsubscribe/disconnect/
//! history_remove) omit `result`, matching centrifugo v2.8.6.
//!
//! The API Publication shape is `{uid?, data, info?}` (no seq/gen/offset) —
//! distinct from the client-protocol Publication.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use centrifugo_core::Node;
use centrifugo_grpc::pb;
use centrifugo_protocol::messages::ClientInfo;
use centrifugo_protocol::Raw as ProtoRaw;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::grpc::{
    api_err, channel_caps as pb_channel_caps, to_pb_client_info, to_pb_publication,
};

type Raw = Box<RawValue>;

/// API auth config carried as an axum Extension.
#[derive(Clone)]
pub struct ApiAuth {
    pub key: String,
    pub insecure: bool,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

#[derive(Deserialize, Default)]
struct ApiCommand {
    #[serde(default)]
    id: u32,
    #[serde(default)]
    method: String,
    #[serde(default)]
    params: Option<Raw>,
}

#[derive(Serialize, Default)]
struct ApiReply {
    #[serde(skip_serializing_if = "is_zero")]
    id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ApiError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Raw>,
}

#[derive(Serialize)]
struct ApiError {
    code: u32,
    message: String,
}

#[derive(Deserialize, Default)]
struct PublishReq {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    data: Option<Raw>,
}

#[derive(Deserialize, Default)]
struct BroadcastReq {
    #[serde(default)]
    channels: Vec<String>,
    #[serde(default)]
    data: Option<Raw>,
}

#[derive(Deserialize, Default)]
struct ChannelReq {
    #[serde(default)]
    channel: String,
}

#[derive(Deserialize, Default)]
struct UnsubscribeReq {
    #[serde(default)]
    user: String,
    #[serde(default)]
    channel: String,
}

#[derive(Deserialize, Default)]
struct DisconnectReq {
    #[serde(default)]
    user: String,
}

#[derive(Serialize)]
struct PresenceResult {
    presence: HashMap<String, ClientInfo>,
}

#[derive(Serialize)]
struct PresenceStatsResult {
    num_clients: u32,
    num_users: u32,
}

#[derive(Serialize)]
struct ApiPublication {
    #[serde(skip_serializing_if = "String::is_empty")]
    uid: String,
    data: Option<ProtoRaw>,
    #[serde(skip_serializing_if = "Option::is_none")]
    info: Option<ClientInfo>,
}

#[derive(Serialize)]
struct HistoryResult {
    publications: Vec<ApiPublication>,
}

#[derive(Serialize)]
struct ChannelsResult {
    channels: Vec<String>,
}

#[derive(Serialize)]
struct InfoResult {
    nodes: Vec<NodeResult>,
}

#[derive(Serialize)]
struct NodeResult {
    uid: String,
    name: String,
    version: String,
    num_clients: u32,
    num_users: u32,
    num_channels: u32,
    uptime: u32,
    // Go emits `"metrics":null` (non-omitempty); per-node metrics are not collected.
    metrics: Option<serde_json::Value>,
}

/// `POST /api` — apikey-authenticated server API (matches Go's apiKeyAuth).
pub async fn api_handler(
    State(node): State<Arc<Node>>,
    Extension(auth): Extension<ApiAuth>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    if !auth.insecure && !authorized(&auth, &headers, query.as_deref()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    run_api(&node, &headers, &body).await
}

/// `POST /admin/api` — admin-token-authenticated server API. Go guards this
/// endpoint with `Authorization: token <admin-token>` (scheme is literally
/// `token`, not `Bearer`), validated against `admin_secret`. The command
/// surface is identical to `/api`.
pub async fn admin_api_handler(
    State(node): State<Arc<Node>>,
    Extension(admin): Extension<crate::admin::AdminConfig>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !admin.enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    // Insecure admin (Go `admin_insecure`) skips token auth entirely.
    if !admin.insecure {
        if admin.secret.is_empty() {
            return StatusCode::NOT_FOUND.into_response();
        }
        let authed = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.split_once(' '))
            .is_some_and(|(scheme, val)| {
                scheme.eq_ignore_ascii_case("token")
                    && centrifugo_auth::verify_admin_token(&admin.secret, val)
            });
        if !authed {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    run_api(&node, &headers, &body).await
}

/// Dispatch the API request, picking the codec by Content-Type: a protobuf
/// `application/octet-stream` body is a uvarint-length-delimited pb Command
/// stream; anything else is NDJSON JSON commands.
async fn run_api(node: &Arc<Node>, headers: &HeaderMap, body: &[u8]) -> Response {
    let ct = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if ct.starts_with("application/octet-stream") {
        run_protobuf(node, body).await
    } else {
        match std::str::from_utf8(body) {
            Ok(s) => run_commands(node, s).await,
            Err(_) => (StatusCode::BAD_REQUEST, "Bad Request").into_response(),
        }
    }
}

/// Protobuf API: decode the uvarint-length-delimited pb Command stream, dispatch
/// each, and return the uvarint-length-delimited pb Reply stream.
async fn run_protobuf(node: &Arc<Node>, body: &[u8]) -> Response {
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "Bad Request").into_response();
    }
    let mut buf = body;
    let mut out = Vec::new();
    while !buf.is_empty() {
        let cmd = match pb::Command::decode_length_delimited(&mut buf) {
            Ok(c) => c,
            Err(_) => return (StatusCode::BAD_REQUEST, "Bad Request").into_response(),
        };
        let reply = dispatch_pb(node, cmd).await;
        if reply.encode_length_delimited(&mut out).is_err() {
            return (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response();
        }
    }
    (
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        out,
    )
        .into_response()
}

/// Decode the NDJSON command body, dispatch each, and return the NDJSON replies.
async fn run_commands(node: &Arc<Node>, body: &str) -> Response {
    if body.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "Bad Request").into_response();
    }
    let mut buf = String::new();
    let de = serde_json::Deserializer::from_str(body);
    for cmd in de.into_iter::<ApiCommand>() {
        let cmd = match cmd {
            Ok(c) => c,
            Err(_) => return (StatusCode::BAD_REQUEST, "Bad Request").into_response(),
        };
        let reply = dispatch(node, cmd).await;
        buf.push_str(&serde_json::to_string(&reply).unwrap_or_else(|_| "{}".into()));
        buf.push('\n');
    }
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        buf,
    )
        .into_response()
}

/// `Authorization: apikey <KEY>` (case-insensitive scheme) or `?api_key=<KEY>`.
/// An empty configured api key never authorizes (matches Go's apiKeyAuth, which
/// accepts only the `apikey` scheme — admin tokens go to `/admin/api`).
fn authorized(auth: &ApiAuth, headers: &HeaderMap, query: Option<&str>) -> bool {
    if !auth.key.is_empty() {
        if let Some(h) = headers.get(axum::http::header::AUTHORIZATION) {
            if let Ok(s) = h.to_str() {
                let mut parts = s.split_whitespace();
                if let (Some(scheme), Some(val)) = (parts.next(), parts.next()) {
                    if scheme.eq_ignore_ascii_case("apikey") && val == auth.key {
                        return true;
                    }
                }
            }
        }
        if let Some(q) = query {
            for pair in q.split('&') {
                let mut it = pair.splitn(2, '=');
                if it.next() == Some("api_key") && it.next() == Some(auth.key.as_str()) {
                    return true;
                }
            }
        }
    }
    false
}

fn parse<T: serde::de::DeserializeOwned + Default>(params: &Option<Raw>) -> Option<T> {
    match params {
        None => Some(T::default()),
        Some(r) => serde_json::from_str(r.get()).ok(),
    }
}

fn ok<T: Serialize>(id: u32, value: &T) -> ApiReply {
    ApiReply {
        id,
        error: None,
        result: serde_json::value::RawValue::from_string(
            serde_json::to_string(value).unwrap_or_else(|_| "null".into()),
        )
        .ok(),
    }
}

fn void(id: u32) -> ApiReply {
    ApiReply {
        id,
        error: None,
        result: None,
    }
}

fn err(id: u32, code: u32, message: &str) -> ApiReply {
    ApiReply {
        id,
        error: Some(ApiError {
            code,
            message: message.into(),
        }),
        result: None,
    }
}

/// Validate a channel for an API command, mirroring the Go executor: empty
/// channel → BadRequest(107), unknown namespace → NamespaceNotFound(102).
/// Returns `(presence_enabled, history_enabled)` on success.
fn channel_caps(node: &Node, id: u32, channel: &str) -> Result<(bool, bool), ApiReply> {
    if channel.is_empty() {
        return Err(err(id, 107, "bad request"));
    }
    match node.channel_options(channel) {
        Some(o) => Ok((o.presence, o.history_enabled())),
        None => Err(err(id, 102, "namespace not found")),
    }
}

async fn dispatch(node: &Arc<Node>, cmd: ApiCommand) -> ApiReply {
    let id = cmd.id;
    let params = cmd.params;
    macro_rules! req {
        ($t:ty) => {
            match parse::<$t>(&params) {
                Some(r) => r,
                None => return err(id, 107, "bad request"),
            }
        };
    }
    match cmd.method.as_str() {
        "publish" => {
            let r: PublishReq = req!(PublishReq);
            // Go: empty data -> BadRequest(107) (no default-`null` fallback).
            let data = match r.data.as_ref() {
                Some(d) => d.get().as_bytes(),
                None => return err(id, 107, "bad request"),
            };
            if let Err(e) = channel_caps(node, id, &r.channel) {
                return e;
            }
            node.publish(&r.channel, data, None).await;
            void(id)
        }
        "broadcast" => {
            let r: BroadcastReq = req!(BroadcastReq);
            if r.channels.is_empty() {
                return err(id, 107, "bad request");
            }
            let data = match r.data.as_ref() {
                Some(d) => d.get().as_bytes(),
                None => return err(id, 107, "bad request"),
            };
            // Validate every channel first (107/102); only then publish all.
            for ch in &r.channels {
                if let Err(e) = channel_caps(node, id, ch) {
                    return e;
                }
            }
            for ch in &r.channels {
                node.publish(ch, data, None).await;
            }
            void(id)
        }
        "unsubscribe" => {
            // Go Executor.Unsubscribe: user required (107); channel validated if
            // non-empty (102); empty channel = unsubscribe from all channels.
            let r: UnsubscribeReq = req!(UnsubscribeReq);
            if r.user.is_empty() {
                return err(id, 107, "bad request");
            }
            if !r.channel.is_empty() {
                if let Err(e) = channel_caps(node, id, &r.channel) {
                    return e;
                }
            }
            node.unsubscribe_user(&r.user, &r.channel).await;
            void(id)
        }
        "disconnect" => {
            // Go Executor.Disconnect: user required (107); default DisconnectForceNoReconnect.
            let r: DisconnectReq = req!(DisconnectReq);
            if r.user.is_empty() {
                return err(id, 107, "bad request");
            }
            node.disconnect_user(&r.user, 3012, "force disconnect").await;
            void(id)
        }
        "presence" => {
            let r: ChannelReq = req!(ChannelReq);
            match channel_caps(node, id, &r.channel) {
                Ok((presence, _)) if !presence => return err(id, 108, "not available"),
                Ok(_) => {}
                Err(e) => return e,
            }
            ok(
                id,
                &PresenceResult {
                    presence: node.presence(&r.channel).await,
                },
            )
        }
        "presence_stats" => {
            let r: ChannelReq = req!(ChannelReq);
            match channel_caps(node, id, &r.channel) {
                Ok((presence, _)) if !presence => return err(id, 108, "not available"),
                Ok(_) => {}
                Err(e) => return e,
            }
            let (num_clients, num_users) = node.presence_stats(&r.channel).await;
            ok(
                id,
                &PresenceStatsResult {
                    num_clients,
                    num_users,
                },
            )
        }
        "history" => {
            let r: ChannelReq = req!(ChannelReq);
            match channel_caps(node, id, &r.channel) {
                Ok((_, history)) if !history => return err(id, 108, "not available"),
                Ok(_) => {}
                Err(e) => return e,
            }
            let (pubs, _top) = node.history(&r.channel).await;
            let publications = pubs
                .into_iter()
                .map(|p| ApiPublication {
                    uid: p.uid,
                    data: p.data,
                    info: p.info,
                })
                .collect();
            ok(id, &HistoryResult { publications })
        }
        "history_remove" => {
            let r: ChannelReq = req!(ChannelReq);
            match channel_caps(node, id, &r.channel) {
                Ok((_, history)) if !history => return err(id, 108, "not available"),
                Ok(_) => {}
                Err(e) => return e,
            }
            node.remove_history(&r.channel).await;
            void(id)
        }
        "channels" => ok(
            id,
            &ChannelsResult {
                channels: node.hub().channels(),
            },
        ),
        "info" => ok(
            id,
            &InfoResult {
                nodes: vec![NodeResult {
                    uid: node.id().to_string(),
                    name: node.id().to_string(),
                    version: crate::VERSION.to_string(),
                    num_clients: node.hub().num_clients() as u32,
                    num_users: node.hub().num_users() as u32,
                    num_channels: node.hub().num_channels() as u32,
                    uptime: node.uptime(),
                    metrics: None,
                }],
            },
        ),
        _ => err(id, 104, "method not found"),
    }
}

/// Protobuf API dispatch: decode the pb request for `cmd.method`, call the Node,
/// and build a pb Reply (mirrors the JSON `dispatch` + the gRPC service). Void
/// commands carry an empty `result`.
async fn dispatch_pb(node: &Arc<Node>, cmd: pb::Command) -> pb::Reply {
    let id = cmd.id;
    let reply = |error: Option<pb::Error>, result: Vec<u8>| pb::Reply { id, error, result };
    let method = match pb::MethodType::try_from(cmd.method) {
        Ok(m) => m,
        Err(_) => return reply(Some(api_err(104, "method not found")), Vec::new()),
    };
    macro_rules! decode {
        ($t:ty) => {
            match <$t>::decode(&cmd.params[..]) {
                Ok(r) => r,
                Err(_) => return reply(Some(api_err(107, "bad request")), Vec::new()),
            }
        };
    }
    match method {
        pb::MethodType::Publish => {
            let req = decode!(pb::PublishRequest);
            if req.data.is_empty() {
                return reply(Some(api_err(107, "bad request")), Vec::new());
            }
            if let Err(e) = pb_channel_caps(node, &req.channel) {
                return reply(Some(e), Vec::new());
            }
            node.publish(&req.channel, &req.data, None).await;
            reply(None, Vec::new())
        }
        pb::MethodType::Broadcast => {
            let req = decode!(pb::BroadcastRequest);
            if req.channels.is_empty() || req.data.is_empty() {
                return reply(Some(api_err(107, "bad request")), Vec::new());
            }
            if let Some(e) = req.channels.iter().find_map(|ch| pb_channel_caps(node, ch).err()) {
                return reply(Some(e), Vec::new());
            }
            for ch in &req.channels {
                node.publish(ch, &req.data, None).await;
            }
            reply(None, Vec::new())
        }
        pb::MethodType::Unsubscribe => {
            let req = decode!(pb::UnsubscribeRequest);
            if req.user.is_empty() {
                return reply(Some(api_err(107, "bad request")), Vec::new());
            }
            if !req.channel.is_empty() {
                if let Err(e) = pb_channel_caps(node, &req.channel) {
                    return reply(Some(e), Vec::new());
                }
            }
            node.unsubscribe_user(&req.user, &req.channel).await;
            reply(None, Vec::new())
        }
        pb::MethodType::Disconnect => {
            let req = decode!(pb::DisconnectRequest);
            if req.user.is_empty() {
                return reply(Some(api_err(107, "bad request")), Vec::new());
            }
            node.disconnect_user(&req.user, 3012, "force disconnect").await;
            reply(None, Vec::new())
        }
        pb::MethodType::Presence => {
            let req = decode!(pb::PresenceRequest);
            match pb_channel_caps(node, &req.channel) {
                Ok((p, _)) if !p => return reply(Some(api_err(108, "not available")), Vec::new()),
                Ok(_) => {}
                Err(e) => return reply(Some(e), Vec::new()),
            }
            let presence = node
                .presence(&req.channel)
                .await
                .into_iter()
                .map(|(k, v)| (k, to_pb_client_info(v)))
                .collect();
            reply(None, pb::PresenceResult { presence }.encode_to_vec())
        }
        pb::MethodType::PresenceStats => {
            let req = decode!(pb::PresenceStatsRequest);
            match pb_channel_caps(node, &req.channel) {
                Ok((p, _)) if !p => return reply(Some(api_err(108, "not available")), Vec::new()),
                Ok(_) => {}
                Err(e) => return reply(Some(e), Vec::new()),
            }
            let (num_clients, num_users) = node.presence_stats(&req.channel).await;
            reply(
                None,
                pb::PresenceStatsResult {
                    num_clients,
                    num_users,
                }
                .encode_to_vec(),
            )
        }
        pb::MethodType::History => {
            let req = decode!(pb::HistoryRequest);
            match pb_channel_caps(node, &req.channel) {
                Ok((_, h)) if !h => return reply(Some(api_err(108, "not available")), Vec::new()),
                Ok(_) => {}
                Err(e) => return reply(Some(e), Vec::new()),
            }
            let (pubs, _top) = node.history(&req.channel).await;
            let publications = pubs.into_iter().map(to_pb_publication).collect();
            reply(None, pb::HistoryResult { publications }.encode_to_vec())
        }
        pb::MethodType::HistoryRemove => {
            let req = decode!(pb::HistoryRemoveRequest);
            match pb_channel_caps(node, &req.channel) {
                Ok((_, h)) if !h => return reply(Some(api_err(108, "not available")), Vec::new()),
                Ok(_) => {}
                Err(e) => return reply(Some(e), Vec::new()),
            }
            node.remove_history(&req.channel).await;
            reply(None, Vec::new())
        }
        pb::MethodType::Channels => reply(
            None,
            pb::ChannelsResult {
                channels: node.hub().channels(),
            }
            .encode_to_vec(),
        ),
        pb::MethodType::Info => {
            let hub = node.hub();
            let nodes = vec![pb::NodeResult {
                uid: node.id().to_string(),
                name: node.id().to_string(),
                version: crate::VERSION.to_string(),
                num_clients: hub.num_clients() as u32,
                num_users: hub.num_users() as u32,
                num_channels: hub.num_channels() as u32,
                uptime: node.uptime(),
                metrics: None,
            }];
            reply(None, pb::InfoResult { nodes }.encode_to_vec())
        }
        // Go Executor.RPC: empty method -> BadRequest(107); else (no stock handler
        // registered) -> MethodNotFound(104). Never NotAvailable(108).
        pb::MethodType::Rpc => {
            let req = decode!(pb::RpcRequest);
            if req.method.is_empty() {
                return reply(Some(api_err(107, "bad request")), Vec::new());
            }
            reply(Some(api_err(104, "method not found")), Vec::new())
        }
    }
}
