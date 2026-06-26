//! gRPC API — the `api.Centrifugo` service (port 10000 by default). Mirrors the
//! HTTP `POST /api` surface, reusing [`Node`]. Auth via `authorization: apikey
//! <KEY>` request metadata (enforced only when `grpc_api_key` is non-empty, like
//! Go). Void RPCs (publish/broadcast/unsubscribe/disconnect/history_remove)
//! return an empty result, matching centrifugo v2.8.6.

use std::net::SocketAddr;
use std::sync::Arc;

use centrifugo_core::Node;
use centrifugo_grpc::pb;
use centrifugo_grpc::pb::centrifugo_server::{Centrifugo, CentrifugoServer};
use centrifugo_protocol::messages::{ClientInfo as DomainClientInfo, Publication as DomainPublication};
use tonic::{Request, Response, Status};

use crate::VERSION;

pub struct GrpcApi {
    node: Arc<Node>,
}

impl GrpcApi {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }
}

fn to_pb_client_info(ci: DomainClientInfo) -> pb::ClientInfo {
    pb::ClientInfo {
        user: ci.user,
        client: ci.client,
        conn_info: ci.conn_info.map(|r| r.into_bytes()).unwrap_or_default(),
        chan_info: ci.chan_info.map(|r| r.into_bytes()).unwrap_or_default(),
    }
}

fn to_pb_publication(p: DomainPublication) -> pb::Publication {
    pb::Publication {
        uid: p.uid,
        data: p.data.map(|r| r.into_bytes()).unwrap_or_default(),
        info: p.info.map(to_pb_client_info),
    }
}

/// Data bytes for publish/broadcast; an empty payload publishes the JSON `null`,
/// matching the HTTP API's missing-`data` behavior.
fn pub_data(data: Vec<u8>) -> Vec<u8> {
    if data.is_empty() {
        b"null".to_vec()
    } else {
        data
    }
}

#[tonic::async_trait]
impl Centrifugo for GrpcApi {
    async fn publish(
        &self,
        request: Request<pb::PublishRequest>,
    ) -> Result<Response<pb::PublishResponse>, Status> {
        let req = request.into_inner();
        self.node.publish(&req.channel, &pub_data(req.data), None);
        Ok(Response::new(pb::PublishResponse {
            error: None,
            result: None,
        }))
    }

    async fn broadcast(
        &self,
        request: Request<pb::BroadcastRequest>,
    ) -> Result<Response<pb::BroadcastResponse>, Status> {
        let req = request.into_inner();
        let data = pub_data(req.data);
        for ch in &req.channels {
            self.node.publish(ch, &data, None);
        }
        Ok(Response::new(pb::BroadcastResponse {
            error: None,
            result: None,
        }))
    }

    async fn unsubscribe(
        &self,
        _request: Request<pb::UnsubscribeRequest>,
    ) -> Result<Response<pb::UnsubscribeResponse>, Status> {
        // Server-initiated unsubscribe is deferred (needs cross-node client
        // targeting); ack as a no-op for surface parity.
        Ok(Response::new(pb::UnsubscribeResponse {
            error: None,
            result: None,
        }))
    }

    async fn disconnect(
        &self,
        _request: Request<pb::DisconnectRequest>,
    ) -> Result<Response<pb::DisconnectResponse>, Status> {
        // Server-initiated disconnect is deferred; ack as a no-op.
        Ok(Response::new(pb::DisconnectResponse {
            error: None,
            result: None,
        }))
    }

    async fn presence(
        &self,
        request: Request<pb::PresenceRequest>,
    ) -> Result<Response<pb::PresenceResponse>, Status> {
        let req = request.into_inner();
        let presence = self
            .node
            .presence(&req.channel)
            .into_iter()
            .map(|(k, v)| (k, to_pb_client_info(v)))
            .collect();
        Ok(Response::new(pb::PresenceResponse {
            error: None,
            result: Some(pb::PresenceResult { presence }),
        }))
    }

    async fn presence_stats(
        &self,
        request: Request<pb::PresenceStatsRequest>,
    ) -> Result<Response<pb::PresenceStatsResponse>, Status> {
        let req = request.into_inner();
        let (num_clients, num_users) = self.node.presence_stats(&req.channel);
        Ok(Response::new(pb::PresenceStatsResponse {
            error: None,
            result: Some(pb::PresenceStatsResult {
                num_clients,
                num_users,
            }),
        }))
    }

    async fn history(
        &self,
        request: Request<pb::HistoryRequest>,
    ) -> Result<Response<pb::HistoryResponse>, Status> {
        let req = request.into_inner();
        let (pubs, _top) = self.node.history(&req.channel);
        let publications = pubs.into_iter().map(to_pb_publication).collect();
        Ok(Response::new(pb::HistoryResponse {
            error: None,
            result: Some(pb::HistoryResult { publications }),
        }))
    }

    async fn history_remove(
        &self,
        request: Request<pb::HistoryRemoveRequest>,
    ) -> Result<Response<pb::HistoryRemoveResponse>, Status> {
        let req = request.into_inner();
        self.node.remove_history(&req.channel);
        Ok(Response::new(pb::HistoryRemoveResponse {
            error: None,
            result: None,
        }))
    }

    async fn channels(
        &self,
        _request: Request<pb::ChannelsRequest>,
    ) -> Result<Response<pb::ChannelsResponse>, Status> {
        Ok(Response::new(pb::ChannelsResponse {
            error: None,
            result: Some(pb::ChannelsResult {
                channels: self.node.hub().channels(),
            }),
        }))
    }

    async fn info(
        &self,
        _request: Request<pb::InfoRequest>,
    ) -> Result<Response<pb::InfoResponse>, Status> {
        let hub = self.node.hub();
        Ok(Response::new(pb::InfoResponse {
            error: None,
            result: Some(pb::InfoResult {
                nodes: vec![pb::NodeResult {
                    uid: String::new(),
                    name: String::new(),
                    version: VERSION.to_string(),
                    num_clients: hub.num_clients() as u32,
                    num_users: hub.num_users() as u32,
                    num_channels: hub.num_channels() as u32,
                    uptime: 0,
                    metrics: None,
                }],
            }),
        }))
    }

    async fn rpc(
        &self,
        _request: Request<pb::RpcRequest>,
    ) -> Result<Response<pb::RpcResponse>, Status> {
        // No server-side RPC handler registered → not available (108).
        Ok(Response::new(pb::RpcResponse {
            error: Some(pb::Error {
                code: 108,
                message: "not available".into(),
            }),
            result: None,
        }))
    }
}

/// `apikey <KEY>` (case-insensitive scheme) — matches the HTTP API's header form.
fn check_apikey(header: &str, key: &str) -> bool {
    let mut parts = header.split_whitespace();
    matches!(
        (parts.next(), parts.next()),
        (Some(scheme), Some(val)) if scheme.eq_ignore_ascii_case("apikey") && val == key
    )
}

/// Serve the gRPC API on `addr`. When `api_key` is non-empty, every call must
/// carry `authorization: apikey <api_key>` metadata or it is rejected with
/// `Unauthenticated`; an empty key leaves the API open (matches Go).
// `tonic::Status` (the interceptor's mandated Err type) is large by design; we
// cannot box it without breaking the Interceptor signature.
#[allow(clippy::result_large_err)]
pub async fn serve(node: Arc<Node>, addr: SocketAddr, api_key: String) -> anyhow::Result<()> {
    let interceptor = move |req: Request<()>| -> Result<Request<()>, Status> {
        if api_key.is_empty() {
            return Ok(req);
        }
        let ok = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|h| check_apikey(h, &api_key));
        if ok {
            Ok(req)
        } else {
            Err(Status::unauthenticated("unauthorized"))
        }
    };
    let svc = CentrifugoServer::with_interceptor(GrpcApi::new(node), interceptor);
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;
    Ok(())
}
