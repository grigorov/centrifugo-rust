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
use centrifugo_protocol::messages::{
    ClientInfo as DomainClientInfo, Publication as DomainPublication,
};
use tonic::{Request, Response, Status};

pub struct GrpcApi {
    node: Arc<Node>,
}

impl GrpcApi {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }
}

pub(crate) fn to_pb_client_info(ci: DomainClientInfo) -> pb::ClientInfo {
    pb::ClientInfo {
        user: ci.user,
        client: ci.client,
        conn_info: ci.conn_info.map(|r| r.into_bytes()).unwrap_or_default(),
        chan_info: ci.chan_info.map(|r| r.into_bytes()).unwrap_or_default(),
    }
}

pub(crate) fn to_pb_publication(p: DomainPublication) -> pb::Publication {
    pb::Publication {
        uid: p.uid,
        data: p.data.map(|r| r.into_bytes()).unwrap_or_default(),
        info: p.info.map(to_pb_client_info),
    }
}

pub(crate) fn api_err(code: u32, message: &str) -> pb::Error {
    pb::Error {
        code,
        message: message.into(),
    }
}

/// Validate a channel for an API command (Go executor parity): empty channel →
/// BadRequest(107), unknown namespace → NamespaceNotFound(102). Returns
/// `(presence_enabled, history_enabled)` on success.
pub(crate) fn channel_caps(node: &Node, channel: &str) -> Result<(bool, bool), pb::Error> {
    if channel.is_empty() {
        return Err(api_err(107, "bad request"));
    }
    match node.channel_options(channel) {
        Some(o) => Ok((o.presence, o.history_enabled())),
        None => Err(api_err(102, "namespace not found")),
    }
}

#[tonic::async_trait]
impl Centrifugo for GrpcApi {
    async fn publish(
        &self,
        request: Request<pb::PublishRequest>,
    ) -> Result<Response<pb::PublishResponse>, Status> {
        let req = request.into_inner();
        if req.data.is_empty() {
            return Ok(Response::new(pb::PublishResponse {
                error: Some(api_err(107, "bad request")),
                result: None,
            }));
        }
        if let Err(e) = channel_caps(&self.node, &req.channel) {
            return Ok(Response::new(pb::PublishResponse {
                error: Some(e),
                result: None,
            }));
        }
        self.node.publish(&req.channel, &req.data, None).await;
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
        let error = if req.channels.is_empty() || req.data.is_empty() {
            Some(api_err(107, "bad request"))
        } else {
            req.channels
                .iter()
                .find_map(|ch| channel_caps(&self.node, ch).err())
        };
        if error.is_none() {
            for ch in &req.channels {
                self.node.publish(ch, &req.data, None).await;
            }
        }
        Ok(Response::new(pb::BroadcastResponse {
            error,
            result: None,
        }))
    }

    async fn unsubscribe(
        &self,
        request: Request<pb::UnsubscribeRequest>,
    ) -> Result<Response<pb::UnsubscribeResponse>, Status> {
        let req = request.into_inner();
        let error = if req.user.is_empty() {
            Some(api_err(107, "bad request"))
        } else if !req.channel.is_empty() {
            channel_caps(&self.node, &req.channel).err()
        } else {
            None
        };
        if error.is_none() {
            self.node.unsubscribe_user(&req.user, &req.channel).await;
        }
        Ok(Response::new(pb::UnsubscribeResponse {
            error,
            result: None,
        }))
    }

    async fn disconnect(
        &self,
        request: Request<pb::DisconnectRequest>,
    ) -> Result<Response<pb::DisconnectResponse>, Status> {
        let req = request.into_inner();
        let error = if req.user.is_empty() {
            Some(api_err(107, "bad request"))
        } else {
            None
        };
        if error.is_none() {
            self.node
                .disconnect_user(&req.user, 3012, "force disconnect")
                .await;
        }
        Ok(Response::new(pb::DisconnectResponse {
            error,
            result: None,
        }))
    }

    async fn presence(
        &self,
        request: Request<pb::PresenceRequest>,
    ) -> Result<Response<pb::PresenceResponse>, Status> {
        let req = request.into_inner();
        match channel_caps(&self.node, &req.channel) {
            Ok((presence, _)) if !presence => {
                return Ok(Response::new(pb::PresenceResponse {
                    error: Some(api_err(108, "not available")),
                    result: None,
                }));
            }
            Ok(_) => {}
            Err(e) => {
                return Ok(Response::new(pb::PresenceResponse {
                    error: Some(e),
                    result: None,
                }));
            }
        }
        let presence = self
            .node
            .presence(&req.channel)
            .await
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
        match channel_caps(&self.node, &req.channel) {
            Ok((presence, _)) if !presence => {
                return Ok(Response::new(pb::PresenceStatsResponse {
                    error: Some(api_err(108, "not available")),
                    result: None,
                }));
            }
            Ok(_) => {}
            Err(e) => {
                return Ok(Response::new(pb::PresenceStatsResponse {
                    error: Some(e),
                    result: None,
                }));
            }
        }
        let (num_clients, num_users) = self.node.presence_stats(&req.channel).await;
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
        match channel_caps(&self.node, &req.channel) {
            Ok((_, history)) if !history => {
                return Ok(Response::new(pb::HistoryResponse {
                    error: Some(api_err(108, "not available")),
                    result: None,
                }));
            }
            Ok(_) => {}
            Err(e) => {
                return Ok(Response::new(pb::HistoryResponse {
                    error: Some(e),
                    result: None,
                }));
            }
        }
        let (pubs, _top) = self.node.history(&req.channel).await;
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
        match channel_caps(&self.node, &req.channel) {
            Ok((_, history)) if !history => {
                return Ok(Response::new(pb::HistoryRemoveResponse {
                    error: Some(api_err(108, "not available")),
                    result: None,
                }));
            }
            Ok(_) => {}
            Err(e) => {
                return Ok(Response::new(pb::HistoryRemoveResponse {
                    error: Some(e),
                    result: None,
                }));
            }
        }
        self.node.remove_history(&req.channel).await;
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
        let nodes = self
            .node
            .info_nodes()
            .into_iter()
            .map(|n| pb::NodeResult {
                uid: n.uid,
                name: n.name,
                version: n.version,
                num_clients: n.num_clients,
                num_users: n.num_users,
                num_channels: n.num_channels,
                uptime: n.uptime,
                metrics: None,
            })
            .collect();
        Ok(Response::new(pb::InfoResponse {
            error: None,
            result: Some(pb::InfoResult { nodes }),
        }))
    }

    async fn rpc(
        &self,
        request: Request<pb::RpcRequest>,
    ) -> Result<Response<pb::RpcResponse>, Status> {
        // Go Executor.RPC: empty method -> BadRequest(107); else (no stock RPC
        // handler registered) -> MethodNotFound(104). Never NotAvailable(108).
        let (code, message) = if request.into_inner().method.is_empty() {
            (107, "bad request")
        } else {
            (104, "method not found")
        };
        Ok(Response::new(pb::RpcResponse {
            error: Some(pb::Error {
                code,
                message: message.into(),
            }),
            result: None,
        }))
    }
}

/// `apikey <KEY>` (case-insensitive scheme) — matches the HTTP API's header form.
fn check_apikey(header: &str, key: &str) -> bool {
    // Go compares the full metadata value against `"apikey " + key` exactly
    // (case-sensitive scheme, single space) — not a lenient scheme split.
    header.as_bytes() == format!("apikey {key}").as_bytes()
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
            // Match Go's grpc-message exactly (status code already Unauthenticated).
            Err(Status::unauthenticated("unauthenticated"))
        }
    };
    let svc = CentrifugoServer::with_interceptor(GrpcApi::new(node), interceptor);
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;
    Ok(())
}
