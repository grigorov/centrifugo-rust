//! Conversions between the serde domain types (JSON repr, `Raw` byte fields) and
//! the prost `pb` types (protobuf repr, `Vec<u8>` byte fields).
//!
//! Lossy only on fields unused in this protocol era (deprecated `seq`/`gen`,
//! server-side `subs` maps) — set to defaults in the protobuf direction and
//! ignored in the domain direction.

use crate::messages::{
    ClientInfo, ConnectRequest, ConnectResult, HistoryRequest, HistoryResult, Join, Leave,
    PresenceRequest, PresenceResult, PresenceStatsRequest, PresenceStatsResult, Publication,
    PublishRequest, PublishResult, RefreshRequest, RefreshResult, RpcRequest, RpcResult,
    SubRefreshRequest, SubRefreshResult, SubscribeRequest, SubscribeResult, UnsubscribeRequest,
    UnsubscribeResult,
};
use crate::raw::Raw;
use crate::{pb, Command, Error, Push, Reply};

pub(crate) fn raw_to_vec(r: Option<Raw>) -> Vec<u8> {
    r.map(Raw::into_bytes).unwrap_or_default()
}

pub(crate) fn vec_to_raw(v: Vec<u8>) -> Option<Raw> {
    if v.is_empty() {
        None
    } else {
        Some(Raw(v))
    }
}

// ---- Error ----

impl From<Error> for pb::Error {
    fn from(e: Error) -> Self {
        pb::Error {
            code: e.code,
            message: e.message,
        }
    }
}
impl From<pb::Error> for Error {
    fn from(e: pb::Error) -> Self {
        Error {
            code: e.code,
            message: e.message,
        }
    }
}

// ---- Command / Reply / Push envelopes ----

impl From<Command> for pb::Command {
    fn from(c: Command) -> Self {
        pb::Command {
            id: c.id,
            method: c.method as i32,
            params: raw_to_vec(c.params),
        }
    }
}

impl From<Reply> for pb::Reply {
    fn from(r: Reply) -> Self {
        pb::Reply {
            id: r.id,
            error: r.error.map(Into::into),
            result: raw_to_vec(r.result),
        }
    }
}
impl From<pb::Reply> for Reply {
    fn from(r: pb::Reply) -> Self {
        Reply {
            id: r.id,
            error: r.error.map(Into::into),
            result: vec_to_raw(r.result),
        }
    }
}

impl From<Push> for pb::Push {
    fn from(p: Push) -> Self {
        pb::Push {
            r#type: p.r#type as i32,
            channel: p.channel,
            data: raw_to_vec(p.data),
        }
    }
}

// ---- ClientInfo ----

impl From<ClientInfo> for pb::ClientInfo {
    fn from(c: ClientInfo) -> Self {
        pb::ClientInfo {
            user: c.user,
            client: c.client,
            conn_info: raw_to_vec(c.conn_info),
            chan_info: raw_to_vec(c.chan_info),
        }
    }
}
impl From<pb::ClientInfo> for ClientInfo {
    fn from(c: pb::ClientInfo) -> Self {
        ClientInfo {
            user: c.user,
            client: c.client,
            conn_info: vec_to_raw(c.conn_info),
            chan_info: vec_to_raw(c.chan_info),
        }
    }
}

// ---- Publication ----

impl From<Publication> for pb::Publication {
    fn from(p: Publication) -> Self {
        pb::Publication {
            seq: p.seq,
            gen: p.gen,
            uid: p.uid,
            data: raw_to_vec(p.data),
            info: p.info.map(Into::into),
            offset: p.offset,
        }
    }
}
impl From<pb::Publication> for Publication {
    fn from(p: pb::Publication) -> Self {
        Publication {
            seq: p.seq,
            gen: p.gen,
            uid: p.uid,
            data: vec_to_raw(p.data),
            info: p.info.map(Into::into),
            offset: p.offset,
        }
    }
}

// ---- Connect ----

impl From<pb::ConnectRequest> for ConnectRequest {
    fn from(r: pb::ConnectRequest) -> Self {
        ConnectRequest {
            token: r.token,
            data: vec_to_raw(r.data),
            name: r.name,
            version: r.version,
            subs: r.subs.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}
impl From<ConnectRequest> for pb::ConnectRequest {
    fn from(r: ConnectRequest) -> Self {
        pb::ConnectRequest {
            token: r.token,
            data: raw_to_vec(r.data),
            subs: r.subs.into_iter().map(|(k, v)| (k, v.into())).collect(),
            name: r.name,
            version: r.version,
        }
    }
}

impl From<ConnectResult> for pb::ConnectResult {
    fn from(r: ConnectResult) -> Self {
        pb::ConnectResult {
            client: r.client,
            version: r.version,
            expires: r.expires,
            ttl: r.ttl,
            data: raw_to_vec(r.data),
            subs: r.subs.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}
impl From<pb::ConnectResult> for ConnectResult {
    fn from(r: pb::ConnectResult) -> Self {
        ConnectResult {
            client: r.client,
            version: r.version,
            expires: r.expires,
            ttl: r.ttl,
            data: vec_to_raw(r.data),
            subs: r.subs.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

// ---- Subscribe ----

impl From<pb::SubscribeRequest> for SubscribeRequest {
    fn from(r: pb::SubscribeRequest) -> Self {
        SubscribeRequest {
            channel: r.channel,
            token: r.token,
            recover: r.recover,
            seq: r.seq,
            gen: r.gen,
            epoch: r.epoch,
            offset: r.offset,
        }
    }
}
impl From<SubscribeRequest> for pb::SubscribeRequest {
    fn from(r: SubscribeRequest) -> Self {
        pb::SubscribeRequest {
            channel: r.channel,
            token: r.token,
            recover: r.recover,
            seq: r.seq,
            gen: r.gen,
            epoch: r.epoch,
            offset: r.offset,
        }
    }
}

impl From<SubscribeResult> for pb::SubscribeResult {
    fn from(r: SubscribeResult) -> Self {
        pb::SubscribeResult {
            expires: r.expires,
            ttl: r.ttl,
            recoverable: r.recoverable,
            seq: r.seq,
            gen: r.gen,
            epoch: r.epoch,
            publications: r.publications.into_iter().map(Into::into).collect(),
            recovered: r.recovered,
            offset: r.offset,
        }
    }
}
impl From<pb::SubscribeResult> for SubscribeResult {
    fn from(r: pb::SubscribeResult) -> Self {
        SubscribeResult {
            expires: r.expires,
            ttl: r.ttl,
            recoverable: r.recoverable,
            seq: r.seq,
            gen: r.gen,
            epoch: r.epoch,
            publications: r.publications.into_iter().map(Into::into).collect(),
            recovered: r.recovered,
            offset: r.offset,
        }
    }
}

// ---- Presence ----

impl From<pb::PresenceRequest> for PresenceRequest {
    fn from(r: pb::PresenceRequest) -> Self {
        PresenceRequest { channel: r.channel }
    }
}
impl From<PresenceRequest> for pb::PresenceRequest {
    fn from(r: PresenceRequest) -> Self {
        pb::PresenceRequest { channel: r.channel }
    }
}

impl From<PresenceResult> for pb::PresenceResult {
    fn from(r: PresenceResult) -> Self {
        pb::PresenceResult {
            presence: r.presence.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}
impl From<pb::PresenceResult> for PresenceResult {
    fn from(r: pb::PresenceResult) -> Self {
        PresenceResult {
            presence: r.presence.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl From<pb::PresenceStatsRequest> for PresenceStatsRequest {
    fn from(r: pb::PresenceStatsRequest) -> Self {
        PresenceStatsRequest { channel: r.channel }
    }
}
impl From<PresenceStatsRequest> for pb::PresenceStatsRequest {
    fn from(r: PresenceStatsRequest) -> Self {
        pb::PresenceStatsRequest { channel: r.channel }
    }
}

impl From<PresenceStatsResult> for pb::PresenceStatsResult {
    fn from(r: PresenceStatsResult) -> Self {
        pb::PresenceStatsResult {
            num_clients: r.num_clients,
            num_users: r.num_users,
        }
    }
}
impl From<pb::PresenceStatsResult> for PresenceStatsResult {
    fn from(r: pb::PresenceStatsResult) -> Self {
        PresenceStatsResult {
            num_clients: r.num_clients,
            num_users: r.num_users,
        }
    }
}

// ---- History ----

impl From<pb::HistoryRequest> for HistoryRequest {
    fn from(r: pb::HistoryRequest) -> Self {
        HistoryRequest { channel: r.channel }
    }
}
impl From<HistoryRequest> for pb::HistoryRequest {
    fn from(r: HistoryRequest) -> Self {
        pb::HistoryRequest { channel: r.channel }
    }
}

impl From<HistoryResult> for pb::HistoryResult {
    fn from(r: HistoryResult) -> Self {
        pb::HistoryResult {
            publications: r.publications.into_iter().map(Into::into).collect(),
        }
    }
}
impl From<pb::HistoryResult> for HistoryResult {
    fn from(r: pb::HistoryResult) -> Self {
        HistoryResult {
            publications: r.publications.into_iter().map(Into::into).collect(),
        }
    }
}

// ---- Join / Leave ----

impl From<Join> for pb::Join {
    fn from(j: Join) -> Self {
        pb::Join {
            info: Some(j.info.into()),
        }
    }
}
impl From<pb::Join> for Join {
    fn from(j: pb::Join) -> Self {
        Join {
            info: j.info.map(Into::into).unwrap_or_default(),
        }
    }
}
impl From<Leave> for pb::Leave {
    fn from(l: Leave) -> Self {
        pb::Leave {
            info: Some(l.info.into()),
        }
    }
}
impl From<pb::Leave> for Leave {
    fn from(l: pb::Leave) -> Self {
        Leave {
            info: l.info.map(Into::into).unwrap_or_default(),
        }
    }
}

// ---- Refresh ----

impl From<pb::RefreshRequest> for RefreshRequest {
    fn from(r: pb::RefreshRequest) -> Self {
        RefreshRequest { token: r.token }
    }
}
impl From<RefreshRequest> for pb::RefreshRequest {
    fn from(r: RefreshRequest) -> Self {
        pb::RefreshRequest { token: r.token }
    }
}

impl From<RefreshResult> for pb::RefreshResult {
    fn from(r: RefreshResult) -> Self {
        pb::RefreshResult {
            client: r.client,
            version: r.version,
            expires: r.expires,
            ttl: r.ttl,
        }
    }
}
impl From<pb::RefreshResult> for RefreshResult {
    fn from(r: pb::RefreshResult) -> Self {
        RefreshResult {
            client: r.client,
            version: r.version,
            expires: r.expires,
            ttl: r.ttl,
        }
    }
}

impl From<pb::SubRefreshRequest> for SubRefreshRequest {
    fn from(r: pb::SubRefreshRequest) -> Self {
        SubRefreshRequest {
            channel: r.channel,
            token: r.token,
        }
    }
}
impl From<SubRefreshRequest> for pb::SubRefreshRequest {
    fn from(r: SubRefreshRequest) -> Self {
        pb::SubRefreshRequest {
            channel: r.channel,
            token: r.token,
        }
    }
}
impl From<SubRefreshResult> for pb::SubRefreshResult {
    fn from(r: SubRefreshResult) -> Self {
        pb::SubRefreshResult {
            expires: r.expires,
            ttl: r.ttl,
        }
    }
}
impl From<pb::SubRefreshResult> for SubRefreshResult {
    fn from(r: pb::SubRefreshResult) -> Self {
        SubRefreshResult {
            expires: r.expires,
            ttl: r.ttl,
        }
    }
}

// ---- Publish / Unsubscribe / Ping ----

impl From<pb::PublishRequest> for PublishRequest {
    fn from(r: pb::PublishRequest) -> Self {
        PublishRequest {
            channel: r.channel,
            data: vec_to_raw(r.data),
        }
    }
}
impl From<PublishRequest> for pb::PublishRequest {
    fn from(r: PublishRequest) -> Self {
        pb::PublishRequest {
            channel: r.channel,
            data: raw_to_vec(r.data),
        }
    }
}

impl From<PublishResult> for pb::PublishResult {
    fn from(_: PublishResult) -> Self {
        pb::PublishResult {}
    }
}

impl From<pb::RpcRequest> for RpcRequest {
    fn from(r: pb::RpcRequest) -> Self {
        RpcRequest {
            method: r.method,
            data: vec_to_raw(r.data),
        }
    }
}
impl From<RpcRequest> for pb::RpcRequest {
    fn from(r: RpcRequest) -> Self {
        pb::RpcRequest {
            method: r.method,
            data: raw_to_vec(r.data),
        }
    }
}
impl From<RpcResult> for pb::RpcResult {
    fn from(r: RpcResult) -> Self {
        pb::RpcResult {
            data: raw_to_vec(r.data),
        }
    }
}
impl From<pb::RpcResult> for RpcResult {
    fn from(r: pb::RpcResult) -> Self {
        RpcResult {
            data: vec_to_raw(r.data),
        }
    }
}
impl From<pb::PublishResult> for PublishResult {
    fn from(_: pb::PublishResult) -> Self {
        PublishResult {}
    }
}

impl From<pb::UnsubscribeRequest> for UnsubscribeRequest {
    fn from(r: pb::UnsubscribeRequest) -> Self {
        UnsubscribeRequest { channel: r.channel }
    }
}
impl From<UnsubscribeRequest> for pb::UnsubscribeRequest {
    fn from(r: UnsubscribeRequest) -> Self {
        pb::UnsubscribeRequest { channel: r.channel }
    }
}

impl From<UnsubscribeResult> for pb::UnsubscribeResult {
    fn from(_: UnsubscribeResult) -> Self {
        pb::UnsubscribeResult {}
    }
}
impl From<pb::UnsubscribeResult> for UnsubscribeResult {
    fn from(_: pb::UnsubscribeResult) -> Self {
        UnsubscribeResult {}
    }
}

impl From<crate::messages::PingResult> for pb::PingResult {
    fn from(_: crate::messages::PingResult) -> Self {
        pb::PingResult {}
    }
}
impl From<pb::PingResult> for crate::messages::PingResult {
    fn from(_: pb::PingResult) -> Self {
        crate::messages::PingResult {}
    }
}
