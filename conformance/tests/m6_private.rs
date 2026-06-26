//! M6.3: private channels (`$`-prefixed) require a subscription token whose
//! client + channel match. Missing/invalid/mismatched -> PermissionDenied(103),
//! expired -> TokenExpired(109). Golden-checked vs the Go oracle.

use conformance::oracle::Oracle;
use conformance::{key_shape, Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

const CFG: &str = r#"{"client_insecure":true,"token_hmac_secret_key":"secret"}"#;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn sub_token(client: &str, channel: &str, exp: Option<i64>) -> String {
    let mut claims = serde_json::json!({"client": client, "channel": channel});
    if let Some(e) = exp {
        claims["exp"] = e.into();
    }
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"secret"),
    )
    .unwrap()
}

#[tokio::test]
async fn private_channel_valid_token_subscribes() {
    let s = Server::start_with_config(CFG).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let id = c.connect_command().await;
    let token = sub_token(&id, "$secret", None);
    let sub = c.subscribe_token(2, "$secret", &token).await;
    assert!(sub.get("error").is_none(), "sub error: {sub}");
}

#[tokio::test]
async fn private_channel_missing_token_denied() {
    let s = Server::start_with_config(CFG).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    let sub = c.subscribe(2, "$secret").await; // no token
    assert_eq!(sub["error"]["code"], 103, "sub: {sub}");
}

#[tokio::test]
async fn private_channel_wrong_channel_denied() {
    let s = Server::start_with_config(CFG).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let id = c.connect_command().await;
    let token = sub_token(&id, "$other", None); // token for a different channel
    let sub = c.subscribe_token(2, "$secret", &token).await;
    assert_eq!(sub["error"]["code"], 103, "sub: {sub}");
}

#[tokio::test]
async fn private_channel_expired_token() {
    let s = Server::start_with_config(CFG).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let id = c.connect_command().await;
    let token = sub_token(&id, "$secret", Some(now() - 100));
    let sub = c.subscribe_token(2, "$secret", &token).await;
    assert_eq!(sub["error"]["code"], 109, "sub: {sub}");
}

#[tokio::test]
async fn private_channel_matches_go() {
    let Some(go) = Oracle::start_with_config(CFG).await else {
        return;
    };
    let rust = Server::start_with_config(CFG).await;
    assert_eq!(
        key_shape(&private_subscribe(&go.ws_url()).await),
        key_shape(&private_subscribe(&rust.ws_url()).await),
        "private subscribe reply shape differs"
    );
}

async fn private_subscribe(url: &str) -> serde_json::Value {
    let mut c = WsJsonClient::connect(url).await;
    let id = c.connect_command().await;
    let token = sub_token(&id, "$secret", None);
    c.subscribe_token(2, "$secret", &token).await
}
