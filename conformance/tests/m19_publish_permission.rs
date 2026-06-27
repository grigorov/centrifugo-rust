//! Client publish permission (Go OnPublish): a non-insecure client may only
//! publish to a channel whose `publish` option is enabled, else
//! PermissionDenied(103). (Insecure mode bypasses, which is why the other
//! suites — all insecure — are unaffected.) Golden-checked vs the Go oracle.

use conformance::oracle::Oracle;
use conformance::{Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

fn token() -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({ "sub": "u" }),
        &EncodingKey::from_secret(b"secret"),
    )
    .unwrap()
}

async fn publish_code(ws_url: &str) -> u64 {
    let mut c = WsJsonClient::connect(ws_url).await;
    assert!(c.connect_with_token(&token()).await["error"].is_null());
    let r = c.publish(2, "room", r#"{"x":1}"#).await;
    r["error"]["code"].as_u64().unwrap_or(0)
}

#[tokio::test]
async fn client_publish_denied_without_option() {
    let s = Server::start_with_config(r#"{"token_hmac_secret_key":"secret"}"#).await;
    assert_eq!(publish_code(&s.ws_url()).await, 103, "publish must be denied");
}

#[tokio::test]
async fn client_publish_allowed_with_option() {
    let s = Server::start_with_config(r#"{"token_hmac_secret_key":"secret","publish":true}"#).await;
    assert_eq!(publish_code(&s.ws_url()).await, 0, "publish must be allowed");
}

#[tokio::test]
async fn publish_permission_matches_go() {
    let cfg = r#"{"token_hmac_secret_key":"secret"}"#;
    let Some(go) = Oracle::start_with_config(cfg).await else {
        return;
    };
    let rust = Server::start_with_config(cfg).await;
    assert_eq!(
        publish_code(&go.ws_url()).await,
        publish_code(&rust.ws_url()).await,
        "publish-denied code must match Go"
    );
}
