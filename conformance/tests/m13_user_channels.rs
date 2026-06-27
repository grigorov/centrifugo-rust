//! Phase 1a: user-limited channels (`name#u1,u2`). Only the users listed after
//! `#` may subscribe; others get PermissionDenied(103). Identity comes from the
//! connection JWT (`sub`), so this runs in token mode. Includes a golden diff of
//! the allow/deny outcome vs the Go oracle.

use conformance::oracle::Oracle;
use conformance::{Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

const SECRET: &str = "secret";
const CFG: &str = r#"{"token_hmac_secret_key":"secret"}"#;

fn token(user: &str) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &serde_json::json!({ "sub": user }),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

/// Subscribe `user` to `channel` on `ws_url`; return the subscribe reply error
/// code (0 = success).
async fn sub_error_code(ws_url: &str, user: &str, channel: &str) -> u64 {
    let mut c = WsJsonClient::connect(ws_url).await;
    assert!(c.connect_with_token(&token(user)).await["error"].is_null());
    let r = c.subscribe(2, channel).await;
    r["error"]["code"].as_u64().unwrap_or(0)
}

#[tokio::test]
async fn listed_user_subscribes_others_denied() {
    let s = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    // Listed users may subscribe.
    assert_eq!(
        sub_error_code(&s.ws_url(), "alice", "dialog#alice,bob").await,
        0
    );
    assert_eq!(
        sub_error_code(&s.ws_url(), "bob", "dialog#alice,bob").await,
        0
    );
    // An unlisted user is denied (103).
    assert_eq!(
        sub_error_code(&s.ws_url(), "carol", "dialog#alice,bob").await,
        103
    );
    // Single-user channel.
    assert_eq!(sub_error_code(&s.ws_url(), "42", "personal#42").await, 0);
    assert_eq!(sub_error_code(&s.ws_url(), "99", "personal#42").await, 103);
}

#[tokio::test]
async fn user_channel_outcome_matches_go() {
    let Some(go) = Oracle::start_with_config(CFG).await else {
        return;
    };
    let rust = Server::start_with(&["--token_hmac_secret_key", SECRET]).await;
    let ch = "dialog#alice,bob";
    for (user, who) in [("alice", "listed"), ("carol", "unlisted")] {
        let go_code = sub_error_code(&go.ws_url(), user, ch).await;
        let rust_code = sub_error_code(&rust.ws_url(), user, ch).await;
        assert_eq!(
            go_code, rust_code,
            "{who} user {user}: go={go_code} rust={rust_code}"
        );
    }
}
