//! M10 (JWKS): the server fetches a JWKS endpoint at startup and verifies a
//! connection token by its `kid`. A mock HTTP server serves an `oct` JWKS (fast;
//! the kid-selection path is identical for RSA/ECDSA keys).

use base64::Engine;
use conformance::{Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::json;

const SECRET: &[u8] = b"jwks-shared-secret";

/// Spawn a minimal HTTP server that returns `body` (as JSON) for any request.
/// Returns the JWKS URL. The task is detached; the test process reaps it.
async fn spawn_jwks(body: String) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let body = body.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 2048];
                let _ = sock.read(&mut buf).await; // drain the request line/headers
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });
    format!("http://127.0.0.1:{}/jwks", addr.port())
}

fn jwks_doc() -> String {
    let k = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(SECRET);
    format!(r#"{{"keys":[{{"kty":"oct","kid":"key1","k":"{k}","alg":"HS256"}}]}}"#)
}

fn kid_token(kid: &str) -> String {
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(kid.into());
    encode(
        &header,
        &json!({"sub": "jwks-user"}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

#[tokio::test]
async fn jwks_connect_with_matching_kid_succeeds() {
    let url = spawn_jwks(jwks_doc()).await;
    let cfg = format!(r#"{{"token_jwks_public_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;

    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_with_token(&kid_token("key1")).await;
    assert!(reply["error"].is_null(), "connect error: {reply}");
    assert!(
        reply["result"]["client"].as_str().is_some(),
        "expected client id: {reply}"
    );
}

#[tokio::test]
async fn jwks_unknown_kid_is_rejected() {
    let url = spawn_jwks(jwks_doc()).await;
    let cfg = format!(r#"{{"token_jwks_public_endpoint":"{url}"}}"#);
    let s = Server::start_with_config(&cfg).await;

    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.send_raw(&format!(
        r#"{{"id":1,"params":{{"token":"{}"}}}}"#,
        kid_token("nope")
    ))
    .await;
    // An unverifiable token closes the connection (invalid-token disconnect).
    let (code, _reason) = c.next_close().await;
    assert!(code >= 3000, "expected a disconnect close code, got {code}");
}
