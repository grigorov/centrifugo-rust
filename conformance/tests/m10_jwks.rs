//! M10 (JWKS): the server fetches a JWKS endpoint at startup and verifies a
//! connection token by its `kid`. Centrifugo's JWKS path is RSA-only, so the
//! mock endpoint serves an RSA signature key (matching Go, which rejects
//! non-RSA / non-`sig` JWKs).

use conformance::{Server, WsJsonClient};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::json;

/// 2048-bit RSA test keypair: the private key (PKCS#8 PEM) signs tokens; the
/// JWKS publishes its public modulus (`RSA_N`, exponent AQAB / 65537).
const RSA_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC/nEiIHJm+3VBp
QJu0zv3y7FZ+2gzqo6DVW9njmTpYDVFPQHH7Wa0baiZDlZCYceeFe1KNQ1FkJiBX
GNgb3fFMXUkcqnFRqWlFplYKUxePXZOdi/GlBbqC99dIsnfeOOPYTy0+axCA5s2N
SOSROw1kNsifElPz154txfqADf+ByHpNW4ggOjUXQkfd3J1NW4B0xDOHExZ6gYcx
hVaBucJR0xNzTTDdWQGqnPdFPwWWp6YK77mxwV7pIWUCIukVoLmv2rWJ2KeihMze
HJ0zWJbNJhABfEdy8Rbq/9HUhRq+t1q7O6bBSFrrcZg6dHARiYc9ckQqDWuRYI0r
V/HfTiEJAgMBAAECggEAAfb9EbbWXXWyqVBvtrUzkP1BeIDgpnrM6RP6Kkcg1dr1
tGkp8HMa1YsOlaPH24WGfRDjl7FJWT+TvKtNH846sLoy20CeeeeE13Se6v5Vx/y+
CRZqNDNrwx198QZFRaUO2Nt1/UiW688O3xXwlxJaxXls1pNXHLF4/+/o5ppOJMAb
X6v7kmpbEZU7JcAv0aZx/kpVVoCZV79WWfgBCicBv6mml5NawoPBG/9d8PvAtAst
8GYwFP2JlQorJa6lk1uKLhu2LxsNSDtIw3+jrLYssEpkJFRxu2glhdpMtJE/X0/R
AXssACqCWnqgqJuKX9Co3Iq2vbl1qr/6SiOHv/ARAQKBgQD5hoodKISvWUL1ZRjO
k8R7ClSrFVNQDxbPzkRvQodLdxAFyYjn4z+EDwfmSnHnhLdZbAZhbk+Vyoigsif7
9RD+8p+fVW8C5TFeg6q/8Ij9LjUMBR5lhB7RhHClfa7nLyM0AJxeUuQJ+1Xm3k9b
uvLmWJ4HWbQUiZXtJ6hs+anVAQKBgQDElQvHq2zYcEurbYEu9uWBpSqLOJqWfUpb
KQZoM5bhElwt3vdOydkt240Dg21Omlv1giGDA1Wuzi4vcQTzlRqHawSYsrL1r4oK
VcISTma6SkHPSNX4AgJ+/62nmrWT9pa01sNTnclOE2s6V2l5awW4EFni/juSqaUH
y3OaFuGkCQKBgQCjDzt0QIUsvW0XRcCHRmMwcJjR0DbIa4Phuo5YEqatNxoeXgv8
VTGtj9D+ugljXQQgCIrG4rpZTagpMyMT8JrxsAWFruPDhZjUhcBwe7RZlveNak7p
0gP9sMmYK+C/LLuZgQiuTwa8SyVgoEhFzo5q3uAuN32JqjtyZecXh7NnAQKBgFrd
ng1UQsKk3YVG36CqxRkxFEI4DtSi4zzR8ME3n3U3vF4DowLLMFUPF9ZY6KydkwYf
eYgKgY+EhDqvnh9Ne26+2+gNKcWAt2jhjQxTKw7PBi5fN3Ak1ayIWGeRjn7vS2gZ
oT3EQGmTdkwIXZufCYy0GihfZX/8ZGj+9Ndz3iapAoGAVMhJJZ9NFnQg8FnTWVMz
32lrQ7ySHBZkWb4hLh+x3wLNNy32hHlxgSHwcD/euZGyqsPh7gHbF8JYMkiwAhVx
ueHGIivqCFfLTIiPy+cHUusq4lwDbpNg+ELrDuqQXAOiUyjlBUscR0X+VdSAHuSU
WGE/+8RJi42fdcJbdeguCLA=
-----END PRIVATE KEY-----";
const RSA_N: &str = "v5xIiByZvt1QaUCbtM798uxWftoM6qOg1VvZ45k6WA1RT0Bx-1mtG2omQ5WQmHHnhXtSjUNRZCYgVxjYG93xTF1JHKpxUalpRaZWClMXj12TnYvxpQW6gvfXSLJ33jjj2E8tPmsQgObNjUjkkTsNZDbInxJT89eeLcX6gA3_gch6TVuIIDo1F0JH3dydTVuAdMQzhxMWeoGHMYVWgbnCUdMTc00w3VkBqpz3RT8FlqemCu-5scFe6SFlAiLpFaC5r9q1idinooTM3hydM1iWzSYQAXxHcvEW6v_R1IUavrdauzumwUha63GYOnRwEYmHPXJEKg1rkWCNK1fx304hCQ";

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
    format!(
        r#"{{"keys":[{{"kty":"RSA","use":"sig","alg":"RS256","kid":"key1","n":"{RSA_N}","e":"AQAB"}}]}}"#
    )
}

fn kid_token(kid: &str) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.into());
    encode(
        &header,
        &json!({"sub": "jwks-user"}),
        &EncodingKey::from_rsa_pem(RSA_PRIV_PEM.as_bytes()).unwrap(),
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
