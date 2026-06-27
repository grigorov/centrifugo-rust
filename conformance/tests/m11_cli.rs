//! M11 (CLI subcommands): `gentoken` produces a token the server accepts, and
//! `checkconfig` validates / rejects config files.

use conformance::{run_cli, Server, WsJsonClient};

#[tokio::test]
async fn gentoken_produces_valid_connection_token() {
    let (code, out) = run_cli(&[
        "gentoken",
        "--token_hmac_secret_key",
        "secret",
        "-u",
        "alice",
    ]);
    assert_eq!(code, 0, "gentoken exit code");
    let token = out.trim().to_string();
    assert!(!token.is_empty(), "empty token");

    let s = Server::start_with_config(r#"{"token_hmac_secret_key":"secret"}"#).await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let reply = c.connect_with_token(&token).await;
    assert!(reply["error"].is_null(), "connect error: {reply}");
    assert!(
        reply["result"]["client"].as_str().is_some(),
        "expected client id: {reply}"
    );
}

#[test]
fn checkconfig_accepts_valid_and_rejects_invalid() {
    let dir = std::env::temp_dir();
    let good = dir.join("centrifugo-cli-good.json");
    let bad = dir.join("centrifugo-cli-bad.json");
    std::fs::write(&good, r#"{"token_hmac_secret_key":"x","namespaces":[]}"#).unwrap();
    std::fs::write(&bad, "{ not json").unwrap();

    let (code, _) = run_cli(&["checkconfig", "-c", good.to_str().unwrap()]);
    assert_eq!(code, 0, "valid config should pass");

    let (code, _) = run_cli(&["checkconfig", "-c", bad.to_str().unwrap()]);
    assert_ne!(code, 0, "invalid config should fail");
}
