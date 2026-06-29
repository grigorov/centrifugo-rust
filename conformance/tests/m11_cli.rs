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
    // gentoken prints a descriptive header line then the token on the last line.
    let token = out.lines().last().unwrap_or("").trim().to_string();
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

#[test]
fn checkconfig_enforces_validation_rules() {
    // M6: checkconfig rejects the same semantic errors Go rejects (exit 1):
    // history_recover with no window, a malformed namespace name, duplicate
    // namespaces, and a personal-channel namespace that does not exist.
    let dir = std::env::temp_dir();
    let cases: &[(&str, &str)] = &[
        ("m6-recover", r#"{"history_recover":true}"#),
        ("m6-badns", r#"{"namespaces":[{"name":"ba!d"}]}"#),
        (
            "m6-dupns",
            r#"{"namespaces":[{"name":"news"},{"name":"news"}]}"#,
        ),
        (
            "m6-personal",
            r#"{"user_personal_channel_namespace":"nope"}"#,
        ),
    ];
    for (tag, body) in cases {
        let p = dir.join(format!("centrifugo-cli-{tag}.json"));
        std::fs::write(&p, body).unwrap();
        let (code, _) = run_cli(&["checkconfig", "-c", p.to_str().unwrap()]);
        assert_ne!(code, 0, "config {tag} must be rejected: {body}");
    }

    // A well-formed config with a valid namespace + recovery window passes.
    let good = dir.join("centrifugo-cli-m6-good.json");
    std::fs::write(
        &good,
        r#"{"history_size":10,"history_lifetime":60,"history_recover":true,"user_personal_channel_namespace":"personal","namespaces":[{"name":"personal"}]}"#,
    )
    .unwrap();
    let (code, _) = run_cli(&["checkconfig", "-c", good.to_str().unwrap()]);
    assert_eq!(code, 0, "valid config must pass checkconfig");
}
