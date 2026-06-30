//! M11 (env config): `CENTRIFUGO_*` environment variables fill settings not set
//! by flags/file. Here `CENTRIFUGO_API_KEY` enables HTTP-API auth with no
//! `--api_key` flag.

use conformance::{api_post, api_status, Server, WsJsonClient};

#[tokio::test]
async fn env_provides_api_key() {
    let s = Server::start_env(&[("CENTRIFUGO_API_KEY", "envkey")], &["--client_insecure"]).await;

    // The env-provided key authorizes the HTTP API.
    let r = api_post(&s.http, "envkey", r#"{"method":"info","params":{}}"#).await;
    assert!(r["result"]["nodes"].is_array(), "info: {r}");

    // A wrong key is still rejected.
    let code = api_status(&s.http, "wrong", r#"{"method":"info","params":{}}"#).await;
    assert_eq!(code, 401);
}

#[tokio::test]
async fn env_beats_config_file_api_key() {
    // M5: a CENTRIFUGO_* env var must beat a config-file value (viper precedence
    // flag > env > file). Here the stale file api_key must NOT authorize; the env
    // one must — the baked-config + per-pod-env container pattern.
    let cfg = std::env::temp_dir().join("centrifugo-rust-m5-envfile.json");
    std::fs::write(&cfg, r#"{"client_insecure":true,"api_key":"filekey"}"#).expect("write config");
    let s = Server::start_env(
        &[("CENTRIFUGO_API_KEY", "envkey")],
        &["-c", cfg.to_str().unwrap()],
    )
    .await;

    let r = api_post(&s.http, "envkey", r#"{"method":"info","params":{}}"#).await;
    assert!(
        r["result"]["nodes"].is_array(),
        "env api_key must authorize: {r}"
    );
    let code = api_status(&s.http, "filekey", r#"{"method":"info","params":{}}"#).await;
    assert_eq!(code, 401, "stale config-file api_key must be rejected");
}

#[tokio::test]
async fn env_overrides_channel_max_length() {
    // channel_max_length is bound via viper.BindEnv in Go (main.go:185), read with
    // v.GetInt -> a present CENTRIFUGO_* env overrides the config file.
    let cfg = std::env::temp_dir().join("centrifugo-rust-env-chanlen.json");
    std::fs::write(&cfg, r#"{"client_insecure":true,"channel_max_length":255}"#).expect("write");
    let s = Server::start_env(
        &[("CENTRIFUGO_CHANNEL_MAX_LENGTH", "10")],
        &["-c", cfg.to_str().unwrap()],
    )
    .await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.connect_command().await;
    // 11-char channel exceeds the env limit of 10 -> ErrorLimitExceeded (106).
    let r = c.subscribe(2, "12345678901").await;
    assert_eq!(
        r["error"]["code"], 106,
        "env channel_max_length=10 must override file 255 and reject an 11-char channel: {r}"
    );
}

#[tokio::test]
async fn env_provides_allowed_origins() {
    // allowed_origins is bound via viper.BindEnv in Go (main.go:224), read with
    // v.GetStringSlice (whitespace-split) -> env can enable origin checking.
    let s = Server::start_env(
        &[("CENTRIFUGO_ALLOWED_ORIGINS", "https://good.example")],
        &["--client_insecure"],
    )
    .await;
    match WsJsonClient::connect_with_origin(&s.ws_url(), "https://evil.example").await {
        Err(code) => assert_eq!(code, 403, "env allowlist must reject a disallowed origin"),
        Ok(_) => panic!("env allowed_origins must enable origin checking (evil must be rejected)"),
    }
    assert!(
        WsJsonClient::connect_with_origin(&s.ws_url(), "https://good.example")
            .await
            .is_ok(),
        "the env-allowed origin must connect"
    );
}

#[tokio::test]
async fn env_bool_false_overrides_config_file_true() {
    // viper parity: a present CENTRIFUGO_* bool env overrides the config file even
    // when it is `false`. The file sets `client_insecure:true`; the env `false`
    // must turn it off, so a tokenless CONNECT is rejected (close 3003) rather than
    // accepted. (Go binds these bools via viper.BindEnv; a present env beats file.)
    let cfg = std::env::temp_dir().join("centrifugo-rust-envbool-false.json");
    std::fs::write(&cfg, r#"{"client_insecure":true}"#).expect("write config");
    let s = Server::start_env(
        &[("CENTRIFUGO_CLIENT_INSECURE", "false")],
        &["-c", cfg.to_str().unwrap()],
    )
    .await;
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    c.send_raw(r#"{"id":1,"params":{}}"#).await;
    let (code, _) = c.next_close().await;
    assert_eq!(
        code, 3003,
        "env CLIENT_INSECURE=false must override the file's true (insecure off → tokenless connect rejected)"
    );
}
