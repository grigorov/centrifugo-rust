//! M11 (env config): `CENTRIFUGO_*` environment variables fill settings not set
//! by flags/file. Here `CENTRIFUGO_API_KEY` enables HTTP-API auth with no
//! `--api_key` flag.

use conformance::{api_post, api_status, Server};

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
