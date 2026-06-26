//! M11 (env config): `CENTRIFUGO_*` environment variables fill settings not set
//! by flags/file. Here `CENTRIFUGO_API_KEY` enables HTTP-API auth with no
//! `--api_key` flag.

use conformance::{api_post, api_status, Server};

#[tokio::test]
async fn env_provides_api_key() {
    let s = Server::start_env(
        &[("CENTRIFUGO_API_KEY", "envkey")],
        &["--client_insecure"],
    )
    .await;

    // The env-provided key authorizes the HTTP API.
    let r = api_post(&s.http, "envkey", r#"{"method":"info","params":{}}"#).await;
    assert!(r["result"]["nodes"].is_array(), "info: {r}");

    // A wrong key is still rejected.
    let code = api_status(&s.http, "wrong", r#"{"method":"info","params":{}}"#).await;
    assert_eq!(code, 401);
}
