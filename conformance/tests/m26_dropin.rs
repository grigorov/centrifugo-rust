//! Drop-in compatibility with the official Centrifugo v2.8.6 operator surface
//! (see docs/COMPATIBILITY_v2.8.6.md): the bare root command is the server, legacy
//! flags don't abort startup, `CENTRIFUGO_*` env vars are honored, and the
//! gentoken/checktoken subcommands match Go's behavior.

use conformance::{run_cli, Server, WsJsonClient};

/// An existing official command line may carry flags this build doesn't implement
/// (TLS, NATS broker, internal port, …). They must be accepted (warn-and-ignore),
/// not abort startup — otherwise `Server::start_with` would never become healthy.
#[tokio::test]
async fn unsupported_flags_do_not_abort_startup() {
    let s = Server::start_with(&[
        "--client_insecure",
        "--tls",
        "--broker",
        "nats",
        "--internal_port",
        "9999",
        "--log_level",
        "debug",
        "--prometheus",
    ])
    .await;
    // Healthy (start_with panics otherwise) and actually serving clients:
    // connect_command returns the assigned client id (it panics if the CONNECT
    // reply has no result), so a non-empty id proves the server is live.
    let mut c = WsJsonClient::connect(&s.ws_url()).await;
    let client_id = c.connect_command().await;
    assert!(
        !client_id.is_empty(),
        "expected a client id, got: {client_id}"
    );
}

/// `CENTRIFUGO_ADMIN` / `CENTRIFUGO_ADMIN_INSECURE` enable the admin endpoints via
/// env (Go viper convention); without them `/admin/auth` is 404.
#[tokio::test]
async fn admin_enabled_via_env() {
    let s = Server::start_env(
        &[
            ("CENTRIFUGO_ADMIN", "true"),
            ("CENTRIFUGO_ADMIN_INSECURE", "true"),
        ],
        &["--client_insecure"],
    )
    .await;
    let code = reqwest::Client::new()
        .post(format!("{}/admin/auth", s.http))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16();
    assert_eq!(
        code, 200,
        "admin enabled via env → /admin/auth returns a token"
    );
}

/// `gentoken` defaults to a 7-day (604800s) TTL like Go (was 0 / no-expiry).
#[tokio::test]
async fn gentoken_defaults_to_7day_ttl() {
    let (code, out) = run_cli(&["gentoken", "-u", "alice", "--token_hmac_secret_key", "s"]);
    assert_eq!(code, 0, "gentoken exit");
    assert!(
        out.contains("604800"),
        "gentoken should default to a 7-day TTL: {out}"
    );
}

/// `checktoken` verifies a token minted by `gentoken` (exit 0) and rejects a bogus
/// one (non-zero) — the subcommand the official image has and ours previously lacked.
#[tokio::test]
async fn checktoken_verifies_gentoken_token() {
    let (code, out) = run_cli(&[
        "gentoken",
        "-u",
        "bob",
        "--token_hmac_secret_key",
        "s",
        "-t",
        "60",
    ]);
    assert_eq!(code, 0, "gentoken exit");
    let token = out.lines().last().unwrap_or("").trim().to_string();

    let (code, out) = run_cli(&["checktoken", "--token_hmac_secret_key", "s", &token]);
    assert_eq!(code, 0, "checktoken should accept a valid token: {out}");
    assert!(
        out.contains("bob"),
        "checktoken should print the user: {out}"
    );

    let (code, _) = run_cli(&["checktoken", "--token_hmac_secret_key", "s", "not.a.jwt"]);
    assert_ne!(code, 0, "checktoken should reject a bogus token");
}
