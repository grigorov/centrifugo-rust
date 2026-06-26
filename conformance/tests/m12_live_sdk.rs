//! M12: the strongest compatibility proof — drive the Rust binary with the
//! **real** `centrifuge-go` SDK (v0.6.2, the protocol-v0.3.4 / seq-gen era this
//! server targets). An unmodified client SDK connects, subscribes, publishes,
//! and receives the publication back. Skips when `go` is unavailable.
//!
//! The same probe is run against the Go oracle in the project's manual checks to
//! confirm the probe itself is correct; here we assert it succeeds against Rust.

use conformance::{run_cli, run_go_client, run_go_client_token, Server};

#[tokio::test]
async fn centrifuge_go_sdk_roundtrip_against_rust() {
    let s = Server::start().await; // --client_insecure
    let Some((code, output)) = run_go_client(&s.ws_url()) else {
        return; // go not installed
    };
    assert_eq!(code, 0, "centrifuge-go probe failed:\n{output}");
    assert!(
        output.contains("OK"),
        "expected OK from centrifuge-go probe, got:\n{output}"
    );
}

#[tokio::test]
async fn centrifuge_go_sdk_authenticates_with_jwt() {
    // A token-mode server (no client_insecure): the SDK must authenticate with a
    // JWT minted by our own `gentoken` for the round-trip to work.
    let s = Server::start_with_config(r#"{"token_hmac_secret_key":"m12secret"}"#).await;
    let (code, token) = run_cli(&["gentoken", "--token_hmac_secret_key", "m12secret", "-u", "sdk-user"]);
    assert_eq!(code, 0, "gentoken failed");
    let token = token.trim();

    let Some((code, output)) = run_go_client_token(&s.ws_url(), token) else {
        return; // go not installed
    };
    assert_eq!(code, 0, "centrifuge-go token probe failed:\n{output}");
    assert!(
        output.contains("OK"),
        "expected OK from token probe, got:\n{output}"
    );
}
