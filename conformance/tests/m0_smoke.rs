//! M0 smoke: the spawned binary becomes healthy.

#[tokio::test]
async fn health_is_ok() {
    let s = conformance::Server::start().await;
    let resp = reqwest::get(format!("{}/health", s.http)).await.unwrap();
    assert!(resp.status().is_success());
}
