//! Phase 6: admin web UI. When admin is enabled, the vendored centrifugal/web
//! bundle is served at the root (index.html + bundle.js + styles.css + favicon).

use conformance::Server;

#[tokio::test]
async fn admin_ui_served_when_enabled() {
    let s = Server::start_with_config(r#"{"admin":true,"admin_password":"pw","admin_secret":"s"}"#)
        .await;

    let index = reqwest::get(format!("{}/", s.http)).await.unwrap();
    assert_eq!(index.status().as_u16(), 200);
    let html = index.text().await.unwrap();
    assert!(html.contains("Centrifugo admin panel"), "index: {html}");
    assert!(html.contains("bundle.js"), "index references bundle: {html}");

    let bundle = reqwest::get(format!("{}/bundle.js", s.http)).await.unwrap();
    assert_eq!(bundle.status().as_u16(), 200);
    let ct = bundle
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(ct.contains("javascript"), "bundle content-type: {ct}");
    assert!(!bundle.bytes().await.unwrap().is_empty(), "bundle empty");

    let styles = reqwest::get(format!("{}/styles.css", s.http)).await.unwrap();
    assert_eq!(styles.status().as_u16(), 200);
}

#[tokio::test]
async fn admin_ui_absent_when_disabled() {
    let s = Server::start().await; // admin disabled
    let index = reqwest::get(format!("{}/", s.http)).await.unwrap();
    assert_eq!(index.status().as_u16(), 404, "root must 404 without admin");
}
