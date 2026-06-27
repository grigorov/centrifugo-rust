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

#[tokio::test]
async fn admin_web_path_serves_arbitrary_tree() {
    // admin_web_path serves the whole directory (not just the 4 embedded names).
    let dir = std::env::temp_dir().join("centrifugo-webpath-test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("index.html"), b"<html>custom admin panel</html>").unwrap();
    std::fs::write(dir.join("vendor.js"), b"console.log('vendor chunk');").unwrap();

    let cfg = format!(
        r#"{{"admin":true,"admin_password":"pw","admin_secret":"s","admin_web_path":"{}"}}"#,
        dir.display()
    );
    let s = Server::start_with_config(&cfg).await;

    let index = reqwest::get(format!("{}/", s.http)).await.unwrap();
    assert_eq!(index.status().as_u16(), 200);
    assert!(index.text().await.unwrap().contains("custom admin panel"));

    // An extra asset beyond the embedded 4 is served from the tree.
    let vendor = reqwest::get(format!("{}/vendor.js", s.http)).await.unwrap();
    assert_eq!(vendor.status().as_u16(), 200);
    let ct = vendor
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(ct.contains("javascript"), "vendor content-type: {ct}");
    assert!(vendor.text().await.unwrap().contains("vendor chunk"));

    // A missing file under the path 404s.
    let missing = reqwest::get(format!("{}/nope.js", s.http)).await.unwrap();
    assert_eq!(missing.status().as_u16(), 404);

    let _ = std::fs::remove_dir_all(&dir);
}
