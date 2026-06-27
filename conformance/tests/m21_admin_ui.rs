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
async fn admin_api_info_serves_live_node_stats() {
    // The admin SPA polls POST /admin/api {method:"info"} on an interval (there is
    // no admin WebSocket in centrifugo v2.8.6) and renders the node stats. Verify
    // the info reply carries the fields it reads.
    let s = Server::start_with_config(r#"{"admin":true,"admin_password":"pw","admin_secret":"sec"}"#)
        .await;
    let client = reqwest::Client::new();

    let auth = client
        .post(format!("{}/admin/auth", s.http))
        .form(&[("password", "pw")])
        .send()
        .await
        .unwrap();
    let token = auth.json::<serde_json::Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{}/admin/api", s.http))
        .header("Authorization", format!("token {token}"))
        .body(r#"{"method":"info","params":{}}"#)
        .send()
        .await
        .unwrap();
    let text = resp.text().await.unwrap();
    let r: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    let node = &r["result"]["nodes"][0];
    assert!(!node["uid"].as_str().unwrap_or("").is_empty(), "node uid: {r}");
    assert!(node["num_clients"].is_number(), "num_clients: {r}");
    assert!(node["num_users"].is_number(), "num_users: {r}");
    assert!(node["num_channels"].is_number(), "num_channels: {r}");
    assert!(node["uptime"].is_number(), "uptime: {r}");
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
