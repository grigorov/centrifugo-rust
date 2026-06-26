//! M11 (admin): `POST /admin/auth` exchanges the password for a session token
//! that authorizes the server API as a `Bearer` credential.

use conformance::Server;

#[tokio::test]
async fn admin_auth_issues_token_that_authorizes_api() {
    let s =
        Server::start_with_config(r#"{"admin":true,"admin_password":"pw","admin_secret":"sec"}"#)
            .await;
    let client = reqwest::Client::new();

    // Wrong password is rejected.
    let bad = client
        .post(format!("{}/admin/auth", s.http))
        .body(r#"{"password":"nope"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status().as_u16(), 401);

    // Correct password yields a token.
    let ok = client
        .post(format!("{}/admin/auth", s.http))
        .body(r#"{"password":"pw"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status().as_u16(), 200);
    let body: serde_json::Value = ok.json().await.unwrap();
    let token = body["token"].as_str().expect("admin token").to_string();
    assert!(!token.is_empty());

    // The admin token authorizes /admin/api with the `token` scheme (Go form).
    let admin_api = client
        .post(format!("{}/admin/api", s.http))
        .header("Authorization", format!("token {token}"))
        .body(r#"{"method":"info","params":{}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(admin_api.status().as_u16(), 200);
    let text = admin_api.text().await.unwrap();
    let r: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert!(r["result"]["nodes"].is_array(), "info: {r}");

    // /admin/api with no credential is rejected.
    let unauth = client
        .post(format!("{}/admin/api", s.http))
        .body(r#"{"method":"info","params":{}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status().as_u16(), 401);

    // The admin token must NOT authorize the main /api (Go's apiKeyAuth only
    // accepts the `apikey` scheme); api_key is unset so /api always 401s here.
    let main_api = client
        .post(format!("{}/api", s.http))
        .header("Authorization", format!("token {token}"))
        .body(r#"{"method":"info","params":{}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(main_api.status().as_u16(), 401, "admin token must not work on /api");
}
