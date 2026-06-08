use rs_poke::{
    CredentialsStore, FetchWithAuthOptions, LoginOptions, Poke, PokeOptions, fetch_with_auth, login,
    login_fresh,
};
use std::time::Duration;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn fetch_with_auth_maps_403_to_permission_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forbidden"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;

    let err = fetch_with_auth(FetchWithAuthOptions {
        path: "/forbidden",
        method: reqwest::Method::GET,
        body: None,
        token: Some("pk_test".into()),
        base_url: Some(server.uri()),
        client: None,
    })
    .await
    .expect_err("403 should fail");

    assert!(matches!(err, rs_poke::Error::Auth(message) if message == "api key lacks permission"));
}

#[tokio::test]
async fn login_device_flow_persists_token_from_mock_api() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/cli-auth/code"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deviceCode": "device-123",
            "userCode": "ABCD-1234"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/cli-auth/poll/device-123$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "authenticated",
            "token": "pk_mock_token"
        })))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = CredentialsStore::new(dir.path().join("credentials.json"));
    let mut options = LoginOptions::new(store.clone());
    options.api_base = server.uri();
    options.frontend_base = "https://poke.test".into();
    options.open_browser = false;
    options.timeout = Duration::from_secs(5);
    options.poll_interval = Duration::from_millis(50);
    let result = login(options)
    .await
    .expect("login should succeed");

    assert_eq!(result.token, "pk_mock_token");
    assert_eq!(
        store
            .read()
            .expect("read credentials")
            .expect("token stored")
            .token,
        "pk_mock_token"
    );
}

#[tokio::test]
async fn login_fresh_ignores_cached_credentials() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/cli-auth/code"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "deviceCode": "device-fresh",
            "userCode": "FRESH-9999"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/cli-auth/poll/device-fresh$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "authenticated",
            "token": "pk_fresh_token"
        })))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = CredentialsStore::new(dir.path().join("credentials.json"));
    store.write("pk_stale").expect("seed stale token");

    let mut options = LoginOptions::new(store);
    options.api_base = server.uri();
    options.frontend_base = "https://poke.test".into();
    options.open_browser = false;
    options.timeout = Duration::from_secs(5);
    options.poll_interval = Duration::from_millis(50);
    let result = login_fresh(options)
    .await
    .expect("fresh login should succeed");

    assert_eq!(result.token, "pk_fresh_token");
}

#[tokio::test]
async fn tunnel_create_posts_to_cli_connections_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/mcp/connections/cli"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "conn-42",
            "serverUrl": "https://tunnel.poke.test/conn-42",
            "tunnel": {
                "token": "tunnel-token",
                "upstreamUrl": "wss://upstream.poke.test"
            }
        })))
        .mount(&server)
        .await;

    let poke = Poke::new(PokeOptions {
        api_key: Some("pk_test".into()),
        base_url: server.uri(),
    })
    .expect("poke client");

    let response = poke
        .raw_auth(
            reqwest::Method::POST,
            "/mcp/connections/cli",
            Some(serde_json::json!({
                "name": "test-integration",
                "serverUrl": "http://127.0.0.1:52333/mcp",
                "tunnel": true
            })),
        )
        .await
        .expect("tunnel create request");

    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.expect("json body");
    assert_eq!(body["id"], "conn-42");
    assert_eq!(body["serverUrl"], "https://tunnel.poke.test/conn-42");
    assert_eq!(body["tunnel"]["token"], "tunnel-token");
}