use crate::http_client::shared_http_client;
use crate::{Error, Result, get_token};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::fmt;

const DEFAULT_API: &str = "https://poke.com/api/v1";

fn default_true() -> bool {
    true
}

/// Configuration for [`Poke`] client construction.
#[derive(Clone, Debug)]
pub struct PokeOptions {
    /// API key. Falls back to `POKE_API_KEY` env or stored credentials.
    pub api_key: Option<String>,
    /// API base URL. Defaults to `POKE_API` or `https://poke.com/api/v1`.
    pub base_url: String,
}

impl Default for PokeOptions {
    fn default() -> Self {
        Self {
            api_key: None,
            base_url: std::env::var("POKE_API").unwrap_or_else(|_| DEFAULT_API.to_string()),
        }
    }
}

/// Authenticated HTTP client for the Poke API.
#[derive(Clone, Debug)]
pub struct Poke {
    api_key: String,
    base_url: String,
    client: reqwest::Client,
}

/// Response from [`Poke::send_message`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendMessageResponse {
    /// Whether the API accepted the message.
    #[serde(default = "default_true")]
    pub success: bool,
    /// Optional status text from the API.
    #[serde(default)]
    pub message: String,
}

/// Response from [`Poke::send_webhook`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendWebhookResponse {
    /// Whether the webhook accepted the payload.
    #[serde(default = "default_true")]
    pub success: bool,
}

/// Response from [`Poke::create_webhook`].
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateWebhookResponse {
    /// Server-assigned trigger identifier.
    #[serde(rename = "triggerId", default)]
    pub trigger_id: String,
    /// URL to POST webhook payloads to.
    #[serde(rename = "webhookUrl")]
    pub webhook_url: String,
    /// Bearer token for webhook authentication.
    #[serde(rename = "webhookToken")]
    pub webhook_token: String,
}

/// Request body for [`Poke::create_webhook`].
#[derive(Clone, Debug, Serialize)]
pub struct CreateWebhook<'a> {
    /// Trigger condition expression.
    pub condition: &'a str,
    /// Action to run when the condition matches.
    pub action: &'a str,
}

/// Authentication-specific error type returned by auth helpers.
#[derive(Debug)]
pub struct PokeAuthError {
    message: String,
}

impl PokeAuthError {
    /// Create a new authentication error.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for PokeAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PokeAuthError {}

impl From<PokeAuthError> for Error {
    fn from(value: PokeAuthError) -> Self {
        Self::Auth(value.message)
    }
}

pub(crate) fn auth_error(message: impl Into<String>) -> Error {
    Error::from(PokeAuthError::new(message))
}

/// Options for [`fetch_with_auth`].
#[derive(Clone, Debug)]
pub struct FetchWithAuthOptions<'a> {
    /// API path appended to the base URL.
    pub path: &'a str,
    /// HTTP method for the request.
    pub method: reqwest::Method,
    /// Optional JSON request body.
    pub body: Option<Value>,
    /// Bearer token override. Falls back to env or stored credentials.
    pub token: Option<String>,
    /// API base URL override. Falls back to `POKE_API` or the default host.
    pub base_url: Option<String>,
    /// HTTP client override. Falls back to the shared client.
    pub client: Option<reqwest::Client>,
}

impl Poke {
    /// Create a new authenticated client.
    pub fn new(options: PokeOptions) -> Result<Self> {
        let api_key = options
            .api_key
            .or_else(|| std::env::var("POKE_API_KEY").ok())
            .or_else(|| get_token().ok().flatten())
            .ok_or(Error::NotLoggedIn)?;
        Ok(Self {
            api_key,
            base_url: options.base_url,
            client: shared_http_client().clone(),
        })
    }

    /// Return the configured API key.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Return the configured API base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) fn http_client(&self) -> reqwest::Client {
        self.client.clone()
    }

    /// Send a message through the Poke API.
    pub async fn send_message(&self, message: &str) -> Result<SendMessageResponse> {
        self.post_json(
            "/inbound/api-message",
            &serde_json::json!({ "message": message }),
        )
        .await
    }

    /// Create a webhook trigger.
    pub async fn create_webhook(&self, request: CreateWebhook<'_>) -> Result<CreateWebhookResponse> {
        self.post_json("/api-keys/webhook", &request).await
    }

    /// POST JSON to a webhook URL.
    pub async fn send_webhook(
        &self,
        webhook_url: &str,
        webhook_token: &str,
        data: Value,
    ) -> Result<SendWebhookResponse> {
        let response = self
            .client
            .post(webhook_url)
            .bearer_auth(webhook_token)
            .json(&data)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(Error::Api(format_web_error("Poke webhook", response).await));
        }
        Ok(response.json().await?)
    }

    /// Authenticated JSON POST helper.
    pub async fn post_json<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<R> {
        let response = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await?;
        map_api_response("Poke API", response)
            .await?
            .json()
            .await
            .map_err(Into::into)
    }

    /// Low-level authenticated request using this client's credentials.
    pub async fn raw_auth(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<reqwest::Response> {
        fetch_with_auth(FetchWithAuthOptions {
            path,
            method,
            body,
            token: Some(self.api_key.clone()),
            base_url: Some(self.base_url.clone()),
            client: Some(self.client.clone()),
        })
        .await
    }
}

/// Perform an authenticated HTTP request against the Poke API.
pub async fn fetch_with_auth(options: FetchWithAuthOptions<'_>) -> Result<reqwest::Response> {
    let token = options
        .token
        .or_else(|| std::env::var("POKE_API_KEY").ok())
        .or_else(|| get_token().ok().flatten())
        .ok_or_else(|| auth_error("not logged in. Run 'poke login'."))?;
    let base_url = options
        .base_url
        .unwrap_or_else(|| std::env::var("POKE_API").unwrap_or_else(|_| DEFAULT_API.to_string()));
    let client = options
        .client
        .unwrap_or_else(|| shared_http_client().clone());
    let mut request = client
        .request(options.method, format!("{}{}", base_url, options.path))
        .bearer_auth(token);
    if let Some(body) = options.body {
        request = request.json(&body);
    }
    let response = request.send().await?;
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err(auth_error("session expired. Run 'poke login' again."));
    }
    if response.status() == reqwest::StatusCode::FORBIDDEN {
        return Err(auth_error("api key lacks permission"));
    }
    Ok(response)
}

pub(crate) async fn map_api_response(
    prefix: &str,
    response: reqwest::Response,
) -> Result<reqwest::Response> {
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err(auth_error("invalid api key"));
    }
    if response.status() == reqwest::StatusCode::FORBIDDEN {
        return Err(auth_error("api key lacks permission"));
    }
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(Error::Api(format!(
            "{prefix} error (429): rate limited. Please slow down and retry."
        )));
    }
    if !response.status().is_success() {
        return Err(Error::Api(format_web_error(prefix, response).await));
    }
    Ok(response)
}

pub(crate) async fn format_web_error(prefix: &str, response: reqwest::Response) -> String {
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    let detail = serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .or_else(|| value.get("message"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .filter(|value| !value.is_empty());
    let detail = match detail {
        Some(detail) if detail != status.canonical_reason().unwrap_or_default() => detail,
        _ if !text.trim().is_empty() => text.trim().chars().take(500).collect(),
        _ => status.to_string(),
    };
    format!("{prefix} error ({status}): {detail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_default_uses_poke_api_env() {
        unsafe {
            std::env::set_var("POKE_API", "http://127.0.0.1:1/api");
        }
        assert_eq!(PokeOptions::default().base_url, "http://127.0.0.1:1/api");
        unsafe {
            std::env::remove_var("POKE_API");
        }
    }

    #[test]
    fn client_accepts_explicit_api_key() {
        let client = Poke::new(PokeOptions {
            api_key: Some("pk_test".into()),
            base_url: "http://127.0.0.1:1".into(),
        })
        .expect("client");
        assert_eq!(client.api_key(), "pk_test");
    }

    #[test]
    fn send_message_response_defaults_missing_fields() {
        let response: SendMessageResponse =
            serde_json::from_str("{}").expect("empty object should deserialize");
        assert!(response.success);
        assert!(response.message.is_empty());
    }

    #[test]
    fn poke_auth_error_converts_to_error_variant() {
        let err: Error = PokeAuthError::new("expired").into();
        assert!(matches!(err, Error::Auth(message) if message == "expired"));
    }

    #[tokio::test]
    async fn fetch_with_auth_maps_401_to_session_expired() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path("/protected"))
            .respond_with(wiremock::ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let err = fetch_with_auth(FetchWithAuthOptions {
            path: "/protected",
            method: reqwest::Method::GET,
            body: None,
            token: Some("pk_test".into()),
            base_url: Some(server.uri()),
            client: None,
        })
        .await
        .expect_err("401 should fail");

        assert!(
            matches!(err, Error::Auth(message) if message == "session expired. Run 'poke login' again.")
        );
    }
}