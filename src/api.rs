use crate::{Error, Result, get_token};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

const DEFAULT_API: &str = "https://poke.com/api/v1";

#[derive(Clone, Debug)]
pub struct PokeOptions {
    pub api_key: Option<String>,
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

#[derive(Clone, Debug)]
pub struct Poke {
    api_key: String,
    base_url: String,
    client: reqwest::Client,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Webhook {
    #[serde(rename = "webhookUrl")]
    pub webhook_url: String,
    #[serde(rename = "webhookToken")]
    pub webhook_token: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct CreateWebhook<'a> {
    pub condition: &'a str,
    pub action: &'a str,
}

impl Poke {
    pub fn new(options: PokeOptions) -> Result<Self> {
        let api_key = options
            .api_key
            .or_else(|| std::env::var("POKE_API_KEY").ok())
            .or_else(|| get_token().ok().flatten())
            .ok_or(Error::NotLoggedIn)?;
        Ok(Self {
            api_key,
            base_url: options.base_url,
            client: reqwest::Client::new(),
        })
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub async fn send_message(&self, message: &str) -> Result<Value> {
        self.post_json(
            "/inbound/api-message",
            &serde_json::json!({ "message": message }),
        )
        .await
    }

    pub async fn create_webhook(&self, request: CreateWebhook<'_>) -> Result<Webhook> {
        self.post_json("/api-keys/webhook", &request).await
    }

    pub async fn send_webhook(
        &self,
        webhook_url: &str,
        webhook_token: &str,
        data: Value,
    ) -> Result<Value> {
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
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(Error::Auth("invalid api key".into()));
        }
        if response.status() == reqwest::StatusCode::FORBIDDEN {
            return Err(Error::Auth("api key lacks permission".into()));
        }
        if !response.status().is_success() {
            return Err(Error::Api(format_web_error("Poke API", response).await));
        }
        Ok(response.json().await?)
    }

    pub async fn raw_auth(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<reqwest::Response> {
        let mut request = self
            .client
            .request(method, format!("{}{}", self.base_url, path))
            .bearer_auth(&self.api_key);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(Error::Auth("session expired".into()));
        }
        Ok(response)
    }
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
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| status.to_string());
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
}
