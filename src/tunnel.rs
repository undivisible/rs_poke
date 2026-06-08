use crate::api::{self, fetch_with_auth, FetchWithAuthOptions, Poke};
use crate::piko::{PikoConfig, run_client};
use crate::{Error, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
pub struct TunnelOptions {
    pub url: String,
    pub name: String,
    pub token: Option<String>,
    pub base_url: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub cleanup_on_stop: bool,
    pub sync_interval: Duration,
    pub startup_timeout: Duration,
}

impl Default for TunnelOptions {
    fn default() -> Self {
        Self {
            url: String::new(),
            name: String::new(),
            token: None,
            base_url: None,
            client_id: None,
            client_secret: None,
            cleanup_on_stop: true,
            sync_interval: Duration::from_secs(300),
            startup_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct CreateConnectionResponse {
    id: String,
    #[serde(rename = "serverUrl")]
    server_url: String,
    tunnel: TunnelConfig,
}

#[derive(Clone, Debug, Deserialize)]
struct TunnelConfig {
    token: String,
    #[serde(rename = "upstreamUrl")]
    upstream_url: String,
}

#[derive(Clone, Debug)]
pub struct TunnelInfo {
    pub connection_id: String,
    pub tunnel_url: String,
    pub local_url: String,
    pub name: String,
}

#[derive(Clone, Debug)]
pub enum TunnelEvent {
    Created(TunnelInfo),
    Connected(TunnelInfo),
    Disconnected,
    ToolsSynced { tool_count: usize },
    OAuthRequired { auth_url: String },
    Error(String),
}

type TunnelHandler = Box<dyn Fn(&TunnelEvent) + Send + Sync>;

pub struct TunnelRunner {
    client: Poke,
    options: TunnelOptions,
    events: broadcast::Sender<TunnelEvent>,
    handlers: Arc<Mutex<HashMap<String, Vec<TunnelHandler>>>>,
    stop: Option<watch::Sender<bool>>,
    sync_stop: Option<watch::Sender<bool>>,
    sync_task: Option<JoinHandle<()>>,
    info: Option<TunnelInfo>,
    connected: bool,
}

impl TunnelRunner {
    pub fn new(client: Poke, options: TunnelOptions) -> Self {
        let (events, _) = broadcast::channel(64);
        Self {
            client,
            options,
            events,
            handlers: Arc::new(Mutex::new(HashMap::new())),
            stop: None,
            sync_stop: None,
            sync_task: None,
            info: None,
            connected: false,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TunnelEvent> {
        self.events.subscribe()
    }

    pub fn on<F>(&self, event: &str, handler: F) -> &Self
    where
        F: Fn(&TunnelEvent) + Send + Sync + 'static,
    {
        self.handlers
            .lock()
            .expect("handlers lock")
            .entry(event.to_string())
            .or_default()
            .push(Box::new(handler));
        self
    }

    pub fn off(&self, event: &str) {
        self.handlers.lock().expect("handlers lock").remove(event);
    }

    pub fn info(&self) -> Option<&TunnelInfo> {
        self.info.as_ref()
    }

    pub fn connected(&self) -> bool {
        self.connected
    }

    pub async fn start(&mut self) -> Result<TunnelInfo> {
        self.start_inner().await
    }

    async fn start_inner(&mut self) -> Result<TunnelInfo> {
        let mut body = serde_json::json!({
            "name": self.options.name,
            "serverUrl": self.options.url,
            "tunnel": true
        });
        if let Some(client_id) = &self.options.client_id {
            body["clientId"] = Value::String(client_id.clone());
        }
        if let Some(client_secret) = &self.options.client_secret {
            body["clientSecret"] = Value::String(client_secret.clone());
        }
        let response = self.fetch_auth_json("/mcp/connections/cli", body).await?;
        let response: CreateConnectionResponse = serde_json::from_value(response)?;
        let info = TunnelInfo {
            connection_id: response.id.clone(),
            tunnel_url: response.server_url,
            local_url: self.options.url.clone(),
            name: self.options.name.clone(),
        };
        self.info = Some(info.clone());
        self.emit(TunnelEvent::Created(info.clone()));
        let local_addr = local_addr(&self.options.url)?;
        let (stop_tx, stop_rx) = watch::channel(false);
        let (connected_tx, mut connected_rx) = mpsc::unbounded_channel();
        let (error_tx, mut error_rx) = mpsc::unbounded_channel();
        let config = PikoConfig {
            upstream_url: response.tunnel.upstream_url,
            endpoint_id: response.id.clone(),
            token: response.tunnel.token,
            local_addr,
            connect_timeout: self.options.startup_timeout.min(Duration::from_secs(10)),
            connected: Some(connected_tx),
            errors: Some(error_tx),
        };
        let events = self.events.clone();
        let handlers = Arc::clone(&self.handlers);
        tokio::spawn(async move {
            if let Err(err) = run_client(config, stop_rx).await {
                emit_event(&events, &handlers, TunnelEvent::Error(err.to_string()));
            }
            emit_event(&events, &handlers, TunnelEvent::Disconnected);
        });
        self.stop = Some(stop_tx);
        let deadline = tokio::time::sleep(self.options.startup_timeout);
        tokio::pin!(deadline);
        let mut last_error = None;
        loop {
            tokio::select! {
                connected = connected_rx.recv() => {
                    connected.ok_or_else(|| Error::Protocol("connection closed before upstream connected".into()))?;
                    break;
                }
                error = error_rx.recv() => {
                    if let Some(error) = error {
                        last_error = Some(error);
                    }
                }
                _ = &mut deadline => {
                    let detail = last_error
                        .map(|error| format!(": {error}"))
                        .unwrap_or_default();
                    return Err(Error::Protocol(format!("connection timeout{detail}")));
                }
            }
        }
        tokio::select! {
            result = self.activate_tunnel(&info.connection_id) => result?,
            _ = &mut deadline => return Err(Error::Protocol("connection timeout during activation".into())),
        }
        self.connected = true;
        self.emit(TunnelEvent::Connected(info.clone()));
        self.start_sync_timer();
        Ok(info)
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.stop_sync_timer();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(true);
        }
        if self.options.cleanup_on_stop
            && let Some(info) = self.info.take()
        {
            let _ = self.delete_connection(&info.connection_id).await;
        }
        self.connected = false;
        Ok(())
    }

    pub async fn create_recipe(&self, name: Option<&str>) -> Result<String> {
        let Some(info) = &self.info else {
            return Err(Error::msg("tunnel is not started"));
        };
        let response = self
            .fetch_auth(
                reqwest::Method::POST,
                &format!("/mcp/connections/{}/create-recipe", info.connection_id),
                Some(serde_json::json!({
                    "name": name.unwrap_or(&self.options.name)
                })),
            )
            .await?;
        if !response.status().is_success() {
            return Err(Error::Api(format!(
                "failed to create recipe (HTTP {})",
                response.status()
            )));
        }
        let body = response.json::<Value>().await?;
        body.get("link")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| Error::Api("create-recipe response missing link".into()))
    }

    pub async fn sync_tools(&self) -> Result<usize> {
        let Some(info) = &self.info else {
            return Err(Error::msg("tunnel is not started"));
        };
        let response = self
            .fetch_auth(
                reqwest::Method::POST,
                &format!("/mcp/connections/{}/sync-tools", info.connection_id),
                None,
            )
            .await?;
        if !response.status().is_success() {
            let message = api::format_web_error("sync-tools", response).await;
            eprintln!("\x1b[2m[bridge] sync-tools failed (non-fatal): {message}\x1b[0m");
            return Err(Error::Api(message));
        }
        let body = response.json::<Value>().await?;
        parse_sync_tools_body(&body, &self.events, &self.handlers)
    }

    async fn activate_tunnel(&self, connection_id: &str) -> Result<()> {
        let response = self
            .fetch_auth(
                reqwest::Method::POST,
                &format!("/mcp/connections/{connection_id}/activate-tunnel"),
                None,
            )
            .await?;
        if response.status().is_success() {
            let body = response.json::<Value>().await?;
            if body.get("status").and_then(Value::as_str) == Some("oauth_required")
                && let Some(url) = body.get("authUrl").and_then(Value::as_str)
            {
                self.emit(TunnelEvent::OAuthRequired {
                    auth_url: url.to_string(),
                });
            }
        } else {
            let message = api::format_web_error("activate-tunnel", response).await;
            eprintln!("\x1b[2m[bridge] activate-tunnel failed (non-fatal): {message}\x1b[0m");
            let _ = self.sync_tools().await;
        }
        Ok(())
    }

    pub async fn delete_connection(&self, connection_id: &str) -> Result<()> {
        let _ = self
            .fetch_auth(
                reqwest::Method::DELETE,
                &format!("/mcp/connections/{connection_id}"),
                None,
            )
            .await?;
        Ok(())
    }

    async fn fetch_auth(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<reqwest::Response> {
        fetch_with_auth(FetchWithAuthOptions {
            path,
            method,
            body,
            token: self
                .options
                .token
                .clone()
                .or_else(|| Some(self.client.api_key().to_string())),
            base_url: self
                .options
                .base_url
                .clone()
                .or_else(|| Some(self.client.base_url().to_string())),
        })
        .await
    }

    async fn fetch_auth_json(&self, path: &str, body: Value) -> Result<Value> {
        let response = self
            .fetch_auth(reqwest::Method::POST, path, Some(body))
            .await?;
        if !response.status().is_success() {
            let message = api::format_web_error("tunnel", response).await;
            return Err(Error::Api(format!("failed to create tunnel: {message}")));
        }
        Ok(response.json().await?)
    }

    fn emit(&self, event: TunnelEvent) {
        emit_event(&self.events, &self.handlers, event);
    }

    fn start_sync_timer(&mut self) {
        if self.options.sync_interval.is_zero() {
            return;
        }
        let (sync_stop_tx, mut sync_stop_rx) = watch::channel(false);
        let interval = self.options.sync_interval;
        let events = self.events.clone();
        let handlers = Arc::clone(&self.handlers);
        let runner = SyncRunner {
            client: self.client.clone(),
            options: self.options.clone(),
            info: self.info.clone(),
        };
        self.sync_stop = Some(sync_stop_tx);
        self.sync_task = Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = sync_stop_rx.changed() => {
                        if *sync_stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        if let Some(info) = &runner.info
                            && let Ok(response) = runner
                                .fetch_auth(
                                    reqwest::Method::POST,
                                    &format!("/mcp/connections/{}/sync-tools", info.connection_id),
                                    None,
                                )
                                .await
                            && response.status().is_success()
                            && let Ok(body) = response.json::<Value>().await
                        {
                            let _ = parse_sync_tools_body(&body, &events, &handlers);
                        }
                    }
                }
            }
        }));
    }

    fn stop_sync_timer(&mut self) {
        if let Some(sync_stop) = self.sync_stop.take() {
            let _ = sync_stop.send(true);
        }
        if let Some(task) = self.sync_task.take() {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct SyncRunner {
    client: Poke,
    options: TunnelOptions,
    info: Option<TunnelInfo>,
}

impl SyncRunner {
    async fn fetch_auth(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<reqwest::Response> {
        fetch_with_auth(FetchWithAuthOptions {
            path,
            method,
            body,
            token: self
                .options
                .token
                .clone()
                .or_else(|| Some(self.client.api_key().to_string())),
            base_url: self
                .options
                .base_url
                .clone()
                .or_else(|| Some(self.client.base_url().to_string())),
        })
        .await
    }
}

fn emit_event(
    events: &broadcast::Sender<TunnelEvent>,
    handlers: &Arc<Mutex<HashMap<String, Vec<TunnelHandler>>>>,
    event: TunnelEvent,
) {
    let key = match &event {
        TunnelEvent::Connected(_) => "connected",
        TunnelEvent::Disconnected => "disconnected",
        TunnelEvent::Error(_) => "error",
        TunnelEvent::ToolsSynced { .. } => "toolsSynced",
        TunnelEvent::OAuthRequired { .. } => "oauthRequired",
        TunnelEvent::Created(_) => "created",
    };
    if let Ok(guard) = handlers.lock()
        && let Some(list) = guard.get(key)
    {
        for handler in list {
            handler(&event);
        }
    }
    let _ = events.send(event);
}

fn parse_sync_tools_body(
    body: &Value,
    events: &broadcast::Sender<TunnelEvent>,
    handlers: &Arc<Mutex<HashMap<String, Vec<TunnelHandler>>>>,
) -> Result<usize> {
    if body
        .get("requiresOAuth")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && let Some(url) = body.get("oauthUrl").and_then(Value::as_str)
    {
        emit_event(
            events,
            handlers,
            TunnelEvent::OAuthRequired {
                auth_url: url.to_string(),
            },
        );
        return Ok(0);
    }
    let count = parse_tool_count(body);
    if count == 0 {
        eprintln!("\x1b[2m[bridge] sync-tools response: {body}\x1b[0m");
        if let Some(status) = body.get("status").and_then(Value::as_str) {
            eprintln!("\x1b[2m[bridge] sync-tools returned 0 tools (status: {status})\x1b[0m");
        }
    }
    emit_event(
        events,
        handlers,
        TunnelEvent::ToolsSynced { tool_count: count },
    );
    Ok(count)
}

fn parse_tool_count(body: &Value) -> usize {
    if body
        .get("requiresOAuth")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return 0;
    }
    for key in ["toolCount", "count", "numTools"] {
        if let Some(count) = body.get(key).and_then(Value::as_u64) {
            return count as usize;
        }
    }
    body.get("tools")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0)
}

fn local_addr(url: &str) -> Result<String> {
    let url = url::Url::parse(url)?;
    let host = url
        .host_str()
        .ok_or_else(|| Error::Protocol("local url missing host".into()))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| Error::Protocol("local url missing port".into()))?;
    Ok(format!("{host}:{port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_local_addr_from_mcp_url() {
        assert_eq!(
            local_addr("http://127.0.0.1:52333/mcp").expect("local addr"),
            "127.0.0.1:52333"
        );
    }

    #[test]
    fn default_tunnel_options_use_thirty_second_startup_timeout() {
        assert_eq!(
            TunnelOptions::default().startup_timeout,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn parse_sync_tools_body_counts_tools() {
        let (events, _) = broadcast::channel(1);
        let handlers = Arc::new(Mutex::new(HashMap::new()));
        let body = serde_json::json!({
            "tools": [{ "name": "run_command" }, { "name": "read_file" }]
        });
        let count = parse_sync_tools_body(&body, &events, &handlers).expect("tools parse");
        assert_eq!(count, 2);
    }

    #[test]
    fn parse_sync_tools_body_emits_oauth_required() {
        let (events, mut rx) = broadcast::channel(1);
        let handlers = Arc::new(Mutex::new(HashMap::new()));
        let body = serde_json::json!({
            "requiresOAuth": true,
            "oauthUrl": "https://poke.com/oauth"
        });
        let count = parse_sync_tools_body(&body, &events, &handlers).expect("oauth parse");
        assert_eq!(count, 0);
        match rx.try_recv().expect("oauth event") {
            TunnelEvent::OAuthRequired { auth_url } => {
                assert_eq!(auth_url, "https://poke.com/oauth");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}