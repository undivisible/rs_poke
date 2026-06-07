use crate::api::Poke;
use crate::piko::{PikoConfig, run_client};
use crate::{Error, Result};
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};

#[derive(Clone, Debug)]
pub struct TunnelOptions {
    pub url: String,
    pub name: String,
    pub cleanup_on_stop: bool,
    pub sync_interval: Duration,
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
    Connected(TunnelInfo),
    Disconnected,
    ToolsSynced { tool_count: usize },
    OAuthRequired { auth_url: String },
    Error(String),
}

pub struct TunnelRunner {
    client: Poke,
    options: TunnelOptions,
    events: broadcast::Sender<TunnelEvent>,
    stop: Option<watch::Sender<bool>>,
    info: Option<TunnelInfo>,
}

impl TunnelRunner {
    pub fn new(client: Poke, options: TunnelOptions) -> Self {
        let (events, _) = broadcast::channel(64);
        Self {
            client,
            options,
            events,
            stop: None,
            info: None,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TunnelEvent> {
        self.events.subscribe()
    }

    pub fn info(&self) -> Option<&TunnelInfo> {
        self.info.as_ref()
    }

    pub async fn start(&mut self) -> Result<TunnelInfo> {
        let response: CreateConnectionResponse = self
            .client
            .post_json(
                "/mcp/connections/cli",
                &serde_json::json!({
                    "name": self.options.name,
                    "serverUrl": self.options.url,
                    "tunnel": true
                }),
            )
            .await?;
        let local_addr = local_addr(&self.options.url)?;
        let (stop_tx, stop_rx) = watch::channel(false);
        let (connected_tx, mut connected_rx) = mpsc::unbounded_channel();
        let config = PikoConfig {
            upstream_url: response.tunnel.upstream_url,
            endpoint_id: response.id.clone(),
            token: response.tunnel.token,
            local_addr,
            connected: Some(connected_tx),
        };
        let events = self.events.clone();
        tokio::spawn(async move {
            if let Err(err) = run_client(config, stop_rx).await {
                let _ = events.send(TunnelEvent::Error(err.to_string()));
            }
            let _ = events.send(TunnelEvent::Disconnected);
        });
        self.stop = Some(stop_tx);
        tokio::time::timeout(Duration::from_secs(30), connected_rx.recv())
            .await
            .map_err(|_| Error::Protocol("connection timeout".into()))?
            .ok_or_else(|| Error::Protocol("connection closed before upstream connected".into()))?;
        let info = TunnelInfo {
            connection_id: response.id,
            tunnel_url: response.server_url,
            local_url: self.options.url.clone(),
            name: self.options.name.clone(),
        };
        self.activate_tunnel(&info.connection_id).await?;
        self.info = Some(info.clone());
        let _ = self.events.send(TunnelEvent::Connected(info.clone()));
        Ok(info)
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(true);
        }
        if self.options.cleanup_on_stop
            && let Some(info) = self.info.take()
        {
            let _ = self.delete_connection(&info.connection_id).await;
        }
        Ok(())
    }

    pub async fn sync_tools(&self) -> Result<usize> {
        let Some(info) = &self.info else {
            return Err(Error::msg("tunnel is not started"));
        };
        let response = self
            .client
            .raw_auth(
                reqwest::Method::POST,
                &format!("/mcp/connections/{}/sync-tools", info.connection_id),
                None,
            )
            .await?;
        if response.status().is_success() {
            let body = response.json::<Value>().await?;
            if body
                .get("requiresOAuth")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                && let Some(url) = body.get("oauthUrl").and_then(Value::as_str)
            {
                let _ = self.events.send(TunnelEvent::OAuthRequired {
                    auth_url: url.to_string(),
                });
                return Ok(0);
            }
            let count = body
                .get("tools")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or(0);
            let _ = self
                .events
                .send(TunnelEvent::ToolsSynced { tool_count: count });
            Ok(count)
        } else {
            Ok(0)
        }
    }

    async fn activate_tunnel(&self, connection_id: &str) -> Result<()> {
        let response = self
            .client
            .raw_auth(
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
                let _ = self.events.send(TunnelEvent::OAuthRequired {
                    auth_url: url.to_string(),
                });
            }
        } else {
            let _ = self.sync_tools().await;
        }
        Ok(())
    }

    pub async fn delete_connection(&self, connection_id: &str) -> Result<()> {
        let _ = self
            .client
            .raw_auth(
                reqwest::Method::DELETE,
                &format!("/mcp/connections/{connection_id}"),
                None,
            )
            .await?;
        Ok(())
    }
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
}
