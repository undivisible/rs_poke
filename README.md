# rs_poke

Rust client and tunnel bridge for Poke.

A library and CLI for interacting with the Poke API, providing both an HTTP client interface and an asynchronous WebSocket tunnel for MCP (Model Context Protocol) connections. API parity with the official `poke@0.4.2` npm SDK.

## Features

- **HTTP Client**: Authenticated API client for Poke services with automatic token management
- **Tunnel Bridge**: WebSocket-based tunnel for MCP protocol connections
- **Webhook Support**: Create and manage webhooks programmatically
- **Message Passing**: Send messages through the Poke API
- **CLI**: `poke login`, `poke logout`, `poke whoami`, `poke mcp add`
- **Async-First**: Built on Tokio for efficient async/await patterns

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rs_poke = "0.2.1"
```

Or install the CLI:

```bash
cargo install rs_poke
```

## Usage

### HTTP Client

```rust
use rs_poke::{Poke, PokeOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Poke::new(PokeOptions::default())?;

    let response = client.send_message("Hello, world!").await?;
    println!("{}", response.message);

    let webhook = client.create_webhook(&rs_poke::CreateWebhook {
        condition: "event.type == 'trigger'",
        action: "POST",
    }).await?;
    println!("trigger: {}", webhook.trigger_id);

    Ok(())
}
```

### Tunnel Bridge

```rust
use rs_poke::{Poke, TunnelOptions, TunnelRunner};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let poke = Poke::new(rs_poke::PokeOptions::default())?;
    let mut runner = TunnelRunner::new(poke, TunnelOptions {
        url: "http://127.0.0.1:52333/mcp".into(),
        name: "my-tunnel".into(),
        cleanup_on_stop: true,
        sync_interval: std::time::Duration::from_secs(300),
        startup_timeout: std::time::Duration::from_secs(30),
        ..TunnelOptions::default()
    });

    let info = runner.start().await?;
    println!("Tunnel connected: {}", info.tunnel_url);

    let mut events = runner.subscribe();
    while let Ok(event) = events.recv().await {
        match event {
            rs_poke::TunnelEvent::Connected(info) => {
                println!("Connected: {}", info.tunnel_url);
            }
            rs_poke::TunnelEvent::Disconnected => break,
            _ => {}
        }
    }

    runner.stop().await?;
    Ok(())
}
```

### Authentication

```rust
use rs_poke::{login, logout, is_logged_in, CredentialsStore, LoginOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = CredentialsStore::default_store()?;
    let result = login(
        LoginOptions::new(store).on_code(|info| {
            println!("Open {} and enter {}", info.login_url, info.user_code);
        }),
    )
    .await?;

    if is_logged_in() {
        println!("Authenticated as {}", result.token);
    }

    logout().await?;
    Ok(())
}
```

## Configuration

### Environment Variables

- `POKE_API` - Base URL for Poke API (default: `https://poke.com/api/v1`)
- `POKE_API_KEY` - API key for authentication
- `POKE_FRONTEND` - Frontend URL for device login (default: `https://poke.com`)

### Credentials Store

Credentials are stored in the user's configuration directory:
- **Linux**: `~/.config/poke/credentials.json`
- **macOS**: `~/Library/Application Support/poke/credentials.json` (via XDG_CONFIG_HOME or `~/.config/poke`)
- **Windows**: `%APPDATA%\poke\credentials.json` (via XDG_CONFIG_HOME)

## API Reference

### `Poke`

- `new(options: PokeOptions)` - Create a new client
- `api_key()` / `base_url()` - Get configured credentials and endpoint
- `send_message(message)` - Send a message to Poke
- `create_webhook(request)` - Create a new webhook (returns `trigger_id`)
- `send_webhook(url, token, data)` - Send data to a webhook endpoint
- `post_json(path, body)` - Authenticated JSON POST helper
- `raw_auth(method, path, body)` - Low-level authenticated request

### `TunnelRunner`

- `new(client, options)` - Create a runner
- `start()` - Start the tunnel connection
- `stop()` - Stop and optionally cleanup the tunnel
- `sync_tools()` - Synchronize MCP tools
- `create_recipe(name)` - Create a shareable recipe link
- `connected()` - Whether the tunnel is active
- `subscribe()` / `on(event, handler)` / `off(event)` - Event handling

### CLI

```bash
poke login
poke logout
poke whoami
poke mcp add http://127.0.0.1:52333/mcp -n my-server --recipe
```

## Development

```bash
cargo test
cargo clippy --all-targets --all-features
cargo fmt
```

## License

Licensed under the Mozilla Public License 2.0. See [LICENSE](LICENSE) for details.