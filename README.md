# rs_poke

Rust client and tunnel bridge for Poke.

A library for interacting with the Poke API, providing both a synchronous HTTP client interface and an asynchronous WebSocket tunnel for MCP (Model Context Protocol) connections.

## Features

- **HTTP Client**: Authenticated API client for Poke services with automatic token management
- **Tunnel Bridge**: WebSocket-based tunnel for MCP protocol connections
- **Webhook Support**: Create and manage webhooks programmatically
- **Message Passing**: Send messages through the Poke API
- **Async-First**: Built on Tokio for efficient async/await patterns

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rs_poke = "0.1.3"
```

## Usage

### HTTP Client

```rust
use rs_poke::{Poke, PokeOptions};

async fn example() -> Result<(), Box<dyn std::error::Error> {
    let client = Poke::new(PokeOptions::default())?;

    // Send a message
    let response = client.send_message("Hello, world!").await?;

    // Create a webhook
    let webhook = client.create_webhook(&rs_poke::CreateWebhook {
        condition: "event.type == 'trigger'",
        action: "POST",
    }).await?;

    Ok(())
}
```

### Tunnel Bridge

```rust
use rs_poke::{Poke, TunnelOptions, TunnelRunner};
use tokio::sync::broadcast;

async fn tunnel_example() -> Result<(), Box<dyn std::error::Error> {
    let poke = Poke::new(rs_poke::PokeOptions::default())?;
    let mut runner = TunnelRunner::new(poke, TunnelOptions {
        url: "http://127.0.0.1:52333/mcp".into(),
        name: "my-tunnel".into(),
        cleanup_on_stop: true,
        sync_interval: std::time::Duration::from_secs(30),
        startup_timeout: std::time::Duration::from_secs(30),
    });

    // Start the tunnel
    let info = runner.start().await?;

    // Subscribe to events
    let mut events = runner.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            match event {
                rs_poke::TunnelEvent::Connected(info) => {
                    println!("Tunnel connected: {}", info.tunnel_url);
                }
                rs_poke::TunnelEvent::Disconnected => {
                    println!("Tunnel disconnected");
                }
                _ => {}
            }
        }
    });

    Ok(())
}
```

### Authentication

```rust
use rs_poke::{login, logout, is_logged_in};

async fn auth_example() -> Result<(), Box<dyn std::error::Error>> {
    // Login with credentials
    login(rs_poke::LoginOptions {
        api_key: Some("your-api-key".into()),
        base_url: Some("https://poke.com/api/v1".into()),
    }).await?;

    // Check auth status
    if is_logged_in().await {
        println!("Already authenticated");
    }

    // Logout
    logout().await?;

    Ok(())
}
```

## Configuration

### Environment Variables

- `POKE_API_URL` - Base URL for Poke API (default: `https://poke.com/api/v1`)
- `POKE_API_KEY` - API key for authentication

### Credentials Store

Credentials are stored in the user's configuration directory:
- **Linux**: `~/.config/rs_poke/credentials.json`
- **macOS**: `~/Library/Application Support/rs_poke/credentials.json`
- **Windows**: `%APPDATA%\rs_poke\credentials.json`

## API Reference

### `Poke`

Main API client struct.

- `new(options: PokeOptions)` - Create a new client
- `api_key()` - Get the configured API key
- `send_message(message: &str)` - Send a message to Poke
- `create_webhook(request: CreateWebhook)` - Create a new webhook
- `send_webhook(url, token, data)` - Send data to a webhook endpoint

### `TunnelRunner`

Manages MCP tunnel connections.

- `new(client: Poke, options: TunnelOptions)` - Create a runner
- `start()` - Start the tunnel connection
- `stop()` - Stop and optionally cleanup the tunnel
- `sync_tools()` - Synchronize MCP tools
- `subscribe()` - Subscribe to tunnel events

## Development

```bash
# Clone the repository
git clone https://github.com/undivisible/rs_poke

# Run tests
cargo test

# Run with linting
cargo clippy --all-targets --all-features

# Format code
cargo fmt
```

## License

Licensed under the Mozilla Public License 2.0. See [LICENSE](LICENSE) for details.
