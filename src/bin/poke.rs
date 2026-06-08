use clap::{Parser, Subcommand};
use rs_poke::{
    CredentialsStore, FetchWithAuthOptions, LoginOptions, Poke, PokeOptions, TunnelOptions,
    TunnelRunner, fetch_with_auth, get_token, is_logged_in, login, logout,
};
#[derive(Parser)]
#[command(name = "poke", about = "Poke CLI - Create tunnels to expose local servers")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Login,
    Logout,
    Whoami,
    Mcp {
        #[command(subcommand)]
        command: McpCommands,
    },
}

#[derive(Subcommand)]
enum McpCommands {
    Add {
        url: String,
        #[arg(short = 'n', long = "name")]
        name: String,
        #[arg(long = "recipe")]
        recipe: bool,
        #[arg(long = "client-id")]
        client_id: Option<String>,
        #[arg(long = "client-secret")]
        client_secret: Option<String>,
        #[arg(short = 'k', long = "api-key")]
        api_key: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse().command {
        Commands::Login => cmd_login().await?,
        Commands::Logout => cmd_logout().await?,
        Commands::Whoami => cmd_whoami().await?,
        Commands::Mcp { command } => match command {
            McpCommands::Add {
                url,
                name,
                recipe,
                client_id,
                client_secret,
                api_key,
            } => {
                cmd_mcp_add(url, name, recipe, client_id, client_secret, api_key).await?;
            }
        },
    }
    Ok(())
}

async fn cmd_login() -> Result<(), Box<dyn std::error::Error>> {
    let store = CredentialsStore::default_store()?;
    let options = LoginOptions::new(store).on_code(|info| {
        println!("Visit {} and enter code {}", info.login_url, info.user_code);
    });
    login(options).await?;
    println!("Logged in.");
    Ok(())
}

async fn cmd_logout() -> Result<(), Box<dyn std::error::Error>> {
    logout().await?;
    println!("Logged out.");
    Ok(())
}

async fn cmd_whoami() -> Result<(), Box<dyn std::error::Error>> {
    if !is_logged_in() {
        return Err("Not logged in. Run 'poke login'.".into());
    }
    let response = fetch_with_auth(FetchWithAuthOptions {
        path: "/user/profile",
        method: reqwest::Method::GET,
        body: None,
        token: None,
        base_url: None,
        client: None,
    })
    .await?;
    if !response.status().is_success() {
        return Err("Failed to fetch profile".into());
    }
    let body = response.json::<serde_json::Value>().await?;
    let label = body
        .get("name")
        .or_else(|| body.get("email"))
        .or_else(|| body.get("id"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Logged in");
    println!("{label}");
    Ok(())
}

async fn cmd_mcp_add(
    url: String,
    name: String,
    recipe: bool,
    client_id: Option<String>,
    client_secret: Option<String>,
    api_key: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let token = api_key
        .or_else(|| get_token().ok().flatten())
        .ok_or("Not logged in. Run 'poke login'.")?;
    let poke = Poke::new(PokeOptions {
        api_key: Some(token),
        ..PokeOptions::default()
    })?;
    let mut runner = TunnelRunner::new(
        poke,
        TunnelOptions {
            url,
            name: name.clone(),
            client_id,
            client_secret,
            cleanup_on_stop: true,
            ..TunnelOptions::default()
        },
    );
    let info = runner.start().await?;
    println!("Tunnel is active!");
    println!("  Name:  {}", info.name);
    println!("  Local: {}", info.local_url);
    println!("  URL:   {}", info.tunnel_url);
    if recipe {
        let link = runner.create_recipe(Some(&name)).await?;
        println!("  Recipe: {link}");
    }
    println!("Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    runner.stop().await?;
    Ok(())
}