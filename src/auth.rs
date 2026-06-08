use crate::api::auth_error;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DEFAULT_API: &str = "https://poke.com/api/v1";
const DEFAULT_FRONTEND: &str = "https://poke.com";

/// Stored API credentials.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Token {
    /// Bearer token value.
    pub token: String,
}

/// JSON credentials file backed by a filesystem path.
#[derive(Clone, Debug)]
pub struct CredentialsStore {
    path: PathBuf,
}

impl CredentialsStore {
    /// Resolve the default credentials file path under the config directory.
    pub fn default_path() -> Result<PathBuf> {
        let base = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| dirs::home_dir().map(|home| home.join(".config")))
            .ok_or(Error::NotLoggedIn)?;
        Ok(base.join("poke").join("credentials.json"))
    }

    /// Create a store that reads and writes credentials at `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Create a store using [`Self::default_path`].
    pub fn default_store() -> Result<Self> {
        Ok(Self::new(Self::default_path()?))
    }

    /// Read stored credentials, returning `None` when the file is absent.
    pub fn read(&self) -> Result<Option<Token>> {
        match std::fs::read_to_string(&self.path) {
            Ok(data) => Ok(Some(serde_json::from_str(&data)?)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Persist a token to disk with restrictive permissions on Unix.
    pub fn write(&self, token: &str) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(
            &self.path,
            serde_json::to_string_pretty(&Token {
                token: token.to_string(),
            })?,
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    /// Delete the credentials file if it exists.
    pub fn remove(&self) -> Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

/// Return the Poke config directory (`~/.config/poke` or `$XDG_CONFIG_HOME/poke`).
pub fn config_dir() -> Result<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".config")))
        .ok_or(Error::NotLoggedIn)?;
    Ok(base.join("poke"))
}

/// Return the default credentials file path.
pub fn credentials_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("credentials.json"))
}

/// Save credentials to the default store.
pub fn save_credentials(token: &str) -> Result<()> {
    CredentialsStore::default_store()?.write(token)
}

/// Load credentials from the default store.
pub fn load_credentials() -> Result<Option<Token>> {
    CredentialsStore::default_store()?.read()
}

/// Delete credentials from the default store.
pub fn delete_credentials() -> Result<()> {
    CredentialsStore::default_store()?.remove()
}

/// Device-login code and URL shown to the user during CLI auth.
#[derive(Clone, Debug)]
pub struct LoginCodeInfo {
    /// Short code the user enters in the browser.
    pub user_code: String,
    /// Full device-login URL.
    pub login_url: String,
}

/// Options for [`login`] and [`login_fresh`].
pub struct LoginOptions {
    /// API base URL for device-login endpoints.
    pub api_base: String,
    /// Frontend base URL used to build the device-login page.
    pub frontend_base: String,
    /// Whether to open the device-login URL in a browser.
    pub open_browser: bool,
    /// Maximum time to wait for the user to complete login.
    pub timeout: Duration,
    /// Delay between poll requests while waiting for login.
    pub poll_interval: Duration,
    /// Credentials store used to persist the resulting token.
    pub store: CredentialsStore,
    /// Skip cached credentials and always start a new device-login flow.
    pub force_new: bool,
    on_code: Option<Box<dyn Fn(LoginCodeInfo) + Send + Sync>>,
}

impl LoginOptions {
    /// Create login options with API and frontend defaults.
    pub fn new(store: CredentialsStore) -> Self {
        Self {
            api_base: std::env::var("POKE_API").unwrap_or_else(|_| DEFAULT_API.to_string()),
            frontend_base: std::env::var("POKE_FRONTEND")
                .unwrap_or_else(|_| DEFAULT_FRONTEND.to_string()),
            open_browser: true,
            timeout: Duration::from_secs(300),
            poll_interval: Duration::from_secs(2),
            store,
            force_new: false,
            on_code: None,
        }
    }

    /// Register a callback invoked when the device-login code is available.
    pub fn on_code<F>(mut self, handler: F) -> Self
    where
        F: Fn(LoginCodeInfo) + Send + Sync + 'static,
    {
        self.on_code = Some(Box::new(handler));
        self
    }
}

/// Successful CLI login result.
#[derive(Clone, Debug)]
pub struct LoginResult {
    /// Authenticated API token.
    pub token: String,
}

#[derive(Deserialize)]
struct LoginCode {
    #[serde(rename = "deviceCode")]
    device_code: String,
    #[serde(rename = "userCode")]
    user_code: String,
}

#[derive(Deserialize)]
struct PollResponse {
    status: String,
    token: Option<String>,
}

/// Load the stored API token, if any.
pub fn get_token() -> Result<Option<String>> {
    Ok(CredentialsStore::default_store()?
        .read()?
        .map(|token| token.token))
}

/// Return whether credentials are stored locally.
pub fn is_logged_in() -> bool {
    get_token().ok().flatten().is_some()
}

/// Authenticate via device login, reusing stored credentials when present.
pub async fn login(options: LoginOptions) -> Result<LoginResult> {
    if !options.force_new
        && let Some(token) = options.store.read()?
    {
        return Ok(LoginResult {
            token: token.token,
        });
    }
    if options.force_new {
        options.store.remove()?;
    }
    let client = reqwest::Client::new();
    let code = client
        .post(format!("{}/cli-auth/code", options.api_base))
        .send()
        .await?
        .error_for_status()?
        .json::<LoginCode>()
        .await?;
    let login_url = format!(
        "{}/device?code={}",
        options.frontend_base,
        url::form_urlencoded::byte_serialize(code.user_code.as_bytes()).collect::<String>()
    );
    if let Some(on_code) = &options.on_code {
        on_code(LoginCodeInfo {
            user_code: code.user_code.clone(),
            login_url: login_url.clone(),
        });
    }
    if options.open_browser {
        open_browser(&login_url);
    }
    let deadline = Instant::now() + options.timeout;
    while Instant::now() < deadline {
        tokio::time::sleep(options.poll_interval).await;
        let response = client
            .get(format!(
                "{}/cli-auth/poll/{}",
                options.api_base, code.device_code
            ))
            .send()
            .await?
            .error_for_status()?
            .json::<PollResponse>()
            .await?;
        match response.status.as_str() {
            "authenticated" => {
                let token = response
                    .token
                    .ok_or_else(|| auth_error("login response did not include a token"))?;
                options.store.write(&token)?;
                return Ok(LoginResult { token });
            }
            "expired" => return Err(auth_error("login code expired")),
            "invalid" => return Err(auth_error("invalid login code")),
            _ => {}
        }
    }
    Err(auth_error("login timed out"))
}

/// Clear stored credentials and start a fresh device-login flow.
pub async fn login_fresh(options: LoginOptions) -> Result<LoginResult> {
    let mut options = options;
    options.force_new = true;
    login(options).await
}

/// Delete stored credentials.
pub async fn logout() -> Result<()> {
    delete_credentials()
}

fn open_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let command = ("open", vec![url]);
    #[cfg(target_os = "windows")]
    let command = ("cmd", vec!["/C", "start", "", url]);
    #[cfg(all(unix, not(target_os = "macos")))]
    let command = ("xdg-open", vec![url]);
    let _ = std::process::Command::new(command.0)
        .args(command.1)
        .spawn();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_store_round_trips_token() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = CredentialsStore::new(dir.path().join("credentials.json"));
        store.write("pk_test").expect("write token");
        assert_eq!(
            store.read().expect("read token").expect("token").token,
            "pk_test"
        );
        store.remove().expect("remove token");
        assert!(store.read().expect("read after remove").is_none());
    }
}