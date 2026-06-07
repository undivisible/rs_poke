use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DEFAULT_API: &str = "https://poke.com/api/v1";
const DEFAULT_FRONTEND: &str = "https://poke.com";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Token {
    pub token: String,
}

#[derive(Clone, Debug)]
pub struct CredentialsStore {
    path: PathBuf,
}

impl CredentialsStore {
    pub fn default_path() -> Result<PathBuf> {
        let base = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| dirs::home_dir().map(|home| home.join(".config")))
            .ok_or(Error::NotLoggedIn)?;
        Ok(base.join("poke").join("credentials.json"))
    }

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn default_store() -> Result<Self> {
        Ok(Self::new(Self::default_path()?))
    }

    pub fn read(&self) -> Result<Option<Token>> {
        match std::fs::read_to_string(&self.path) {
            Ok(data) => Ok(Some(serde_json::from_str(&data)?)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

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

    pub fn remove(&self) -> Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoginOptions {
    pub api_base: String,
    pub frontend_base: String,
    pub open_browser: bool,
    pub timeout: Duration,
    pub poll_interval: Duration,
    pub store: CredentialsStore,
}

impl LoginOptions {
    pub fn new(store: CredentialsStore) -> Self {
        Self {
            api_base: std::env::var("POKE_API").unwrap_or_else(|_| DEFAULT_API.to_string()),
            frontend_base: std::env::var("POKE_FRONTEND")
                .unwrap_or_else(|_| DEFAULT_FRONTEND.to_string()),
            open_browser: true,
            timeout: Duration::from_secs(300),
            poll_interval: Duration::from_secs(2),
            store,
        }
    }
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

pub fn get_token() -> Result<Option<String>> {
    Ok(CredentialsStore::default_store()?
        .read()?
        .map(|token| token.token))
}

pub fn is_logged_in() -> bool {
    get_token().ok().flatten().is_some()
}

pub async fn login(options: LoginOptions) -> Result<String> {
    if let Some(token) = options.store.read()? {
        return Ok(token.token);
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
                    .ok_or_else(|| Error::Auth("login response did not include a token".into()))?;
                options.store.write(&token)?;
                return Ok(token);
            }
            "expired" => return Err(Error::Auth("login code expired".into())),
            "invalid" => return Err(Error::Auth("invalid login code".into())),
            _ => {}
        }
    }
    Err(Error::Auth("login timed out".into()))
}

pub fn logout() -> Result<()> {
    CredentialsStore::default_store()?.remove()
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
