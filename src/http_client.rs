use std::sync::OnceLock;

/// Shared HTTP client reused across API calls.
pub(crate) fn shared_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}