use thiserror::Error;

/// Result type used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the Poke client and tunnel bridge.
#[derive(Debug, Error)]
pub enum Error {
    /// No stored credentials or explicit API key were available.
    #[error("not logged in")]
    NotLoggedIn,
    /// Authentication failed or the session is no longer valid.
    #[error("authentication failed: {0}")]
    Auth(String),
    /// The API rejected the request or returned an unexpected payload.
    #[error("api error: {0}")]
    Api(String),
    /// An HTTP transport error occurred.
    #[error("http error: {0}")]
    Http(String),
    /// A filesystem error occurred.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization or deserialization failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// URL parsing failed.
    #[error("url error: {0}")]
    Url(#[from] url::ParseError),
    /// The tunnel websocket reported an error.
    #[error("websocket error: {0}")]
    WebSocket(String),
    /// Tunnel protocol state was invalid or timed out.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// A generic error with a user-facing message.
    #[error("{0}")]
    Message(String),
}

impl Error {
    /// Create a generic message error.
    pub fn msg(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value.to_string())
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocket(value.to_string())
    }
}
