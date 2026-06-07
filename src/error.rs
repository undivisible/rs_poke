use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("not logged in")]
    NotLoggedIn,
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("api error: {0}")]
    Api(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("url error: {0}")]
    Url(#[from] url::ParseError),
    #[error("websocket error: {0}")]
    WebSocket(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("{0}")]
    Message(String),
}

impl Error {
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
