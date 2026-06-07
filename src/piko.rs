use crate::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use http::HeaderValue;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, watch};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

const HEADER_LEN: usize = 12;
const DEFAULT_WINDOW: u32 = 256 * 1024;
const MAX_RESPONSE_BODY: usize = 64 * 1024 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum FrameType {
    Data = 0,
    WindowUpdate = 1,
    Ping = 2,
    GoAway = 3,
}

impl TryFrom<u8> for FrameType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::WindowUpdate),
            2 => Ok(Self::Ping),
            3 => Ok(Self::GoAway),
            _ => Err(Error::Protocol(format!("unknown frame type {value}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Flags(u16);

impl Flags {
    const SYN: Self = Self(1);
    const ACK: Self = Self(2);
    const FIN: Self = Self(4);
    const RST: Self = Self(8);

    fn contains(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }

    fn without(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FrameHeader {
    frame_type: FrameType,
    flags: Flags,
    stream_id: u32,
    length: u32,
}

impl FrameHeader {
    fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(HEADER_LEN);
        bytes.put_u8(0);
        bytes.put_u8(self.frame_type as u8);
        bytes.put_u16(self.flags.0);
        bytes.put_u32(self.stream_id);
        bytes.put_u32(self.length);
        bytes.freeze()
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::Protocol("buffer too small for frame header".into()));
        }
        if bytes[0] != 0 {
            return Err(Error::Protocol(format!(
                "unsupported frame version {}",
                bytes[0]
            )));
        }
        Ok(Self {
            frame_type: FrameType::try_from(bytes[1])?,
            flags: Flags(u16::from_be_bytes([bytes[2], bytes[3]])),
            stream_id: u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            length: u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PikoConfig {
    pub upstream_url: String,
    pub endpoint_id: String,
    pub token: String,
    pub local_addr: String,
    pub connected: Option<mpsc::UnboundedSender<()>>,
}

pub(crate) fn upstream_ws_url(upstream_url: &str, endpoint_id: &str) -> String {
    let base = upstream_url
        .trim_end_matches('/')
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    format!("{base}/piko/v1/upstream/{endpoint_id}")
}

pub(crate) fn retry_delay(attempt: u32) -> Duration {
    let base = (100_u64.saturating_mul(2_u64.saturating_pow(attempt))).min(15_000);
    let jitter = rand::rng().random_range(-0.3_f64..=0.3_f64);
    Duration::from_millis(((base as f64) + (base as f64 * jitter)).max(100.0) as u64)
}

pub(crate) async fn run_client(config: PikoConfig, mut stop: watch::Receiver<bool>) -> Result<()> {
    let mut attempt = 0;
    while !*stop.borrow() {
        let result = tokio::select! {
            _ = stop.changed() => return Ok(()),
            result = connect_and_serve(config.clone()) => result,
        };
        match result {
            Ok(()) => attempt = 0,
            Err(err) if is_retriable(&err) => {
                attempt += 1;
                tokio::select! {
                    _ = stop.changed() => return Ok(()),
                    _ = tokio::time::sleep(retry_delay(attempt)) => {}
                }
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn is_retriable(error: &Error) -> bool {
    match error {
        Error::Auth(_) => false,
        Error::Api(message) => {
            let lower = message.to_lowercase();
            !(lower.contains("401")
                || lower.contains("403")
                || lower.contains("unauthorized")
                || lower.contains("forbidden"))
        }
        _ => true,
    }
}

async fn connect_and_serve(config: PikoConfig) -> Result<()> {
    let mut request =
        upstream_ws_url(&config.upstream_url, &config.endpoint_id).into_client_request()?;
    request.headers_mut().insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", config.token))
            .map_err(|err| Error::Protocol(err.to_string()))?,
    );
    let (ws, _) = connect_async(request).await?;
    if let Some(connected) = &config.connected {
        let _ = connected.send(());
    }
    let (write, read) = ws.split();
    let write = Arc::new(Mutex::new(write));
    let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<(FrameHeader, Bytes)>();
    tokio::spawn(async move {
        let mut read = read;
        while let Some(message) = read.next().await {
            match message {
                Ok(Message::Binary(bytes)) if bytes.len() >= HEADER_LEN => {
                    let Ok(header) = FrameHeader::decode(&bytes[..HEADER_LEN]) else {
                        break;
                    };
                    let payload = bytes.slice(HEADER_LEN..);
                    if frame_tx.send((header, payload)).is_err() {
                        break;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });
    let streams = Arc::new(Mutex::new(HashMap::<u32, StreamState>::new()));
    while let Some((header, payload)) = frame_rx.recv().await {
        match header.frame_type {
            FrameType::Ping if header.flags.contains(Flags::SYN) => {
                send_frame(
                    &write,
                    FrameHeader {
                        frame_type: FrameType::Ping,
                        flags: Flags::ACK,
                        stream_id: 0,
                        length: header.length,
                    },
                    Bytes::new(),
                )
                .await?;
            }
            FrameType::GoAway => return Ok(()),
            FrameType::Data | FrameType::WindowUpdate => {
                handle_stream_frame(&config, &write, &streams, header, payload).await?;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn handle_stream_frame<W>(
    config: &PikoConfig,
    write: &Arc<Mutex<W>>,
    streams: &Arc<Mutex<HashMap<u32, StreamState>>>,
    header: FrameHeader,
    payload: Bytes,
) -> Result<()>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin + Send + 'static,
{
    if header.flags.contains(Flags::SYN) {
        let (tx, rx) = mpsc::unbounded_channel();
        streams
            .lock()
            .await
            .insert(header.stream_id, StreamState { tx });
        let local_addr = config.local_addr.clone();
        let write = Arc::clone(write);
        let streams = Arc::clone(streams);
        let stream_id = header.stream_id;
        send_frame(
            &write,
            FrameHeader {
                frame_type: FrameType::WindowUpdate,
                flags: Flags::ACK,
                stream_id,
                length: 0,
            },
            Bytes::new(),
        )
        .await?;
        tokio::spawn(async move {
            let _ = proxy_stream(stream_id, local_addr, write, rx).await;
            streams.lock().await.remove(&stream_id);
        });
    }
    let flags = header.flags.without(Flags::SYN);
    if header.frame_type == FrameType::Data && !payload.is_empty() {
        let streams = streams.lock().await;
        if let Some(stream) = streams.get(&header.stream_id) {
            let _ = stream.tx.send(StreamInput::Data(payload));
        }
    }
    if flags.contains(Flags::FIN) || flags.contains(Flags::RST) {
        let streams = streams.lock().await;
        if let Some(stream) = streams.get(&header.stream_id) {
            let _ = stream.tx.send(StreamInput::Finish);
        }
    }
    Ok(())
}

struct StreamState {
    tx: mpsc::UnboundedSender<StreamInput>,
}

enum StreamInput {
    Data(Bytes),
    Finish,
}

async fn proxy_stream<W>(
    stream_id: u32,
    local_addr: String,
    write: Arc<Mutex<W>>,
    mut input: mpsc::UnboundedReceiver<StreamInput>,
) -> Result<()>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin + Send + 'static,
{
    let request = read_http_request(&mut input).await?;
    let response = forward_http(&local_addr, request)
        .await
        .unwrap_or_else(|_| {
            Bytes::from_static(b"HTTP/1.1 502 Bad Gateway\r\ncontent-length: 11\r\n\r\nBad Gateway")
        });
    send_data(&write, stream_id, response).await?;
    send_frame(
        &write,
        FrameHeader {
            frame_type: FrameType::WindowUpdate,
            flags: Flags::FIN,
            stream_id,
            length: 0,
        },
        Bytes::new(),
    )
    .await?;
    Ok(())
}

async fn read_http_request(input: &mut mpsc::UnboundedReceiver<StreamInput>) -> Result<Bytes> {
    let mut bytes = BytesMut::new();
    while let Some(item) = input.recv().await {
        match item {
            StreamInput::Data(chunk) => {
                bytes.extend_from_slice(&chunk);
                if bytes.len() > MAX_HEADER_BYTES && find_header_end(&bytes).is_none() {
                    return Err(Error::Protocol("headers too large".into()));
                }
                if let Some(header_end) = find_header_end(&bytes) {
                    let content_length = parse_content_length(&bytes[..header_end]).unwrap_or(0);
                    let total = header_end + content_length;
                    while bytes.len() < total {
                        match input.recv().await {
                            Some(StreamInput::Data(chunk)) => bytes.extend_from_slice(&chunk),
                            _ => break,
                        }
                    }
                    return Ok(bytes.freeze());
                }
            }
            StreamInput::Finish => return Ok(bytes.freeze()),
        }
    }
    Ok(bytes.freeze())
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

fn parse_content_length(headers: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(headers).ok()?;
    for line in text.lines() {
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            return value.trim().parse().ok();
        }
    }
    None
}

async fn forward_http(local_addr: &str, request: Bytes) -> Result<Bytes> {
    let mut stream = tokio::net::TcpStream::connect(local_addr).await?;
    stream.write_all(&request).await?;
    stream.shutdown().await?;
    let mut response = Vec::new();
    stream
        .take(MAX_RESPONSE_BODY as u64)
        .read_to_end(&mut response)
        .await?;
    Ok(Bytes::from(response))
}

async fn send_data<W>(write: &Arc<Mutex<W>>, stream_id: u32, data: Bytes) -> Result<()>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin + Send + 'static,
{
    for chunk in data.chunks(DEFAULT_WINDOW as usize) {
        send_frame(
            write,
            FrameHeader {
                frame_type: FrameType::Data,
                flags: Flags(0),
                stream_id,
                length: chunk.len() as u32,
            },
            Bytes::copy_from_slice(chunk),
        )
        .await?;
    }
    Ok(())
}

async fn send_frame<W>(write: &Arc<Mutex<W>>, header: FrameHeader, payload: Bytes) -> Result<()>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin + Send + 'static,
{
    let mut bytes = BytesMut::with_capacity(HEADER_LEN + payload.len());
    bytes.extend_from_slice(&header.encode());
    bytes.extend_from_slice(&payload);
    write
        .lock()
        .await
        .send(Message::Binary(bytes.freeze()))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_header_matches_sdk_layout() {
        let header = FrameHeader {
            frame_type: FrameType::WindowUpdate,
            flags: Flags::SYN,
            stream_id: 1,
            length: 0,
        };
        assert_eq!(
            header.encode().as_ref(),
            &[0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0]
        );
        assert_eq!(
            FrameHeader::decode(&header.encode()).expect("decode"),
            header
        );
    }

    #[test]
    fn upstream_url_matches_sdk_route() {
        assert_eq!(
            upstream_ws_url("https://tunnel.poke.com/", "abc"),
            "wss://tunnel.poke.com/piko/v1/upstream/abc"
        );
        assert_eq!(
            upstream_ws_url("http://127.0.0.1:3000", "abc"),
            "ws://127.0.0.1:3000/piko/v1/upstream/abc"
        );
    }

    #[test]
    fn reads_content_length_case_insensitively() {
        assert_eq!(
            parse_content_length(b"POST / HTTP/1.1\r\nContent-Length: 12\r\n\r\n"),
            Some(12)
        );
    }
}
