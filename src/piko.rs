use crate::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use http::HeaderValue;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, watch};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

const HEADER_LEN: usize = 12;
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
    pub connect_timeout: Duration,
    pub connected: Option<mpsc::UnboundedSender<()>>,
    pub errors: Option<mpsc::UnboundedSender<String>>,
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
                if let Some(errors) = &config.errors {
                    let _ = errors.send(err.to_string());
                }
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
    let (ws, _) = tokio::time::timeout(config.connect_timeout, connect_async(request))
        .await
        .map_err(|_| {
            Error::Protocol(format!(
                "upstream websocket timeout after {:?}",
                config.connect_timeout
            ))
        })??;
    if let Some(connected) = &config.connected {
        let _ = connected.send(());
    }
    let (write, read) = ws.split();
    let write = Arc::new(Mutex::new(write));
    let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<(FrameHeader, Bytes)>();
    tokio::spawn(async move {
        let mut read = read;
        let mut buffer = BytesMut::new();
        while let Some(message) = read.next().await {
            match message {
                Ok(Message::Binary(bytes)) => {
                    buffer.extend_from_slice(&bytes);
                    loop {
                        if buffer.len() < HEADER_LEN {
                            break;
                        }
                        let Ok(header) = FrameHeader::decode(&buffer[..HEADER_LEN]) else {
                            return;
                        };
                        let total = HEADER_LEN + header.length as usize;
                        if buffer.len() < total {
                            break;
                        }
                        let frame = buffer.split_to(total).freeze();
                        let payload = frame.slice(HEADER_LEN..);
                        if frame_tx.send((header, payload)).is_err() {
                            return;
                        }
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
    let request = read_http_request(&mut input)
        .await
        .and_then(parse_request)?;
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
    loop {
        match input.recv().await {
            Some(StreamInput::Data(chunk)) => {
                bytes.extend_from_slice(&chunk);
                if bytes.len() > MAX_HEADER_BYTES && find_header_end(&bytes).is_none() {
                    return Err(Error::Protocol("headers too large".into()));
                }
                if let Some(header_end) = find_header_end(&bytes) {
                    let headers = &bytes[..header_end];
                    if let Some(content_length) = parse_content_length(headers) {
                        if content_length > MAX_RESPONSE_BODY {
                            return Err(Error::Protocol(format!(
                                "request body too large: {content_length} bytes"
                            )));
                        }
                        let total = header_end + content_length;
                        while bytes.len() < total {
                            match input.recv().await {
                                Some(StreamInput::Data(chunk)) => bytes.extend_from_slice(&chunk),
                                Some(StreamInput::Finish) => {
                                    return Err(Error::Protocol(
                                        "request body incomplete before stream closed".into(),
                                    ));
                                }
                                None => {
                                    return Err(Error::Protocol(
                                        "request body incomplete before stream closed".into(),
                                    ));
                                }
                            }
                        }
                        return Ok(bytes.freeze());
                    }
                    if parse_transfer_encoding_chunked(headers) {
                        return read_chunked_body(input, &mut bytes, header_end).await;
                    }
                    return Ok(bytes.freeze());
                }
            }
            Some(StreamInput::Finish) => return Ok(bytes.freeze()),
            None => return Ok(bytes.freeze()),
        }
    }
}

async fn read_chunked_body(
    input: &mut mpsc::UnboundedReceiver<StreamInput>,
    bytes: &mut BytesMut,
    header_end: usize,
) -> Result<Bytes> {
    let mut body = BytesMut::new();
    let mut pending = bytes.split_off(header_end).freeze();
    loop {
        while find_crlf(&pending).is_none() {
            match input.recv().await {
                Some(StreamInput::Data(chunk)) => {
                    let mut merged = BytesMut::from(pending.as_ref());
                    merged.extend_from_slice(&chunk);
                    pending = merged.freeze();
                }
                Some(StreamInput::Finish) | None => {
                    return Err(Error::Protocol("chunked body incomplete".into()));
                }
            }
        }
        let line_end = find_crlf(&pending).expect("chunk size line");
        let size_line = std::str::from_utf8(&pending[..line_end])
            .map_err(|err| Error::Protocol(err.to_string()))?;
        let chunk_size = usize::from_str_radix(size_line.trim(), 16)
            .map_err(|err| Error::Protocol(err.to_string()))?;
        pending = pending.slice(line_end + 2..);
        if chunk_size == 0 {
            bytes.extend_from_slice(&body);
            return Ok(bytes.clone().freeze());
        }
        if body.len() + chunk_size > MAX_RESPONSE_BODY {
            return Err(Error::Protocol("chunked body too large".into()));
        }
        while pending.len() < chunk_size + 2 {
            match input.recv().await {
                Some(StreamInput::Data(chunk)) => {
                    let mut merged = BytesMut::from(pending.as_ref());
                    merged.extend_from_slice(&chunk);
                    pending = merged.freeze();
                }
                Some(StreamInput::Finish) | None => {
                    return Err(Error::Protocol("chunked body incomplete".into()));
                }
            }
        }
        body.extend_from_slice(&pending[..chunk_size]);
        pending = pending.slice(chunk_size + 2..);
    }
}

fn find_crlf(bytes: impl AsRef<[u8]>) -> Option<usize> {
    let bytes = bytes.as_ref();
    bytes.windows(2).position(|window| window == b"\r\n")
}

fn parse_transfer_encoding_chunked(headers: &[u8]) -> bool {
    let text = match std::str::from_utf8(headers) {
        Ok(text) => text,
        Err(_) => return false,
    };
    for line in text.lines() {
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("transfer-encoding")
        {
            return value.to_ascii_lowercase().contains("chunked");
        }
    }
    false
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct HttpRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Bytes,
}

fn parse_request(bytes: Bytes) -> Result<HttpRequest> {
    let header_end =
        find_header_end(&bytes).ok_or_else(|| Error::Protocol("headers incomplete".into()))?;
    let header_text = std::str::from_utf8(&bytes[..header_end])
        .map_err(|err| Error::Protocol(err.to_string()))?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| Error::Protocol("request line missing".into()))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| Error::Protocol("request method missing".into()))?
        .to_string();
    let raw_path = parts
        .next()
        .ok_or_else(|| Error::Protocol("request path missing".into()))?;
    let path = normalize_request_path(raw_path);
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if is_hop_by_hop_header(name) || name.eq_ignore_ascii_case("host") {
            continue;
        }
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    Ok(HttpRequest {
        method,
        path,
        headers,
        body: bytes.slice(header_end..),
    })
}

fn normalize_request_path(raw_path: &str) -> String {
    if let Ok(url) = url::Url::parse(raw_path) {
        let mut path = url.path().to_string();
        if path.is_empty() {
            path.push('/');
        }
        if let Some(query) = url.query() {
            path.push('?');
            path.push_str(query);
        }
        path
    } else if raw_path.starts_with('/') {
        raw_path.to_string()
    } else {
        format!("/{raw_path}")
    }
}

fn is_hop_by_hop_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "host" | "transfer-encoding" | "connection" | "keep-alive" | "te" | "trailer" | "upgrade"
    )
}

async fn forward_http(local_addr: &str, request: HttpRequest) -> Result<Bytes> {
    let mut stream = TcpStream::connect(local_addr).await?;
    stream
        .write_all(&encode_forward_request(local_addr, &request))
        .await?;
    read_http_response(&mut stream).await
}

fn encode_forward_request(local_addr: &str, request: &HttpRequest) -> Bytes {
    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(format!("{} {} HTTP/1.1\r\n", request.method, request.path).as_bytes());
    bytes.extend_from_slice(format!("host: {local_addr}\r\n").as_bytes());
    let mut has_content_length = false;
    for (name, value) in &request.headers {
        if name.eq_ignore_ascii_case("content-length") {
            has_content_length = true;
        }
        bytes.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    if !request.body.is_empty() && !has_content_length {
        bytes.extend_from_slice(format!("content-length: {}\r\n", request.body.len()).as_bytes());
    }
    bytes.extend_from_slice(b"\r\n");
    bytes.extend_from_slice(&request.body);
    bytes.freeze()
}

async fn read_http_response(stream: &mut TcpStream) -> Result<Bytes> {
    let mut bytes = BytesMut::new();
    let mut chunk = [0u8; 32_768];
    loop {
        if let Some(header_end) = find_header_end(&bytes) {
            let headers = &bytes[..header_end];
            if let Some(content_length) = parse_content_length(headers) {
                if content_length > MAX_RESPONSE_BODY {
                    return Err(Error::Protocol("response body exceeds size limit".into()));
                }
                let total = header_end + content_length;
                while bytes.len() < total {
                    let read = stream.read(&mut chunk).await?;
                    if read == 0 {
                        return Err(Error::Protocol("response body incomplete".into()));
                    }
                    bytes.extend_from_slice(&chunk[..read]);
                }
                return Ok(normalize_response(bytes.freeze()));
            }
            if parse_transfer_encoding_chunked(headers) {
                let body = read_chunked_response_body(stream, bytes.split_off(header_end).freeze())
                    .await?;
                bytes.extend_from_slice(&body);
                return Ok(normalize_response(bytes.freeze()));
            }
            return Ok(normalize_response(bytes.freeze()));
        }
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
        if bytes.len() > MAX_HEADER_BYTES && find_header_end(&bytes).is_none() {
            return Err(Error::Protocol("response headers too large".into()));
        }
    }
    if bytes.is_empty() {
        return Err(Error::Protocol("empty response from upstream".into()));
    }
    Ok(normalize_response(bytes.freeze()))
}

async fn read_chunked_response_body(stream: &mut TcpStream, mut pending: Bytes) -> Result<Bytes> {
    let mut body = BytesMut::new();
    let mut chunk = [0u8; 32_768];
    loop {
        while find_crlf(&pending).is_none() {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                return Err(Error::Protocol("chunked response incomplete".into()));
            }
            let mut merged = BytesMut::from(pending.as_ref());
            merged.extend_from_slice(&chunk[..read]);
            pending = merged.freeze();
        }
        let line_end = find_crlf(&pending).expect("chunk size line");
        let size_line = std::str::from_utf8(&pending[..line_end])
            .map_err(|err| Error::Protocol(err.to_string()))?;
        let chunk_size = usize::from_str_radix(size_line.trim(), 16)
            .map_err(|err| Error::Protocol(err.to_string()))?;
        pending = pending.slice(line_end + 2..);
        if chunk_size == 0 {
            return Ok(body.freeze());
        }
        if body.len() + chunk_size > MAX_RESPONSE_BODY {
            return Err(Error::Protocol("response body exceeds size limit".into()));
        }
        while pending.len() < chunk_size + 2 {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                return Err(Error::Protocol("chunked response incomplete".into()));
            }
            let mut merged = BytesMut::from(pending.as_ref());
            merged.extend_from_slice(&chunk[..read]);
            pending = merged.freeze();
        }
        body.extend_from_slice(&pending[..chunk_size]);
        pending = pending.slice(chunk_size + 2..);
    }
}

fn normalize_response(raw: Bytes) -> Bytes {
    let header_end = match find_header_end(&raw) {
        Some(end) => end,
        None => return raw,
    };
    let header_text = match std::str::from_utf8(&raw[..header_end]) {
        Ok(text) => text,
        Err(_) => return raw,
    };
    let mut lines = header_text.split("\r\n");
    let status_line = match lines.next() {
        Some(line) => line,
        None => return raw,
    };
    let mut status_parts = status_line.split_whitespace();
    let version = status_parts.next().unwrap_or("HTTP/1.1");
    let status_code = status_parts.next().unwrap_or("500");
    let reason = status_parts.collect::<Vec<_>>().join(" ");
    let reason = if reason.is_empty() {
        "OK"
    } else {
        reason.as_str()
    };
    let body = raw.slice(header_end..);
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if is_hop_by_hop_header(name) || name.eq_ignore_ascii_case("content-length") {
            continue;
        }
        headers.push((
            sanitize_header_value(name.trim()),
            sanitize_header_value(value.trim()),
        ));
    }
    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(format!("{version} {status_code} {reason}\r\n").as_bytes());
    for (name, value) in headers {
        bytes.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    bytes.extend_from_slice(format!("content-length: {}\r\n\r\n", body.len()).as_bytes());
    bytes.extend_from_slice(&body);
    bytes.freeze()
}

fn sanitize_header_value(value: &str) -> String {
    value.replace(['\r', '\n'], " ")
}

async fn send_data<W>(write: &Arc<Mutex<W>>, stream_id: u32, data: Bytes) -> Result<()>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin + Send + 'static,
{
    for chunk in data.chunks(256 * 1024) {
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

    #[test]
    fn parses_request_like_js_proxy() {
        let request = parse_request(Bytes::from_static(
            b"POST http://tunnel.poke.com/abc/mcp?x=1 HTTP/1.1\r\nHost: tunnel.poke.com\r\nConnection: keep-alive\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
        ))
        .expect("request");
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/abc/mcp?x=1");
        assert_eq!(
            request.headers,
            vec![
                ("Content-Type".to_string(), "application/json".to_string()),
                ("Content-Length".to_string(), "2".to_string())
            ]
        );
        assert_eq!(request.body.as_ref(), b"{}");
    }

    #[tokio::test]
    async fn read_http_request_waits_for_full_content_length_body() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let payload = br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
        let request = format!(
            "POST /mcp HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            payload.len()
        );
        tx.send(StreamInput::Data(Bytes::from(request))).unwrap();
        tx.send(StreamInput::Data(Bytes::copy_from_slice(
            &payload[..payload.len() / 2],
        )))
        .unwrap();
        tx.send(StreamInput::Data(Bytes::copy_from_slice(
            &payload[payload.len() / 2..],
        )))
        .unwrap();
        let bytes = read_http_request(&mut rx).await.expect("full request");
        let parsed = parse_request(bytes).expect("parsed request");
        assert_eq!(parsed.body.as_ref(), payload);
    }

    #[tokio::test]
    async fn forward_http_round_trips_through_local_tcp_server() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buffer = [0u8; 4096];
            let read = socket.read(&mut buffer).await.expect("read request");
            let request = std::str::from_utf8(&buffer[..read]).expect("utf8 request");
            assert!(request.contains("POST /mcp HTTP/1.1"));
            assert!(request.contains(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#));
            let body = br#"{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"run_command"}]}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write headers");
            socket.write_all(body).await.expect("write body");
        });
        let request = parse_request(Bytes::from_static(
            b"POST /mcp HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 45\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}",
        ))
        .expect("request");
        let response = forward_http(&addr.to_string(), request)
            .await
            .expect("forward");
        let response_text = std::str::from_utf8(&response).expect("utf8 response");
        assert!(response_text.contains("HTTP/1.1 200 OK"));
        assert!(response_text.contains("run_command"));
        assert!(
            response_text
                .to_ascii_lowercase()
                .contains("content-length:")
        );
        server.await.expect("server");
    }
}
