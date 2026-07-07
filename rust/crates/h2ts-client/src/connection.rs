//! The HTTP/2 connection — port of `connection.ts` (+ `stream.ts`, `types.ts`).
//!
//! Owns the [`Transport`], drives read/write loops, multiplexes streams, and
//! implements the request/response flow (RFC 7540 §5–6). Opens with the
//! connection preface + SETTINGS and issues the first request immediately —
//! prior knowledge, no `Upgrade` round-trip.
//!
//! Faithful to the single-threaded JS object model via `Rc<RefCell<_>>`; the read
//! loop and the (channel-serialized) write loop run as one [`connect`]-returned
//! driver future the caller spawns.
//!
//! Deferred from the TS (marked `TODO`): server-push callbacks (pushes are
//! refused), streaming *request* upload from a `Stream` (in-memory bodies work),
//! `ping` RTT timing (needs a wasm clock), and abort signals.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::task::Poll;

use futures::channel::{mpsc, oneshot};
use futures::{future::poll_fn, SinkExt, StreamExt};

use crate::errors::{ErrorCode, H2Error};
use crate::flow::SendWindow;
use crate::frames::{serialize_frame, Frame, FrameDecoder, Settings, DEFAULT_MAX_FRAME_SIZE};
use crate::hpack::{Header, HpackDecoder, HpackEncoder};
use crate::transport::Transport;

const CONNECTION_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const SPEC_INITIAL_WINDOW: i64 = 65535;
const FORBIDDEN_HEADERS: [&str; 6] =
    ["connection", "host", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade"];

// --- public request/response types (port of types.ts) ---

/// A request to issue. Missing fields default (`GET` / `/` / `http`).
#[derive(Default)]
pub struct RequestInit {
    pub method: Option<String>,
    pub path: Option<String>,
    pub authority: Option<String>,
    pub scheme: Option<String>,
    pub headers: Vec<(String, String)>,
    /// In-memory request body (empty = no body). Streaming upload is a TODO.
    pub body: Vec<u8>,
}

/// A response. The body is a stream of byte chunks; `text`/`bytes` buffer it.
pub struct Response {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub raw_headers: Vec<Header>,
    body: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl Response {
    /// The response body as a stream of chunks.
    pub fn into_body(self) -> mpsc::UnboundedReceiver<Vec<u8>> {
        self.body
    }

    /// Buffer the whole body.
    pub async fn bytes(mut self) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = self.body.next().await {
            out.extend_from_slice(&chunk);
        }
        out
    }

    /// Buffer the body and decode it as UTF-8 (lossy).
    pub async fn text(self) -> String {
        String::from_utf8_lossy(&self.bytes().await).into_owned()
    }
}

/// Settings we advertise + push handling (port of `ConnectOptions`).
#[derive(Default)]
pub struct ConnectOptions {
    pub header_table_size: Option<usize>,
    pub enable_push: Option<bool>,
    pub initial_window_size: Option<u32>,
    pub max_frame_size: Option<usize>,
    // TODO: on_push callback (pushes are currently refused).
}

// --- per-stream state (port of stream.ts) ---

struct Head {
    status: u16,
    headers: HashMap<String, String>,
    raw: Vec<Header>,
}

fn collect_headers(raw: Vec<Header>) -> Head {
    let mut headers: HashMap<String, String> = HashMap::new();
    let mut status = 0u16;
    for h in &raw {
        if h.name == ":status" {
            status = h.value.parse().unwrap_or(0);
            continue;
        }
        if h.name.starts_with(':') {
            continue;
        }
        match headers.get(&h.name) {
            Some(existing) => {
                let sep = if h.name == "cookie" { "; " } else { ", " };
                let joined = format!("{existing}{sep}{}", h.value);
                headers.insert(h.name.clone(), joined);
            }
            None => {
                headers.insert(h.name.clone(), h.value.clone());
            }
        }
    }
    Head { status, headers, raw }
}

struct StreamState {
    id: u32,
    send_window: SendWindow,
    head_tx: Option<oneshot::Sender<Result<Head, H2Error>>>,
    body_tx: Option<mpsc::UnboundedSender<Vec<u8>>>,
    got_head: bool,
    body_closed: bool,
}

impl StreamState {
    fn new(id: u32, initial_send_window: i64) -> Self {
        Self {
            id,
            send_window: SendWindow::new(initial_send_window),
            head_tx: None,
            body_tx: None,
            got_head: false,
            body_closed: false,
        }
    }

    fn receive_headers(&mut self, raw: Vec<Header>, end_stream: bool) {
        if !self.got_head {
            self.got_head = true;
            if let Some(tx) = self.head_tx.take() {
                let _ = tx.send(Ok(collect_headers(raw)));
            }
        }
        // A second HEADERS block would be trailers (not surfaced yet).
        if end_stream {
            self.end_body();
        }
    }

    fn receive_data(&mut self, data: &[u8], end_stream: bool) {
        if !self.body_closed && !data.is_empty() {
            if let Some(tx) = &self.body_tx {
                let _ = tx.unbounded_send(data.to_vec());
            }
        }
        if end_stream {
            self.end_body();
        }
    }

    fn receive_reset(&mut self, error_code: u32) {
        let code = ErrorCode::from_value(error_code).unwrap_or(ErrorCode::ProtocolError);
        self.fail(H2Error::stream(code, format!("stream {} reset by peer", self.id), self.id));
    }

    fn fail(&mut self, err: H2Error) {
        self.send_window.close();
        if !self.got_head {
            self.got_head = true;
            if let Some(tx) = self.head_tx.take() {
                let _ = tx.send(Err(err));
            }
        }
        // Dropping the body sender ends the body stream (EOF). (TS errors it.)
        self.end_body();
    }

    fn end_body(&mut self) {
        if self.body_closed {
            return;
        }
        self.body_closed = true;
        self.body_tx = None;
    }
}

// --- connection state ---

struct RemoteSettings {
    initial_window_size: i64,
    max_frame_size: usize,
    #[allow(dead_code)]
    header_table_size: usize,
    #[allow(dead_code)]
    enable_push: bool,
}

impl Default for RemoteSettings {
    fn default() -> Self {
        Self {
            initial_window_size: SPEC_INITIAL_WINDOW,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            header_table_size: 4096,
            enable_push: true,
        }
    }
}

enum HeaderBlockKind {
    Response,
    Push,
}

struct PendingHeaderBlock {
    stream_id: u32,
    kind: HeaderBlockKind,
    end_stream: bool,
    promised_stream_id: Option<u32>,
    fragments: Vec<Vec<u8>>,
}

struct ConnState {
    out_tx: mpsc::UnboundedSender<Vec<u8>>,
    encoder: HpackEncoder,
    decoder: HpackDecoder,
    frame_decoder: FrameDecoder,
    streams: HashMap<u32, StreamState>,
    next_stream_id: u32,
    conn_send_window: SendWindow,
    remote: RemoteSettings,
    pending_header_block: Option<PendingHeaderBlock>,
    pings: HashMap<[u8; 8], oneshot::Sender<()>>,
    ping_counter: u32,
    closed: bool,
    close_error: Option<H2Error>,
    goaway_received: bool,
    highest_promised: u32,
}

impl ConnState {
    fn write_raw(&self, bytes: Vec<u8>) {
        if !self.closed {
            let _ = self.out_tx.unbounded_send(bytes);
        }
    }

    fn send_frame(&self, frame: Frame) {
        self.write_raw(serialize_frame(&frame));
    }

    fn on_bytes(&mut self, chunk: &[u8]) {
        let frames = match self.frame_decoder.push(chunk) {
            Ok(f) => f,
            Err(e) => {
                self.connection_error(e);
                return;
            }
        };
        for frame in frames {
            if let Err(e) = self.dispatch(frame) {
                self.connection_error(e);
                return;
            }
        }
    }

    fn dispatch(&mut self, frame: Frame) -> Result<(), H2Error> {
        // A pending header block only allows CONTINUATION on the same stream (§6.2).
        if self.pending_header_block.is_some() && !matches!(frame, Frame::Continuation { .. }) {
            return Err(H2Error::new(ErrorCode::ProtocolError, "expected CONTINUATION frame"));
        }

        match frame {
            Frame::Settings { ack, settings } => {
                if ack {
                    return Ok(());
                }
                self.apply_remote_settings(&settings);
                self.send_frame(Frame::Settings { ack: true, settings: Settings::default() });
            }
            Frame::Headers { stream_id, header_block_fragment, end_stream, end_headers, .. } => {
                self.pending_header_block = Some(PendingHeaderBlock {
                    stream_id,
                    kind: HeaderBlockKind::Response,
                    end_stream,
                    promised_stream_id: None,
                    fragments: vec![header_block_fragment],
                });
                if end_headers {
                    self.complete_header_block()?;
                }
            }
            Frame::Continuation { stream_id, header_block_fragment, end_headers } => {
                match &mut self.pending_header_block {
                    Some(pb) if pb.stream_id == stream_id => pb.fragments.push(header_block_fragment),
                    _ => return Err(H2Error::new(ErrorCode::ProtocolError, "unexpected CONTINUATION")),
                }
                if end_headers {
                    self.complete_header_block()?;
                }
            }
            Frame::PushPromise { stream_id, promised_stream_id, header_block_fragment, end_headers } => {
                self.pending_header_block = Some(PendingHeaderBlock {
                    stream_id,
                    kind: HeaderBlockKind::Push,
                    end_stream: false,
                    promised_stream_id: Some(promised_stream_id),
                    fragments: vec![header_block_fragment],
                });
                if end_headers {
                    self.complete_header_block()?;
                }
            }
            Frame::Data { stream_id, data, end_stream } => {
                let has_stream = self.streams.contains_key(&stream_id);
                if !data.is_empty() {
                    // Replenish on receipt (simple constant receive window).
                    self.send_frame(Frame::WindowUpdate { stream_id: 0, window_size_increment: data.len() as u32 });
                    if has_stream && !end_stream {
                        self.send_frame(Frame::WindowUpdate { stream_id, window_size_increment: data.len() as u32 });
                    }
                }
                if let Some(s) = self.streams.get_mut(&stream_id) {
                    s.receive_data(&data, end_stream);
                }
                if end_stream {
                    self.streams.remove(&stream_id);
                }
            }
            Frame::RstStream { stream_id, error_code } => {
                if let Some(mut s) = self.streams.remove(&stream_id) {
                    s.receive_reset(error_code);
                }
            }
            Frame::WindowUpdate { stream_id, window_size_increment } => {
                if window_size_increment == 0 {
                    if stream_id == 0 {
                        return Err(H2Error::new(ErrorCode::ProtocolError, "zero WINDOW_UPDATE"));
                    }
                    self.reset_stream(stream_id, ErrorCode::ProtocolError);
                    return Ok(());
                }
                if stream_id == 0 {
                    self.conn_send_window.update(window_size_increment as i64);
                } else if let Some(s) = self.streams.get_mut(&stream_id) {
                    s.send_window.update(window_size_increment as i64);
                }
            }
            Frame::Ping { ack, opaque_data } => {
                if ack {
                    if let Some(tx) = self.pings.remove(&opaque_data) {
                        let _ = tx.send(());
                    }
                } else {
                    self.send_frame(Frame::Ping { ack: true, opaque_data });
                }
            }
            Frame::Goaway { last_stream_id, error_code, .. } => {
                self.goaway_received = true;
                let code = ErrorCode::from_value(error_code).unwrap_or(ErrorCode::NoError);
                let err = H2Error::new(code, "peer sent GOAWAY");
                let doomed: Vec<u32> =
                    self.streams.keys().copied().filter(|&id| id > last_stream_id).collect();
                for id in doomed {
                    if let Some(mut s) = self.streams.remove(&id) {
                        s.fail(err.clone());
                    }
                }
                if error_code != 0 {
                    self.destroy(err);
                }
            }
            Frame::Priority { .. } => {} // prioritization not implemented
        }
        Ok(())
    }

    fn complete_header_block(&mut self) -> Result<(), H2Error> {
        let pb = self.pending_header_block.take().expect("header block present");
        let block: Vec<u8> = if pb.fragments.len() == 1 {
            pb.fragments.into_iter().next().unwrap()
        } else {
            pb.fragments.concat()
        };
        let headers = self.decoder.decode(&block)?;

        match pb.kind {
            HeaderBlockKind::Response => {
                if let Some(s) = self.streams.get_mut(&pb.stream_id) {
                    s.receive_headers(headers, pb.end_stream);
                }
                if pb.end_stream {
                    self.streams.remove(&pb.stream_id);
                }
            }
            HeaderBlockKind::Push => {
                let promised = pb.promised_stream_id.unwrap_or(0);
                if promised > self.highest_promised {
                    self.highest_promised = promised;
                }
                // TODO: surface pushes via an on_push callback. For now, refuse.
                self.send_frame(Frame::RstStream {
                    stream_id: promised,
                    error_code: ErrorCode::RefusedStream.value(),
                });
            }
        }
        Ok(())
    }

    fn apply_remote_settings(&mut self, s: &Settings) {
        if let Some(iw) = s.initial_window_size {
            let delta = iw as i64 - self.remote.initial_window_size;
            self.remote.initial_window_size = iw as i64;
            for stream in self.streams.values_mut() {
                stream.send_window.adjust(delta);
            }
        }
        if let Some(mfs) = s.max_frame_size {
            self.remote.max_frame_size = mfs as usize;
        }
        if let Some(hts) = s.header_table_size {
            self.remote.header_table_size = hts as usize;
        }
        if let Some(ep) = s.enable_push {
            self.remote.enable_push = ep;
        }
    }

    fn send_headers(&self, id: u32, block: Vec<u8>, end_stream: bool) {
        let max = self.remote.max_frame_size;
        if block.len() <= max {
            self.send_frame(Frame::Headers {
                stream_id: id,
                header_block_fragment: block,
                end_stream,
                end_headers: true,
                priority: None,
            });
            return;
        }
        // Split an oversized block into HEADERS + CONTINUATION frames.
        self.send_frame(Frame::Headers {
            stream_id: id,
            header_block_fragment: block[..max].to_vec(),
            end_stream,
            end_headers: false,
            priority: None,
        });
        let mut offset = max;
        while offset < block.len() {
            let next = (offset + max).min(block.len());
            self.send_frame(Frame::Continuation {
                stream_id: id,
                header_block_fragment: block[offset..next].to_vec(),
                end_headers: next >= block.len(),
            });
            offset = next;
        }
    }

    fn reset_stream(&mut self, id: u32, code: ErrorCode) {
        self.send_frame(Frame::RstStream { stream_id: id, error_code: code.value() });
        self.streams.remove(&id);
    }

    fn connection_error(&mut self, err: H2Error) {
        self.send_frame(Frame::Goaway {
            last_stream_id: self.highest_promised,
            error_code: err.code.value(),
            debug_data: Vec::new(),
        });
        self.destroy(err);
    }

    fn destroy(&mut self, err: H2Error) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.close_error = Some(err.clone());
        self.conn_send_window.close();
        let ids: Vec<u32> = self.streams.keys().copied().collect();
        for id in ids {
            if let Some(mut s) = self.streams.remove(&id) {
                s.fail(err.clone());
            }
        }
        self.pings.clear();
    }
}

/// The HTTP/2 connection handle. Cheap to clone (shares one `Rc` state).
#[derive(Clone)]
pub struct H2Connection {
    shared: Rc<RefCell<ConnState>>,
}

impl H2Connection {
    /// True once the connection has been torn down.
    pub fn is_closed(&self) -> bool {
        self.shared.borrow().closed
    }

    /// The negotiated WebSocket subprotocol, if opened via a WebSocket (set by
    /// the caller). Empty otherwise.
    // (Kept minimal here; `connect_websocket` sets it in the web layer.)
    pub async fn request(&self, init: RequestInit) -> Result<Response, H2Error> {
        let id;
        let head_rx;
        let body_rx;
        {
            let mut st = self.shared.borrow_mut();
            if st.closed {
                return Err(st
                    .close_error
                    .clone()
                    .unwrap_or_else(|| H2Error::new(ErrorCode::InternalError, "connection closed")));
            }
            if st.goaway_received {
                return Err(H2Error::new(ErrorCode::RefusedStream, "connection is going away"));
            }

            id = st.next_stream_id;
            st.next_stream_id += 2;

            let (htx, hrx) = oneshot::channel();
            let (btx, brx) = mpsc::unbounded();
            let initial = st.remote.initial_window_size;
            let mut stream = StreamState::new(id, initial);
            stream.head_tx = Some(htx);
            stream.body_tx = Some(btx);
            st.streams.insert(id, stream);
            head_rx = hrx;
            body_rx = brx;

            let headers = build_request_headers(&init);
            let has_body = !init.body.is_empty();
            let block = st.encoder.encode(&headers);
            st.send_headers(id, block, !has_body);
        }

        // Flow-controlled body upload (in-memory). No-op for bodyless requests.
        if !init.body.is_empty() {
            self.pump_body(id, &init.body).await;
        }

        match head_rx.await {
            Ok(Ok(head)) => Ok(Response {
                status: head.status,
                headers: head.headers,
                raw_headers: head.raw,
                body: body_rx,
            }),
            Ok(Err(e)) => Err(e),
            Err(_canceled) => Err(self
                .shared
                .borrow()
                .close_error
                .clone()
                .unwrap_or_else(|| H2Error::new(ErrorCode::InternalError, "connection closed"))),
        }
    }

    async fn pump_body(&self, id: u32, body: &[u8]) {
        let mut offset = 0;
        while offset < body.len() {
            // Await positive connection- and stream-level send windows.
            let alive = poll_fn(|cx| {
                let mut st = self.shared.borrow_mut();
                if st.closed || !st.streams.contains_key(&id) {
                    return Poll::Ready(false);
                }
                let conn_ready = st.conn_send_window.is_ready();
                let stream_ready = st.streams.get(&id).map(|s| s.send_window.is_ready()).unwrap_or(false);
                if conn_ready && stream_ready {
                    Poll::Ready(true)
                } else {
                    if !conn_ready {
                        st.conn_send_window.register_waker(cx.waker());
                    }
                    if !stream_ready {
                        if let Some(s) = st.streams.get_mut(&id) {
                            s.send_window.register_waker(cx.waker());
                        }
                    }
                    Poll::Pending
                }
            })
            .await;
            if !alive {
                return;
            }

            let mut st = self.shared.borrow_mut();
            if st.closed || st.streams.get(&id).map(|s| s.send_window.is_closed()).unwrap_or(true) {
                return;
            }
            let conn_w = st.conn_send_window.value();
            let stream_w = st.streams.get(&id).unwrap().send_window.value();
            let max = st.remote.max_frame_size as i64;
            let remaining = (body.len() - offset) as i64;
            let grant = remaining.min(conn_w).min(stream_w).min(max);
            if grant <= 0 {
                continue; // windows changed under us; re-await
            }
            st.conn_send_window.consume(grant);
            st.streams.get_mut(&id).unwrap().send_window.consume(grant);
            let slice = body[offset..offset + grant as usize].to_vec();
            st.send_frame(Frame::Data { stream_id: id, data: slice, end_stream: false });
            offset += grant as usize;
        }
        let st = self.shared.borrow();
        st.send_frame(Frame::Data { stream_id: id, data: Vec::new(), end_stream: true });
    }

    /// Send a PING and resolve when the matching PONG arrives.
    // TODO: return the round-trip time (needs a wasm-compatible clock).
    pub async fn ping(&self) -> Result<(), H2Error> {
        let rx = {
            let mut st = self.shared.borrow_mut();
            if st.closed {
                return Err(st
                    .close_error
                    .clone()
                    .unwrap_or_else(|| H2Error::new(ErrorCode::InternalError, "connection closed")));
            }
            st.ping_counter = st.ping_counter.wrapping_add(1);
            let mut opaque = [0u8; 8];
            opaque[4..8].copy_from_slice(&st.ping_counter.to_be_bytes());
            let (tx, rx) = oneshot::channel();
            st.pings.insert(opaque, tx);
            st.send_frame(Frame::Ping { ack: false, opaque_data: opaque });
            rx
        };
        rx.await.map_err(|_| H2Error::new(ErrorCode::InternalError, "connection closed"))
    }

    /// Gracefully close: send GOAWAY, then tear down.
    pub fn close(&self) {
        let mut st = self.shared.borrow_mut();
        if st.closed {
            return;
        }
        st.send_frame(Frame::Goaway {
            last_stream_id: st.highest_promised,
            error_code: 0,
            debug_data: Vec::new(),
        });
        st.destroy(H2Error::new(ErrorCode::NoError, "connection closed by client"));
    }
}

fn build_request_headers(init: &RequestInit) -> Vec<Header> {
    let method = init.method.clone().unwrap_or_else(|| "GET".into()).to_uppercase();
    let scheme = init.scheme.clone().unwrap_or_else(|| "http".into());
    let path = init.path.clone().unwrap_or_else(|| "/".into());

    let mut headers = vec![Header::new(":method", method), Header::new(":scheme", scheme)];
    if let Some(auth) = &init.authority {
        headers.push(Header::new(":authority", auth.clone()));
    }
    headers.push(Header::new(":path", path));

    for (raw_name, value) in &init.headers {
        let name = raw_name.to_ascii_lowercase();
        if name.starts_with(':') || FORBIDDEN_HEADERS.contains(&name.as_str()) {
            continue;
        }
        if name == "authorization" || name == "cookie" {
            headers.push(Header::never_indexed(name, value.clone()));
        } else {
            headers.push(Header::new(name, value.clone()));
        }
    }
    headers
}

/// Create an HTTP/2 client over a byte [`Transport`], speaking prior knowledge.
///
/// Returns the connection handle plus a **driver** future that runs the read and
/// write loops; the caller must spawn/poll it (on wasm, `spawn_local`). The
/// preface + SETTINGS are queued immediately, so [`H2Connection::request`] may be
/// called right away.
pub fn connect(transport: Transport, options: ConnectOptions) -> (H2Connection, impl Future<Output = ()>) {
    let (out_tx, out_rx) = mpsc::unbounded();

    let local_max_frame_size = options.max_frame_size.unwrap_or(DEFAULT_MAX_FRAME_SIZE);
    let local_initial_window = options.initial_window_size.unwrap_or(1024 * 1024);
    let header_table_size = options.header_table_size.unwrap_or(4096);
    let enable_push = options.enable_push.unwrap_or(true);

    let state = ConnState {
        out_tx,
        encoder: HpackEncoder::new(),
        decoder: HpackDecoder::new(header_table_size),
        frame_decoder: FrameDecoder::new(local_max_frame_size),
        streams: HashMap::new(),
        next_stream_id: 1,
        conn_send_window: SendWindow::new(SPEC_INITIAL_WINDOW),
        remote: RemoteSettings::default(),
        pending_header_block: None,
        pings: HashMap::new(),
        ping_counter: 0,
        closed: false,
        close_error: None,
        goaway_received: false,
        highest_promised: 0,
    };
    let shared = Rc::new(RefCell::new(state));

    // Client connection preface + our SETTINGS, sent immediately (§3.5).
    {
        let st = shared.borrow();
        st.write_raw(CONNECTION_PREFACE.to_vec());
        st.send_frame(Frame::Settings {
            ack: false,
            settings: Settings {
                header_table_size: Some(header_table_size as u32),
                enable_push: Some(enable_push),
                initial_window_size: Some(local_initial_window),
                max_frame_size: Some(local_max_frame_size as u32),
                ..Default::default()
            },
        });
    }

    let driver = drive(shared.clone(), transport.reader, transport.writer, out_rx);
    (H2Connection { shared }, driver)
}

async fn drive(
    shared: Rc<RefCell<ConnState>>,
    mut reader: crate::transport::ByteStream,
    mut writer: crate::transport::ByteSink,
    mut out_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let read = {
        let shared = shared.clone();
        async move {
            while let Some(chunk) = reader.next().await {
                if !chunk.is_empty() {
                    shared.borrow_mut().on_bytes(&chunk);
                }
                if shared.borrow().closed {
                    break;
                }
            }
            shared.borrow_mut().destroy(H2Error::new(ErrorCode::NoError, "transport closed by peer"));
        }
    };

    let write = async move {
        while let Some(bytes) = out_rx.next().await {
            if writer.send(bytes).await.is_err() {
                shared.borrow_mut().destroy(H2Error::new(ErrorCode::InternalError, "transport write failed"));
                break;
            }
        }
    };

    futures::pin_mut!(read, write);
    futures::future::select(read, write).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::LocalPool;
    use futures::task::LocalSpawnExt;

    /// An in-memory transport; returns the client's outbound-read end and the
    /// inbound-write end for the test to drive the "server" side.
    fn mock_transport() -> (Transport, mpsc::UnboundedReceiver<Vec<u8>>, mpsc::UnboundedSender<Vec<u8>>) {
        let (c2s_tx, c2s_rx) = mpsc::unbounded::<Vec<u8>>(); // client -> test
        let (s2c_tx, s2c_rx) = mpsc::unbounded::<Vec<u8>>(); // test -> client
        let writer = Box::pin(c2s_tx.sink_map_err(|e| crate::transport::TransportError(e.to_string())));
        let reader = Box::pin(s2c_rx);
        (Transport::new(reader, writer), c2s_rx, s2c_tx)
    }

    /// Read the client's opening bytes: the 24-byte preface, then `want` frames.
    async fn read_startup(c2s_rx: &mut mpsc::UnboundedReceiver<Vec<u8>>, want: usize) -> Vec<Frame> {
        let mut buf: Vec<u8> = Vec::new();
        while buf.len() < CONNECTION_PREFACE.len() {
            buf.extend(c2s_rx.next().await.expect("client closed before preface"));
        }
        assert_eq!(&buf[..CONNECTION_PREFACE.len()], CONNECTION_PREFACE);
        let mut dec = FrameDecoder::default();
        let mut frames = dec.push(&buf[CONNECTION_PREFACE.len()..]).unwrap();
        while frames.len() < want {
            let chunk = c2s_rx.next().await.expect("client closed before frames");
            frames.extend(dec.push(&chunk).unwrap());
        }
        frames
    }

    #[test]
    fn opens_with_preface_and_settings_not_http1_upgrade() {
        let mut pool = LocalPool::new();
        let (transport, mut c2s_rx, _s2c_tx) = mock_transport();
        let (_conn, driver) = connect(transport, ConnectOptions::default());
        pool.spawner().spawn_local(driver).unwrap();

        pool.run_until(async move {
            let frames = read_startup(&mut c2s_rx, 1).await;
            assert!(matches!(frames[0], Frame::Settings { ack: false, .. }));
        });
    }

    #[test]
    fn sends_first_request_before_any_server_bytes() {
        let mut pool = LocalPool::new();
        let sp = pool.spawner();
        let (transport, mut c2s_rx, _s2c_tx) = mock_transport();
        let (conn, driver) = connect(transport, ConnectOptions::default());
        sp.spawn_local(driver).unwrap();

        // Fire a request; never feed the client any server bytes. If it gated on
        // the server's preface, the HEADERS would never be written.
        let conn2 = conn.clone();
        sp.spawn_local(async move {
            let _ = conn2
                .request(RequestInit {
                    method: Some("GET".into()),
                    path: Some("/hello".into()),
                    authority: Some("example.com".into()),
                    ..Default::default()
                })
                .await;
        })
        .unwrap();

        pool.run_until(async move {
            let frames = read_startup(&mut c2s_rx, 2).await;
            assert!(matches!(frames[0], Frame::Settings { ack: false, .. }));
            assert!(matches!(frames[1], Frame::Headers { stream_id: 1, .. }));
        });
    }

    #[test]
    fn completes_a_request_when_the_server_replies_afterwards() {
        let mut pool = LocalPool::new();
        let sp = pool.spawner();
        let (transport, mut c2s_rx, s2c_tx) = mock_transport();
        let (conn, driver) = connect(transport, ConnectOptions::default());
        sp.spawn_local(driver).unwrap();

        pool.run_until(async move {
            let req = conn.request(RequestInit {
                method: Some("GET".into()),
                path: Some("/hello".into()),
                authority: Some("example.com".into()),
                ..Default::default()
            });

            let server = async move {
                // Wait until the client has written its request HEADERS (stream 1).
                let frames = read_startup(&mut c2s_rx, 2).await;
                assert!(matches!(frames[1], Frame::Headers { stream_id: 1, .. }));
                // Reply after the request was already sent (prior-knowledge ordering).
                s2c_tx
                    .unbounded_send(serialize_frame(&Frame::Settings { ack: false, settings: Settings::default() }))
                    .unwrap();
                let block = HpackEncoder::new().encode(&[Header::new(":status", "200")]);
                s2c_tx
                    .unbounded_send(serialize_frame(&Frame::Headers {
                        stream_id: 1,
                        header_block_fragment: block,
                        end_stream: false,
                        end_headers: true,
                        priority: None,
                    }))
                    .unwrap();
                s2c_tx
                    .unbounded_send(serialize_frame(&Frame::Data {
                        stream_id: 1,
                        data: b"ok".to_vec(),
                        end_stream: true,
                    }))
                    .unwrap();
            };

            let (res, ()) = futures::join!(req, server);
            let res = res.unwrap();
            assert_eq!(res.status, 200);
            assert_eq!(res.text().await, "ok");
        });
    }
}
