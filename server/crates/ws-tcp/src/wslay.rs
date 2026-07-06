//! wslay framing (the WebSocket ⇄ byte-stream pump).
//!
//! wslay, driven through its event API with `no_buffering` enabled, delivers
//! each frame's payload **incrementally** via `on_frame_recv_chunk_callback` —
//! so we never hold a whole frame in memory, no matter how large. Control frames
//! (ping / close) are auto-handled by wslay.
//!
//! wslay is a synchronous, I/O-agnostic state machine: it never touches the
//! socket itself, only in-memory buffers via callbacks. We drive it from async
//! Rust — feeding it bytes we read from the WebSocket and flushing bytes it
//! produces. The HTTP upgrade is done separately in [`accept`](crate::accept),
//! which hands us the raw upgraded byte stream (no frames have been read yet, so
//! nothing is lost).
//!
//! The wslay context and the shared-state cell are reached from the two
//! directions as `usize` addresses (which are `Send`), cast back to raw pointers
//! only inside the synchronous `unsafe` sections. This keeps the future `Send`
//! (so it composes like a normal async fn) while never letting a bare pointer
//! live across an `.await`. It is sound because everything runs on one task: the
//! two directions interleave at awaits but never execute concurrently.

use std::cell::RefCell;
use std::ffi::c_void;
use std::future;
use std::io;
use std::os::raw::c_int;
use std::ptr;
use std::slice;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};
use wslay_sys::*;

const READ_CHUNK: usize = 64 * 1024;

/// Single-task state, shared between async Rust (between wslay calls) and the C
/// callbacks (during wslay calls). No borrow is ever held across an `.await`,
/// and everything runs on one task, so the accesses never overlap.
#[derive(Default)]
struct Shared {
    /// Raw WS bytes read from the socket, consumed by `recv_cb`.
    inbound: Vec<u8>,
    inbound_pos: usize,
    /// Bytes wslay wants sent on the WS (framed data + pong/close replies).
    outbound: Vec<u8>,
    /// Decoded payload of data frames, to forward to the peer.
    to_peer: Vec<u8>,
    /// Whether the frame currently being received is a data (non-control) frame.
    cur_is_data: bool,
    /// Received control frames (close/ping/pong), surfaced to the bridge's hooks.
    control_events: Vec<ControlEvent>,
}

/// An inbound WebSocket control frame, surfaced from a wslay callback.
enum ControlEvent {
    Close { code: u16, reason: Vec<u8> },
    Ping(Vec<u8>),
    Pong(Vec<u8>),
}

#[inline]
unsafe fn shared<'a>(user_data: *mut c_void) -> &'a RefCell<Shared> {
    &*(user_data as *const RefCell<Shared>)
}

/// wslay wants raw bytes from the peer; feed it from `inbound`.
unsafe extern "C" fn recv_cb(
    ctx: wslay_event_context_ptr,
    buf: *mut u8,
    len: usize,
    _flags: c_int,
    user_data: *mut c_void,
) -> isize {
    let mut s = shared(user_data).borrow_mut();
    let avail = s.inbound.len() - s.inbound_pos;
    if avail == 0 {
        wslay_event_set_error(ctx, wslay_error_WSLAY_ERR_WOULDBLOCK); // non-blocking: stop
        return -1;
    }
    let n = len.min(avail);
    let from = s.inbound_pos;
    ptr::copy_nonoverlapping(s.inbound.as_ptr().add(from), buf, n);
    s.inbound_pos += n;
    n as isize
}

/// wslay wants to send raw bytes; buffer them for the async writer.
unsafe extern "C" fn send_cb(
    _ctx: wslay_event_context_ptr,
    data: *const u8,
    len: usize,
    _flags: c_int,
    user_data: *mut c_void,
) -> isize {
    shared(user_data)
        .borrow_mut()
        .outbound
        .extend_from_slice(slice::from_raw_parts(data, len));
    len as isize
}

/// A new frame started; remember whether it carries data (vs. a control frame).
unsafe extern "C" fn frame_start_cb(
    _ctx: wslay_event_context_ptr,
    arg: *const wslay_event_on_frame_recv_start_arg,
    user_data: *mut c_void,
) {
    // is_ctrl_frame(opcode) == (opcode >> 3) & 1
    shared(user_data).borrow_mut().cur_is_data = ((*arg).opcode >> 3) & 1 == 0;
}

/// A chunk of the current frame's payload arrived — forward it if it's data.
/// This is the incremental, never-buffer-a-whole-frame path.
unsafe extern "C" fn frame_chunk_cb(
    _ctx: wslay_event_context_ptr,
    arg: *const wslay_event_on_frame_recv_chunk_arg,
    user_data: *mut c_void,
) {
    let mut s = shared(user_data).borrow_mut();
    if s.cur_is_data {
        let chunk = slice::from_raw_parts((*arg).data, (*arg).data_length);
        s.to_peer.extend_from_slice(chunk);
    }
}

/// A complete message was received. Under `no_buffering`, data messages arrive
/// here with a NULL payload (already streamed via `frame_chunk_cb`); control
/// frames (close/ping/pong) are still delivered complete. Surface the control
/// ones so the bridge can hand them to the caller's hooks. wslay has already
/// auto-queued the pong (for a ping) / close echo by the time this runs.
unsafe extern "C" fn msg_recv_cb(
    _ctx: wslay_event_context_ptr,
    arg: *const wslay_event_on_msg_recv_arg,
    user_data: *mut c_void,
) {
    let arg = &*arg;
    // Opcodes: 0x8 close, 0x9 ping, 0xA pong. Data frames (0x0/0x1/0x2) arrive
    // with a NULL msg under no_buffering and are ignored here.
    let event = match arg.opcode {
        0x8 => {
            // Close payload is [2-byte code][reason]; wslay pre-parsed the code.
            let reason = if arg.msg_length >= 2 && !arg.msg.is_null() {
                slice::from_raw_parts(arg.msg.add(2), arg.msg_length - 2).to_vec()
            } else {
                Vec::new()
            };
            Some(ControlEvent::Close {
                code: arg.status_code,
                reason,
            })
        }
        0x9 | 0xA => {
            let payload = if arg.msg.is_null() || arg.msg_length == 0 {
                Vec::new()
            } else {
                slice::from_raw_parts(arg.msg, arg.msg_length).to_vec()
            };
            Some(if arg.opcode == 0x9 {
                ControlEvent::Ping(payload)
            } else {
                ControlEvent::Pong(payload)
            })
        }
        _ => None,
    };
    if let Some(ev) = event {
        shared(user_data).borrow_mut().control_events.push(ev);
    }
}

/// Frees the wslay context on drop (even on early return / panic). Holds the
/// context as a `usize` address so the guard is naturally `Send` and no bare
/// pointer is ever kept across an `.await`.
struct CtxGuard(usize);
impl Drop for CtxGuard {
    fn drop(&mut self) {
        unsafe { wslay_event_context_free(self.0 as wslay_event_context_ptr) };
    }
}

/// A WebSocket close: status code and (UTF-8) reason. Uses RFC 6455 close codes;
/// the reason should be at most 123 bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseFrame {
    pub code: u16,
    pub reason: String,
}

impl Default for CloseFrame {
    /// 1000 (Normal Closure), empty reason.
    fn default() -> Self {
        Self {
            code: wslay_status_code_WSLAY_CODE_NORMAL_CLOSURE as u16,
            reason: String::new(),
        }
    }
}

/// A control frame to send into a running bridge (see [`WsControl`]).
enum ControlCmd {
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(CloseFrame),
}

/// Sends WebSocket control frames into a running [`bridge_with`]. Cheaply
/// cloneable; call from any task. Frames are queued and flushed by the bridge on
/// its next turn; a send fails only once the bridge has ended.
#[derive(Clone)]
pub struct WsControl {
    tx: mpsc::UnboundedSender<ControlCmd>,
}

impl WsControl {
    /// Send a Ping with `payload` (≤125 bytes). The peer's Pong surfaces via
    /// [`BridgeConfig::on_pong`].
    pub fn ping(&self, payload: impl Into<Vec<u8>>) -> io::Result<()> {
        self.send(ControlCmd::Ping(payload.into()))
    }

    /// Send an unsolicited Pong with `payload` (≤125 bytes).
    pub fn pong(&self, payload: impl Into<Vec<u8>>) -> io::Result<()> {
        self.send(ControlCmd::Pong(payload.into()))
    }

    /// Queue a Close with `code` and `reason` (≤123 bytes); the bridge winds down
    /// after the peer's close echo.
    pub fn close(&self, code: u16, reason: impl Into<String>) -> io::Result<()> {
        self.send(ControlCmd::Close(CloseFrame {
            code,
            reason: reason.into(),
        }))
    }

    fn send(&self, cmd: ControlCmd) -> io::Result<()> {
        self.tx
            .send(cmd)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "bridge has ended"))
    }
}

/// The receiving half of a [`control_channel`]; give it to
/// [`BridgeConfig::control`].
pub struct ControlReceiver(mpsc::UnboundedReceiver<ControlCmd>);

/// Create a control channel. Keep the [`WsControl`] to send control frames; put
/// the [`ControlReceiver`] in [`BridgeConfig::control`].
pub fn control_channel() -> (WsControl, ControlReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    (WsControl { tx }, ControlReceiver(rx))
}

/// Hook invoked with the close frame describing why the connection ended.
pub type CloseHook = Box<dyn FnMut(&CloseFrame) + Send>;
/// Hook invoked with a received Ping/Pong payload.
pub type ControlHook = Box<dyn FnMut(&[u8]) + Send>;

/// Server-initiated keepalive: send a Ping when the connection goes idle, and
/// disconnect the peer if it doesn't respond in time.
///
/// This is the *proactive* half of ping/pong — wslay already auto-answers
/// *incoming* pings, but never initiates its own. Browsers can't send pings from
/// JavaScript, so liveness for an h2ts client must be driven from the server;
/// the browser's platform auto-answers these pings transparently.
#[derive(Debug, Clone)]
pub struct KeepAlive {
    /// Send a Ping once the connection has been idle (no frame received) this long.
    pub interval: Duration,
    /// If no frame arrives within this long after that Ping, close the peer.
    pub timeout: Duration,
    /// Close frame sent to the peer, and surfaced to [`BridgeConfig::on_close`],
    /// when keepalive fails. Defaults to 1001 (Going Away), "keepalive timeout".
    pub close: CloseFrame,
}

impl KeepAlive {
    /// Keepalive with the given ping `interval` and response `timeout`, closing
    /// with 1001 (Going Away) on failure.
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self {
            interval,
            timeout,
            close: CloseFrame {
                code: wslay_status_code_WSLAY_CODE_GOING_AWAY as u16,
                reason: "keepalive timeout".to_string(),
            },
        }
    }
}

/// Control-frame configuration and hooks for [`bridge_with`] /
/// [`serve_h2_with`](crate::serve_h2_with).
///
/// [`BridgeConfig::default`] reproduces plain [`bridge`] behaviour: wslay
/// auto-answers pings with pongs, a Normal Closure is sent on teardown, no
/// keepalive, and no hooks fire. All fields are opt-in.
#[derive(Default)]
pub struct BridgeConfig {
    /// Close frame this side sends when it starts the close (e.g. the peer /
    /// upstream reached EOF). Defaults to 1000 Normal Closure, empty reason.
    pub close: CloseFrame,
    /// Server-initiated keepalive (ping-and-timeout). `None` disables it — send
    /// your own pings via [`control_channel`] instead.
    pub keepalive: Option<KeepAlive>,
    /// Receiver from [`control_channel`], to send control frames while running.
    pub control: Option<ControlReceiver>,
    /// Called once when the connection ends, with the close frame describing why:
    /// the peer's Close, the keepalive-timeout close, the teardown close, or
    /// 1006 (Abnormal) if the transport dropped without a Close.
    pub on_close: Option<CloseHook>,
    /// Called when a Ping is received (wslay has already auto-queued the Pong).
    pub on_ping: Option<ControlHook>,
    /// Called when a Pong is received.
    pub on_pong: Option<ControlHook>,
}

/// Why the bridge ended — used to surface a close reason to `on_close`.
enum EndReason {
    /// The peer sent a Close with this code + reason.
    PeerClose(CloseFrame),
    /// This side closed (peer/upstream EOF, or keepalive failure).
    LocalClose(CloseFrame),
    /// The WS transport ended without a Close frame (abnormal).
    Abnormal,
}

/// Queue a control/data message of `opcode` carrying `payload` (wslay copies it).
unsafe fn queue_msg(ctx: wslay_event_context_ptr, opcode: u8, payload: &[u8]) {
    let msg = wslay_event_msg {
        opcode,
        msg: payload.as_ptr(),
        msg_length: payload.len(),
    };
    wslay_event_queue_msg(ctx, &msg);
}

/// Queue a close with `code` + `reason`.
unsafe fn queue_close(ctx: wslay_event_context_ptr, code: u16, reason: &[u8]) {
    let (ptr, len) = if reason.is_empty() {
        (ptr::null(), 0)
    } else {
        (reason.as_ptr(), reason.len())
    };
    wslay_event_queue_close(ctx, code, ptr, len);
}

/// Pump bytes full-duplex between a WebSocket and a byte-stream peer until either
/// side closes, using wslay for framing. Equivalent to [`bridge_with`] with a
/// default [`BridgeConfig`].
///
/// `ws_io` is the raw upgraded WebSocket byte stream from [`accept`](crate::accept).
/// This is item 3's core: `bridge(ws_io, TcpStream::connect(upstream))` is a
/// websockify-equivalent WS→TCP proxy.
pub async fn bridge<S, P>(ws_io: S, peer: P) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    P: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    bridge_with(ws_io, peer, BridgeConfig::default()).await
}

/// Like [`bridge`], but with control-frame configuration and hooks
/// ([`BridgeConfig`]): send control frames via [`control_channel`], observe
/// received close/ping/pong, and set the close sent on teardown.
///
/// WS message payloads flow to `peer`; `peer` bytes flow back as binary WS
/// frames — all streamed incrementally, never buffering a whole frame.
pub async fn bridge_with<S, P>(ws_io: S, peer: P, config: BridgeConfig) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    P: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let BridgeConfig {
        close,
        keepalive,
        control,
        mut on_close,
        mut on_ping,
        mut on_pong,
    } = config;

    let (mut ws_read, ws_write) = tokio::io::split(ws_io);
    let (mut peer_read, mut peer_write) = tokio::io::split(peer);
    let ws_write = Arc::new(Mutex::new(ws_write));

    // Last time a frame was received from the WS peer; read by the keepalive
    // direction to decide when to ping and whether a pong came back. Shared and
    // `Send`; only ever locked briefly (never across an await).
    let last_activity = Arc::new(std::sync::Mutex::new(Instant::now()));

    // Lives for the whole future; the callbacks and every direction reach it via
    // its address (see the module note on Send-safety).
    let shared: Box<RefCell<Shared>> = Box::new(RefCell::new(Shared::default()));
    let sp_addr = &*shared as *const RefCell<Shared> as usize;

    // Confine every bare pointer to this block so none can live across an await.
    let (ctx_addr, _guard) = {
        let callbacks = wslay_event_callbacks {
            recv_callback: Some(recv_cb),
            send_callback: Some(send_cb),
            genmask_callback: None, // server role never masks
            on_frame_recv_start_callback: Some(frame_start_cb),
            on_frame_recv_chunk_callback: Some(frame_chunk_cb),
            on_frame_recv_end_callback: None,
            on_msg_recv_callback: Some(msg_recv_cb), // surface control frames
        };
        let mut ctx_raw: wslay_event_context_ptr = ptr::null_mut();
        let rc = unsafe {
            wslay_event_context_server_init(&mut ctx_raw, &callbacks, sp_addr as *mut c_void)
        };
        if rc != 0 || ctx_raw.is_null() {
            return Err(io::Error::other("wslay_event_context_server_init failed"));
        }
        unsafe { wslay_event_config_set_no_buffering(ctx_raw, 1) }; // incremental frames
        let addr = ctx_raw as usize;
        (addr, CtxGuard(addr))
    };

    // Direction 1: WebSocket -> peer. Also fires ping/pong hooks and flushes
    // wslay's auto-queued pong/close replies. Reports why it ends.
    let ws_to_peer = {
        let ws_write = ws_write.clone();
        let last_activity = last_activity.clone();
        async move {
            let mut buf = vec![0u8; READ_CHUNK];
            loop {
                let n = ws_read.read(&mut buf).await?;
                if n == 0 {
                    let _ = peer_write.shutdown().await;
                    return Ok(EndReason::Abnormal); // WS transport EOF, no Close
                }
                *last_activity.lock().unwrap() = Instant::now();
                let (to_peer, outbound, closed, events) = unsafe {
                    let ctx = ctx_addr as wslay_event_context_ptr;
                    let sp = &*(sp_addr as *const RefCell<Shared>);
                    sp.borrow_mut().inbound.extend_from_slice(&buf[..n]);
                    let recv_rc = wslay_event_recv(ctx); // fires callbacks
                    wslay_event_send(ctx); // flush auto-queued pong/close
                    let mut s = sp.borrow_mut();
                    let pos = s.inbound_pos;
                    s.inbound.drain(..pos);
                    s.inbound_pos = 0;
                    let closed = recv_rc != 0 || wslay_event_get_close_received(ctx) != 0;
                    (
                        std::mem::take(&mut s.to_peer),
                        std::mem::take(&mut s.outbound),
                        closed,
                        std::mem::take(&mut s.control_events),
                    )
                };
                if !to_peer.is_empty() {
                    peer_write.write_all(&to_peer).await?;
                }
                let mut peer_close = None;
                for event in events {
                    match event {
                        ControlEvent::Close { code, reason } => {
                            let code = if code == 0 {
                                wslay_status_code_WSLAY_CODE_NO_STATUS_RCVD as u16
                            } else {
                                code
                            };
                            peer_close = Some(CloseFrame {
                                code,
                                reason: String::from_utf8_lossy(&reason).into_owned(),
                            });
                        }
                        ControlEvent::Ping(p) => {
                            if let Some(cb) = on_ping.as_mut() {
                                cb(&p);
                            }
                        }
                        ControlEvent::Pong(p) => {
                            if let Some(cb) = on_pong.as_mut() {
                                cb(&p);
                            }
                        }
                    }
                }
                if !outbound.is_empty() {
                    ws_write.lock().await.write_all(&outbound).await?;
                }
                if closed {
                    let _ = peer_write.shutdown().await;
                    return Ok(peer_close.map_or(EndReason::Abnormal, EndReason::PeerClose));
                }
            }
        }
    };

    // Direction 2: peer -> WebSocket (framed as binary messages).
    let peer_to_ws = {
        let ws_write = ws_write.clone();
        async move {
            let mut buf = vec![0u8; READ_CHUNK];
            loop {
                let n = match peer_read.read(&mut buf).await {
                    Ok(0) | Err(_) => {
                        let outbound = unsafe {
                            let ctx = ctx_addr as wslay_event_context_ptr;
                            queue_close(ctx, close.code, close.reason.as_bytes());
                            wslay_event_send(ctx);
                            std::mem::take(&mut (*(sp_addr as *const RefCell<Shared>)).borrow_mut().outbound)
                        };
                        if !outbound.is_empty() {
                            let _ = ws_write.lock().await.write_all(&outbound).await;
                        }
                        return Ok(EndReason::LocalClose(close));
                    }
                    Ok(n) => n,
                };
                let outbound = unsafe {
                    let ctx = ctx_addr as wslay_event_context_ptr;
                    queue_msg(ctx, wslay_opcode_WSLAY_BINARY_FRAME as u8, &buf[..n]);
                    wslay_event_send(ctx);
                    std::mem::take(&mut (*(sp_addr as *const RefCell<Shared>)).borrow_mut().outbound)
                };
                if !outbound.is_empty() {
                    ws_write.lock().await.write_all(&outbound).await?;
                }
            }
        }
    };

    // Direction 3: application-injected control frames (ping/pong/close).
    let control_dir = {
        let ws_write = ws_write.clone();
        async move {
            let mut rx = match control {
                Some(ControlReceiver(rx)) => rx,
                // No control channel: park forever so this branch never ends the
                // bridge on its own.
                None => return future::pending::<io::Result<EndReason>>().await,
            };
            while let Some(cmd) = rx.recv().await {
                let outbound = unsafe {
                    let ctx = ctx_addr as wslay_event_context_ptr;
                    match &cmd {
                        ControlCmd::Ping(p) => queue_msg(ctx, wslay_opcode_WSLAY_PING as u8, p),
                        ControlCmd::Pong(p) => queue_msg(ctx, wslay_opcode_WSLAY_PONG as u8, p),
                        ControlCmd::Close(cf) => queue_close(ctx, cf.code, cf.reason.as_bytes()),
                    }
                    wslay_event_send(ctx);
                    std::mem::take(&mut (*(sp_addr as *const RefCell<Shared>)).borrow_mut().outbound)
                };
                if !outbound.is_empty() {
                    ws_write.lock().await.write_all(&outbound).await?;
                }
            }
            // All senders dropped: park rather than tear down the live bridge.
            future::pending::<io::Result<EndReason>>().await
        }
    };

    // Direction 4: server-initiated keepalive (ping when idle, close on timeout).
    let keepalive_dir = {
        let ws_write = ws_write.clone();
        let last_activity = last_activity.clone();
        async move {
            let ka = match keepalive {
                Some(ka) => ka,
                None => return future::pending::<io::Result<EndReason>>().await,
            };
            loop {
                // Wait until the connection has been idle for a full interval.
                let idle = last_activity.lock().unwrap().elapsed();
                if idle < ka.interval {
                    tokio::time::sleep(ka.interval - idle).await;
                    continue;
                }
                // Idle: send a Ping and note when.
                let outbound = unsafe {
                    let ctx = ctx_addr as wslay_event_context_ptr;
                    queue_msg(ctx, wslay_opcode_WSLAY_PING as u8, b"");
                    wslay_event_send(ctx);
                    std::mem::take(&mut (*(sp_addr as *const RefCell<Shared>)).borrow_mut().outbound)
                };
                if !outbound.is_empty() {
                    ws_write.lock().await.write_all(&outbound).await?;
                }
                let pinged_at = Instant::now();
                tokio::time::sleep(ka.timeout).await;
                // No frame (pong or data) since our ping? Disconnect.
                if *last_activity.lock().unwrap() <= pinged_at {
                    let outbound = unsafe {
                        let ctx = ctx_addr as wslay_event_context_ptr;
                        queue_close(ctx, ka.close.code, ka.close.reason.as_bytes());
                        wslay_event_send(ctx);
                        std::mem::take(&mut (*(sp_addr as *const RefCell<Shared>)).borrow_mut().outbound)
                    };
                    if !outbound.is_empty() {
                        let _ = ws_write.lock().await.write_all(&outbound).await;
                    }
                    return Ok(EndReason::LocalClose(ka.close));
                }
            }
        }
    };

    // Single task, cooperative: the directions interleave only at awaits, so the
    // synchronous wslay sections never overlap. First to finish tears down.
    let result = tokio::select! {
        r = ws_to_peer => r,
        r = peer_to_ws => r,
        r = control_dir => r,
        r = keepalive_dir => r,
    };
    drop(_guard); // free ctx while `shared` is still alive
    drop(shared);

    // Surface the close reason once, whatever ended the bridge.
    let closed_with = match &result {
        Ok(EndReason::PeerClose(cf)) | Ok(EndReason::LocalClose(cf)) => cf.clone(),
        Ok(EndReason::Abnormal) => CloseFrame {
            code: wslay_status_code_WSLAY_CODE_ABNORMAL_CLOSURE as u16,
            reason: String::new(),
        },
        Err(e) => CloseFrame {
            code: wslay_status_code_WSLAY_CODE_ABNORMAL_CLOSURE as u16,
            reason: e.to_string(),
        },
    };
    if let Some(cb) = on_close.as_mut() {
        cb(&closed_with);
    }
    result.map(|_| ())
}
