//! wslay framing backend (feature `wslay`).
//!
//! [`fastwebsockets`] buffers a whole frame before yielding it. wslay, driven
//! through its event API with `no_buffering` enabled, delivers each frame's
//! payload **incrementally** via `on_frame_recv_chunk_callback` — so we never
//! hold a whole frame in memory, no matter how large. Control frames (ping /
//! close) are still auto-handled by wslay.
//!
//! wslay is a synchronous, I/O-agnostic state machine: it never touches the
//! socket itself, only in-memory buffers via callbacks. We drive it from async
//! Rust — feeding it bytes we read from the WebSocket and flushing bytes it
//! produces. The HTTP upgrade is still done by hyper/fastwebsockets in
//! [`accept`](crate::accept); here we take the raw upgraded stream via
//! [`WebSocket::into_inner`] (no frames have been read yet, so nothing is lost).
//!
//! The wslay context and the shared-state cell are reached from the two
//! directions as `usize` addresses (which are `Send`), cast back to raw pointers
//! only inside the synchronous `unsafe` sections. This keeps the future `Send`
//! (so it composes like [`bridge`](crate::bridge)) while never letting a bare
//! pointer live across an `.await`. It is sound because everything runs on one
//! task: the two directions interleave at awaits but never execute concurrently.

use std::cell::RefCell;
use std::ffi::c_void;
use std::io;
use std::os::raw::c_int;
use std::ptr;
use std::slice;
use std::sync::Arc;

use fastwebsockets::WebSocket;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
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

/// Frees the wslay context on drop (even on early return / panic). Holds the
/// context as a `usize` address so the guard is naturally `Send` and no bare
/// pointer is ever kept across an `.await`.
struct CtxGuard(usize);
impl Drop for CtxGuard {
    fn drop(&mut self) {
        unsafe { wslay_event_context_free(self.0 as wslay_event_context_ptr) };
    }
}

/// Full-duplex byte pump between a WebSocket and a peer, using wslay for framing.
/// The wslay analogue of [`bridge`](crate::bridge): WS payloads flow to `peer`;
/// `peer` bytes flow back as binary WS frames — all streamed incrementally,
/// never buffering a whole frame.
pub async fn wslay_bridge<S, P>(ws: WebSocket<S>, peer: P) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    P: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Take the raw upgraded stream; no frames have been read yet.
    let raw = ws.into_inner();
    let (mut ws_read, ws_write) = tokio::io::split(raw);
    let (mut peer_read, mut peer_write) = tokio::io::split(peer);
    let ws_write = Arc::new(Mutex::new(ws_write));

    // Lives for the whole future; the callbacks and both directions reach it via
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
            on_msg_recv_callback: None,
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

    // Direction 1: WebSocket -> peer (also flushes wslay's pong/close replies).
    let ws_to_peer = {
        let ws_write = ws_write.clone();
        async move {
            let mut buf = vec![0u8; READ_CHUNK];
            loop {
                let n = ws_read.read(&mut buf).await?;
                if n == 0 {
                    break; // WS transport EOF
                }
                let (to_peer, outbound, closed) = unsafe {
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
                    )
                };
                if !to_peer.is_empty() {
                    peer_write.write_all(&to_peer).await?;
                }
                if !outbound.is_empty() {
                    ws_write.lock().await.write_all(&outbound).await?;
                }
                if closed {
                    break;
                }
            }
            let _ = peer_write.shutdown().await;
            Ok::<(), io::Error>(())
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
                            wslay_event_queue_close(
                                ctx,
                                wslay_status_code_WSLAY_CODE_NORMAL_CLOSURE as u16,
                                ptr::null(),
                                0,
                            );
                            wslay_event_send(ctx);
                            std::mem::take(&mut (*(sp_addr as *const RefCell<Shared>)).borrow_mut().outbound)
                        };
                        if !outbound.is_empty() {
                            let _ = ws_write.lock().await.write_all(&outbound).await;
                        }
                        break;
                    }
                    Ok(n) => n,
                };
                let outbound = unsafe {
                    let ctx = ctx_addr as wslay_event_context_ptr;
                    let msg = wslay_event_msg {
                        opcode: wslay_opcode_WSLAY_BINARY_FRAME as u8,
                        msg: buf.as_ptr(),
                        msg_length: n,
                    };
                    wslay_event_queue_msg(ctx, &msg); // copies the payload
                    wslay_event_send(ctx);
                    std::mem::take(&mut (*(sp_addr as *const RefCell<Shared>)).borrow_mut().outbound)
                };
                if !outbound.is_empty() {
                    ws_write.lock().await.write_all(&outbound).await?;
                }
            }
            Ok::<(), io::Error>(())
        }
    };

    // Single task, cooperative: the two directions interleave only at awaits, so
    // the synchronous wslay sections never overlap. First to finish tears down.
    let result = tokio::select! {
        r = ws_to_peer => r,
        r = peer_to_ws => r,
    };
    drop(_guard); // free ctx while `shared` is still alive
    drop(shared);
    result
}
