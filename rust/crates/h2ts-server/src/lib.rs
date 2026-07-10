//! Make a WebSocket carry a raw byte stream, and serve/proxy HTTP/2 over it.
//!
//! [`accept`] performs the server-side WebSocket handshake on a hyper request
//! (pluggable into any hyper/axum route) and yields the upgraded
//! connection as a byte stream. [`bridge`] then pumps bytes full-duplex between
//! that stream and any `AsyncRead + AsyncWrite` peer (a TCP upstream, an
//! in-process h2c server, …). WebSocket message *payloads* become a continuous
//! byte stream, so h2c framing rides straight through.
//!
//! Framing is done by [`wslay`](https://github.com/tatsuhiro-t/wslay) (vendored
//! C, via the `wslay-sys` crate). Driven through its event API with buffering
//! off, wslay streams each frame's payload **incrementally** — it never holds a
//! whole frame in memory, no matter how large — and auto-handles ping/close.
//!
//! Three entry points sit on top of [`bridge`]:
//! - [`WsByteStream`] — the WebSocket as an `AsyncRead + AsyncWrite` handle.
//! - [`serve_h2`] — run any hyper `Service` as HTTP/2 over the tunnel.
//! - the `h2ts-proxy` binary — a standalone WS→upstream-h2c proxy.
//!
//! Part of [h2ts](https://github.com/debdattabasu/h2ts): this server pairs with
//! the TypeScript [`@debdattabasu/h2ts`](https://www.npmjs.com/package/@debdattabasu/h2ts)
//! and Rust/WASM [`h2ts-client`](https://crates.io/crates/h2ts-client) frontend
//! clients — both behavior-mirrored against it by the shared conformance suite —
//! or any client that forwards a byte stream over a WebSocket.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::body::{Body, Incoming};
use hyper::server::conn::http2;
use hyper::service::Service;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

mod handshake;
mod idle;
mod wslay;

pub use handshake::{
    accept, accept_with, accept_with_options, is_upgrade_request, offered_protocols, AcceptOptions,
    UpgradedIo, WebSocketError, DEFAULT_SUBPROTOCOL,
};
pub use wslay::{
    bridge, bridge_with, control_channel, BridgeConfig, CloseFrame, CloseHook, ControlHook,
    ControlReceiver, KeepAlive, WsControl,
};

/// Size of the in-memory duplex between the WebSocket pump and the app side.
const DUPLEX_BUF: usize = 64 * 1024;

/// A WebSocket presented as a raw byte duplex (`AsyncRead + AsyncWrite`) — item
/// 1 in its "looks like a TCP stream" form.
///
/// A [`bridge`] runs on a spawned task pumping the WebSocket to one end of an
/// in-memory [`tokio::io::duplex`]; this handle is the other end. That makes it
/// usable anywhere a TCP stream is — most importantly, handed straight to an
/// h2c server via `serve_connection` to run an in-process HTTP/2 service over
/// the tunnel (item 2).
pub struct WsByteStream {
    inner: DuplexStream,
}

impl WsByteStream {
    /// Wrap an upgraded WebSocket byte stream (from [`accept`]), spawning the
    /// bridge pump onto the current tokio runtime.
    pub fn new<S>(ws_io: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::with_config(ws_io, BridgeConfig::default())
    }

    /// Like [`WsByteStream::new`], but with control-frame configuration and hooks
    /// ([`BridgeConfig`]) — e.g. a [`control_channel`] to send control frames, or
    /// `on_close` to observe the peer's close reason.
    pub fn with_config<S>(ws_io: S, config: BridgeConfig) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (app_side, ws_side) = tokio::io::duplex(DUPLEX_BUF);
        tokio::spawn(async move {
            let _ = bridge_with(ws_io, ws_side, config).await;
        });
        Self { inner: app_side }
    }
}

impl AsyncRead for WsByteStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for WsByteStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Serve **any** hyper HTTP/2 service over a WebSocket tunnel (item 2).
///
/// `service` is any `hyper::service::Service<Request<Incoming>>` — a
/// `service_fn`, an `axum::Router`, a `tower` service via hyper's compat, etc.
/// The WebSocket is presented as a byte stream via [`WsByteStream`] and the
/// request/response traffic is real HTTP/2 (h2c, prior-knowledge) on top.
///
/// The tunnel runs server-initiated WebSocket keepalive **on by default**
/// ([`BridgeConfig::default`]) so a silently-dead client can't leak the
/// connection; use [`serve_h2_with`] with a custom [`BridgeConfig`] to tune or
/// disable it (`keepalive: None`).
///
/// Typical use in a route handler:
/// ```ignore
/// let (response, ws_fut) = h2ts_server::accept(&mut req)?;
/// tokio::spawn(async move {
///     if let Ok(ws) = ws_fut.await {
///         let _ = h2ts_server::serve_h2(ws, my_service).await;
///     }
/// });
/// Ok(response) // send the 101 back
/// ```
///
/// For custom HTTP/2 settings, build the connection yourself over
/// [`WsByteStream::new`] instead.
pub async fn serve_h2<S, Svc, B>(ws_io: S, service: Svc) -> hyper::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    Svc: Service<Request<Incoming>, Response = Response<B>> + Send + 'static,
    Svc::Future: Send + 'static,
    Svc::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    serve_h2_with(ws_io, service, BridgeConfig::default()).await
}

/// Like [`serve_h2`], but with control-frame configuration and hooks
/// ([`BridgeConfig`]) applied to the underlying WebSocket bridge — send control
/// frames via a [`control_channel`], observe the peer's close reason, etc.
///
/// For an HTTP/2 idle timeout too, use [`serve_h2_with_config`].
pub async fn serve_h2_with<S, Svc, B>(
    ws_io: S,
    service: Svc,
    config: BridgeConfig,
) -> hyper::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    Svc: Service<Request<Incoming>, Response = Response<B>> + Send + 'static,
    Svc::Future: Send + 'static,
    Svc::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    serve_h2_with_config(
        ws_io,
        service,
        ServeConfig {
            bridge: config,
            idle_timeout: None,
        },
    )
    .await
}

/// Configuration for [`serve_h2_with_config`]: the WebSocket [`BridgeConfig`]
/// plus an optional HTTP/2 idle timeout.
#[derive(Default)]
pub struct ServeConfig {
    /// WebSocket bridge configuration (keepalive, control frames, close hooks).
    /// [`ServeConfig::default`] uses [`BridgeConfig::default`] — keepalive on.
    pub bridge: BridgeConfig,
    /// If `Some`, gracefully close the connection — an HTTP/2 `GOAWAY`, then the
    /// WebSocket close — once it has had **no open HTTP/2 streams** for this long,
    /// reaping a tunnel that is healthy but idle.
    ///
    /// This is distinct from keepalive ([`BridgeConfig::keepalive`]), which
    /// detects a *dead* peer: a client that dutifully answers keepalive pings but
    /// opens no streams for `idle_timeout` is still reaped. WebSocket and HTTP/2
    /// pings are **not** activity — only streams reset the timer. `None` (the
    /// default) never reaps on idle, matching hyper.
    pub idle_timeout: Option<Duration>,
}

/// Like [`serve_h2_with`], but also accepts an HTTP/2 [`idle_timeout`] via
/// [`ServeConfig`]: after that long with no open HTTP/2 streams, the connection
/// is closed with a graceful `GOAWAY` (then the WebSocket close), so a healthy
/// but idle client reconnects fresh. hyper has no built-in idle timeout, so this
/// tracks open streams and drives `graceful_shutdown` itself; a live stream
/// (even a quiet one) is never reaped, and pings never reset the timer.
///
/// [`idle_timeout`]: ServeConfig::idle_timeout
pub async fn serve_h2_with_config<S, Svc, B>(
    ws_io: S,
    service: Svc,
    config: ServeConfig,
) -> hyper::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    Svc: Service<Request<Incoming>, Response = Response<B>> + Send + 'static,
    Svc::Future: Send + 'static,
    Svc::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let ServeConfig {
        bridge,
        idle_timeout,
    } = config;
    let io = TokioIo::new(WsByteStream::with_config(ws_io, bridge));

    // Wrap the service so we can count open streams and reap on idle.
    let counter = idle::StreamCounter::new();
    let service = idle::TrackedService::new(service, counter.clone());
    let conn = http2::Builder::new(TokioExecutor::new()).serve_connection(io, service);

    let Some(idle) = idle_timeout else {
        return conn.await;
    };

    let watch = counter.watch();
    tokio::pin!(conn);
    tokio::select! {
        res = conn.as_mut() => res,
        // No open streams for `idle`: GOAWAY, admit nothing new, drive to close.
        () = idle::wait_idle(watch, idle) => {
            conn.as_mut().graceful_shutdown();
            conn.as_mut().await
        }
    }
}
