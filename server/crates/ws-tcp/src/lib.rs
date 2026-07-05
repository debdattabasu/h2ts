//! Make a WebSocket carry a raw byte stream, and serve/proxy HTTP/2 over it.
//!
//! [`accept`] performs the server-side WebSocket handshake on a hyper request
//! (pluggable into any hyper/axum route — item 4) and yields the upgraded
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
//! - [`serve_h2`] — run any hyper `Service` as HTTP/2 over the tunnel (item 2).
//! - the `h2ts-proxy` binary — a standalone WS→upstream-h2c proxy (item 3).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper::body::{Body, Incoming};
use hyper::server::conn::http2;
use hyper::service::Service;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

mod handshake;
mod wslay;

pub use handshake::{accept, is_upgrade_request, UpgradedIo, WebSocketError};
pub use wslay::bridge;

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
        let (app_side, ws_side) = tokio::io::duplex(DUPLEX_BUF);
        tokio::spawn(async move {
            let _ = bridge(ws_io, ws_side).await;
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
/// Typical use in a route handler:
/// ```ignore
/// let (response, ws_fut) = ws_tcp::accept(&mut req)?;
/// tokio::spawn(async move {
///     if let Ok(ws) = ws_fut.await {
///         let _ = ws_tcp::serve_h2(ws, my_service).await;
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
    let io = TokioIo::new(WsByteStream::new(ws_io));
    http2::Builder::new(TokioExecutor::new())
        .serve_connection(io, service)
        .await
}
