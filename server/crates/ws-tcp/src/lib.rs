//! Make a WebSocket carry a raw byte stream (item 1 of the server plan).
//!
//! [`accept`] performs the server-side WebSocket handshake on a hyper request
//! (pluggable into any hyper/axum route — item 4). [`bridge`] then pumps bytes
//! full-duplex between that WebSocket and any `AsyncRead + AsyncWrite` peer (a
//! TCP upstream, an in-process h2c server, …). WebSocket message *payloads*
//! become a continuous byte stream, so h2c framing rides straight through.
//!
//! The framing backend is an implementation detail. Today it's [`fastwebsockets`]
//! (pure Rust, no C dependency). A future wslay-based backend that streams
//! sub-frame (never buffering a whole frame) can replace [`bridge`]'s internals
//! without changing this surface.
//!
//! Note: `fastwebsockets-stream`'s `AsyncRead+AsyncWrite` adapter is *half
//! duplex* (it moves the single socket into the read future, so a concurrent
//! write fails with "Websocket not available"). A proxy needs full duplex, so we
//! drive the split read/write halves ourselves.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use fastwebsockets::upgrade::UpgradeFut;
use fastwebsockets::{Frame, OpCode, Payload, WebSocket};
use http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL};
use http_body_util::Empty;
use hyper::body::{Body, Incoming};
use hyper::server::conn::http2;
use hyper::service::Service;
use hyper::upgrade::Upgraded;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::Mutex;

pub use fastwebsockets::upgrade::is_upgrade_request;
pub use fastwebsockets::WebSocketError;

/// The concrete WebSocket type produced by [`accept`] over a hyper upgrade.
pub type UpgradedWebSocket = WebSocket<TokioIo<Upgraded>>;

/// Whether the request offers the given WebSocket subprotocol.
fn offers_protocol<B>(request: &Request<B>, name: &str) -> bool {
    request
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(|list| list.split(',').any(|p| p.trim().eq_ignore_ascii_case(name)))
        .unwrap_or(false)
}

/// Accept a WebSocket upgrade on a hyper request.
///
/// Returns the `101 Switching Protocols` response to send back immediately, plus
/// a future that resolves to the upgraded [`WebSocket`]. Drive the response
/// through your framework; spawn the future and hand the socket to [`bridge`].
///
/// If the client offered the `binary` subprotocol (h2ts / websockify clients do)
/// it is echoed.
pub fn accept<B>(
    request: &mut Request<B>,
) -> Result<
    (
        Response<Empty<Bytes>>,
        impl std::future::Future<Output = Result<UpgradedWebSocket, WebSocketError>>,
    ),
    WebSocketError,
> {
    let echo_binary = offers_protocol(request, "binary");

    let (mut response, upgrade_fut): (_, UpgradeFut) =
        fastwebsockets::upgrade::upgrade(&mut *request)?;
    if echo_binary {
        response
            .headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("binary"));
    }

    let fut = async move {
        let mut ws = upgrade_fut.await?;
        ws.set_auto_pong(true); // reply to WS pings
        ws.set_auto_close(true); // reply to WS close frames
        Ok(ws)
    };
    Ok((response, fut))
}

/// Read buffer size for the peer→WebSocket direction.
const COPY_BUF: usize = 64 * 1024;

/// Pump bytes full-duplex between a WebSocket and a byte-stream peer until either
/// side closes. WS message payloads flow to `peer`; `peer` bytes flow back as
/// binary WS frames.
///
/// This is item 3's core: `bridge(ws, TcpStream::connect(upstream))` is a
/// websockify-equivalent WS→TCP proxy.
pub async fn bridge<S, P>(ws: WebSocket<S>, peer: P) -> Result<(), WebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    P: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut ws_read, ws_write) = ws.split(|s| tokio::io::split(s));
    let (mut peer_read, mut peer_write) = tokio::io::split(peer);

    // The write half is shared: both the peer→WS direction and the read side's
    // obligated pong/close replies write through it.
    let ws_write = Arc::new(Mutex::new(ws_write));

    // WebSocket -> peer
    let ws_to_peer = {
        let ws_write = ws_write.clone();
        async move {
            loop {
                // Obligated control frames (pong/close) are copied to owned bytes
                // so nothing borrows the read buffer across the write .await.
                let mut send_fn = |frame: Frame<'_>| {
                    let ws_write = ws_write.clone();
                    let opcode = frame.opcode;
                    let payload = frame.payload.to_vec();
                    async move {
                        ws_write
                            .lock()
                            .await
                            .write_frame(Frame::new(true, opcode, None, Payload::Owned(payload)))
                            .await
                    }
                };
                let frame = ws_read.read_frame(&mut send_fn).await?;
                match frame.opcode {
                    OpCode::Binary | OpCode::Text | OpCode::Continuation => {
                        if peer_write.write_all(&frame.payload).await.is_err() {
                            break;
                        }
                    }
                    OpCode::Close => break,
                    OpCode::Ping | OpCode::Pong => {} // handled via send_fn / ignored
                }
            }
            let _ = peer_write.shutdown().await;
            Ok::<(), WebSocketError>(())
        }
    };

    // peer -> WebSocket
    let peer_to_ws = {
        let ws_write = ws_write.clone();
        async move {
            let mut buf = vec![0u8; COPY_BUF];
            loop {
                match peer_read.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let frame = Frame::binary(Payload::Owned(buf[..n].to_vec()));
                        if ws_write.lock().await.write_frame(frame).await.is_err() {
                            break;
                        }
                    }
                }
            }
            let _ = ws_write
                .lock()
                .await
                .write_frame(Frame::close(1000, b""))
                .await;
            Ok::<(), WebSocketError>(())
        }
    };

    // Whichever side ends first tears down the bridge.
    tokio::select! {
        r = ws_to_peer => r,
        r = peer_to_ws => r,
    }
}

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
    /// Wrap an upgraded WebSocket, spawning the bridge pump onto the current
    /// tokio runtime.
    pub fn from_websocket<S>(ws: WebSocket<S>) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (app_side, ws_side) = tokio::io::duplex(COPY_BUF);
        tokio::spawn(async move {
            let _ = bridge(ws, ws_side).await;
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
/// [`WsByteStream::from_websocket`] instead.
pub async fn serve_h2<S, Svc, B>(ws: WebSocket<S>, service: Svc) -> hyper::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    Svc: Service<Request<Incoming>, Response = Response<B>> + Send + 'static,
    Svc::Future: Send + 'static,
    Svc::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let io = TokioIo::new(WsByteStream::from_websocket(ws));
    http2::Builder::new(TokioExecutor::new())
        .serve_connection(io, service)
        .await
}

#[cfg(feature = "wslay")]
mod wslay;
#[cfg(feature = "wslay")]
pub use wslay::wslay_bridge;

/// Like [`serve_h2`], but frames the WebSocket with wslay (feature `wslay`),
/// which streams frame payloads incrementally rather than buffering whole
/// frames. The service side is served over an in-memory duplex; the WebSocket
/// side is pumped by [`wslay_bridge`].
#[cfg(feature = "wslay")]
pub async fn wslay_serve_h2<Svc, B>(ws: UpgradedWebSocket, service: Svc) -> std::io::Result<()>
where
    Svc: Service<Request<Incoming>, Response = Response<B>> + Send + 'static,
    Svc::Future: Send + 'static,
    Svc::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let (app_side, ws_side) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let io = TokioIo::new(app_side);
        let _ = http2::Builder::new(TokioExecutor::new())
            .serve_connection(io, service)
            .await;
    });
    wslay_bridge(ws, ws_side).await
}

#[cfg(test)]
mod tests {
    use super::offers_protocol;
    use http::header::SEC_WEBSOCKET_PROTOCOL;
    use hyper::Request;

    fn req(protocol: Option<&str>) -> Request<()> {
        let mut b = Request::builder();
        if let Some(p) = protocol {
            b = b.header(SEC_WEBSOCKET_PROTOCOL, p);
        }
        b.body(()).unwrap()
    }

    #[test]
    fn offers_protocol_is_case_insensitive_and_list_aware() {
        assert!(offers_protocol(&req(Some("binary")), "binary"));
        assert!(offers_protocol(&req(Some("chat, binary")), "binary"));
        assert!(offers_protocol(&req(Some(" BINARY ")), "binary"));
        assert!(!offers_protocol(&req(Some("chat")), "binary"));
        assert!(!offers_protocol(&req(None), "binary"));
    }
}
