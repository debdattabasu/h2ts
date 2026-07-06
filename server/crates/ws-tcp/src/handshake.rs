//! Server-side WebSocket handshake (RFC 6455).
//!
//! It's small: parse the upgrade headers, compute `Sec-WebSocket-Accept` (SHA-1
//! of the client key plus the RFC 6455 magic GUID, base64-encoded), reply `101`,
//! and hand back hyper's upgraded connection as a raw byte stream. The framing on
//! top of that stream is done by [`bridge`](crate::bridge) using wslay.

use std::fmt;
use std::future::Future;

use base64::Engine as _;
use bytes::Bytes;
use http::header::{
    HeaderName, HeaderValue, CONNECTION, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY,
    SEC_WEBSOCKET_PROTOCOL, UPGRADE,
};
use http_body_util::Empty;
use hyper::upgrade::Upgraded;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use sha1::{Digest, Sha1};

/// RFC 6455 §4.2.2: the magic GUID appended to the client key before hashing.
const WS_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// The subprotocol h2ts clients offer by default. [`accept`] echoes it when the
/// client offered it and the handler didn't choose another (see [`accept_with`]).
pub const DEFAULT_SUBPROTOCOL: &str = "h2ts";

/// The upgraded WebSocket connection, presented as a raw tokio byte stream
/// (`AsyncRead + AsyncWrite`). Hand it to [`bridge`](crate::bridge),
/// [`serve_h2`](crate::serve_h2), or [`WsByteStream`](crate::WsByteStream).
pub type UpgradedIo = TokioIo<Upgraded>;

/// Errors from the WebSocket handshake.
#[derive(Debug)]
pub enum WebSocketError {
    /// The request was not a valid WebSocket upgrade (missing/invalid
    /// `Upgrade` / `Connection` / `Sec-WebSocket-Key`).
    NotUpgradeRequest,
    /// hyper failed to upgrade the connection after the `101` was sent.
    Upgrade(hyper::Error),
}

impl fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WebSocketError::NotUpgradeRequest => f.write_str("not a WebSocket upgrade request"),
            WebSocketError::Upgrade(e) => write!(f, "WebSocket upgrade failed: {e}"),
        }
    }
}

impl std::error::Error for WebSocketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WebSocketError::Upgrade(e) => Some(e),
            WebSocketError::NotUpgradeRequest => None,
        }
    }
}

/// Whether `header` is present and lists `needle` as one of its comma-separated,
/// case-insensitive tokens.
fn header_lists<B>(request: &Request<B>, header: HeaderName, needle: &str) -> bool {
    request
        .headers()
        .get(header)
        .and_then(|v| v.to_str().ok())
        .map(|list| list.split(',').any(|t| t.trim().eq_ignore_ascii_case(needle)))
        .unwrap_or(false)
}

/// Whether `request` is a WebSocket upgrade: `Upgrade: websocket`,
/// `Connection: upgrade`, and a `Sec-WebSocket-Key`.
pub fn is_upgrade_request<B>(request: &Request<B>) -> bool {
    header_lists(request, UPGRADE, "websocket")
        && header_lists(request, CONNECTION, "upgrade")
        && request.headers().contains_key(SEC_WEBSOCKET_KEY)
}

/// The WebSocket subprotocols the client offered, in order, whitespace-trimmed;
/// empty if it offered none. Gives a handler full visibility into the offer so
/// it can pass one to [`accept_with`].
pub fn offered_protocols<B>(request: &Request<B>) -> Vec<&str> {
    request
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(|list| {
            list.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Compute the `Sec-WebSocket-Accept` value for a client `Sec-WebSocket-Key`.
fn accept_key(key: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key);
    hasher.update(WS_GUID);
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

/// Accept a WebSocket upgrade, choosing which offered subprotocol to echo.
///
/// `select` is handed the full list of subprotocols the client offered (the same
/// thing [`offered_protocols`] returns) and returns the one to echo in the `101`,
/// or `None` to decline. When it declines, [`DEFAULT_SUBPROTOCOL`] (`h2ts`) is
/// echoed if the client offered it; otherwise nothing is echoed.
///
/// Per RFC 6455 a client that offered subprotocols will *fail* the connection if
/// the server echoes one it did not offer, so `select` should return a member of
/// the offered list (or `None`).
///
/// See [`accept`] for the common case.
///
/// ```ignore
/// // Prefer "chat" if the client offered it, otherwise fall back to h2ts.
/// let (response, ws_fut) = ws_tcp::accept_with(&mut req, |offered| {
///     offered.iter().find(|p| p.eq_ignore_ascii_case("chat")).map(|p| p.to_string())
/// })?;
/// ```
// The returned `impl Future` can't be aliased away, so the tuple reads as
// "complex"; it's just (response, upgrade-future).
#[allow(clippy::type_complexity)]
pub fn accept_with<B, F>(
    request: &mut Request<B>,
    select: F,
) -> Result<
    (
        Response<Empty<Bytes>>,
        impl Future<Output = Result<UpgradedIo, WebSocketError>>,
    ),
    WebSocketError,
>
where
    F: FnOnce(&[&str]) -> Option<String>,
{
    if !is_upgrade_request(request) {
        return Err(WebSocketError::NotUpgradeRequest);
    }
    // Present because `is_upgrade_request` checked for it.
    let key = request
        .headers()
        .get(SEC_WEBSOCKET_KEY)
        .ok_or(WebSocketError::NotUpgradeRequest)?;
    let accept_value = accept_key(key.as_bytes());

    // Let the handler pick from the offered list; otherwise echo the default
    // subprotocol when the client offered it.
    let offered = offered_protocols(request);
    let chosen = select(&offered).or_else(|| {
        offered
            .iter()
            .find(|p| p.eq_ignore_ascii_case(DEFAULT_SUBPROTOCOL))
            .map(|p| p.to_string())
    });
    drop(offered); // release the immutable borrow before upgrading below

    // base64 output is always valid header-value ASCII, so this never fails.
    let accept_header =
        HeaderValue::from_str(&accept_value).expect("base64 is valid header ASCII");
    let mut response = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(CONNECTION, HeaderValue::from_static("Upgrade"))
        .header(UPGRADE, HeaderValue::from_static("websocket"))
        .header(SEC_WEBSOCKET_ACCEPT, accept_header)
        .body(Empty::<Bytes>::new())
        .expect("static 101 response is well-formed");
    if let Some(proto) = chosen {
        // Skip a subprotocol that isn't a valid header value rather than fail.
        if let Ok(value) = HeaderValue::from_str(&proto) {
            response
                .headers_mut()
                .insert(SEC_WEBSOCKET_PROTOCOL, value);
        }
    }

    // Capture the upgrade future now; it resolves once the 101 is flushed and
    // hyper hands over the connection.
    let on_upgrade = hyper::upgrade::on(&mut *request);
    let fut = async move {
        let upgraded = on_upgrade.await.map_err(WebSocketError::Upgrade)?;
        Ok(TokioIo::new(upgraded))
    };
    Ok((response, fut))
}

/// Accept a WebSocket upgrade, echoing [`DEFAULT_SUBPROTOCOL`] (`h2ts`) when the
/// client offered it.
///
/// Returns the `101 Switching Protocols` response to send back immediately, plus
/// a future that resolves to the upgraded connection as a byte stream
/// ([`UpgradedIo`]). Drive the response through your framework; spawn the future
/// and hand the stream to [`bridge`](crate::bridge) or
/// [`serve_h2`](crate::serve_h2).
///
/// Use [`accept_with`] to inspect the offered subprotocols (see
/// [`offered_protocols`]) and choose which to echo. Control frames (ping/close)
/// are handled downstream by wslay.
///
/// The outer HTTP/1 connection must be served with upgrades enabled
/// (`http1::Builder::serve_connection(..).with_upgrades()`).
#[allow(clippy::type_complexity)]
pub fn accept<B>(
    request: &mut Request<B>,
) -> Result<
    (
        Response<Empty<Bytes>>,
        impl Future<Output = Result<UpgradedIo, WebSocketError>>,
    ),
    WebSocketError,
> {
    accept_with(request, |_offered| None)
}

#[cfg(test)]
mod tests {
    use super::{is_upgrade_request, offered_protocols};
    use http::header::{
        CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, UPGRADE,
    };
    use hyper::Request;

    fn with_protocol(protocol: Option<&str>) -> Request<()> {
        let mut b = Request::builder();
        if let Some(p) = protocol {
            b = b.header(SEC_WEBSOCKET_PROTOCOL, p);
        }
        b.body(()).unwrap()
    }

    #[test]
    fn offered_protocols_parses_the_list_in_order() {
        assert_eq!(offered_protocols(&with_protocol(Some("h2ts"))), ["h2ts"]);
        assert_eq!(
            offered_protocols(&with_protocol(Some("chat, h2ts, binary"))),
            ["chat", "h2ts", "binary"]
        );
        assert_eq!(offered_protocols(&with_protocol(Some("  h2ts  "))), ["h2ts"]);
        assert!(offered_protocols(&with_protocol(Some(""))).is_empty());
        assert!(offered_protocols(&with_protocol(None)).is_empty());
    }

    #[test]
    fn is_upgrade_request_requires_all_three_signals() {
        let full = Request::builder()
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "Upgrade")
            .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .body(())
            .unwrap();
        assert!(is_upgrade_request(&full));

        // Connection may be a token list ("keep-alive, Upgrade").
        let listed = Request::builder()
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "keep-alive, Upgrade")
            .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
            .body(())
            .unwrap();
        assert!(is_upgrade_request(&listed));

        // Missing the key.
        let no_key = Request::builder()
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "Upgrade")
            .body(())
            .unwrap();
        assert!(!is_upgrade_request(&no_key));

        // A plain request.
        assert!(!is_upgrade_request(&Request::builder().body(()).unwrap()));
    }

    #[test]
    fn accept_key_matches_rfc_6455_example() {
        // RFC 6455 §1.3: key "dGhlIHNhbXBsZSBub25jZQ==" -> accept
        // "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".
        assert_eq!(
            super::accept_key(b"dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }
}
