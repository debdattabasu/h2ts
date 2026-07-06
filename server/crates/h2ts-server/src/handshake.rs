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

/// Options controlling how the handshake picks a subprotocol to echo when the
/// handler's selector declines (see [`accept_with_options`]).
#[derive(Debug, Clone, Copy, Default)]
pub struct AcceptOptions {
    /// When the selector returns `None` and the client did **not** offer
    /// [`DEFAULT_SUBPROTOCOL`] (`h2ts`), accept the client's first offered
    /// subprotocol (or, if it offered none, complete with no subprotocol)
    /// instead of **rejecting** the handshake.
    ///
    /// Off by default: a client that speaks neither `h2ts` nor a
    /// handler-selected protocol is rejected with
    /// [`WebSocketError::UnsupportedSubprotocol`]. Turn this on for a
    /// codec-agnostic tunnel that accepts whatever framing the client offered
    /// (e.g. a raw/binary tunnel, or a websockify-style `binary` client).
    pub allow_implicit_codec: bool,
}

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
    /// The client offered no subprotocol the server accepts: it didn't offer
    /// [`DEFAULT_SUBPROTOCOL`] (`h2ts`), the handler's selector declined, and
    /// [`AcceptOptions::allow_implicit_codec`] was off. Reject it with a `400`
    /// (see [`WebSocketError::rejection_response`]).
    UnsupportedSubprotocol,
    /// hyper failed to upgrade the connection after the `101` was sent.
    Upgrade(hyper::Error),
}

impl WebSocketError {
    /// The HTTP response to reject this handshake with, for the pre-upgrade
    /// errors returned by [`accept`] / [`accept_with`] / [`accept_with_options`]:
    /// `426 Upgrade Required` for a non-WebSocket request, `400 Bad Request` for
    /// an unsupported subprotocol. (An [`Upgrade`](WebSocketError::Upgrade) error
    /// happens *after* the `101` and has no meaningful rejection response; it maps
    /// to `500` for completeness.)
    ///
    /// ```ignore
    /// let (response, ws_fut) = match h2ts_server::accept(&mut req) {
    ///     Ok(pair) => pair,
    ///     Err(err) => return Ok(err.rejection_response()), // send the 4xx back
    /// };
    /// ```
    pub fn rejection_response(&self) -> Response<Empty<Bytes>> {
        let status = match self {
            WebSocketError::NotUpgradeRequest => StatusCode::UPGRADE_REQUIRED,
            WebSocketError::UnsupportedSubprotocol => StatusCode::BAD_REQUEST,
            WebSocketError::Upgrade(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Response::builder()
            .status(status)
            .body(Empty::new())
            .expect("static rejection response is well-formed")
    }
}

impl fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WebSocketError::NotUpgradeRequest => f.write_str("not a WebSocket upgrade request"),
            WebSocketError::UnsupportedSubprotocol => {
                f.write_str("client offered no supported subprotocol")
            }
            WebSocketError::Upgrade(e) => write!(f, "WebSocket upgrade failed: {e}"),
        }
    }
}

impl std::error::Error for WebSocketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WebSocketError::Upgrade(e) => Some(e),
            WebSocketError::NotUpgradeRequest | WebSocketError::UnsupportedSubprotocol => None,
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

/// The outcome of the default subprotocol fallback, when the handler's selector
/// declined.
#[derive(Debug, PartialEq, Eq)]
enum Fallback {
    /// Complete the handshake, echoing this subprotocol (`Some`) or none (`None`).
    Accept(Option<String>),
    /// Reject the handshake: the client offered no acceptable subprotocol and
    /// implicit codecs are disabled.
    Reject,
}

/// The subprotocol decision when the handler's selector declined.
///
/// Prefers [`DEFAULT_SUBPROTOCOL`] (`h2ts`) whenever the client offered it (in
/// its exact offered casing). Otherwise, with `allow_implicit_codec`, accept the
/// client's first offered subprotocol (or none, if it offered nothing); without
/// it, [`Fallback::Reject`] — we don't commit to a codec we don't understand.
fn fallback_subprotocol(offered: &[&str], allow_implicit_codec: bool) -> Fallback {
    if let Some(p) = offered
        .iter()
        .find(|p| p.eq_ignore_ascii_case(DEFAULT_SUBPROTOCOL))
    {
        return Fallback::Accept(Some(p.to_string()));
    }
    if allow_implicit_codec {
        return Fallback::Accept(offered.first().map(|p| p.to_string()));
    }
    Fallback::Reject
}

/// Compute the `Sec-WebSocket-Accept` value for a client `Sec-WebSocket-Key`.
fn accept_key(key: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key);
    hasher.update(WS_GUID);
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

/// Accept a WebSocket upgrade, choosing which offered subprotocol to echo, with
/// explicit [`AcceptOptions`].
///
/// `select` is handed the full list of subprotocols the client offered (the same
/// thing [`offered_protocols`] returns) and returns the one to echo in the `101`,
/// or `None` to decline. When it declines, [`DEFAULT_SUBPROTOCOL`] (`h2ts`) is
/// echoed if the client offered it; otherwise the handshake is **rejected** with
/// [`WebSocketError::UnsupportedSubprotocol`] — unless
/// [`AcceptOptions::allow_implicit_codec`] is set, which instead accepts the
/// client's first offered codec (or completes with none if it offered nothing).
/// Turn a rejection into a `4xx` to send back via
/// [`WebSocketError::rejection_response`].
///
/// Per RFC 6455 a client that offered subprotocols will *fail* the connection if
/// the server echoes one it did not offer, so `select` should return a member of
/// the offered list (or `None`).
///
/// See [`accept_with`] (default options) and [`accept`] (the common case).
// The returned `impl Future` can't be aliased away, so the tuple reads as
// "complex"; it's just (response, upgrade-future).
#[allow(clippy::type_complexity)]
pub fn accept_with_options<B, F>(
    request: &mut Request<B>,
    select: F,
    options: AcceptOptions,
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

    // Let the handler pick from the offered list; otherwise apply the default
    // fallback (h2ts if offered, then optionally the first offered codec, else
    // reject).
    let offered = offered_protocols(request);
    let decision = match select(&offered) {
        Some(proto) => Fallback::Accept(Some(proto)),
        None => fallback_subprotocol(&offered, options.allow_implicit_codec),
    };
    drop(offered); // release the immutable borrow before upgrading below
    let chosen = match decision {
        Fallback::Accept(proto) => proto,
        Fallback::Reject => return Err(WebSocketError::UnsupportedSubprotocol),
    };

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

/// Accept a WebSocket upgrade, choosing which offered subprotocol to echo.
///
/// Like [`accept_with_options`] with the default [`AcceptOptions`]: when `select`
/// declines, [`DEFAULT_SUBPROTOCOL`] (`h2ts`) is echoed if the client offered it;
/// otherwise the handshake is **rejected**
/// ([`WebSocketError::UnsupportedSubprotocol`]). Use [`accept_with_options`] with
/// `allow_implicit_codec` to instead accept the client's first offered codec.
///
/// ```ignore
/// // Prefer "chat" if the client offered it, otherwise fall back to h2ts.
/// let (response, ws_fut) = h2ts_server::accept_with(&mut req, |offered| {
///     offered.iter().find(|p| p.eq_ignore_ascii_case("chat")).map(|p| p.to_string())
/// })?;
/// ```
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
    accept_with_options(request, select, AcceptOptions::default())
}

/// Accept a WebSocket upgrade, requiring the [`DEFAULT_SUBPROTOCOL`] (`h2ts`).
///
/// Echoes `h2ts` when the client offered it; a client that doesn't offer `h2ts`
/// is **rejected** with [`WebSocketError::UnsupportedSubprotocol`] (send
/// [`rejection_response`](WebSocketError::rejection_response) back as a `400`).
/// Use [`accept_with`] to select a different offered subprotocol, or
/// [`accept_with_options`] with `allow_implicit_codec` to accept any codec.
///
/// On success, returns the `101 Switching Protocols` response to send back
/// immediately, plus a future that resolves to the upgraded connection as a byte
/// stream ([`UpgradedIo`]). Drive the response through your framework; spawn the
/// future and hand the stream to [`bridge`](crate::bridge) or
/// [`serve_h2`](crate::serve_h2). Control frames (ping/close) are handled
/// downstream by wslay.
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
    use super::{
        accept, accept_with, accept_with_options, fallback_subprotocol, is_upgrade_request,
        offered_protocols, AcceptOptions, WebSocketError,
    };
    use http::header::{CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, UPGRADE};
    use hyper::{Request, StatusCode};

    fn with_protocol(protocol: Option<&str>) -> Request<()> {
        let mut b = Request::builder();
        if let Some(p) = protocol {
            b = b.header(SEC_WEBSOCKET_PROTOCOL, p);
        }
        b.body(()).unwrap()
    }

    /// A well-formed WebSocket upgrade request offering `offer` (if any).
    fn upgrade_request(offer: Option<&str>) -> Request<()> {
        let mut b = Request::builder()
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "Upgrade")
            .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==");
        if let Some(o) = offer {
            b = b.header(SEC_WEBSOCKET_PROTOCOL, o);
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
    fn fallback_prefers_h2ts_then_optionally_the_first_offered() {
        use super::Fallback::{Accept, Reject};

        // h2ts always wins when offered, in its exact casing, regardless of the flag.
        assert_eq!(fallback_subprotocol(&["h2ts"], false), Accept(Some("h2ts".into())));
        assert_eq!(
            fallback_subprotocol(&["chat", "h2ts"], false),
            Accept(Some("h2ts".into())),
            "h2ts is preferred even when it isn't first"
        );
        assert_eq!(
            fallback_subprotocol(&["H2TS"], false),
            Accept(Some("H2TS".into())),
            "matched case-insensitively but echoed in the offered casing"
        );

        // No h2ts, flag off: reject (an empty offer is also rejected).
        assert_eq!(fallback_subprotocol(&["chat"], false), Reject);
        assert_eq!(fallback_subprotocol(&["chat", "binary"], false), Reject);
        assert_eq!(fallback_subprotocol(&[], false), Reject);

        // No h2ts, flag on: implicitly accept the first offered codec...
        assert_eq!(
            fallback_subprotocol(&["chat", "binary"], true),
            Accept(Some("chat".into()))
        );
        // ...and an empty offer completes with no subprotocol.
        assert_eq!(fallback_subprotocol(&[], true), Accept(None));
    }

    #[test]
    fn accept_echoes_h2ts_when_offered() {
        let mut req = upgrade_request(Some("h2ts"));
        let (resp, _fut) = accept(&mut req).unwrap();
        assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(resp.headers().get(SEC_WEBSOCKET_PROTOCOL).unwrap(), "h2ts");

        // Preferred even when offered alongside others.
        let mut req = upgrade_request(Some("chat, h2ts"));
        let (resp, _fut) = accept(&mut req).unwrap();
        assert_eq!(resp.headers().get(SEC_WEBSOCKET_PROTOCOL).unwrap(), "h2ts");
    }

    #[test]
    fn accept_rejects_when_h2ts_absent_by_default() {
        // A non-h2ts offer and an empty offer both reject with a 400 response,
        // rather than completing the handshake.
        for offer in [Some("mystery"), Some("chat, binary"), None] {
            let mut req = upgrade_request(offer);
            let err = accept(&mut req).err().expect("should reject");
            assert!(
                matches!(&err, WebSocketError::UnsupportedSubprotocol),
                "offer {offer:?}"
            );
            assert_eq!(err.rejection_response().status(), StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn accept_with_honors_a_selection_even_without_h2ts() {
        // The handler selecting an offered codec accepts it — no reject, no h2ts.
        let mut req = upgrade_request(Some("chat, binary"));
        let (resp, _fut) = accept_with(&mut req, |offered| {
            offered.iter().find(|p| **p == "binary").map(|p| p.to_string())
        })
        .unwrap();
        assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(resp.headers().get(SEC_WEBSOCKET_PROTOCOL).unwrap(), "binary");
    }

    #[test]
    fn allow_implicit_codec_accepts_the_first_offered_codec() {
        let opts = AcceptOptions {
            allow_implicit_codec: true,
        };
        // Unknown codec: accepted, first offered echoed.
        let mut req = upgrade_request(Some("mystery, other"));
        let (resp, _fut) = accept_with_options(&mut req, |_| None, opts).unwrap();
        assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(resp.headers().get(SEC_WEBSOCKET_PROTOCOL).unwrap(), "mystery");

        // No offer: still accepted (permissive), nothing echoed.
        let mut req = upgrade_request(None);
        let (resp, _fut) = accept_with_options(&mut req, |_| None, opts).unwrap();
        assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert!(resp.headers().get(SEC_WEBSOCKET_PROTOCOL).is_none());
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
