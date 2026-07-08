//! WebSocket upgrade / subprotocol-negotiation tests — exercises the public
//! handshake API (`accept`, `accept_with`, `accept_with_options`,
//! `offered_protocols`, `is_upgrade_request`). The internal fallback and
//! `Sec-WebSocket-Accept` derivation are covered through their observable
//! effects on the handshake response, so no crate internals are exposed.

use h2ts_server::{
    accept, accept_with, accept_with_options, is_upgrade_request, offered_protocols, AcceptOptions,
    WebSocketError,
};
use http::header::{
    CONNECTION, ORIGIN, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, UPGRADE,
};
use hyper::{Request, StatusCode};

fn with_protocol(protocol: Option<&str>) -> Request<()> {
    let mut b = Request::builder();
    if let Some(p) = protocol {
        b = b.header(SEC_WEBSOCKET_PROTOCOL, p);
    }
    b.body(()).unwrap()
}

/// A well-formed WebSocket upgrade request offering `offer` (if any). The key is
/// the RFC 6455 §1.3 example, so the response's `Sec-WebSocket-Accept` is known.
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

/// Run the handshake with a `|_| None` handler (so the built-in fallback alone
/// decides), returning the echoed subprotocol on accept, or `None` on a 400
/// reject — i.e. the observable outcome of the internal fallback logic.
fn fallback(offer: Option<&str>, allow_implicit_codec: bool) -> Option<Option<String>> {
    let mut req = upgrade_request(offer);
    let opts = AcceptOptions {
        allow_implicit_codec,
        ..Default::default()
    };
    match accept_with_options(&mut req, |_| None, opts) {
        Ok((resp, _fut)) => {
            assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
            Some(
                resp.headers()
                    .get(SEC_WEBSOCKET_PROTOCOL)
                    .map(|v| v.to_str().unwrap().to_string()),
            )
        }
        Err(err) => {
            assert!(matches!(err, WebSocketError::UnsupportedSubprotocol));
            assert_eq!(err.rejection_response().status(), StatusCode::BAD_REQUEST);
            None
        }
    }
}

#[test]
fn offered_protocols_parses_the_list_in_order() {
    assert_eq!(offered_protocols(&with_protocol(Some("h2ts"))), ["h2ts"]);
    assert_eq!(
        offered_protocols(&with_protocol(Some("chat, h2ts, binary"))),
        ["chat", "h2ts", "binary"]
    );
    assert_eq!(
        offered_protocols(&with_protocol(Some("  h2ts  "))),
        ["h2ts"]
    );
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
    // h2ts always wins when offered, in its exact casing, regardless of the flag.
    assert_eq!(fallback(Some("h2ts"), false), Some(Some("h2ts".into())));
    assert_eq!(
        fallback(Some("chat, h2ts"), false),
        Some(Some("h2ts".into())),
        "h2ts is preferred even when it isn't first"
    );
    assert_eq!(
        fallback(Some("H2TS"), false),
        Some(Some("H2TS".into())),
        "matched case-insensitively but echoed in the offered casing"
    );

    // No h2ts, flag off: reject (an empty offer is also rejected).
    assert_eq!(fallback(Some("chat"), false), None);
    assert_eq!(fallback(Some("chat, binary"), false), None);
    assert_eq!(fallback(None, false), None);

    // No h2ts, flag on: implicitly accept the first offered codec...
    assert_eq!(
        fallback(Some("chat, binary"), true),
        Some(Some("chat".into()))
    );
    // ...and an empty offer completes with no subprotocol.
    assert_eq!(fallback(None, true), Some(None));
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
        offered
            .iter()
            .find(|p| **p == "binary")
            .map(|p| p.to_string())
    })
    .unwrap();
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        resp.headers().get(SEC_WEBSOCKET_PROTOCOL).unwrap(),
        "binary"
    );
}

/// A valid h2ts upgrade request with an optional `Origin` header.
fn upgrade_with_origin(origin: Option<&str>) -> Request<()> {
    let mut b = Request::builder()
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "Upgrade")
        .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(SEC_WEBSOCKET_PROTOCOL, "h2ts");
    if let Some(o) = origin {
        b = b.header(ORIGIN, o);
    }
    b.body(()).unwrap()
}

fn origin_opts() -> AcceptOptions {
    AcceptOptions {
        allowed_origins: Some(vec!["https://app.example.com".into()]),
        ..Default::default()
    }
}

#[test]
fn allowed_origins_accepts_a_listed_origin() {
    // A listed origin (matched ASCII case-insensitively) completes the handshake.
    let mut req = upgrade_with_origin(Some("https://APP.example.com"));
    let (resp, _fut) = accept_with_options(&mut req, |_| None, origin_opts()).unwrap();
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
}

#[test]
fn allowed_origins_rejects_an_unlisted_origin_with_403() {
    let mut req = upgrade_with_origin(Some("https://evil.example.com"));
    let err = accept_with_options(&mut req, |_| None, origin_opts())
        .err()
        .expect("an unlisted origin must be rejected");
    assert!(matches!(err, WebSocketError::ForbiddenOrigin));
    assert_eq!(err.rejection_response().status(), StatusCode::FORBIDDEN);
}

#[test]
fn allowed_origins_rejects_a_missing_origin() {
    // With an allowlist set, a request with no Origin is rejected too.
    let mut req = upgrade_with_origin(None);
    let err = accept_with_options(&mut req, |_| None, origin_opts())
        .err()
        .expect("a missing origin must be rejected when an allowlist is set");
    assert!(matches!(err, WebSocketError::ForbiddenOrigin));
}

#[test]
fn no_allowlist_accepts_any_origin() {
    // The default (no allowlist) is permissive — like nginx — so any origin works.
    let mut req = upgrade_with_origin(Some("https://anything.example.com"));
    let (resp, _fut) = accept(&mut req).unwrap();
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
}

#[test]
fn accept_with_never_echoes_an_unoffered_subprotocol() {
    // A misbehaving handler returns a subprotocol the client did NOT offer. The
    // server must not echo it (RFC 6455 §4.2.2 — the client would fail the
    // connection); it falls back to the default policy. Here the client also
    // offered h2ts, so h2ts is echoed instead of the un-offered "evil".
    let mut req = upgrade_request(Some("h2ts, chat"));
    let (resp, _fut) = accept_with(&mut req, |_offered| Some("evil".to_string())).unwrap();
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(resp.headers().get(SEC_WEBSOCKET_PROTOCOL).unwrap(), "h2ts");

    // With no acceptable fallback (no h2ts offered, implicit codec off), an
    // un-offered selection rejects rather than being echoed.
    let mut req = upgrade_request(Some("chat, binary"));
    let err = accept_with(&mut req, |_offered| Some("evil".to_string()))
        .err()
        .expect("un-offered selection with no fallback must reject");
    assert!(matches!(err, WebSocketError::UnsupportedSubprotocol));
}

#[test]
fn allow_implicit_codec_accepts_the_first_offered_codec() {
    let opts = AcceptOptions {
        allow_implicit_codec: true,
        ..Default::default()
    };
    // Unknown codec: accepted, first offered echoed.
    let mut req = upgrade_request(Some("mystery, other"));
    let (resp, _fut) = accept_with_options(&mut req, |_| None, opts.clone()).unwrap();
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        resp.headers().get(SEC_WEBSOCKET_PROTOCOL).unwrap(),
        "mystery"
    );

    // No offer: still accepted (permissive), nothing echoed.
    let mut req = upgrade_request(None);
    let (resp, _fut) = accept_with_options(&mut req, |_| None, opts).unwrap();
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert!(resp.headers().get(SEC_WEBSOCKET_PROTOCOL).is_none());
}

/// A request that isn't a WebSocket upgrade is rejected with `NotUpgradeRequest`,
/// whose rejection response is `426 Upgrade Required` — the arm all the accept
/// tests above (which use valid upgrades) never reach.
#[test]
fn accept_rejects_a_non_upgrade_request_with_426() {
    // A plain GET: no Upgrade / Connection / Sec-WebSocket-Key.
    let mut plain = Request::builder().body(()).unwrap();
    let err = accept(&mut plain)
        .err()
        .expect("a non-upgrade request must be rejected");
    assert!(matches!(err, WebSocketError::NotUpgradeRequest));
    assert_eq!(
        err.rejection_response().status(),
        StatusCode::UPGRADE_REQUIRED
    );

    // A partial upgrade (all signals but the key) is also not an upgrade → 426,
    // not the 400 an unsupported-subprotocol reject would give.
    let mut no_key = Request::builder()
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "Upgrade")
        .body(())
        .unwrap();
    let err = accept(&mut no_key)
        .err()
        .expect("a request missing Sec-WebSocket-Key must be rejected");
    assert!(matches!(err, WebSocketError::NotUpgradeRequest));
    assert_eq!(
        err.rejection_response().status(),
        StatusCode::UPGRADE_REQUIRED
    );
}

/// `rejection_response` maps each *constructible* pre-upgrade error to its status.
/// (`Upgrade(_)` maps to `500`, but wraps a `hyper::Error` with no public
/// constructor, so it's only reachable through a live post-101 upgrade failure and
/// isn't unit-constructible here.)
#[test]
fn rejection_response_maps_constructible_errors_to_status() {
    assert_eq!(
        WebSocketError::NotUpgradeRequest
            .rejection_response()
            .status(),
        StatusCode::UPGRADE_REQUIRED
    );
    assert_eq!(
        WebSocketError::UnsupportedSubprotocol
            .rejection_response()
            .status(),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn accept_key_derivation_matches_rfc_6455_example() {
    // RFC 6455 §1.3: key "dGhlIHNhbXBsZSBub25jZQ==" -> Sec-WebSocket-Accept
    // "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=". `upgrade_request` sends that exact key.
    let mut req = upgrade_request(Some("h2ts"));
    let (resp, _fut) = accept(&mut req).unwrap();
    assert_eq!(
        resp.headers().get(SEC_WEBSOCKET_ACCEPT).unwrap(),
        "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    );
}
