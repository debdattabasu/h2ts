//! Process-level tests for the `h2ts-proxy` binary. Each test spawns the *real*
//! compiled binary (`CARGO_BIN_EXE_h2ts-proxy`) against a throwaway h2c upstream
//! and drives traffic/handshakes through it, exercising the CLI argument wiring
//! (subprotocol policy, `--allow-implicit-codec`, keepalive) end to end.
mod common;
use common::{
    connect_h2, connect_ws_raw, get, handshake_status, start_h2c_upstream, start_quiet_upstream,
};

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bytes::Bytes;
use fastwebsockets::OpCode;
use http_body_util::{BodyExt, Full};
use hyper::{Request, StatusCode};
use tokio::net::TcpStream;

/// A spawned `h2ts-proxy` child process, killed when the test drops it.
struct Proxy(Child);

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A free TCP port (bind `:0`, read the port, release it). There's a tiny window
/// before the proxy re-binds it, but it's more than good enough for a local test.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Spawn `h2ts-proxy <listen> <upstream> <extra…>` and wait until it's accepting
/// connections. `extra` carries the optional keepalive-secs positional and/or the
/// `--allow-implicit-codec` flag.
async fn spawn_proxy(listen: SocketAddr, upstream: SocketAddr, extra: &[&str]) -> Proxy {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_h2ts-proxy"));
    cmd.arg(listen.to_string()).arg(upstream.to_string());
    cmd.args(extra);
    // The binary logs to stderr; silence it so test output stays clean.
    cmd.stdout(Stdio::null()).stderr(Stdio::null());
    // Own the child immediately so every exit path (including the panic below)
    // goes through `Proxy`'s kill+wait `Drop` — no leaked process.
    let proxy = Proxy(cmd.spawn().expect("spawn h2ts-proxy binary"));

    // Poll until the listener is up (bind + first accept ready).
    for _ in 0..200 {
        if TcpStream::connect(listen).await.is_ok() {
            return proxy;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("h2ts-proxy never started listening on {listen}");
}

/// Reserve a listen port and spawn a proxy in front of a fresh h2c upstream.
async fn proxy_to_upstream(extra: &[&str]) -> (SocketAddr, Proxy) {
    let upstream = start_h2c_upstream().await;
    let listen: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let proxy = spawn_proxy(listen, upstream, extra).await;
    (listen, proxy)
}

/// The real binary tunnels HTTP/2 from a WebSocket client through to the upstream:
/// GET, POST echo, and a multi-frame body all round-trip across the process
/// boundary and the raw TCP bridge.
#[tokio::test]
async fn proxy_tunnels_h2_to_upstream() {
    let (listen, _proxy) = proxy_to_upstream(&[]).await;

    let (mut sender, negotiated) = connect_h2(listen, &["h2ts"]).await;
    assert_eq!(negotiated.as_deref(), Some("h2ts"), "proxy echoes h2ts");

    // GET /hello -> upstream.
    let res = sender.send_request(get(listen, "/hello")).await.unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.into_body().collect().await.unwrap().to_bytes(),
        Bytes::from_static(b"hi")
    );

    // POST /echo round-trips a body through the tunnel.
    let echo = Request::builder()
        .method("POST")
        .uri(format!("http://{listen}/echo"))
        .body(Full::new(Bytes::from_static(b"through-the-proxy")))
        .unwrap();
    let res = sender.send_request(echo).await.unwrap();
    assert_eq!(
        res.into_body().collect().await.unwrap().to_bytes(),
        Bytes::from_static(b"through-the-proxy")
    );

    // /big -> 100 KiB, exercising multi-frame DATA across the real socket bridge.
    let res = sender.send_request(get(listen, "/big")).await.unwrap();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), 100 * 1024);
    assert!(body.iter().all(|&b| b == b'x'));
}

/// Concurrent multiplexed streams over one WebSocket survive the proxy hop.
#[tokio::test]
async fn proxy_multiplexes_concurrent_streams() {
    let (listen, _proxy) = proxy_to_upstream(&[]).await;
    let (sender, _) = connect_h2(listen, &["h2ts"]).await;

    let mut handles = Vec::new();
    for _ in 0..16 {
        let mut s = sender.clone();
        handles.push(tokio::spawn(async move {
            let res = s.send_request(get(listen, "/hello")).await.unwrap();
            res.into_body().collect().await.unwrap().to_bytes()
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap(), Bytes::from_static(b"hi"));
    }
}

/// By default the proxy requires `h2ts`: a non-h2ts (or absent) subprotocol is
/// rejected with a real `400` over the wire, while an `h2ts` client upgrades.
#[tokio::test]
async fn proxy_rejects_non_h2ts_by_default() {
    let (listen, _proxy) = proxy_to_upstream(&[]).await;

    assert_eq!(
        handshake_status(listen, &["h2ts"]).await,
        StatusCode::SWITCHING_PROTOCOLS,
        "h2ts client upgrades"
    );
    assert_eq!(
        handshake_status(listen, &["binary"]).await,
        StatusCode::BAD_REQUEST,
        "a websockify-style binary client is rejected"
    );
    assert_eq!(
        handshake_status(listen, &[]).await,
        StatusCode::BAD_REQUEST,
        "a client offering nothing is rejected"
    );
}

/// With `--allow-implicit-codec` the proxy is a codec-agnostic byte tunnel: it
/// accepts any offered subprotocol (echoing the first), yet `h2ts` still wins when
/// offered, and traffic flows either way.
#[tokio::test]
async fn proxy_allow_implicit_codec_accepts_any_subprotocol() {
    let (listen, _proxy) = proxy_to_upstream(&["--allow-implicit-codec"]).await;

    // A binary-only client is accepted; its first codec is echoed; the tunnel works.
    let (mut sender, negotiated) = connect_h2(listen, &["binary"]).await;
    assert_eq!(negotiated.as_deref(), Some("binary"));
    assert_eq!(
        sender.send_request(get(listen, "/hello")).await.unwrap().status(),
        200
    );

    // h2ts still preferred when offered alongside others.
    let (_sender, negotiated) = connect_h2(listen, &["chat", "h2ts"]).await;
    assert_eq!(negotiated.as_deref(), Some("h2ts"));
}

/// The keepalive-secs positional wires through to server-initiated keepalive: an
/// idle client that never pongs is pinged and then closed (1001 Going Away).
#[tokio::test]
async fn proxy_keepalive_closes_idle_client() {
    // Keepalive interval == timeout == 1s (the binary's smallest granularity). A
    // quiet upstream sends nothing, so the client sees keepalive frames in isolation.
    let upstream = start_quiet_upstream().await;
    let listen: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let _proxy = spawn_proxy(listen, upstream, &["1"]).await;

    // A raw WS client offering h2ts that never answers pings.
    let mut ws = connect_ws_raw(listen, &["h2ts"]).await;
    ws.set_auto_pong(false);
    ws.set_auto_close(false);

    // First the keepalive Ping arrives...
    let ping = tokio::time::timeout(Duration::from_secs(5), ws.read_frame())
        .await
        .expect("timed out waiting for keepalive ping")
        .unwrap();
    assert_eq!(ping.opcode, OpCode::Ping);

    // ...then, with no pong, the keepalive Close with 1001 Going Away.
    let close = tokio::time::timeout(Duration::from_secs(5), ws.read_frame())
        .await
        .expect("timed out waiting for keepalive close")
        .unwrap();
    assert_eq!(close.opcode, OpCode::Close);
    let payload = close.payload.to_vec();
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1001);
}
