package server

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// testRoutes is a slice of the conformance origin: enough to exercise the h2
// path (status, small round-trip, multi-frame download + flow control, 404).
func testRoutes() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hello":
			io.WriteString(w, "hi")
		case "/echo":
			body, _ := io.ReadAll(r.Body)
			w.Write(body)
		case "/big":
			w.Write(bytes.Repeat([]byte{'x'}, 256*1024))
		case "/headers":
			w.Header().Set("x-saw-custom", r.Header.Get("x-custom"))
			io.WriteString(w, "ok")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

// startGateway starts an HTTP/1.1 gateway that accepts the WebSocket (with the
// given options) and serves handler as HTTP/2 over the tunnel (with cfg). Returns
// its host:port.
func startGateway(t *testing.T, handler http.Handler, opts AcceptOptions, cfg ServeConfig) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := AcceptWithOptions(w, r, opts)
		if err != nil {
			return // AcceptWithOptions already wrote the rejection
		}
		go ServeH2With(conn, handler, cfg)
	}))
	t.Cleanup(ts.Close)
	return ts.Listener.Addr().String()
}

func TestServeH2RoundTrip(t *testing.T) {
	// Keepalive off so it can't interfere with the deterministic assertions.
	addr := startGateway(t, testRoutes(), AcceptOptions{}, ServeConfig{})
	tunnel, proto := wsDial(t, addr, "/", DefaultSubprotocol)
	defer tunnel.Close()
	if proto != DefaultSubprotocol {
		t.Fatalf("negotiated subprotocol = %q, want %q", proto, DefaultSubprotocol)
	}
	cc := dialH2(t, tunnel)

	// GET /hello
	resp := h2do(t, cc, "GET", "/hello", nil, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("GET /hello status = %d", resp.StatusCode)
	}
	if body := readBody(t, resp); body != "hi" {
		t.Fatalf("GET /hello body = %q, want %q", body, "hi")
	}

	// POST /echo — small round-trip.
	resp = h2do(t, cc, "POST", "/echo", strings.NewReader("round-trips!"), nil)
	if body := readBody(t, resp); body != "round-trips!" {
		t.Fatalf("POST /echo body = %q", body)
	}

	// GET /big — 256 KiB, multiple DATA frames + connection/stream flow control.
	resp = h2do(t, cc, "GET", "/big", nil, nil)
	big := readBodyBytes(t, resp)
	if len(big) != 256*1024 {
		t.Fatalf("GET /big size = %d, want %d", len(big), 256*1024)
	}
	for i, b := range big {
		if b != 'x' {
			t.Fatalf("GET /big byte %d = %q, want 'x'", i, b)
		}
	}

	// Custom request header round-trips.
	resp = h2do(t, cc, "GET", "/headers", nil, map[string]string{"x-custom": "h2ts-rocks"})
	if got := resp.Header.Get("x-saw-custom"); got != "h2ts-rocks" {
		t.Fatalf("custom header reflected = %q", got)
	}
	readBody(t, resp)

	// 404.
	resp = h2do(t, cc, "GET", "/nope", nil, nil)
	if resp.StatusCode != 404 {
		t.Fatalf("GET /nope status = %d, want 404", resp.StatusCode)
	}
	readBody(t, resp)
}

func TestServeH2ConcurrentStreams(t *testing.T) {
	addr := startGateway(t, testRoutes(), AcceptOptions{}, ServeConfig{})
	tunnel, _ := wsDial(t, addr, "/", DefaultSubprotocol)
	defer tunnel.Close()
	cc := dialH2(t, tunnel)

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := h2do(t, cc, "POST", "/echo", strings.NewReader(fmt.Sprintf("stream-%d", i)), nil)
			if body := readBody(t, resp); body != fmt.Sprintf("stream-%d", i) {
				errs <- fmt.Errorf("stream %d echoed %q", i, body)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestServeH2OnClose asserts the teardown reason surfaces via OnClose: a client
// that vanishes without a WebSocket Close yields 1006 (abnormal).
func TestServeH2OnClose(t *testing.T) {
	got := make(chan CloseFrame, 1)
	cfg := ServeConfig{OnClose: func(cf CloseFrame) { got <- cf }}
	addr := startGateway(t, testRoutes(), AcceptOptions{}, cfg)

	tunnel, _ := wsDial(t, addr, "/", DefaultSubprotocol)
	cc := dialH2(t, tunnel)
	readBody(t, h2do(t, cc, "GET", "/hello", nil, nil)) // one real request first

	// Drop the transport abruptly (no WS close) — the server should see 1006.
	tunnel.Conn.Close()

	select {
	case cf := <-got:
		if cf.Code != closeAbnormal {
			t.Fatalf("OnClose code = %d, want %d (abnormal)", cf.Code, closeAbnormal)
		}
	case <-timeoutC(t, 2):
		t.Fatal("OnClose never fired after the transport dropped")
	}
}

// TestServeH2ControlFrameHooks exercises the control-frame surface during real
// serving, both directions: the server's Conn.Ping is auto-ponged by the client
// and surfaces via OnPong; a client ping surfaces via OnPing (and is auto-ponged).
func TestServeH2ControlFrameHooks(t *testing.T) {
	connCh := make(chan *Conn, 1)
	pingCh := make(chan []byte, 1)
	pongCh := make(chan []byte, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := Accept(w, r)
		if err != nil {
			return
		}
		connCh <- conn
		go ServeH2With(conn, testRoutes(), ServeConfig{
			OnPing: func(p []byte) { pingCh <- append([]byte(nil), p...) },
			OnPong: func(p []byte) { pongCh <- append([]byte(nil), p...) },
		})
	}))
	t.Cleanup(ts.Close)

	tunnel, _ := wsDial(t, ts.Listener.Addr().String(), "/", DefaultSubprotocol)
	defer tunnel.Close()
	cc := dialH2(t, tunnel)
	readBody(t, h2do(t, cc, "GET", "/hello", nil, nil)) // establish; serving is now active
	srvConn := <-connCh

	// server -> client ping -> client auto-pong -> server OnPong.
	if err := srvConn.Ping([]byte("rtt-probe")); err != nil {
		t.Fatalf("server Ping: %v", err)
	}
	select {
	case got := <-pongCh:
		if string(got) != "rtt-probe" {
			t.Fatalf("OnPong = %q, want %q", got, "rtt-probe")
		}
	case <-timeoutC(t, 2):
		t.Fatal("OnPong never fired after the server ping / client auto-pong")
	}

	// client -> server ping -> server OnPing (server also auto-pongs the client).
	if err := tunnel.writeCtl(opPing, []byte("cli-ping")); err != nil {
		t.Fatalf("client ping: %v", err)
	}
	select {
	case got := <-pingCh:
		if string(got) != "cli-ping" {
			t.Fatalf("OnPing = %q, want %q", got, "cli-ping")
		}
	case <-timeoutC(t, 2):
		t.Fatal("OnPing never fired after the client ping")
	}
}

// TestServeH2LargeUpload drives a 1 MiB POST echo through go test directly (the
// external-client conformance covers uploads, but the module's own suite didn't).
// 1 MiB sits right at the default receive window, exercising WINDOW_UPDATE.
func TestServeH2LargeUpload(t *testing.T) {
	addr := startGateway(t, testRoutes(), AcceptOptions{}, ServeConfig{})
	tunnel, _ := wsDial(t, addr, "/", DefaultSubprotocol)
	defer tunnel.Close()
	cc := dialH2(t, tunnel)

	payload := make([]byte, 1<<20) // 1 MiB
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	echoed := readBodyBytes(t, h2do(t, cc, "POST", "/echo", bytes.NewReader(payload), nil))
	if !bytes.Equal(echoed, payload) {
		t.Fatalf("1 MiB upload echo mismatch (got %d bytes)", len(echoed))
	}
}

// TestServeH2KeepAliveStaysUpWhilePeerResponds asserts keepalive does not kill a
// healthy connection: the client auto-pongs the server's pings, so across many
// keepalive intervals the tunnel stays up and still serves. Mirrors the Rust
// server's keepalive_stays_up_while_peer_responds.
func TestServeH2KeepAliveStaysUpWhilePeerResponds(t *testing.T) {
	onClose := make(chan CloseFrame, 1)
	ka := KeepAlive{Interval: 20 * time.Millisecond, Timeout: 20 * time.Millisecond,
		Close: CloseFrame{Code: closeGoingAway, Reason: "keepalive timeout"}}
	cfg := ServeConfig{KeepAlive: &ka, OnClose: func(cf CloseFrame) { onClose <- cf }}
	addr := startGateway(t, testRoutes(), AcceptOptions{}, cfg)

	tunnel, _ := wsDial(t, addr, "/", DefaultSubprotocol)
	defer tunnel.Close()
	cc := dialH2(t, tunnel)
	readBody(t, h2do(t, cc, "GET", "/hello", nil, nil)) // establish; the h2 read loop auto-pongs

	// Idle across ~10 keepalive intervals; the client keeps ponging.
	time.Sleep(200 * time.Millisecond)
	select {
	case cf := <-onClose:
		t.Fatalf("keepalive closed a healthy connection: %+v", cf)
	default:
	}
	// The tunnel is still live.
	resp := h2do(t, cc, "GET", "/hello", nil, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("post-idle request status = %d, want 200", resp.StatusCode)
	}
	readBody(t, resp)
}

// TestServeH2ControlFramesDoNotCorruptData spams pings *during* large concurrent
// transfers; writeMu must keep each control frame between whole data frames, so
// the h2 payloads stay byte-exact. Mirrors the Rust server's serve_h2_with.rs.
func TestServeH2ControlFramesDoNotCorruptData(t *testing.T) {
	connCh := make(chan *Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := Accept(w, r)
		if err != nil {
			return
		}
		connCh <- conn
		go ServeH2With(conn, testRoutes(), ServeConfig{})
	}))
	t.Cleanup(ts.Close)

	tunnel, _ := wsDial(t, ts.Listener.Addr().String(), "/", DefaultSubprotocol)
	defer tunnel.Close()
	cc := dialH2(t, tunnel)
	readBody(t, h2do(t, cc, "GET", "/hello", nil, nil))
	srvConn := <-connCh

	// Interleave a stream of pings with the data traffic.
	stop := make(chan struct{})
	var pinger sync.WaitGroup
	pinger.Add(1)
	go func() {
		defer pinger.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if srvConn.Ping([]byte("probe")) != nil {
					return
				}
				time.Sleep(150 * time.Microsecond)
			}
		}
	}()

	upload := make([]byte, 512*1024)
	for i := range upload {
		upload[i] = byte(i * 7)
	}
	for iter := 0; iter < 4; iter++ {
		big := readBodyBytes(t, h2do(t, cc, "GET", "/big", nil, nil))
		if len(big) != 256*1024 {
			t.Fatalf("iter %d: /big size = %d", iter, len(big))
		}
		for i, b := range big {
			if b != 'x' {
				t.Fatalf("iter %d: /big byte %d = %q (ping corrupted DATA)", iter, i, b)
			}
		}
		echoed := readBodyBytes(t, h2do(t, cc, "POST", "/echo", bytes.NewReader(upload), nil))
		if !bytes.Equal(echoed, upload) {
			t.Fatalf("iter %d: 512 KiB echo mismatch (ping corrupted DATA)", iter)
		}
	}
	close(stop)
	pinger.Wait()
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	return string(readBodyBytes(t, resp))
}

func readBodyBytes(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}
