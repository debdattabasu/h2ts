// Serve HTTP/2 (h2c, prior knowledge) over the WebSocket tunnel, plus the
// server-initiated keepalive that keeps a silently-dead client from leaking it.

package server

import (
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

// KeepAlive configures server-initiated liveness: send a WebSocket ping once the
// connection has been idle for Interval, and close it if no frame arrives within
// a further Timeout. This is the proactive half of ping/pong — an inbound ping is
// always auto-answered — and matters because browsers can't send pings from
// JavaScript, so liveness for an h2ts client is driven from the server.
type KeepAlive struct {
	// Interval is how long the connection may be idle (no frame received) before a
	// keepalive ping is sent.
	Interval time.Duration
	// Timeout is how long to wait after that ping for any frame before closing.
	Timeout time.Duration
	// Close is the close frame sent to the peer, and surfaced to
	// [ServeConfig.OnClose], when keepalive fails.
	Close CloseFrame
}

// DefaultKeepAlive returns sensible server defaults: ping an idle connection
// every 30s and close it (1001 Going Away, "keepalive timeout") if no frame
// arrives within a further 15s. This is what [ServeH2] uses.
func DefaultKeepAlive() KeepAlive {
	return KeepAlive{
		Interval: 30 * time.Second,
		Timeout:  15 * time.Second,
		Close:    CloseFrame{Code: closeGoingAway, Reason: "keepalive timeout"},
	}
}

// ServeConfig tunes [ServeH2With].
type ServeConfig struct {
	// KeepAlive enables server-initiated keepalive. nil disables it (drive your
	// own pings, or rely on transport-level timeouts, instead). [ServeH2] uses
	// [DefaultKeepAlive].
	KeepAlive *KeepAlive
	// OnClose, if set, is called once when the connection ends, with the reason
	// (see [Conn.CloseReason]).
	OnClose func(CloseFrame)
	// OnPing/OnPong, if set, are called with the payload of each received
	// WebSocket ping/pong, inline on the read path — so they must return quickly
	// and must not block (a slow callback stalls inbound HTTP/2). Received pings
	// are still auto-answered with a pong regardless of OnPing. Send your own
	// control frames with [Conn.Ping]/[Conn.Pong].
	OnPing func([]byte)
	OnPong func([]byte)
	// Server, if set, is the h2c server used to serve the connection — set it to
	// customize HTTP/2 settings (max concurrent streams, frame size, …). A fresh
	// &http2.Server{} is used otherwise.
	//
	// Set its WriteByteTimeout to bound a write to a client that has stopped
	// reading: keepalive can't detect that case (its ping shares the write path
	// with the stalled data write), so without a write timeout such a connection
	// lingers until the OS TCP timeout. Off by default (see doc/StatusJul10.md §1).
	Server *http2.Server
}

// ServeH2 serves handler as HTTP/2 (h2c, prior knowledge) over the WebSocket
// tunnel, with server-initiated keepalive on by default ([DefaultKeepAlive]).
//
// ws is the upgraded connection from [Accept]. handler is any
// [net/http.Handler] — an http.ServeMux, a router, a single HandlerFunc. The
// request/response traffic is real multiplexed HTTP/2 on top of the tunnel.
// Blocks until the connection ends; returns the terminal error, if any (nil on a
// clean close). Typically run on its own goroutine:
//
//	conn, err := server.Accept(w, r)
//	if err != nil {
//		return // Accept already wrote the rejection
//	}
//	go server.ServeH2(conn, myHandler)
func ServeH2(ws *Conn, handler http.Handler) error {
	ka := DefaultKeepAlive()
	return ServeH2With(ws, handler, ServeConfig{KeepAlive: &ka})
}

// ServeH2With is [ServeH2] with an explicit [ServeConfig] — tune or disable
// keepalive, observe the close reason, or supply a configured http2.Server.
func ServeH2With(ws *Conn, handler http.Handler, cfg ServeConfig) error {
	srv := cfg.Server
	if srv == nil {
		srv = &http2.Server{}
	}
	// Install the control-frame observation hooks before the read loop starts.
	ws.onPing = cfg.OnPing
	ws.onPong = cfg.OnPong

	var done chan struct{}
	if cfg.KeepAlive != nil {
		done = make(chan struct{})
		go ws.runKeepAlive(*cfg.KeepAlive, done)
	}

	// ServeConn reads the client's HTTP/2 preface, writes the server's, and drives
	// the connection until it ends (there's nothing to return; failures surface as
	// the connection ending). It blocks for the tunnel's lifetime.
	srv.ServeConn(ws, &http2.ServeConnOpts{Handler: handler})

	if done != nil {
		close(done) // stop the keepalive goroutine
	}
	_ = ws.Close()
	if cfg.OnClose != nil {
		cfg.OnClose(ws.CloseReason())
	}
	return nil
}

// runKeepAlive pings the peer when the connection goes idle and closes it if the
// peer stops answering. It exits when done is closed (the connection ended).
func (c *Conn) runKeepAlive(ka KeepAlive, done <-chan struct{}) {
	for {
		idle := time.Since(time.Unix(0, c.lastActivity.Load()))
		if idle < ka.Interval {
			if sleepOrDone(ka.Interval-idle, done) {
				return
			}
			continue
		}

		// Idle long enough: ping, then wait a timeout for any frame back.
		if err := c.writeControl(opPing, nil); err != nil {
			return // transport gone; ServeConn will end
		}
		pingedAt := time.Now().UnixNano()
		if sleepOrDone(ka.Timeout, done) {
			return
		}
		if c.lastActivity.Load() <= pingedAt {
			// No frame (pong or data) since our ping — the peer is dead. Close it,
			// which unblocks ServeConn.
			if c.setReason(ka.Close) {
				_ = c.writeClose(ka.Close.Code, ka.Close.Reason)
			}
			_ = c.conn.Close()
			return
		}
	}
}

// sleepOrDone sleeps for d, returning true if done fired first.
func sleepOrDone(d time.Duration, done <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return false
	case <-done:
		return true
	}
}
