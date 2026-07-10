// Example: serve an in-process HTTP/2 service over a WebSocket tunnel using
// h2ts. This is just a caller of the library — the reusable machinery lives in
// the server package.
//
//	browser (h2ts) --ws--> [Accept -> ServeH2(ws, handler)] --> your handler
//
// The routes mirror the conformance origin (conformance/origin.mjs) and the Rust
// h2-server example, so the shared e2e suite validates this pure-Go path end to
// end: point the suite's WS_URL at this server.
//
// Run: go run ./examples/h2-server 127.0.0.1:8093
package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/debdattabasu/h2ts/go/server"
)

func main() {
	addr := "127.0.0.1:8093"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	// The outer server must be HTTP/1.1 so the WebSocket request is hijackable;
	// the HTTP/2 lives *inside* the tunnel. Disabling h2 on the listener keeps Go
	// from advertising it on the outer connection.
	srv := &http.Server{
		Addr:         addr,
		Handler:      http.HandlerFunc(upgrade),
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
	}
	log.Printf("[h2-server] ws://%s  ->  in-process Go HTTP/2 (h2c) service", addr)
	log.Fatal(srv.ListenAndServe())
}

// upgrade accepts the WebSocket handshake, then serves HTTP/2 over the tunnel.
func upgrade(w http.ResponseWriter, r *http.Request) {
	// Accept requires the h2ts subprotocol; a non-upgrade request (426) or a
	// client without h2ts (400) is rejected with a clean response by Accept.
	conn, err := server.Accept(w, r)
	if err != nil {
		log.Printf("[h2-server] handshake: %v", err)
		return
	}
	// ServeH2 blocks for the tunnel's lifetime; the outer handler must return so
	// the hijacked connection is ours, so serve on a new goroutine.
	go func() {
		if err := server.ServeH2(conn, http.HandlerFunc(route)); err != nil {
			log.Printf("[h2-server] serve: %v", err)
		}
	}()
}

// route is the application served over HTTP/2 — the conformance battery's origin.
func route(w http.ResponseWriter, r *http.Request) {
	log.Printf("[h2-server] %s %s", r.Method, r.URL.Path)
	switch r.URL.Path {
	case "/hello":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, "hello from the in-process Go h2 server over websocket!\n")

	case "/json":
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ok":true,"method":%q,"path":%q,"ts":1234567890}`, r.Method, r.URL.Path)

	case "/big":
		// 256 KiB -> multiple DATA frames, exercises client WINDOW_UPDATE.
		const size = 256 * 1024
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("x-size", fmt.Sprint(size))
		w.Write(bytes.Repeat([]byte{'x'}, size))

	case "/echo":
		// Echo the request body back (tests client upload / outbound flow control).
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("x-echo-bytes", fmt.Sprint(len(body)))
		w.Write(body)

	case "/headers":
		// Reflect a custom request header so the client can verify round-tripping.
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("x-saw-custom", r.Header.Get("x-custom"))
		io.WriteString(w, "ok")

	case "/trailers":
		// Response trailers: a HEADERS block sent AFTER the body. Announce the
		// trailer name up front, then set its value after writing the body.
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Trailer", "x-trailer")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "trailer-body")
		w.Header().Set("x-trailer", "after-body")

	case "/early-hints":
		// A 103 Early Hints informational (1xx) response before the final 200. The
		// client must NOT mistake the 1xx for the final response (RFC 7540 §8.1).
		h := w.Header()
		h.Add("Link", "</style.css>; rel=preload")
		w.WriteHeader(http.StatusEarlyHints)
		h.Del("Link")
		h.Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "final-body")

	default:
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "not found\n")
	}
}
