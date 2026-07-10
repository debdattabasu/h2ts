// Server-side WebSocket handshake (RFC 6455).
//
// It's small: validate the upgrade headers, pick which offered subprotocol to
// echo (see the negotiation policy on [AcceptOptions]), compute
// Sec-WebSocket-Accept (SHA-1 of the client key plus the RFC 6455 magic GUID,
// base64-encoded), hijack the connection, and write the 101. The framing on top
// of the returned byte stream is done by [Conn].

package server

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// wsGUID is the RFC 6455 §4.2.2 magic GUID appended to the client key before
// hashing to derive Sec-WebSocket-Accept.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// AcceptOptions controls how [AcceptWithOptions] negotiates a subprotocol and
// which origins it accepts.
type AcceptOptions struct {
	// Select is handed the full list of subprotocols the client offered (the
	// same thing [OfferedProtocols] returns) and returns the one to echo in the
	// 101, or "" to decline. A selection outside the offered list is treated as a
	// decline (RFC 6455 forbids echoing a subprotocol the client did not offer).
	// When it declines, the default policy applies: echo DefaultSubprotocol
	// (h2ts) if the client offered it, else reject (or, with AllowImplicitCodec,
	// echo the client's first offered codec). nil means always decline.
	Select func(offered []string) string

	// AllowImplicitCodec, when the selector declines and the client did not offer
	// h2ts, accepts the client's first offered subprotocol (or completes with no
	// subprotocol if it offered none) instead of rejecting the handshake. Off by
	// default: a client speaking neither h2ts nor a selected protocol is rejected
	// with 400. Turn it on for a codec-agnostic tunnel (a websockify-style
	// `binary` client, a raw tunnel, …).
	AllowImplicitCodec bool

	// AllowedOrigins is an opt-in Origin allowlist (Cross-Site WebSocket Hijacking
	// defence). When non-nil, only handshakes whose Origin header matches one of
	// these entries (ASCII case-insensitive, e.g. "https://app.example.com") are
	// accepted; everything else — including a missing Origin — is rejected with
	// 403. nil (the default) accepts any origin, mirroring nginx (which does no
	// Origin validation by default); a browser always sends Origin, so a
	// legitimate browser client is unaffected.
	AllowedOrigins []string
}

// HandshakeError is a pre-upgrade handshake rejection. [Accept] /
// [AcceptWithOptions] write the response with [HandshakeError.Status] to the
// [http.ResponseWriter] before returning it, so a handler can simply return; the
// error is exposed for logging and inspection.
type HandshakeError struct {
	// Status is the HTTP status the handshake was rejected with: 426 (not a
	// WebSocket upgrade), 400 (no supported subprotocol), or 403 (forbidden
	// origin).
	Status int
	// Reason is a short human-readable description.
	Reason string
}

func (e *HandshakeError) Error() string {
	return fmt.Sprintf("h2ts handshake rejected: %s (HTTP %d)", e.Reason, e.Status)
}

// The pre-upgrade rejections, mirroring the Rust server's WebSocketError arms.
var (
	errNotUpgrade             = &HandshakeError{http.StatusUpgradeRequired, "not a WebSocket upgrade request"}
	errUnsupportedSubprotocol = &HandshakeError{http.StatusBadRequest, "client offered no supported subprotocol"}
	errForbiddenOrigin        = &HandshakeError{http.StatusForbidden, "origin not allowed"}
)

// headerListContains reports whether any value of header (which may be a
// comma-separated token list) contains needle, ASCII case-insensitively.
func headerListContains(h http.Header, header, needle string) bool {
	for _, v := range h.Values(header) {
		for _, tok := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(tok), needle) {
				return true
			}
		}
	}
	return false
}

// IsUpgradeRequest reports whether r is a WebSocket upgrade request: Upgrade
// lists websocket, Connection lists upgrade, and a Sec-WebSocket-Key is present.
func IsUpgradeRequest(r *http.Request) bool {
	return headerListContains(r.Header, "Upgrade", "websocket") &&
		headerListContains(r.Header, "Connection", "upgrade") &&
		r.Header.Get("Sec-WebSocket-Key") != ""
}

// OfferedProtocols returns the WebSocket subprotocols the client offered, in
// order, whitespace-trimmed and with empties dropped; nil if it offered none.
// Handles both a single comma-separated header and repeated headers.
func OfferedProtocols(r *http.Request) []string {
	var out []string
	for _, v := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, tok := range strings.Split(v, ",") {
			if tok = strings.TrimSpace(tok); tok != "" {
				out = append(out, tok)
			}
		}
	}
	return out
}

// containsFold reports whether list holds s, ASCII case-insensitively.
func containsFold(list []string, s string) bool {
	for _, v := range list {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}

// negotiate decides which subprotocol to echo. It returns (chosen, hasChosen,
// accept): accept=false means reject with 400; hasChosen=false with accept=true
// means complete the handshake echoing no subprotocol.
//
// Prefers DefaultSubprotocol (h2ts) — in its exact offered casing — whenever the
// client offered it. Otherwise, with allowImplicit, echoes the client's first
// offered codec (or none); without it, rejects.
func negotiate(offered []string, sel func([]string) string, allowImplicit bool) (chosen string, hasChosen, accept bool) {
	// Never echo a subprotocol the client did not offer (RFC 6455 §4.2.2 — the
	// client fails the connection if we do); an unoffered selection is a decline.
	if sel != nil {
		if s := sel(offered); s != "" && containsFold(offered, s) {
			return s, true, true
		}
	}
	for _, p := range offered {
		if strings.EqualFold(p, DefaultSubprotocol) {
			return p, true, true
		}
	}
	if allowImplicit {
		if len(offered) > 0 {
			return offered[0], true, true
		}
		return "", false, true
	}
	return "", false, false
}

// acceptKey computes the Sec-WebSocket-Accept value for a client
// Sec-WebSocket-Key.
func acceptKey(key string) string {
	h := sha1.New()
	io.WriteString(h, key)
	io.WriteString(h, wsGUID)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// reject writes he's status and reason to w and returns he, so a handler can do
// `if _, err := Accept(w, r); err != nil { return }`.
func reject(w http.ResponseWriter, he *HandshakeError) error {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if he.Status == http.StatusUpgradeRequired {
		// Advertise what we require (RFC 7231 §6.5.15).
		w.Header().Set("Upgrade", "websocket")
		w.Header().Set("Connection", "Upgrade")
	}
	w.WriteHeader(he.Status)
	io.WriteString(w, he.Reason+"\n")
	return he
}

// Accept performs the server-side WebSocket handshake requiring the
// DefaultSubprotocol (h2ts), hijacks the connection, and returns it as a [*Conn].
//
// Equivalent to [AcceptWithOptions] with a zero [AcceptOptions]: h2ts is echoed
// when the client offered it; a client that doesn't offer h2ts is rejected with
// 400, a non-upgrade request with 426. On any such rejection Accept writes the
// response to w and returns a [*HandshakeError] — the caller should just return.
//
// On success it writes the 101 and returns a ready-to-use [*Conn]; hand it to
// [ServeH2] (typically on a new goroutine so the accepting handler can return).
// The outer server must be plain HTTP/1.1 so the request is hijackable.
func Accept(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	return AcceptWithOptions(w, r, AcceptOptions{})
}

// AcceptWithOptions is [Accept] with an explicit [AcceptOptions] — a custom
// subprotocol selector, implicit-codec acceptance, and/or an Origin allowlist.
func AcceptWithOptions(w http.ResponseWriter, r *http.Request, opts AcceptOptions) (*Conn, error) {
	if !IsUpgradeRequest(r) {
		return nil, reject(w, errNotUpgrade)
	}

	// Opt-in Origin allowlist (CSWSH defence). Off by default (nginx-style): only
	// enforced when the caller configured AllowedOrigins. A missing Origin is
	// rejected when an allowlist is set.
	if opts.AllowedOrigins != nil {
		origin := r.Header.Get("Origin")
		if origin == "" || !containsFold(opts.AllowedOrigins, origin) {
			return nil, reject(w, errForbiddenOrigin)
		}
	}

	offered := OfferedProtocols(r)
	chosen, hasChosen, accept := negotiate(offered, opts.Select, opts.AllowImplicitCodec)
	if !accept {
		return nil, reject(w, errUnsupportedSubprotocol)
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		// The outer connection can't be hijacked (e.g. it's itself HTTP/2). The
		// h2ts gateway must run over HTTP/1.1.
		return nil, reject(w, &HandshakeError{
			http.StatusInternalServerError,
			"connection does not support hijacking (serve the gateway over HTTP/1.1)",
		})
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}

	// Write the 101 through the hijacked buffered writer (flushing any bytes
	// net/http had buffered first), then flush to the socket. Reads use brw.Reader,
	// which may already hold client bytes sent after the handshake (an h2ts client
	// pipelines its HTTP/2 preface immediately) — Conn consumes from it.
	if err := writeSwitchingProtocols(brw, r.Header.Get("Sec-WebSocket-Key"), chosen, hasChosen); err != nil {
		conn.Close()
		return nil, err
	}

	proto := ""
	if hasChosen {
		proto = chosen
	}
	return newConn(conn, brw.Reader, proto), nil
}

// writeSwitchingProtocols emits the 101 handshake response.
func writeSwitchingProtocols(brw *bufio.ReadWriter, key, proto string, hasProto bool) error {
	var b strings.Builder
	b.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	b.WriteString("Upgrade: websocket\r\n")
	b.WriteString("Connection: Upgrade\r\n")
	b.WriteString("Sec-WebSocket-Accept: ")
	b.WriteString(acceptKey(key))
	b.WriteString("\r\n")
	if hasProto {
		b.WriteString("Sec-WebSocket-Protocol: ")
		b.WriteString(proto)
		b.WriteString("\r\n")
	}
	b.WriteString("\r\n")
	if _, err := brw.WriteString(b.String()); err != nil {
		return err
	}
	return brw.Flush()
}
