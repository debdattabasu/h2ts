// The WebSocket presented as a raw byte stream (RFC 6455 framing).
//
// [Conn] is the Go analog of the Rust server's WsByteStream: a [net.Conn] whose
// Read yields the concatenated payloads of inbound data frames and whose Write
// emits binary frames. It runs the whole state machine inline — no background
// pump — so control frames are handled where the byte stream is driven: an
// inbound ping is auto-answered with a pong, a pong just marks liveness, and a
// close (or a transport EOF) ends the stream. Server frames are never masked;
// client frames MUST be (RFC 6455 §5.1). Payloads stream incrementally: a Read
// never buffers a whole frame, so a DATA frame larger than any WebSocket frame
// (or vice versa) rides straight through (spec/protocol.md §4).

package server

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// WebSocket opcodes (RFC 6455 §5.2).
const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// CloseFrame is a WebSocket close: an RFC 6455 status code and (UTF-8) reason.
// The reason should be at most 123 bytes.
type CloseFrame struct {
	Code   uint16
	Reason string
}

// protocolError is a WebSocket protocol violation (RFC 6455 §7.4.1 code 1002),
// e.g. an unmasked client frame or a reserved opcode. It ends the stream after a
// best-effort 1002 close.
type protocolError string

func (e protocolError) Error() string { return "websocket protocol error: " + string(e) }

func protocolErrorf(format string, args ...any) protocolError {
	return protocolError(fmt.Sprintf(format, args...))
}

// Conn is a server-side WebSocket presented as a raw byte stream. It implements
// [net.Conn], so it can be handed to a prior-knowledge h2c server (see
// [ServeH2]) or bridged to any peer with io.Copy.
//
// Read and Write are each safe for concurrent use with the other (Write is
// serialized against control-frame and keepalive writes internally); the h2c
// server relies on exactly that. Read is not meant to be called from multiple
// goroutines at once.
type Conn struct {
	// Subprotocol is the subprotocol negotiated during the handshake, echoed back
	// to the client ("" if none). Informational; the framing does not depend on it.
	Subprotocol string

	conn net.Conn
	r    *bufio.Reader

	writeMu sync.Mutex // serializes every frame written (data, control, keepalive)

	// Inbound data frame currently being streamed to Read. Only touched by Read.
	frameRemaining int64
	maskKey        [4]byte
	maskPos        int

	// Unix-nanos of the last frame received from the peer; read by keepalive to
	// decide when to ping and whether the peer answered.
	lastActivity atomic.Int64

	closeOnce sync.Once
	closed    atomic.Bool

	reasonMu  sync.Mutex
	reason    CloseFrame
	reasonSet bool

	// Optional observation hooks for received control frames, fired inline on the
	// read path (see ServeConfig.OnPing/OnPong). Set once before serving begins,
	// so no synchronization is needed.
	onPing func([]byte)
	onPong func([]byte)
}

var _ net.Conn = (*Conn)(nil)

// newConn wraps a hijacked connection (and its buffered reader, which may already
// hold client bytes) as a WebSocket byte stream.
func newConn(conn net.Conn, r *bufio.Reader, subprotocol string) *Conn {
	c := &Conn{Subprotocol: subprotocol, conn: conn, r: r}
	c.touch()
	return c
}

// touch records that a frame just arrived from the peer (liveness for keepalive).
func (c *Conn) touch() { c.lastActivity.Store(time.Now().UnixNano()) }

// Read returns the decoded payload bytes of inbound data frames as one
// continuous stream, transparently answering pings, absorbing pongs, and
// treating a Close (or a transport EOF) as end-of-stream (io.EOF). It never
// returns more than one data frame's worth per call and never buffers a whole
// frame.
func (c *Conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	// Advance to the next data frame, handling any control frames along the way.
	// A zero-length data frame contributes nothing, so loop until there's payload.
	for c.frameRemaining == 0 {
		if err := c.nextDataFrame(); err != nil {
			return 0, err
		}
	}
	n := int64(len(p))
	if n > c.frameRemaining {
		n = c.frameRemaining
	}
	m, err := c.r.Read(p[:n])
	if m > 0 {
		c.unmask(p[:m])
		c.frameRemaining -= int64(m)
		return m, nil
	}
	// m == 0 implies err != nil (bufio.Read only returns 0,nil for an empty p).
	return 0, c.readErr(err)
}

// nextDataFrame reads frames — auto-answering pings, absorbing pongs — until it
// parses a data frame header (setting frameRemaining/maskKey for Read to stream),
// or the stream ends. Returns io.EOF on a peer Close, or an error otherwise.
func (c *Conn) nextDataFrame() error {
	for {
		opcode, masked, length, err := c.readHeader()
		if err != nil {
			return c.readErr(err)
		}
		c.touch()
		switch opcode {
		case opBinary, opText, opContinuation:
			c.frameRemaining = length
			c.maskPos = 0
			return nil
		case opPing:
			payload, err := c.readControlPayload(masked, length)
			if err != nil {
				return c.readErr(err)
			}
			// Auto-answer with a pong (RFC 6455 §5.5.2), then surface it.
			if err := c.writeControl(opPong, payload); err != nil {
				c.setReason(CloseFrame{Code: closeAbnormal, Reason: err.Error()})
				return err
			}
			if c.onPing != nil {
				c.onPing(payload)
			}
		case opPong:
			payload, err := c.readControlPayload(masked, length)
			if err != nil {
				return c.readErr(err)
			}
			if c.onPong != nil {
				c.onPong(payload)
			}
		case opClose:
			payload, _ := c.readControlPayload(masked, length)
			code, reason := parseClose(payload)
			c.setReason(CloseFrame{Code: code, Reason: reason})
			_ = c.writeClose(code, "") // echo the peer's close, then end
			return io.EOF
		default:
			return c.readErr(protocolErrorf("reserved opcode 0x%x", opcode))
		}
	}
}

// readHeader reads one WebSocket frame header (and, for a masked frame, the
// masking key into c.maskKey), enforcing the server-side constraints: RSV bits
// zero, every client frame masked (RFC 6455 §5.1), and control frames
// unfragmented and ≤125 bytes.
func (c *Conn) readHeader() (opcode byte, masked bool, length int64, err error) {
	var h [2]byte
	if _, err = io.ReadFull(c.r, h[:]); err != nil {
		return
	}
	fin := h[0]&0x80 != 0
	if h[0]&0x70 != 0 {
		err = protocolError("nonzero RSV bits (no extension negotiated)")
		return
	}
	opcode = h[0] & 0x0F
	masked = h[1]&0x80 != 0
	if !masked {
		err = protocolError("unmasked client frame")
		return
	}
	length = int64(h[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c.r, ext[:]); err != nil {
			return
		}
		length = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c.r, ext[:]); err != nil {
			return
		}
		u := binary.BigEndian.Uint64(ext[:])
		if u > math.MaxInt64 {
			err = protocolError("frame length overflows int64")
			return
		}
		length = int64(u)
	}
	// Control frames (opcode high bit set) must not be fragmented and are ≤125B.
	if opcode&0x8 != 0 {
		if !fin {
			err = protocolError("fragmented control frame")
			return
		}
		if length > 125 {
			err = protocolError("control frame payload > 125 bytes")
			return
		}
	}
	if _, err = io.ReadFull(c.r, c.maskKey[:]); err != nil {
		return
	}
	return
}

// readControlPayload reads and unmasks a control frame's (small) payload.
func (c *Conn) readControlPayload(masked bool, length int64) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return nil, err
	}
	if masked {
		for i := range buf {
			buf[i] ^= c.maskKey[i&3]
		}
	}
	return buf, nil
}

// unmask XORs b in place with the current frame's masking key, tracking the
// key offset across Read calls so a frame streamed in chunks unmasks correctly.
func (c *Conn) unmask(b []byte) {
	for i := range b {
		b[i] ^= c.maskKey[(c.maskPos+i)&3]
	}
	c.maskPos = (c.maskPos + len(b)) & 3
}

// readErr maps a read-side error to the close reason recorded for the connection
// and the error Read/nextDataFrame returns. A protocol violation triggers a
// best-effort 1002 close; a clean EOF is 1006 (abnormal: no WebSocket Close was
// seen); anything else is 1006 with the error text.
func (c *Conn) readErr(err error) error {
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	var pe protocolError
	if errors.As(err, &pe) {
		c.setReason(CloseFrame{Code: closeProtocolError, Reason: string(pe)})
		_ = c.writeClose(closeProtocolError, "")
		return err
	}
	if errors.Is(err, io.EOF) {
		c.setReason(CloseFrame{Code: closeAbnormal})
		return io.EOF
	}
	c.setReason(CloseFrame{Code: closeAbnormal, Reason: err.Error()})
	return err
}

// Write sends p as a single binary WebSocket frame (server frames are never
// masked). It returns len(p) on success. An empty write is a no-op.
func (c *Conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var hdr [10]byte
	n := encodeHeader(hdr[:], opBinary, len(p))

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// A single vectored write keeps the payload zero-copy (no masking to apply).
	bufs := net.Buffers{hdr[:n], p}
	if _, err := bufs.WriteTo(c.conn); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Ping sends a WebSocket ping carrying payload (at most 125 bytes). The peer's
// pong is delivered to the OnPong callback (see [ServeConfig]). Safe to call
// concurrently with the served HTTP/2 traffic and the keepalive — control and
// data writes are serialized.
func (c *Conn) Ping(payload []byte) error { return c.writeControl(opPing, payload) }

// Pong sends an unsolicited WebSocket pong carrying payload (at most 125 bytes).
// Received pings are auto-answered already; use this only to send a pong the peer
// didn't ask for.
func (c *Conn) Pong(payload []byte) error { return c.writeControl(opPong, payload) }

// encodeHeader writes a FIN, unmasked frame header for opcode carrying an
// n-byte payload into dst (which must be ≥10 bytes) and returns its length.
func encodeHeader(dst []byte, opcode byte, n int) int {
	dst[0] = 0x80 | opcode // FIN set; a data message is one frame
	switch {
	case n <= 125:
		dst[1] = byte(n)
		return 2
	case n <= math.MaxUint16:
		dst[1] = 126
		binary.BigEndian.PutUint16(dst[2:], uint16(n))
		return 4
	default:
		dst[1] = 127
		binary.BigEndian.PutUint64(dst[2:], uint64(n))
		return 10
	}
}

// writeControl writes a control frame (ping/pong/close) with the given payload
// (≤125 bytes), serialized against Write and every other control write.
func (c *Conn) writeControl(opcode byte, payload []byte) error {
	if len(payload) > 125 {
		return fmt.Errorf("h2ts: control frame payload is %d bytes, max 125", len(payload))
	}
	frame := make([]byte, 0, 2+len(payload))
	frame = append(frame, 0x80|opcode, byte(len(payload)))
	frame = append(frame, payload...)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.conn.Write(frame)
	return err
}

// writeClose sends a Close control frame. A non-sendable pseudo-code (1005/1006/
// 1015, or anything below 1000) is sent as an empty close, per RFC 6455 §7.4.
func (c *Conn) writeClose(code uint16, reason string) error {
	var payload []byte
	if isSendableCloseCode(code) {
		payload = make([]byte, 2+len(reason))
		binary.BigEndian.PutUint16(payload, code)
		copy(payload[2:], reason)
	}
	return c.writeControl(opClose, payload)
}

// Close sends a normal (1000) close if the connection isn't already winding down,
// then closes the underlying transport. Idempotent.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		// Only initiate a close if nothing else already did (peer close, keepalive
		// timeout, protocol error) — those recorded their own reason and close.
		if c.setReason(CloseFrame{Code: closeNormal}) {
			_ = c.writeClose(closeNormal, "")
		}
		_ = c.conn.Close()
	})
	return nil
}

// CloseReason reports why the connection ended: the peer's Close, a keepalive
// timeout, a protocol error, or 1006 (abnormal) if the transport dropped without
// a Close. Meaningful once Read has returned an error or Close has run.
func (c *Conn) CloseReason() CloseFrame {
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	if !c.reasonSet {
		return CloseFrame{Code: closeAbnormal}
	}
	return c.reason
}

// setReason records the close reason if none was set yet, returning whether it
// won the race (first writer). First reason wins, so the true cause is kept.
func (c *Conn) setReason(cf CloseFrame) bool {
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	if c.reasonSet {
		return false
	}
	c.reasonSet = true
	c.reason = cf
	return true
}

// net.Conn plumbing — deadlines and addresses delegate to the transport, which
// is what Read/Write ultimately operate on.

func (c *Conn) LocalAddr() net.Addr                { return c.conn.LocalAddr() }
func (c *Conn) RemoteAddr() net.Addr               { return c.conn.RemoteAddr() }
func (c *Conn) SetDeadline(t time.Time) error      { return c.conn.SetDeadline(t) }
func (c *Conn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

// RFC 6455 §7.4.1 close codes used here.
const (
	closeNormal        uint16 = 1000
	closeGoingAway     uint16 = 1001
	closeProtocolError uint16 = 1002
	closeNoStatus      uint16 = 1005 // pseudo-code: no status in the frame
	closeAbnormal      uint16 = 1006 // pseudo-code: connection dropped without Close
)

// isSendableCloseCode reports whether code may appear on the wire (RFC 6455
// §7.4.1/§7.4.2): the reserved pseudo-codes and anything below 1000 may not.
func isSendableCloseCode(code uint16) bool {
	if code < 1000 {
		return false
	}
	switch code {
	case closeNoStatus, closeAbnormal, 1015:
		return false
	}
	return true
}

// parseClose extracts the code and reason from a Close frame payload. An empty
// payload means "no status received" (1005).
func parseClose(payload []byte) (uint16, string) {
	if len(payload) < 2 {
		return closeNoStatus, ""
	}
	return binary.BigEndian.Uint16(payload[:2]), string(payload[2:])
}
