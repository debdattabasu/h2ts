package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

// A fixed masking key keeps the framing tests deterministic.
var testMask = [4]byte{0x37, 0xfa, 0x21, 0x3d}

// clientFrame builds a masked client->server frame (what the server's Conn reads).
func clientFrame(opcode byte, payload []byte) []byte {
	frame := []byte{0x80 | opcode}
	n := len(payload)
	switch {
	case n <= 125:
		frame = append(frame, 0x80|byte(n))
	case n <= 0xFFFF:
		frame = append(frame, 0x80|126, 0, 0)
		binary.BigEndian.PutUint16(frame[len(frame)-2:], uint16(n))
	default:
		frame = append(frame, 0x80|127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(frame[len(frame)-8:], uint64(n))
	}
	frame = append(frame, testMask[:]...)
	for i, b := range payload {
		frame = append(frame, b^testMask[i&3])
	}
	return frame
}

// unmaskedFrame builds an (illegal) unmasked client frame.
func unmaskedFrame(opcode byte, payload []byte) []byte {
	frame := []byte{0x80 | opcode, byte(len(payload))}
	return append(frame, payload...)
}

// fakeConn is an in-memory net.Conn: reads drain a preloaded byte slice, writes
// accumulate in a buffer for inspection.
type fakeConn struct {
	in  *bytes.Reader
	out bytes.Buffer
}

func newFakeConn(inbound []byte) *fakeConn { return &fakeConn{in: bytes.NewReader(inbound)} }

func (f *fakeConn) Read(p []byte) (int, error)       { return f.in.Read(p) }
func (f *fakeConn) Write(p []byte) (int, error)      { return f.out.Write(p) }
func (f *fakeConn) Close() error                     { return nil }
func (f *fakeConn) LocalAddr() net.Addr              { return dummyAddr{} }
func (f *fakeConn) RemoteAddr() net.Addr             { return dummyAddr{} }
func (f *fakeConn) SetDeadline(time.Time) error      { return nil }
func (f *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "fake" }
func (dummyAddr) String() string  { return "fake" }

// connOver wraps a fakeConn preloaded with the given inbound frames as a Conn.
func connOver(inbound []byte) (*Conn, *fakeConn) {
	f := newFakeConn(inbound)
	return newConn(f, bufio.NewReader(f), DefaultSubprotocol), f
}

// serverFrame parses a single server->client (unmasked) frame from b, returning
// its opcode, payload, and the remaining bytes.
func serverFrame(t *testing.T, b []byte) (opcode byte, payload, rest []byte) {
	t.Helper()
	if len(b) < 2 {
		t.Fatalf("short frame: %d bytes", len(b))
	}
	opcode = b[0] & 0x0F
	if b[1]&0x80 != 0 {
		t.Fatal("server frame must not be masked")
	}
	n := int(b[1] & 0x7F)
	off := 2
	switch n {
	case 126:
		n = int(binary.BigEndian.Uint16(b[2:4]))
		off = 4
	case 127:
		n = int(binary.BigEndian.Uint64(b[2:10]))
		off = 10
	}
	return opcode, b[off : off+n], b[off+n:]
}

func TestConnReadsMaskedBinary(t *testing.T) {
	c, _ := connOver(clientFrame(opBinary, []byte("hello")))
	got, err := io.ReadAll(readerOf(c, 5))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("read = %q, want %q", got, "hello")
	}
}

// TestConnStreamsFrameIncrementally proves a large frame is delivered in chunks
// bounded by the caller's buffer — never buffered whole (spec §4).
func TestConnStreamsFrameIncrementally(t *testing.T) {
	payload := bytes.Repeat([]byte("abcd"), 4096) // 16 KiB in one WS frame
	c, _ := connOver(clientFrame(opBinary, payload))

	var got []byte
	buf := make([]byte, 100)
	for len(got) < len(payload) {
		n, err := c.Read(buf)
		if n > len(buf) {
			t.Fatalf("Read returned %d > buffer %d — buffered a whole frame", n, len(buf))
		}
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("streamed payload mismatch (%d bytes)", len(got))
	}
}

func TestConnAutoAnswersPing(t *testing.T) {
	inbound := append(clientFrame(opPing, []byte("ping-payload")), clientFrame(opBinary, []byte("x"))...)
	c, f := connOver(inbound)

	buf := make([]byte, 8)
	n, err := c.Read(buf)
	if err != nil || string(buf[:n]) != "x" {
		t.Fatalf("read after ping = %q, %v", buf[:n], err)
	}
	op, payload, _ := serverFrame(t, f.out.Bytes())
	if op != opPong {
		t.Fatalf("auto-answer opcode = 0x%x, want pong", op)
	}
	if string(payload) != "ping-payload" {
		t.Fatalf("pong payload = %q, want echo of ping", payload)
	}
}

func TestConnObservesPingAndPong(t *testing.T) {
	inbound := clientFrame(opPing, []byte("P"))
	inbound = append(inbound, clientFrame(opPong, []byte("Q"))...)
	inbound = append(inbound, clientFrame(opBinary, []byte("x"))...)
	c, f := connOver(inbound)

	var sawPing, sawPong []byte
	c.onPing = func(p []byte) { sawPing = append([]byte(nil), p...) }
	c.onPong = func(p []byte) { sawPong = append([]byte(nil), p...) }

	buf := make([]byte, 8)
	if n, err := c.Read(buf); err != nil || string(buf[:n]) != "x" {
		t.Fatalf("read = %q, %v", buf[:n], err)
	}
	if string(sawPing) != "P" {
		t.Fatalf("OnPing saw %q, want %q", sawPing, "P")
	}
	if string(sawPong) != "Q" {
		t.Fatalf("OnPong saw %q, want %q", sawPong, "Q")
	}
	// Auto-pong still happens even though OnPing observed the ping.
	if op, payload, _ := serverFrame(t, f.out.Bytes()); op != opPong || string(payload) != "P" {
		t.Fatalf("auto-answer = op 0x%x %q, want pong P", op, payload)
	}
}

func TestConnPingPongSend(t *testing.T) {
	c, f := connOver(nil)
	if err := c.Ping([]byte("hi")); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := c.Pong([]byte("yo")); err != nil {
		t.Fatalf("Pong: %v", err)
	}
	op, payload, rest := serverFrame(t, f.out.Bytes())
	if op != opPing || string(payload) != "hi" {
		t.Fatalf("Ping wrote op 0x%x %q, want ping hi", op, payload)
	}
	if op, payload, _ = serverFrame(t, rest); op != opPong || string(payload) != "yo" {
		t.Fatalf("Pong wrote op 0x%x %q, want pong yo", op, payload)
	}
	// A control payload over 125 bytes is rejected rather than sent malformed.
	if err := c.Ping(make([]byte, 126)); err == nil {
		t.Fatal("Ping with a 126-byte payload should error")
	}
}

func TestConnPeerCloseEchoesAndEOF(t *testing.T) {
	var closePayload [5]byte
	binary.BigEndian.PutUint16(closePayload[:2], 4000)
	copy(closePayload[2:], "bye")
	c, f := connOver(clientFrame(opClose, closePayload[:]))

	if _, err := c.Read(make([]byte, 8)); err != io.EOF {
		t.Fatalf("read after peer close = %v, want io.EOF", err)
	}
	if r := c.CloseReason(); r.Code != 4000 || r.Reason != "bye" {
		t.Fatalf("CloseReason = %+v, want {4000 bye}", r)
	}
	op, payload, _ := serverFrame(t, f.out.Bytes())
	if op != opClose {
		t.Fatalf("echo opcode = 0x%x, want close", op)
	}
	if code := binary.BigEndian.Uint16(payload[:2]); code != 4000 {
		t.Fatalf("close echo code = %d, want 4000", code)
	}
}

func TestConnRejectsUnmaskedFrame(t *testing.T) {
	c, f := connOver(unmaskedFrame(opBinary, []byte("nope")))
	if _, err := c.Read(make([]byte, 8)); err == nil {
		t.Fatal("expected a protocol error for an unmasked client frame")
	}
	if r := c.CloseReason(); r.Code != closeProtocolError {
		t.Fatalf("CloseReason code = %d, want %d (protocol error)", r.Code, closeProtocolError)
	}
	op, payload, _ := serverFrame(t, f.out.Bytes())
	if op != opClose || binary.BigEndian.Uint16(payload[:2]) != closeProtocolError {
		t.Fatalf("expected a 1002 close, got op 0x%x", op)
	}
}

func TestConnTransportEOFIsAbnormal(t *testing.T) {
	c, _ := connOver(nil) // nothing to read
	if _, err := c.Read(make([]byte, 8)); err != io.EOF {
		t.Fatalf("read on empty transport = %v, want io.EOF", err)
	}
	if r := c.CloseReason(); r.Code != closeAbnormal {
		t.Fatalf("CloseReason code = %d, want %d (abnormal)", r.Code, closeAbnormal)
	}
}

func TestConnWriteEmitsUnmaskedBinary(t *testing.T) {
	c, f := connOver(nil)
	if n, err := c.Write([]byte("world")); n != 5 || err != nil {
		t.Fatalf("Write = %d, %v", n, err)
	}
	op, payload, rest := serverFrame(t, f.out.Bytes())
	if op != opBinary || string(payload) != "world" {
		t.Fatalf("wrote op 0x%x %q", op, payload)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected trailing bytes: %d", len(rest))
	}
}

func TestConnWriteLargeFrameUses64BitLength(t *testing.T) {
	c, f := connOver(nil)
	payload := bytes.Repeat([]byte{'z'}, 70000) // > 65535 -> 127 length form
	if _, err := c.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if b := f.out.Bytes(); b[1]&0x7F != 127 {
		t.Fatalf("length form = %d, want 127 for a 70000-byte payload", b[1]&0x7F)
	}
	_, got, _ := serverFrame(t, f.out.Bytes())
	if !bytes.Equal(got, payload) {
		t.Fatal("large frame payload mismatch")
	}
}

// TestKeepAliveClosesOnNoPong mirrors the Rust server's keepalive test: a peer
// that never answers the ping is sent a ping, then a 1001 close, and the reason
// surfaces via CloseReason.
func TestKeepAliveClosesOnNoPong(t *testing.T) {
	cli, srv := net.Pipe()
	c := newConn(srv, bufio.NewReader(srv), DefaultSubprotocol)
	ka := KeepAlive{
		Interval: 20 * time.Millisecond,
		Timeout:  20 * time.Millisecond,
		Close:    CloseFrame{Code: closeGoingAway, Reason: "keepalive timeout"},
	}
	done := make(chan struct{})
	go c.runKeepAlive(ka, done)
	defer close(done)

	cli.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := bufio.NewReader(cli)

	// First a keepalive Ping (the client never pongs).
	if op, _ := readServerCtl(t, r); op != opPing {
		t.Fatalf("first frame opcode = 0x%x, want ping", op)
	}
	// Then, with no answer, the keepalive Close.
	op, payload := readServerCtl(t, r)
	if op != opClose {
		t.Fatalf("second frame opcode = 0x%x, want close", op)
	}
	if code := binary.BigEndian.Uint16(payload[:2]); code != closeGoingAway {
		t.Fatalf("close code = %d, want %d (going away)", code, closeGoingAway)
	}
	if reason := string(payload[2:]); reason != "keepalive timeout" {
		t.Fatalf("close reason = %q", reason)
	}
	if r := c.CloseReason(); r.Code != closeGoingAway || r.Reason != "keepalive timeout" {
		t.Fatalf("CloseReason = %+v", r)
	}
}

// readServerCtl reads one full server->client frame (used for control frames).
func readServerCtl(t *testing.T, r *bufio.Reader) (byte, []byte) {
	t.Helper()
	var h [2]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		t.Fatalf("read frame header: %v", err)
	}
	n := int64(h[1] & 0x7F)
	switch n {
	case 126:
		var e [2]byte
		io.ReadFull(r, e[:])
		n = int64(binary.BigEndian.Uint16(e[:]))
	case 127:
		var e [8]byte
		io.ReadFull(r, e[:])
		n = int64(binary.BigEndian.Uint64(e[:]))
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		t.Fatalf("read frame payload: %v", err)
	}
	return h[0] & 0x0F, payload
}

// readerOf adapts Conn.Read to an io.Reader that stops after total bytes.
func readerOf(c *Conn, total int) io.Reader { return io.LimitReader(c, int64(total)) }

// timeoutC returns a channel that fires after secs seconds.
func timeoutC(t *testing.T, secs int) <-chan time.Time {
	t.Helper()
	return time.After(time.Duration(secs) * time.Second)
}
