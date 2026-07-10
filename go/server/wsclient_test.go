package server

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"strings"
	"sync"
	"testing"

	"golang.org/x/net/http2"
)

// wsClient is a minimal client-side WebSocket presented as a net.Conn, used to
// drive the server's Conn in tests. It masks the binary frames it sends (RFC 6455
// requires clients to mask), auto-answers pings, and reports a Close as io.EOF —
// the mirror of what a real h2ts client's transport does. Server frames are never
// masked, so the read side just streams their payloads.
type wsClient struct {
	net.Conn // for LocalAddr/RemoteAddr/deadlines (so this satisfies net.Conn)
	r        *bufio.Reader
	wmu      sync.Mutex
	rem      int64 // remaining bytes in the inbound data frame being streamed
}

// wsHandshakeRaw dials addr and performs the WebSocket client handshake for path,
// offering protocols and sending any extra headers. It returns the response
// status code, the response headers, and the client (usable once status is 101).
func wsHandshakeRaw(t *testing.T, addr, path string, extra map[string]string, protocols ...string) (int, textproto.MIMEHeader, *wsClient) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}

	var key [16]byte
	rand.Read(key[:])
	var req strings.Builder
	fmt.Fprintf(&req, "GET %s HTTP/1.1\r\n", path)
	req.WriteString("Host: h2ts.test\r\n")
	req.WriteString("Upgrade: websocket\r\n")
	req.WriteString("Connection: Upgrade\r\n")
	fmt.Fprintf(&req, "Sec-WebSocket-Key: %s\r\n", base64.StdEncoding.EncodeToString(key[:]))
	req.WriteString("Sec-WebSocket-Version: 13\r\n")
	if len(protocols) > 0 {
		fmt.Fprintf(&req, "Sec-WebSocket-Protocol: %s\r\n", strings.Join(protocols, ", "))
	}
	for k, v := range extra {
		fmt.Fprintf(&req, "%s: %s\r\n", k, v)
	}
	req.WriteString("\r\n")
	if _, err := conn.Write([]byte(req.String())); err != nil {
		t.Fatalf("write handshake: %v", err)
	}

	r := bufio.NewReader(conn)
	tp := textproto.NewReader(r)
	statusLine, err := tp.ReadLine()
	if err != nil {
		t.Fatalf("read status line: %v", err)
	}
	hdr, err := tp.ReadMIMEHeader()
	if err != nil {
		t.Fatalf("read headers: %v", err)
	}
	var code int
	fmt.Sscanf(statusLine, "HTTP/1.1 %d", &code)
	return code, hdr, &wsClient{Conn: conn, r: r}
}

// wsDial performs the handshake, fails the test unless it's a 101, and returns
// the established tunnel plus the negotiated subprotocol.
func wsDial(t *testing.T, addr, path string, protocols ...string) (*wsClient, string) {
	t.Helper()
	code, hdr, cli := wsHandshakeRaw(t, addr, path, nil, protocols...)
	if code != 101 {
		cli.Conn.Close()
		t.Fatalf("handshake status = %d, want 101", code)
	}
	return cli, hdr.Get("Sec-WebSocket-Protocol")
}

// Read yields the payloads of inbound (unmasked) data frames, auto-answering
// pings and ending on a Close.
func (c *wsClient) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for c.rem == 0 {
		op, length, err := c.readFrameHeader()
		if err != nil {
			return 0, err
		}
		switch op {
		case opBinary, opText, opContinuation:
			c.rem = length
		case opPing:
			payload := make([]byte, length)
			if _, err := io.ReadFull(c.r, payload); err != nil {
				return 0, err
			}
			if err := c.writeCtl(opPong, payload); err != nil {
				return 0, err
			}
		case opPong:
			if _, err := io.CopyN(io.Discard, c.r, length); err != nil {
				return 0, err
			}
		case opClose:
			_, _ = io.CopyN(io.Discard, c.r, length)
			return 0, io.EOF
		default:
			return 0, fmt.Errorf("wsclient: unexpected opcode 0x%x", op)
		}
	}
	n := int64(len(p))
	if n > c.rem {
		n = c.rem
	}
	m, err := c.r.Read(p[:n])
	if m > 0 {
		c.rem -= int64(m)
		return m, nil
	}
	return 0, err
}

// readFrameHeader reads a server->client frame header (server frames are never
// masked) and returns its opcode and payload length.
func (c *wsClient) readFrameHeader() (opcode byte, length int64, err error) {
	var h [2]byte
	if _, err = io.ReadFull(c.r, h[:]); err != nil {
		return
	}
	opcode = h[0] & 0x0F
	length = int64(h[1] & 0x7F)
	switch length {
	case 126:
		var e [2]byte
		if _, err = io.ReadFull(c.r, e[:]); err != nil {
			return
		}
		length = int64(binary.BigEndian.Uint16(e[:]))
	case 127:
		var e [8]byte
		if _, err = io.ReadFull(c.r, e[:]); err != nil {
			return
		}
		length = int64(binary.BigEndian.Uint64(e[:]))
	}
	return
}

// Write sends p as a single masked binary frame.
func (c *wsClient) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var hdr [14]byte
	hdr[0] = 0x80 | opBinary
	var hl int
	switch n := len(p); {
	case n <= 125:
		hdr[1] = 0x80 | byte(n)
		hl = 2
	case n <= 0xFFFF:
		hdr[1] = 0x80 | 126
		binary.BigEndian.PutUint16(hdr[2:], uint16(n))
		hl = 4
	default:
		hdr[1] = 0x80 | 127
		binary.BigEndian.PutUint64(hdr[2:], uint64(len(p)))
		hl = 10
	}
	var mask [4]byte
	rand.Read(mask[:])
	copy(hdr[hl:], mask[:])
	hl += 4
	masked := make([]byte, len(p))
	for i := range p {
		masked[i] = p[i] ^ mask[i&3]
	}

	c.wmu.Lock()
	defer c.wmu.Unlock()
	if _, err := c.Conn.Write(hdr[:hl]); err != nil {
		return 0, err
	}
	if _, err := c.Conn.Write(masked); err != nil {
		return 0, err
	}
	return len(p), nil
}

// writeCtl sends a masked control frame (payload <= 125 bytes).
func (c *wsClient) writeCtl(opcode byte, payload []byte) error {
	var mask [4]byte
	rand.Read(mask[:])
	frame := make([]byte, 0, 6+len(payload))
	frame = append(frame, 0x80|opcode, 0x80|byte(len(payload)))
	frame = append(frame, mask[:]...)
	for i, b := range payload {
		frame = append(frame, b^mask[i&3])
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.Conn.Write(frame)
	return err
}

// Close sends a masked normal close and closes the transport.
func (c *wsClient) Close() error {
	var code [2]byte
	binary.BigEndian.PutUint16(code[:], closeNormal)
	_ = c.writeCtl(opClose, code[:])
	return c.Conn.Close()
}

// dialH2 runs prior-knowledge h2c over an established tunnel and returns the
// client connection.
func dialH2(t *testing.T, tunnel net.Conn) *http2.ClientConn {
	t.Helper()
	tr := &http2.Transport{AllowHTTP: true}
	cc, err := tr.NewClientConn(tunnel)
	if err != nil {
		t.Fatalf("h2 handshake over tunnel: %v", err)
	}
	return cc
}

// h2do performs one request over the tunnel and returns the response.
func h2do(t *testing.T, cc *http2.ClientConn, method, path string, body io.Reader, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, "http://h2ts.test"+path, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := cc.RoundTrip(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}
