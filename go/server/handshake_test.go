package server

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestIsUpgradeRequest(t *testing.T) {
	full := httptest.NewRequest("GET", "/", nil)
	full.Header.Set("Upgrade", "websocket")
	full.Header.Set("Connection", "Upgrade")
	full.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	if !IsUpgradeRequest(full) {
		t.Fatal("a complete upgrade request should be recognized")
	}

	// Connection may be a token list.
	full.Header.Set("Connection", "keep-alive, Upgrade")
	if !IsUpgradeRequest(full) {
		t.Fatal("Connection as a token list should still match")
	}

	// Missing the key.
	noKey := httptest.NewRequest("GET", "/", nil)
	noKey.Header.Set("Upgrade", "websocket")
	noKey.Header.Set("Connection", "Upgrade")
	if IsUpgradeRequest(noKey) {
		t.Fatal("a request without Sec-WebSocket-Key is not an upgrade")
	}

	if IsUpgradeRequest(httptest.NewRequest("GET", "/", nil)) {
		t.Fatal("a plain request is not an upgrade")
	}
}

func TestOfferedProtocols(t *testing.T) {
	cases := []struct {
		header string
		want   []string
	}{
		{"h2ts", []string{"h2ts"}},
		{"chat, h2ts, binary", []string{"chat", "h2ts", "binary"}},
		{"  h2ts  ", []string{"h2ts"}},
		{"", nil},
	}
	for _, tc := range cases {
		r := httptest.NewRequest("GET", "/", nil)
		if tc.header != "" {
			r.Header.Set("Sec-WebSocket-Protocol", tc.header)
		}
		if got := OfferedProtocols(r); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("OfferedProtocols(%q) = %v, want %v", tc.header, got, tc.want)
		}
	}
	// No header at all.
	if got := OfferedProtocols(httptest.NewRequest("GET", "/", nil)); got != nil {
		t.Errorf("OfferedProtocols(none) = %v, want nil", got)
	}
}

func TestNegotiate(t *testing.T) {
	pickBinary := func(offered []string) string {
		if containsFold(offered, "binary") {
			return "binary"
		}
		return ""
	}
	pickEvil := func([]string) string { return "evil" } // never offered

	cases := []struct {
		name      string
		offered   []string
		sel       func([]string) string
		implicit  bool
		chosen    string
		hasChosen bool
		accept    bool
	}{
		{"h2ts offered", []string{"h2ts"}, nil, false, "h2ts", true, true},
		{"h2ts preferred not first", []string{"chat", "h2ts"}, nil, false, "h2ts", true, true},
		{"h2ts offered casing kept", []string{"H2TS"}, nil, false, "H2TS", true, true},
		{"no h2ts strict rejects", []string{"chat"}, nil, false, "", false, false},
		{"empty strict rejects", nil, nil, false, "", false, false},
		{"implicit accepts first", []string{"chat", "binary"}, nil, true, "chat", true, true},
		{"implicit empty accepts none", nil, nil, true, "", false, true},
		{"selector picks offered", []string{"chat", "binary"}, pickBinary, false, "binary", true, true},
		{"unoffered selection falls back to h2ts", []string{"h2ts", "chat"}, pickEvil, false, "h2ts", true, true},
		{"unoffered selection no fallback rejects", []string{"chat", "binary"}, pickEvil, false, "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chosen, hasChosen, accept := negotiate(tc.offered, tc.sel, tc.implicit)
			if chosen != tc.chosen || hasChosen != tc.hasChosen || accept != tc.accept {
				t.Errorf("negotiate = (%q, %v, %v), want (%q, %v, %v)",
					chosen, hasChosen, accept, tc.chosen, tc.hasChosen, tc.accept)
			}
		})
	}
}

func TestAcceptKeyRFC6455(t *testing.T) {
	// RFC 6455 §1.3: key "dGhlIHNhbXBsZSBub25jZQ==" -> Sec-WebSocket-Accept
	// "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=".
	if got := acceptKey("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("acceptKey = %q, want the RFC 6455 example value", got)
	}
}

func TestHandshakeSubprotocolNegotiationOverTheWire(t *testing.T) {
	addr := startGateway(t, testRoutes(), AcceptOptions{}, ServeConfig{})

	cases := []struct {
		name       string
		offer      []string
		wantStatus int
		wantProto  string
	}{
		{"h2ts accepted", []string{"h2ts"}, 101, "h2ts"},
		{"h2ts preferred", []string{"chat", "h2ts"}, 101, "h2ts"},
		{"non-h2ts rejected", []string{"mystery"}, 400, ""},
		{"empty offer rejected", nil, 400, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, hdr, cli := wsHandshakeRaw(t, addr, "/", nil, tc.offer...)
			defer cli.Conn.Close()
			if code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", code, tc.wantStatus)
			}
			if got := hdr.Get("Sec-WebSocket-Protocol"); got != tc.wantProto {
				t.Fatalf("echoed subprotocol = %q, want %q", got, tc.wantProto)
			}
			if code == 101 && hdr.Get("Sec-WebSocket-Accept") == "" {
				t.Fatal("101 response missing Sec-WebSocket-Accept")
			}
		})
	}
}

func TestHandshakeRejectsNonUpgradeWith426(t *testing.T) {
	addr := startGateway(t, testRoutes(), AcceptOptions{}, ServeConfig{})
	resp, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("plain GET status = %d, want 426", resp.StatusCode)
	}
}

func TestHandshakeImplicitCodec(t *testing.T) {
	addr := startGateway(t, testRoutes(), AcceptOptions{AllowImplicitCodec: true}, ServeConfig{})

	// A non-h2ts codec is now accepted, its first offered value echoed.
	code, hdr, cli := wsHandshakeRaw(t, addr, "/", nil, "mystery", "other")
	cli.Conn.Close()
	if code != 101 || hdr.Get("Sec-WebSocket-Protocol") != "mystery" {
		t.Fatalf("implicit codec: status %d proto %q, want 101 mystery", code, hdr.Get("Sec-WebSocket-Protocol"))
	}

	// h2ts still wins when offered.
	code, hdr, cli = wsHandshakeRaw(t, addr, "/", nil, "chat", "h2ts")
	cli.Conn.Close()
	if code != 101 || hdr.Get("Sec-WebSocket-Protocol") != "h2ts" {
		t.Fatalf("implicit codec with h2ts: proto %q, want h2ts", hdr.Get("Sec-WebSocket-Protocol"))
	}
}

func TestHandshakeOriginAllowlist(t *testing.T) {
	opts := AcceptOptions{AllowedOrigins: []string{"https://app.example.com"}}
	addr := startGateway(t, testRoutes(), opts, ServeConfig{})

	cases := []struct {
		name       string
		origin     string
		hasOrigin  bool
		wantStatus int
	}{
		{"listed origin (case-insensitive)", "https://APP.example.com", true, 101},
		{"unlisted origin", "https://evil.example.com", true, 403},
		{"missing origin", "", false, 403},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var extra map[string]string
			if tc.hasOrigin {
				extra = map[string]string{"Origin": tc.origin}
			}
			code, _, cli := wsHandshakeRaw(t, addr, "/", extra, "h2ts")
			cli.Conn.Close()
			if code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", code, tc.wantStatus)
			}
		})
	}
}
