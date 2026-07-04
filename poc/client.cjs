// Client: http2.js speaking HTTP/2 over a WebSocket-backed duplex stream.
// Flow: this client --ws://:8080--> websockify --tcp://:8000--> h2c server
const http2 = require('../http2.js/lib');
const websocket = require('../http2.js/node_modules/websocket-stream');

const WS_URL = 'ws://127.0.0.1:8090';

// websocket-stream wraps the browser/ws WebSocket into a node Duplex of raw bytes.
// Request the 'binary' subprotocol so websockify forwards frames verbatim (no base64).
const transport = websocket(WS_URL, ['binary'], {
  perMessageDeflate: false,
  binary: true,
});

transport.on('error', (err) => {
  console.error('[client] transport error:', err.message);
  process.exit(1);
});

// The underlying ws socket; wait for it to open before issuing the request.
transport.socket.on('open', () => {
  console.error('[client] websocket open ->', WS_URL);

  const req = http2.globalAgent.request({
    transport: transport, // <-- the whole point: generic duplex transport
    plain: true,          // h2c semantics
    method: 'GET',
    host: '127.0.0.1',
    path: '/hello',
    headers: {},
  });

  req.on('response', (response) => {
    console.error('[client] response status:', response.statusCode);
    console.error('[client] response headers:', JSON.stringify(response.headers));
    let body = '';
    response.on('data', (c) => (body += c));
    response.on('end', () => {
      console.error('[client] response body:\n' + body);
      console.error('[client] SUCCESS');
      process.exit(0);
    });
  });

  req.on('error', (err) => {
    console.error('[client] request error:', err.message);
    process.exit(1);
  });

  req.end();
});

setTimeout(() => {
  console.error('[client] TIMEOUT - no response in 8s');
  process.exit(2);
}, 8000);
