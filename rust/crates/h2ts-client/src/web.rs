//! Browser `WebSocket` transport + `connect_websocket` — port of
//! `transport/websocket.ts` and the WebSocket half of `client.ts`. **wasm32 only**
//! (behind the default `web` feature); pulls in `web-sys`, still no tokio/hyper.

use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::channel::{mpsc, oneshot};
use futures::Sink;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{BinaryType, MessageEvent, WebSocket};

use crate::connection::{connect, ConnectOptions, H2Connection};
use crate::transport::{ByteSink, ByteStream, Transport, TransportError};

/// Open a WebSocket to `url`, wait for it to connect, and start an HTTP/2 client
/// tunneled over it (prior knowledge). The default `h2ts` subprotocol should be
/// offered first via `protocols` (the gateway echoes it).
pub async fn connect_websocket(
    url: &str,
    protocols: &[&str],
    options: ConnectOptions,
) -> Result<H2Connection, JsValue> {
    let ws = open(url, protocols).await?;
    let transport = websocket_transport(&ws);
    let (conn, driver) = connect(transport, options);
    wasm_bindgen_futures::spawn_local(driver);
    Ok(conn)
}

/// Construct a WebSocket and resolve once it's OPEN (or reject on failure).
async fn open(url: &str, protocols: &[&str]) -> Result<WebSocket, JsValue> {
    let ws = if protocols.is_empty() {
        WebSocket::new(url)?
    } else {
        let arr = js_sys::Array::new();
        for p in protocols {
            arr.push(&JsValue::from_str(p));
        }
        WebSocket::new_with_str_sequence(url, &JsValue::from(arr))?
    };
    ws.set_binary_type(BinaryType::Arraybuffer);

    let (tx, rx) = oneshot::channel::<Result<(), JsValue>>();
    let tx = Rc::new(RefCell::new(Some(tx)));

    let onopen = {
        let tx = tx.clone();
        Closure::<dyn FnMut()>::wrap(Box::new(move || {
            if let Some(tx) = tx.borrow_mut().take() {
                let _ = tx.send(Ok(()));
            }
        }))
    };
    let onerror = {
        let tx = tx.clone();
        Closure::<dyn FnMut()>::wrap(Box::new(move || {
            if let Some(tx) = tx.borrow_mut().take() {
                let _ = tx.send(Err(JsValue::from_str("WebSocket connection failed")));
            }
        }))
    };
    ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
    ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));

    let result = rx
        .await
        .map_err(|_| JsValue::from_str("WebSocket open canceled"))?;

    ws.set_onopen(None);
    ws.set_onerror(None);
    drop(onopen);
    drop(onerror);
    result.map(|()| ws)
}

/// Adapt an OPEN WebSocket to a byte [`Transport`]. Sets `binaryType=arraybuffer`
/// so messages arrive as binary.
pub fn websocket_transport(ws: &WebSocket) -> Transport {
    ws.set_binary_type(BinaryType::Arraybuffer);

    let (in_tx, in_rx) = mpsc::unbounded::<Vec<u8>>();
    // One sender, shared: `onmessage` pushes through it; `onclose` drops it to EOF
    // the inbound stream.
    let in_tx = Rc::new(RefCell::new(Some(in_tx)));

    let onmessage = {
        let in_tx = in_tx.clone();
        Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |ev: MessageEvent| {
            if let Ok(buf) = ev.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes = js_sys::Uint8Array::new(&buf).to_vec();
                if !bytes.is_empty() {
                    if let Some(tx) = in_tx.borrow().as_ref() {
                        let _ = tx.unbounded_send(bytes);
                    }
                }
            }
        }))
    };
    let onclose = {
        let in_tx = in_tx.clone();
        Closure::<dyn FnMut()>::wrap(Box::new(move || {
            in_tx.borrow_mut().take(); // drop the sender -> inbound stream ends
        }))
    };
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
    // The WebSocket keeps these callbacks; leak them for its lifetime.
    onmessage.forget();
    onclose.forget();

    let reader: ByteStream = Box::pin(in_rx);
    let writer: ByteSink = Box::pin(WsSink { ws: ws.clone() });
    Transport::new(reader, writer)
}

/// A [`Sink`] that writes outbound chunks straight to the WebSocket (synchronous
/// `send`, so it is always ready).
struct WsSink {
    ws: WebSocket,
}

impl Sink<Vec<u8>> for WsSink {
    type Error = TransportError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), TransportError>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), TransportError> {
        self.ws
            .send_with_u8_array(&item)
            .map_err(|e| TransportError(format!("{e:?}")))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), TransportError>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), TransportError>> {
        let _ = self.ws.close();
        Poll::Ready(Ok(()))
    }
}
