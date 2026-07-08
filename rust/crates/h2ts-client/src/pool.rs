//! A pool of HTTP/2-over-WebSocket connections — Go-style multi-connection
//! parallelism (`golang.org/x/net/http2` with `StrictMaxConcurrentStreams =
//! false`). Each request is routed to a connection that still has a free stream
//! slot (per the peer's SETTINGS_MAX_CONCURRENT_STREAMS); when all are saturated,
//! a new connection is opened.
//!
//! The routing logic here is transport-agnostic and host-testable; the wasm
//! `connect_pool` (in `web.rs`) wires it to real `connect_websocket` connections.

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use futures::channel::oneshot;
use futures::future::LocalBoxFuture;
use futures::FutureExt;

use crate::connection::{H2Connection, RequestInit, Response};
use crate::errors::H2Error;

/// A connection a [`H2Pool`] can route over. Implemented for [`H2Connection`];
/// tests supply their own.
pub trait PoolConnection {
    /// Whether the connection has been torn down.
    fn is_closed(&self) -> bool;
    /// Whether a new request would stay within the peer's advertised
    /// SETTINGS_MAX_CONCURRENT_STREAMS.
    fn can_open_stream(&self) -> bool;
    /// Issue a request on this connection.
    fn request(&self, init: RequestInit) -> LocalBoxFuture<'static, Result<Response, H2Error>>;
    /// Gracefully close the connection.
    fn close(&self);
}

impl PoolConnection for H2Connection {
    fn is_closed(&self) -> bool {
        H2Connection::is_closed(self)
    }
    fn can_open_stream(&self) -> bool {
        H2Connection::can_open_stream(self)
    }
    fn request(&self, init: RequestInit) -> LocalBoxFuture<'static, Result<Response, H2Error>> {
        let conn = self.clone();
        async move { conn.request(init).await }.boxed_local()
    }
    fn close(&self) {
        H2Connection::close(self)
    }
}

type Factory = Rc<dyn Fn() -> LocalBoxFuture<'static, Result<Rc<dyn PoolConnection>, H2Error>>>;

struct PoolState {
    conns: Vec<Rc<dyn PoolConnection>>,
    /// One connection is opened at a time; concurrent requests that need a new
    /// connection wait on `open_waiters` rather than each opening their own.
    opening: bool,
    open_waiters: Vec<oneshot::Sender<()>>,
}

/// A pool of HTTP/2 connections. Cheap to clone (shares one `Rc` state).
#[derive(Clone)]
pub struct H2Pool {
    shared: Rc<RefCell<PoolState>>,
    factory: Factory,
    max_connections: usize,
}

impl H2Pool {
    /// Build a pool from a connection `factory` (each call opens a fresh
    /// connection). `max_connections` caps how many connections are opened
    /// (`usize::MAX` for unbounded); past the cap, requests park on an existing
    /// connection (which queues them internally until a stream slot frees).
    pub fn new<F, Fut>(factory: F, max_connections: usize) -> Self
    where
        F: Fn() -> Fut + 'static,
        Fut: Future<Output = Result<Rc<dyn PoolConnection>, H2Error>> + 'static,
    {
        Self {
            shared: Rc::new(RefCell::new(PoolState {
                conns: Vec::new(),
                opening: false,
                open_waiters: Vec::new(),
            })),
            factory: Rc::new(move || factory().boxed_local()),
            max_connections,
        }
    }

    /// Route a request to a connection with a free stream slot, opening a new
    /// connection if all existing ones are saturated.
    pub async fn request(&self, init: RequestInit) -> Result<Response, H2Error> {
        // RequestInit isn't Clone (a body may be a stream), so issue it exactly once.
        let mut init = Some(init);
        loop {
            let chosen: Option<Rc<dyn PoolConnection>> = {
                let mut st = self.shared.borrow_mut();
                st.conns.retain(|c| !c.is_closed());
                if let Some(c) = st.conns.iter().find(|c| c.can_open_stream()) {
                    Some(c.clone())
                } else if !st.conns.is_empty() && st.conns.len() >= self.max_connections {
                    // At the connection cap: park on an existing connection.
                    st.conns.first().cloned()
                } else {
                    None
                }
            };
            if let Some(conn) = chosen {
                return conn.request(init.take().expect("request issued once")).await;
            }

            // All connections are full (and under the cap): open a new one.
            self.open_one().await?;

            // A brand-new connection already at its limit (a server advertising a
            // tiny limit): park on it rather than opening unboundedly many.
            let fresh = self.shared.borrow().conns.last().cloned();
            if let Some(conn) = fresh {
                if !conn.is_closed() && !conn.can_open_stream() {
                    return conn.request(init.take().expect("request issued once")).await;
                }
            }
        }
    }

    /// Open exactly one connection; concurrent callers await the in-flight open.
    async fn open_one(&self) -> Result<(), H2Error> {
        let wait = {
            let mut st = self.shared.borrow_mut();
            if st.opening {
                let (tx, rx) = oneshot::channel();
                st.open_waiters.push(tx);
                Some(rx)
            } else {
                st.opening = true;
                None
            }
        };
        if let Some(rx) = wait {
            let _ = rx.await; // another request is opening; re-check once it lands
            return Ok(());
        }

        // We own the open.
        let result = (self.factory)().await;
        let mut st = self.shared.borrow_mut();
        st.opening = false;
        for tx in st.open_waiters.drain(..) {
            let _ = tx.send(());
        }
        let conn = result?;
        st.conns.push(conn);
        Ok(())
    }

    /// The number of live connections currently in the pool.
    pub fn connections(&self) -> usize {
        self.shared
            .borrow()
            .conns
            .iter()
            .filter(|c| !c.is_closed())
            .count()
    }

    /// Gracefully close every connection in the pool.
    pub fn close(&self) {
        for conn in self.shared.borrow_mut().conns.drain(..) {
            conn.close();
        }
    }
}
