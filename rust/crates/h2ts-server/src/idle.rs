//! Idle-connection reaping for [`serve_h2_with_config`](crate::serve_h2_with_config).
//!
//! hyper's HTTP/2 server has no built-in idle timeout (unlike Go's `net/http2`),
//! so we drive one ourselves. [`TrackedService`] wraps the caller's service and
//! keeps an exact count of **open HTTP/2 streams**: a [`StreamGuard`] is taken
//! when a request arrives and released when its response body is fully sent (or
//! the stream is dropped). [`wait_idle`] watches that count and resolves once it
//! has sat at zero for the configured timeout — at which point the caller runs
//! hyper's `graceful_shutdown` (GOAWAY, admit no new streams, close).
//!
//! Because the signal is *streams* — not bytes, not pings — a tunnel holding a
//! live-but-quiet stream (SSE, a slow upload) is never reaped, and neither
//! WebSocket nor HTTP/2 keepalive pings reset the timer. Only real streams do.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::service::Service;
use hyper::{Request, Response};
use pin_project_lite::pin_project;
use tokio::sync::watch;

/// Shared open-stream count. The watch value is the number of streams currently
/// open; every change is published so [`wait_idle`] can react to it.
pub(crate) struct StreamCounter {
    open: watch::Sender<usize>,
}

impl StreamCounter {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            open: watch::channel(0).0,
        })
    }

    /// Register a newly-opened stream; the returned guard releases it on drop.
    fn enter(self: &Arc<Self>) -> StreamGuard {
        self.open.send_modify(|n| *n += 1);
        StreamGuard(self.clone())
    }

    /// A receiver for the current open-stream count.
    pub(crate) fn watch(&self) -> watch::Receiver<usize> {
        self.open.subscribe()
    }
}

/// Held for the lifetime of one open stream (request arrival → response body
/// finished). Dropping it decrements the open-stream count.
pub(crate) struct StreamGuard(Arc<StreamCounter>);

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.0.open.send_modify(|n| *n = n.saturating_sub(1));
    }
}

/// Wraps a service to count open streams (see the module docs).
pub(crate) struct TrackedService<Svc> {
    inner: Svc,
    counter: Arc<StreamCounter>,
}

impl<Svc> TrackedService<Svc> {
    pub(crate) fn new(inner: Svc, counter: Arc<StreamCounter>) -> Self {
        Self { inner, counter }
    }
}

impl<Svc, B> Service<Request<Incoming>> for TrackedService<Svc>
where
    Svc: Service<Request<Incoming>, Response = Response<B>>,
    Svc::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = Response<GuardedBody<B>>;
    type Error = Svc::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        // The stream is open the instant the request arrives; the guard then
        // rides the response body and releases the count when the body is done.
        let guard = self.counter.enter();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let resp = fut.await?;
            Ok(resp.map(|inner| GuardedBody {
                inner,
                _guard: guard,
            }))
        })
    }
}

pin_project! {
    /// A response body that releases its [`StreamGuard`] when it finishes (is
    /// dropped), marking the stream closed for idle accounting. Otherwise a
    /// transparent pass-through to the inner body.
    pub(crate) struct GuardedBody<B> {
        #[pin]
        inner: B,
        _guard: StreamGuard,
    }
}

impl<B: Body> Body for GuardedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.project().inner.poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Resolve once the open-stream count has been zero continuously for `idle`.
///
/// Any stream opening (activity) restarts the wait. Returns early if the counter
/// is gone (the connection ended) — harmless, since the caller only reaches this
/// branch when the connection future is still pending.
pub(crate) async fn wait_idle(mut open: watch::Receiver<usize>, idle: Duration) {
    loop {
        // Mark the current value seen so `changed()` fires only on the next change.
        let count = *open.borrow_and_update();
        if count != 0 {
            // Busy: wait until a stream closes (the count changes).
            if open.changed().await.is_err() {
                return;
            }
            continue;
        }
        // Idle right now: reap iff it stays zero for the whole window; any change
        // (a stream opened) wins the race and restarts the wait.
        tokio::select! {
            _ = tokio::time::sleep(idle) => return,
            r = open.changed() => {
                if r.is_err() {
                    return;
                }
            }
        }
    }
}
