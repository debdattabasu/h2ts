//! HTTP/2 send-side flow-control window (RFC 7540 §6.9) — port of `flow.ts`.
//!
//! One direction of one window (connection- or stream-level, send side). The
//! window may go negative when the peer lowers SETTINGS_INITIAL_WINDOW_SIZE after
//! we were granted capacity — that is legal and must not underflow a send.
//!
//! The TS version returns a `Promise` from `waitPositive`; here a caller polls
//! [`is_ready`](SendWindow::is_ready) and parks its [`Waker`] via
//! [`register_waker`](SendWindow::register_waker) (see `connection`'s body pump).

use std::task::Waker;

pub struct SendWindow {
    available: i64,
    wakers: Vec<Waker>,
    closed: bool,
}

impl SendWindow {
    pub fn new(initial: i64) -> Self {
        Self {
            available: initial,
            wakers: Vec::new(),
            closed: false,
        }
    }

    pub fn value(&self) -> i64 {
        self.available
    }

    /// Grant more capacity (a WINDOW_UPDATE arrived).
    pub fn update(&mut self, increment: i64) {
        self.available += increment;
        if self.available > 0 {
            self.wake();
        }
    }

    /// Adjust by a SETTINGS_INITIAL_WINDOW_SIZE change (delta may be negative).
    pub fn adjust(&mut self, delta: i64) {
        self.available += delta;
        if self.available > 0 {
            self.wake();
        }
    }

    /// Consume capacity that a positive check already confirmed is available.
    pub fn consume(&mut self, n: i64) {
        self.available -= n;
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Ready to send (positive capacity) or torn down.
    pub fn is_ready(&self) -> bool {
        self.available > 0 || self.closed
    }

    /// Park a waker to be notified when capacity becomes positive (or on close).
    pub fn register_waker(&mut self, waker: &Waker) {
        self.wakers.push(waker.clone());
    }

    /// Abort all waiters (connection/stream tearing down).
    pub fn close(&mut self) {
        self.closed = true;
        self.wake();
    }

    fn wake(&mut self) {
        for w in self.wakers.drain(..) {
            w.wake();
        }
    }
}
