//! Connection-pool routing (mirror of `typescript/client/test/pool.test.ts`):
//! reuse a connection while it has free stream slots, open a new one when all are
//! saturated (Go's default `StrictMaxConcurrentStreams = false`). Driven with fake
//! connections — their `request` returns `Err` (a real `Response` can't be
//! synthesized), so the tests assert on routing counters, not response values.

use std::cell::RefCell;
use std::rc::Rc;

use futures::executor::LocalPool;
use futures::future::LocalBoxFuture;
use futures::FutureExt;

use h2ts_client::pool::{H2Pool, PoolConnection};
use h2ts_client::{ErrorCode, H2Error, RequestInit, Response};

struct FakeConn {
    max: usize,
    active: RefCell<usize>,
    closed: RefCell<bool>,
    calls: RefCell<usize>,
}

impl FakeConn {
    fn new(max: usize) -> Rc<Self> {
        Rc::new(Self {
            max,
            active: RefCell::new(0),
            closed: RefCell::new(false),
            calls: RefCell::new(0),
        })
    }
    fn release(&self) {
        *self.active.borrow_mut() -= 1;
    }
    fn force_close(&self) {
        *self.closed.borrow_mut() = true;
    }
    fn calls(&self) -> usize {
        *self.calls.borrow()
    }
    fn active(&self) -> usize {
        *self.active.borrow()
    }
}

impl PoolConnection for FakeConn {
    fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }
    fn can_open_stream(&self) -> bool {
        !*self.closed.borrow() && *self.active.borrow() < self.max
    }
    fn request(&self, _init: RequestInit) -> LocalBoxFuture<'static, Result<Response, H2Error>> {
        *self.calls.borrow_mut() += 1;
        *self.active.borrow_mut() += 1;
        async { Err(H2Error::new(ErrorCode::InternalError, "fake")) }.boxed_local()
    }
    fn close(&self) {
        self.force_close();
    }
}

/// A pool whose factory hands out `conns` in order, counting how many it opened.
fn pool_over(conns: Vec<Rc<FakeConn>>, max_connections: usize) -> (H2Pool, Rc<RefCell<usize>>) {
    let created = Rc::new(RefCell::new(0usize));
    let factory = {
        let conns = conns.clone();
        let created = created.clone();
        move || {
            let i = *created.borrow();
            *created.borrow_mut() += 1;
            let conn = conns[i].clone() as Rc<dyn PoolConnection>;
            async move { Ok(conn) }.boxed_local()
        }
    };
    (H2Pool::new(factory, max_connections), created)
}

fn req(path: &str) -> RequestInit {
    RequestInit {
        path: Some(path.into()),
        ..Default::default()
    }
}

#[test]
fn reuses_one_connection_while_it_has_free_slots() {
    let conns = vec![FakeConn::new(5), FakeConn::new(5)];
    let (pool, created) = pool_over(conns.clone(), usize::MAX);

    let mut lp = LocalPool::new();
    lp.run_until(async {
        let _ = pool.request(req("/a")).await;
        let _ = pool.request(req("/b")).await;
        let _ = pool.request(req("/c")).await;
    });

    assert_eq!(*created.borrow(), 1, "all three multiplexed on one connection");
    assert_eq!(conns[0].active(), 3);
    assert_eq!(pool.connections(), 1);
}

#[test]
fn opens_a_new_connection_when_saturated() {
    let conns = vec![FakeConn::new(1), FakeConn::new(1), FakeConn::new(1)];
    let (pool, created) = pool_over(conns.clone(), usize::MAX);

    let mut lp = LocalPool::new();
    lp.run_until(async {
        let _ = pool.request(req("/a")).await; // conn 0 (now full)
        let _ = pool.request(req("/b")).await; // conn 0 full -> open conn 1
        let _ = pool.request(req("/c")).await; // conn 1 full -> open conn 2
    });

    assert_eq!(*created.borrow(), 3);
    assert_eq!(pool.connections(), 3);
}

#[test]
fn prefers_a_freed_slot_over_opening_a_new_connection() {
    let conns = vec![FakeConn::new(1), FakeConn::new(1)];
    let (pool, created) = pool_over(conns.clone(), usize::MAX);

    let mut lp = LocalPool::new();
    lp.run_until(async {
        let _ = pool.request(req("/a")).await; // conn 0 (full)
        conns[0].release(); // slot frees
        let _ = pool.request(req("/b")).await; // reuse conn 0
    });

    assert_eq!(*created.borrow(), 1);
    assert_eq!(conns[0].calls(), 2);
}

#[test]
fn stops_opening_at_max_connections_and_parks() {
    let conns = vec![FakeConn::new(1), FakeConn::new(1)];
    let (pool, created) = pool_over(conns.clone(), 1); // cap: one connection

    let mut lp = LocalPool::new();
    lp.run_until(async {
        let _ = pool.request(req("/a")).await; // conn 0 (full)
        let _ = pool.request(req("/b")).await; // at the cap -> park on conn 0
    });

    assert_eq!(*created.borrow(), 1, "no new connection past the cap");
    assert_eq!(conns[0].calls(), 2, "both requests routed to conn 0");
}

#[test]
fn skips_a_closed_connection_and_opens_a_fresh_one() {
    let conns = vec![FakeConn::new(5), FakeConn::new(5)];
    let (pool, created) = pool_over(conns.clone(), usize::MAX);

    let mut lp = LocalPool::new();
    lp.run_until(async {
        let _ = pool.request(req("/a")).await; // conn 0
        conns[0].force_close(); // connection dies
        let _ = pool.request(req("/b")).await; // conn 0 gone -> open conn 1
    });

    assert_eq!(*created.borrow(), 2);
    assert_eq!(pool.connections(), 1);
}
