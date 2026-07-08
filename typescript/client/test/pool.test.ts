// The connection pool routes each request to a connection with a free stream
// slot, opening a new connection only when all existing ones are saturated
// (Go's default `StrictMaxConcurrentStreams = false`). Driven with fake
// connections so the routing logic is tested in isolation.
import { describe, expect, it } from "vitest";
import { H2Pool, type PoolConnection } from "../src/client.js";
import type { H2Response } from "../src/types.js";

class FakeConn implements PoolConnection {
  isClosed = false;
  active = 0;
  constructor(readonly max: number) {}
  canOpenStream(): boolean {
    return !this.isClosed && this.active < this.max;
  }
  async request(): Promise<H2Response> {
    this.active++;
    return { status: 200 } as H2Response;
  }
  release(): void {
    this.active--;
  }
  close(): void {
    this.isClosed = true;
  }
}

function factoryOf(conns: FakeConn[]): { factory: () => Promise<PoolConnection>; created: () => number } {
  let i = 0;
  return {
    factory: async () => conns[i++]!,
    created: () => i,
  };
}

describe("H2Pool routing", () => {
  it("reuses one connection while it has free stream slots", async () => {
    const conns = [new FakeConn(5), new FakeConn(5)];
    const { factory, created } = factoryOf(conns);
    const pool = new H2Pool(factory);

    await pool.request({ path: "/a" });
    await pool.request({ path: "/b" });
    await pool.request({ path: "/c" });

    expect(created()).toBe(1); // all three multiplexed on one connection
    expect(conns[0]!.active).toBe(3);
    expect(pool.connections).toBe(1);
  });

  it("opens a new connection when the current one is saturated", async () => {
    const conns = [new FakeConn(1), new FakeConn(1), new FakeConn(1)];
    const { factory, created } = factoryOf(conns);
    const pool = new H2Pool(factory);

    await pool.request({ path: "/a" }); // conn 0 (active 1/1 — now full)
    await pool.request({ path: "/b" }); // conn 0 full → open conn 1
    await pool.request({ path: "/c" }); // conn 1 full → open conn 2

    expect(created()).toBe(3);
    expect(pool.connections).toBe(3);
  });

  it("prefers a freed slot on an existing connection over opening a new one", async () => {
    const conns = [new FakeConn(1), new FakeConn(1)];
    const { factory, created } = factoryOf(conns);
    const pool = new H2Pool(factory);

    await pool.request({ path: "/a" }); // conn 0 (full)
    conns[0]!.release(); // slot frees
    await pool.request({ path: "/b" }); // reuses conn 0, no new connection

    expect(created()).toBe(1);
  });

  it("stops opening connections at maxConnections and parks on an existing one", async () => {
    const conns = [new FakeConn(1), new FakeConn(1)];
    const { factory, created } = factoryOf(conns);
    const pool = new H2Pool(factory, 1); // cap: one connection

    await pool.request({ path: "/a" }); // conn 0 (full)
    await pool.request({ path: "/b" }); // at the cap → parks on conn 0 (no new conn)

    expect(created()).toBe(1);
    expect(conns[0]!.active).toBe(2); // both requests routed to conn 0
  });

  it("skips a closed connection and opens a fresh one", async () => {
    const conns = [new FakeConn(5), new FakeConn(5)];
    const { factory, created } = factoryOf(conns);
    const pool = new H2Pool(factory);

    await pool.request({ path: "/a" }); // conn 0
    conns[0]!.close(); // connection dies
    await pool.request({ path: "/b" }); // conn 0 gone → open conn 1

    expect(created()).toBe(2);
    expect(pool.connections).toBe(1);
  });
});
