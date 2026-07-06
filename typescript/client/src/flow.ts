// HTTP/2 flow-control window (RFC 7540 §6.9). Models one direction of one
// window (connection-level or stream-level, send side).
//
// The window can go negative when the peer lowers SETTINGS_INITIAL_WINDOW_SIZE
// after we've already been granted capacity — that's legal and must not
// underflow into a send.

export class SendWindow {
  private available: number;
  private waiters: Array<() => void> = [];
  private closed = false;

  constructor(initial: number) {
    this.available = initial;
  }

  get value(): number {
    return this.available;
  }

  /** Grant more capacity (a WINDOW_UPDATE arrived). */
  update(increment: number): void {
    this.available += increment;
    if (this.available > 0) this.wake();
  }

  /**
   * Adjust by a SETTINGS_INITIAL_WINDOW_SIZE change (delta may be negative).
   * Applies to stream windows only.
   */
  adjust(delta: number): void {
    this.available += delta;
    if (this.available > 0) this.wake();
  }

  /** Consume capacity that a positive check already confirmed is available. */
  consume(n: number): void {
    this.available -= n;
  }

  /** Resolve once capacity is positive (or the window is closed). */
  waitPositive(): Promise<void> {
    if (this.available > 0 || this.closed) return Promise.resolve();
    return new Promise<void>((resolve) => this.waiters.push(resolve));
  }

  get isClosed(): boolean {
    return this.closed;
  }

  /** Abort all waiters (connection/stream tearing down). */
  close(): void {
    this.closed = true;
    this.wake();
  }

  private wake(): void {
    if (this.waiters.length === 0) return;
    const pending = this.waiters;
    this.waiters = [];
    for (const resolve of pending) resolve();
  }
}
