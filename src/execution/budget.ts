/** Serialize full-buffer AES work. Plain resources stream with four requests. */
export class BufferBudget {
  private tail = Promise.resolve();
  async run<T>(signal: AbortSignal, work: () => Promise<T>): Promise<T> {
    const previous = this.tail;
    let release!: () => void;
    this.tail = new Promise<void>((r) => {
      release = r;
    });
    try {
      await previous;
      signal.throwIfAborted();
      return await work();
    } finally {
      release();
    }
  }
}
