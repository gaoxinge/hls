export async function parallel<T>(
  items: T[],
  work: (item: T) => Promise<void>,
  controller: AbortController,
  concurrency = 4,
) {
  let next = 0;
  let failure: unknown;
  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, async () => {
      while (!controller.signal.aborted && next < items.length) {
        const item = items[next++];
        try {
          await work(item);
        } catch (error) {
          if (!failure) failure = error;
          controller.abort(error);
        }
      }
    }),
  );
  if (failure) throw failure;
  controller.signal.throwIfAborted();
}
