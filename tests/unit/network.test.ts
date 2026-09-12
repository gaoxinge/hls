import { it, expect, vi } from "vitest";
import { request } from "../../src/network/http";
it("enforces body size even without content length", async () => {
  const fetcher = vi.fn(
    async () => new Response(new Uint8Array(20)),
  ) as unknown as typeof fetch;
  await expect(
    request(
      "https://test.invalid/a",
      10,
      new AbortController().signal,
      fetcher,
    ),
  ).rejects.toThrow("大小");
  expect(fetcher).toHaveBeenCalledTimes(1);
});
it("does not retry unauthorized resources", async () => {
  const fetcher = vi.fn(
    async () => new Response("", { status: 403 }),
  ) as unknown as typeof fetch;
  await expect(
    request(
      "https://test.invalid/a",
      10,
      new AbortController().signal,
      fetcher,
    ),
  ).rejects.toThrow("刷新");
  expect(fetcher).toHaveBeenCalledTimes(1);
});
it("stops before fetching a cancelled request", async () => {
  const controller = new AbortController();
  controller.abort();
  const fetcher = vi.fn();
  await expect(
    request("https://test.invalid/a", 10, controller.signal, fetcher),
  ).rejects.toThrow();
  expect(fetcher).not.toHaveBeenCalled();
});
it("honors finite retries for transient failures", async () => {
  vi.useFakeTimers();
  try {
    const fetcher = vi.fn(
      async () => new Response("", { status: 503 }),
    ) as unknown as typeof fetch;
    const result = request(
      "https://test.invalid/a",
      10,
      new AbortController().signal,
      fetcher,
    ).catch((e) => e);
    await vi.runAllTimersAsync();
    expect((await result).status).toBe(503);
    expect(fetcher).toHaveBeenCalledTimes(4);
  } finally {
    vi.useRealTimers();
  }
});
it("cancels retry backoff without issuing another request", async () => {
  vi.useFakeTimers();
  try {
    const controller = new AbortController();
    const fetcher = vi.fn(
      async () =>
        new Response("", { status: 429, headers: { "Retry-After": "20" } }),
    ) as unknown as typeof fetch;
    const result = request(
      "https://test.invalid/a",
      10,
      controller.signal,
      fetcher,
    ).catch((e) => e);
    await vi.advanceTimersByTimeAsync(1);
    controller.abort();
    await vi.runAllTimersAsync();
    expect(await result).toBeInstanceOf(Error);
    expect(fetcher).toHaveBeenCalledTimes(1);
  } finally {
    vi.useRealTimers();
  }
});
