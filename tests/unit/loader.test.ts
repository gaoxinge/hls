import { it, expect, vi, afterEach } from "vitest";
import { loadPlaylist } from "../../src/hls/loader";
afterEach(() => vi.unstubAllGlobals());
it("resolves child URIs relative to final redirect URL and selects one variant", async () => {
  const calls: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      calls.push(url);
      const body =
        calls.length === 1
          ? "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000,RESOLUTION=640x360\nlow/list.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=2000,RESOLUTION=1280x720\nhigh/list.m3u8"
          : "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\nsegment?token=a=b\n#EXT-X-ENDLIST";
      const r = new Response(body);
      Object.defineProperty(r, "url", {
        value: calls.length === 1 ? "https://cdn.test/final/master.m3u8" : url,
      });
      return r;
    }),
  );
  const p = await loadPlaylist(
    "https://origin.test/master.m3u8",
    new AbortController().signal,
    "best",
  );
  expect(calls).toEqual([
    "https://origin.test/master.m3u8",
    "https://cdn.test/final/high/list.m3u8",
  ]);
  expect(p.segments[0].url).toBe(
    "https://cdn.test/final/high/segment?token=a=b",
  );
});
it("rejects cyclic master playlists", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\nlist.m3u8"),
    ),
  );
  await expect(
    loadPlaylist(
      "https://test.invalid/list.m3u8",
      new AbortController().signal,
      "best",
    ),
  ).rejects.toThrow("循环");
});
