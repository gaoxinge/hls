import { describe, it, expect } from "vitest";
import { attributes, parsePlaylist, ivBytes } from "../../src/hls/parser";
import { decryptSegment, checkTs, parallel } from "../../src/download/engine";
import { addCandidate, isHls } from "../../src/detection/candidates";
const base = "https://test.invalid/nested/list.m3u8?token=x";
const media = (body: string) =>
  `#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:10\n${body}\n#EXT-X-ENDLIST`;
describe("HLS parsing", () => {
  it("parses quoted commas and equals", () => {
    expect(attributes('METHOD=AES-128,URI="key?a=b,c",IV=0x1').URI).toBe(
      "key?a=b,c",
    );
  });
  it("rejects duplicate/malformed attributes", () => {
    expect(() => attributes("A=1,A=2")).toThrow();
    expect(() => attributes('URI="bad')).toThrow();
  });
  it("handles CRLF and extensionless segment URIs without copying parent tokens", () => {
    const p = parsePlaylist(
      media("#EXTINF:4,\na?token=y\n#EXTINF:5,\n../b").replaceAll("\n", "\r\n"),
      base,
    );
    expect(p.segments.map((s) => s.url)).toEqual([
      "https://test.invalid/nested/a?token=y",
      "https://test.invalid/b",
    ]);
  });
  it("keeps variants separate and detects external audio even if declared later", () => {
    const p = parsePlaylist(
      '#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=2000,RESOLUTION=1280x720,AUDIO="a"\nhigh.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=1000\nlow.m3u8\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="audio",URI="audio.m3u8"',
      base,
    );
    expect(p.variants.map((v) => v.supported)).toEqual([false, true]);
    expect(p.segments).toHaveLength(0);
  });
  it("binds changing encryption to segments and clears METHOD=NONE", () => {
    const p = parsePlaylist(
      media(
        '#EXT-X-MEDIA-SEQUENCE:9007199254740993\n#EXT-X-KEY:METHOD=AES-128,URI="key"\n#EXTINF:1,\na\n#EXT-X-KEY:METHOD=AES-128,URI="key",IV=0x2\n#EXTINF:1,\nb\n#EXT-X-KEY:METHOD=NONE\n#EXTINF:1,\nc',
      ),
      base,
    );
    expect(p.segments[0].sequence).toBe("9007199254740993");
    expect(p.segments[0].key?.id).not.toBe(p.segments[1].key?.id);
    expect(p.segments[2].key).toBeUndefined();
    expect(ivBytes(p.segments[1]).at(-1)).toBe(2);
    expect([...ivBytes(p.segments[0])].slice(-7)).toEqual([
      32, 0, 0, 0, 0, 0, 1,
    ]);
  });
  it.each([
    "EXT-X-BYTERANGE:100",
    "EXT-X-DISCONTINUITY",
    'EXT-X-PART:DURATION=1,URI="a"',
    "EXT-X-GAP",
  ])("rejects unsupported %s", (tag) => {
    expect(() =>
      parsePlaylist(media(`#${tag}\n#EXTINF:1,\na`), base),
    ).toThrow();
  });
  it("rejects live, empty, malformed, unsafe and unsupported encryption", () => {
    for (const text of [
      "#EXTM3U\n#EXTINF:1,\na",
      media(""),
      media("#EXTINF:1,"),
      media("#EXTINF:1,\ndata:x"),
      media('#EXT-X-KEY:METHOD=SAMPLE-AES,URI="k"\n#EXTINF:1,\na'),
    ])
      expect(() => parsePlaylist(text, base)).toThrow();
  });
});
describe("crypto and scheduler", () => {
  it("decrypts independent segments with PKCS7 removed exactly once", async () => {
    const raw = new Uint8Array(16).fill(3);
    const key = await crypto.subtle.importKey("raw", raw, "AES-CBC", false, [
      "encrypt",
    ]);
    for (const sequence of ["1", "2"]) {
      const s = { index: 0, sequence, url: base, duration: 1 };
      const plain = new Uint8Array(188).fill(16);
      plain[0] = 0x47;
      const ciphertext = new Uint8Array(
        await crypto.subtle.encrypt(
          { name: "AES-CBC", iv: ivBytes(s) },
          key,
          plain,
        ),
      );
      expect(await decryptSegment(ciphertext, raw, s)).toEqual(plain);
    }
  });
  it("rejects non TS bytes", () => {
    expect(() => checkTs(new Uint8Array(188))).toThrow();
  });
  it("limits concurrency and aborts on failure", async () => {
    let running = 0,
      peak = 0;
    const controller = new AbortController();
    await expect(
      parallel(
        [0, 1, 2, 3, 4, 5],
        async (i) => {
          running++;
          peak = Math.max(peak, running);
          await new Promise((r) => setTimeout(r, 10));
          running--;
          if (i === 1) throw new Error("failure");
        },
        controller,
        2,
      ),
    ).rejects.toThrow("failure");
    expect(peak).toBe(2);
    expect(controller.signal.aborted).toBe(true);
  });
});
describe("discovery", () => {
  it("detects query and MIME, rejects unrelated media", () => {
    expect(isHls(base)).toBe(true);
    expect(
      isHls("https://a/x", "Application/Vnd.Apple.MpegURL; charset=utf-8"),
    ).toBe(true);
    expect(isHls("https://a/index.html")).toBe(false);
  });
  it("deduplicates full URLs, isolates navigation and expires stale results", () => {
    const c = {
      id: "1",
      tabId: 2,
      epoch: "a",
      url: base,
      status: 200,
      firstSeen: 0,
      lastSeen: 100,
    };
    const one = addCandidate([], c, 100);
    expect(addCandidate(one, { ...c, id: "2" }, 100)).toHaveLength(1);
    expect(
      addCandidate(one, { ...c, id: "2", url: base + "2" }, 100),
    ).toHaveLength(2);
    expect(addCandidate(one, { ...c, id: "2", epoch: "b" }, 100)[0].id).toBe(
      "2",
    );
  });
});
