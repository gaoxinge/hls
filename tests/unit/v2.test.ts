import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { parse } from "../../src/adapters/parser/m3u8";
import { assertExecutable } from "../../src/protocol/capabilities";
import { request } from "../../src/network/http";
import { createResolver } from "../../src/planning/resolver";
import { Fmp4Inspector } from "../../src/media/fmp4";
import { inspectTs } from "../../src/media/ts";
const base = "https://example.test/list.m3u8?token=abc";
const media = (s: string) =>
  `#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:5\n${s}\n#EXT-X-ENDLIST`;
const fixture = (path: string) =>
  new Uint8Array(readFileSync(`tests/fixtures/media/${path}`));
describe("v2 protocol contracts", () => {
  it("preserves exact range offsets beyond Number precision", () => {
    const p = parse(
      media(
        "#EXTINF:1,\n#EXT-X-BYTERANGE:188@9007199254740993\nfile.ts\n#EXTINF:1,\n#EXT-X-BYTERANGE:188\nfile.ts",
      ),
      base,
    );
    expect(p.segments[1].range?.offset).toBe("9007199254741181");
  });
  it("rejects an implicit offset after changing URI", () =>
    expect(() =>
      parse(
        media(
          "#EXTINF:1,\n#EXT-X-BYTERANGE:188@0\na\n#EXTINF:1,\n#EXT-X-BYTERANGE:188\nb",
        ),
        base,
      ),
    ).toThrow("隐式"));
  it("resolves scoped definitions and query variables", () => {
    const p = parse(
      media(
        '#EXT-X-DEFINE:IMPORT="dir"\n#EXT-X-DEFINE:QUERYPARAM="token"\n#EXTINF:1,\n{$dir}/s?token={$token}',
      ),
      base,
      { dir: "video" },
    );
    expect(p.segments[0].url).toBe("https://example.test/video/s?token=abc");
  });
  it("rejects undefined and cyclic variables", () => {
    expect(() => parse(media("#EXTINF:1,\n{$unknown}"), base)).toThrow(
      "UNDEFINED_VARIABLE",
    );
    expect(() =>
      parse(
        media('#EXT-X-DEFINE:NAME="a",VALUE="{$a}"\n#EXTINF:1,\n{$a}'),
        base,
      ),
    ).toThrow("VARIABLE_CYCLE");
  });
  it("captures MAP encryption before later key changes", () => {
    const p = parse(
      media(
        '#EXT-X-KEY:METHOD=AES-128,URI="key",IV=0x1\n#EXT-X-MAP:URI="init"\n#EXT-X-KEY:METHOD=NONE\n#EXTINF:1,\ns',
      ),
      base,
    );
    expect(p.segments[0].key).toBeUndefined();
    expect(p.segments[0].init?.key?.iv).toBe("0x1");
  });
  it("rejects encrypted MAP without IV", () =>
    expect(() =>
      parse(
        media(
          '#EXT-X-KEY:METHOD=AES-128,URI="key"\n#EXT-X-MAP:URI="init"\n#EXTINF:1,\ns',
        ),
        base,
      ),
    ).toThrow("INVALID_MAP_IV"));
  it("keeps SESSION-KEY from becoming a media encryption context", () => {
    const p = parse(
      '#EXTM3U\n#EXT-X-SESSION-KEY:METHOD=AES-128,URI="key"\n#EXT-X-STREAM-INF:BANDWIDTH=1\nchild',
      base,
    );
    expect(p.sessionKeys).toHaveLength(1);
  });
  it("separates source errors and unsupported execution", () => {
    const p = parse(media("#EXT-X-DISCONTINUITY\n#EXTINF:1,\ns"), base);
    expect(() => assertExecutable(p)).toThrow("第 4 行");
    expect(() =>
      parse(media('#EXT-X-KEY:METHOD=NONE,URI="key"\n#EXTINF:1,\ns'), base),
    ).toThrow("INVALID_KEY");
  });
  it("validates mandatory duration, version, duplicate tags and uint64", () => {
    for (const text of [
      "#EXTM3U\n#EXTINF:1,\ns\n#EXT-X-ENDLIST",
      media("#EXT-X-VERSION:6\n#EXTINF:1,\ns"),
      media("#EXT-X-MEDIA-SEQUENCE:18446744073709551616\n#EXTINF:1,\ns"),
      media("#EXTINF:5.6,\ns"),
    ])
      expect(() => parse(text, base)).toThrow();
  });
  it("falls back from an unsupported best variant; manual does not", async () => {
    const calls: string[] = [];
    const resolver = createResolver(async (url) => {
      calls.push(url);
      return {
        url,
        bytes: new TextEncoder().encode(
          url === base
            ? "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=2\nhigh\n#EXT-X-STREAM-INF:BANDWIDTH=1\nlow"
            : media(
                (url.endsWith("high") ? "#EXT-X-GAP\n" : "") + "#EXTINF:1,\ns",
              ),
        ),
      };
    }, parse);
    expect(
      (await resolver(base, new AbortController().signal, "best")).url,
    ).toContain("low");
    await expect(
      resolver(
        base,
        new AbortController().signal,
        "best",
        "https://example.test/high",
      ),
    ).rejects.toThrow("GAP");
    expect(calls).toHaveLength(5);
  });
});
describe("range transport", () => {
  it.each([200, 206, 416])("handles status %s strictly", async (status) => {
    const fetcher = async () =>
      new Response(new Uint8Array(3), {
        status,
        headers: status === 206 ? { "content-range": "bytes 5-7/10" } : {},
      });
    const promise = request(
      base,
      10,
      new AbortController().signal,
      fetcher,
      1000,
      { offset: "5", length: "3" },
    );
    if (status === 206) expect((await promise).bytes).toHaveLength(3);
    else await expect(promise).rejects.toThrow();
  });
  it("rejects short or mismatched ranges", async () => {
    for (const [header, size] of [
      ["bytes 4-6/10", 3],
      ["bytes 5-7/10", 2],
    ] as const)
      await expect(
        request(
          base,
          10,
          new AbortController().signal,
          async () =>
            new Response(new Uint8Array(size), {
              status: 206,
              headers: { "content-range": header },
            }),
          1000,
          { offset: "5", length: "3" },
        ),
      ).rejects.toThrow();
  });
});
describe("real media", () => {
  it("inspects real TS and rejects sync-only null packets", () => {
    expect(inspectTs(fixture("ts/segment0.ts")).video).toBeDefined();
    const nulls = new Uint8Array(188 * 3).fill(255);
    for (let i = 0; i < 3; i++) nulls.set([0x47, 0x1f, 0xff, 0x10], i * 188);
    expect(() => inspectTs(nulls)).toThrow("PAT/PMT");
  });
  it("validates continuous fragmented MP4 samples", () => {
    const inspect = new Fmp4Inspector(fixture("fmp4/init.mp4"));
    for (let i = 0; i < 4; i++)
      inspect.inspect(fixture(`fmp4/segment${i}.m4s`));
  });
  it("rejects repeated decode times and truncated fragments", () => {
    const inspect = new Fmp4Inspector(fixture("fmp4/init.mp4"));
    const part = fixture("fmp4/segment0.m4s");
    inspect.inspect(part);
    expect(() => inspect.inspect(part)).toThrow("不连续");
    expect(() =>
      new Fmp4Inspector(fixture("fmp4/init.mp4")).inspect(part.slice(0, -1)),
    ).toThrow();
  });
});
it("falls back on container probe failure without replacing an explicit choice", async () => {
  const read = async (url: string) => ({
    url,
    bytes: new TextEncoder().encode(
      url === base
        ? "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=2\nhigh\n#EXT-X-STREAM-INF:BANDWIDTH=1\nlow"
        : media("#EXTINF:1,\ns"),
    ),
  });
  const probe = async (p: ReturnType<typeof parse>) => {
    if (p.url.endsWith("high")) throw new Error("unsupported container");
  };
  const resolve = createResolver(read, parse, probe);
  expect(
    (await resolve(base, new AbortController().signal, "best")).url,
  ).toContain("low");
  await expect(
    resolve(
      base,
      new AbortController().signal,
      "best",
      "https://example.test/high",
    ),
  ).rejects.toThrow("unsupported container");
});
it("retains unknown metadata and bounds variable expansion", () => {
  const p = parse(media("#EXT-X-FUTURE:VALUE=1\n#EXTINF:1,\ns"), base);
  expect(
    p.diagnostics.some((d) => d.code === "UNKNOWN_TAG" && d.line === 4),
  ).toBe(true);
  let definitions = "";
  for (let i = 0; i < 20; i++)
    definitions += `#EXT-X-DEFINE:NAME="v${i}",VALUE="${i ? "{$v" + (i - 1) + "}{$v" + (i - 1) + "}" : "abc"}"\n`;
  expect(() => parse(media(definitions + "#EXTINF:1,\n{$v19}"), base)).toThrow(
    "VARIABLE_BUDGET",
  );
});
it("never rewrites IV-like URI query parameters while adapting short key IVs", () => {
  const p = parse(
    media(
      '#EXT-X-KEY:METHOD=AES-128,URI="key?IV=0x1",IV=0x2\n#EXTINF:1,\ns?IV=0x3',
    ),
    base,
  );
  expect(p.segments[0].url).toBe("https://example.test/s?IV=0x3");
  expect(p.segments[0].key?.url).toBe("https://example.test/key?IV=0x1");
});
