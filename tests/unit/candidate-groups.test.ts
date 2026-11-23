import { describe, expect, it, vi } from "vitest";
import { parse } from "../../src/adapters/parser/m3u8";
import { createCandidateInspector } from "../../src/application/candidate-inspection";
import {
  selectVisibleCandidates,
  type ObservedCandidate,
} from "../../src/detection/candidate-groups";
import { TTL } from "../../src/detection/candidates";

const base = "https://cdn.test/movie/";
function candidate(
  name: string,
  overrides: Partial<ObservedCandidate> = {},
): ObservedCandidate {
  return {
    id: name,
    tabId: 1,
    epoch: "page",
    url: base + name,
    status: 200,
    firstSeen: 0,
    lastSeen: 100,
    ...overrides,
  };
}
const child = candidate("video?sig=1");
const master = candidate("master", {
  inspection: { until: 300, kind: "master", references: [child.url] },
});
const ids = (items: ObservedCandidate[], now = 100) =>
  selectVisibleCandidates(items, undefined, now).map((c) => c.id);

describe("playlist discovery", () => {
  it("groups a synthetic VOD playlist and preserves the final fractional segment", async () => {
    const root = candidate("sample/main.m3u8");
    const video = candidate("sample/video.m3u8");
    const iframe = candidate("sample/preview.m3u8");
    const multivariant = parse(
      [
        "#EXTM3U",
        '#EXT-X-STREAM-INF:BANDWIDTH=900000,RESOLUTION=640x360,CODECS="avc1.42e01e,mp4a.40.2"',
        "video.m3u8",
        "",
        '#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH=120000,URI="preview.m3u8"',
      ].join("\n"),
      root.url,
    );
    const media = parse(
      [
        "#EXTM3U",
        "#EXT-X-TARGETDURATION:4",
        "#EXT-X-PLAYLIST-TYPE:VOD",
        "#EXT-X-VERSION:3",
        "#EXT-X-MEDIA-SEQUENCE:8",
        "#EXTINF:4,",
        "part-a.ts",
        "#EXTINF:4,",
        "part-b.ts",
        "#EXTINF:1.5,",
        "part-c.ts",
        "#EXT-X-ENDLIST",
      ].join("\n"),
      video.url,
    );
    expect(multivariant.variants.map((v) => v.url)).toEqual([video.url]);
    expect(media.segments).toHaveLength(3);
    expect(media.segments.at(-1)).toMatchObject({
      sequence: "10",
      duration: 1.5,
      url: new URL("part-c.ts", video.url).href,
    });
    expect(media.references).toEqual([]);
    root.inspection = await createCandidateInspector(async () => multivariant)(
      root,
      Date.now() + 2000,
    );
    expect(ids([video])).toEqual([video.id]);
    expect(ids([video, root])).toEqual([root.id]);
    expect(ids([iframe, video, root])).toEqual([root.id]);
  });
  it("extracts playlist references without adding I-frames or renditions to quality choices", () => {
    const manifest = parse(
      [
        "#EXTM3U",
        '#EXT-X-DEFINE:NAME="video",VALUE="720"',
        '#EXT-X-STREAM-INF:BANDWIDTH=800,AUDIO="a"',
        "{$video}?sig=1",
        '#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH=80,URI="../iframe"',
        '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="en",URI="//audio.test/en"',
        '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="muxed"',
        '#EXT-X-SESSION-DATA:DATA-ID="test",URI="metadata"',
      ].join("\n"),
      base + "master?sig=2",
    );
    expect(manifest.references).toEqual([
      { role: "variant", url: base + "720?sig=1" },
      { role: "iframe", url: "https://cdn.test/iframe" },
      { role: "rendition", url: "https://audio.test/en" },
    ]);
    expect(manifest.variants.map((x) => x.url)).toEqual([base + "720?sig=1"]);
  });

  it("preserves independent roots, including two parents sharing the same child", () => {
    expect(ids([child])).toEqual([child.id]);
    const other = candidate("other", { inspection: master.inspection });
    const signed = candidate("video?sig=2");
    expect(ids([child, other, signed, master])).toEqual([
      other.id,
      signed.id,
      master.id,
    ]);
  });

  it("keeps children when evidence belongs to another page, expires, or fails", () => {
    expect(ids([child, { ...master, tabId: 2 }])).toHaveLength(2);
    expect(ids([child, { ...master, epoch: "other" }])).toHaveLength(2);
    expect(ids([child, master], 301)).toHaveLength(2);
    expect(ids([child, { ...master, status: 403 }])).toHaveLength(2);
    expect(ids([child, { ...master, lastSeen: -TTL }])).toEqual([child.id]);
  });

  it("uses unambiguous observed redirects and exposes fallback actions on unsupported masters", () => {
    const redirected = candidate("final", { aliases: [child.url] });
    const unsupported = {
      ...master,
      inspection: { ...master.inspection!, showChildren: true },
    };
    const [root] = selectVisibleCandidates([redirected, unsupported], 1, 100);
    expect(root.id).toBe(master.id);
    expect(root.children?.map((c) => c.id)).toEqual([redirected.id]);
    expect(ids([redirected, child, master])).toHaveLength(3);
  });

  it("shares concurrent reads and retains candidates after inspection errors", async () => {
    const read = vi.fn().mockRejectedValue(new Error("403"));
    const inspect = createCandidateInspector(read);
    const deadline = Date.now() + 2000;
    const a = inspect(master, deadline);
    expect(inspect(master, deadline)).toBe(a);
    await expect(a).resolves.toMatchObject({ kind: "unknown", references: [] });
    expect(read).toHaveBeenCalledTimes(1);
    await expect(inspect(child, 0)).resolves.toBeUndefined();
    expect(read).toHaveBeenCalledTimes(1);
  });
});
