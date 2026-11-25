import { describe, it, expect } from "vitest";
import { resourceRows } from "../../src/application/resource-library";
import { playlistName, primaryTask } from "../../src/ui/resource-list";
import { parse } from "../../src/adapters/parser/m3u8";
import type { Task, VisibleCandidate } from "../../src/shared/model";

const url = "http://127.0.0.1/assets/master.m3u8?fixture=sample";
const root: VisibleCandidate = {
  id: "master",
  url,
  tabId: 1,
  epoch: "page",
  firstSeen: 1,
  lastSeen: 2,
  status: 200,
};
const saved: Task = {
  id: "saved",
  candidateId: "old",
  playlistUrl: url,
  status: "completed",
  createdAt: 1,
  updatedAt: 2,
  total: 2,
  done: 2,
  bytes: 100,
  filename: "output.ts",
  quality: "best",
};

describe("resource table", () => {
  it("joins rediscovered resources by exact URL, retaining every attempt and prioritizing an active task", () => {
    const running: Task = { ...saved, id: "new", status: "downloading" };
    const other = { ...saved, id: "other", playlistUrl: url + "2" };
    const rows = resourceRows([root], [saved, running, other], true);
    expect(rows).toHaveLength(2);
    expect(rows[0].tasks).toHaveLength(2);
    expect(primaryTask(rows[0])).toEqual(running);
    expect(resourceRows([root], [saved, other], false)).toHaveLength(1);
    expect(
      playlistName(
        "http://127.0.0.1/video/%E7%94%B5%E5%BD%B1.m3u8?fixture=sample",
      ),
    ).toBe("电影.m3u8");
  });
  it("keeps a rediscovered child under its confirmed master and respects a recorded parent", () => {
    const child = { id: "new-child", url: "http://127.0.0.1/v" };
    const c = { ...root, groupedCandidates: [child] };
    const t = { ...saved, playlistUrl: child.url };
    expect(resourceRows([c], [t], true).map((r) => r.url)).toEqual([url]);
    const second = { ...c, url: url + "2" };
    expect(
      resourceRows([c, second], [{ ...t, resourceUrl: second.url }], true)[1]
        .tasks,
    ).toHaveLength(1);
  });
  it("uses declared codecs and rendition types, leaving opaque TS segments unclassified", () => {
    const master = parse(
      [
        "#EXTM3U",
        '#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="s",NAME="en",URI="sub"',
        '#EXT-X-MEDIA:TYPE=CLOSED-CAPTIONS,GROUP-ID="cc",NAME="cc",INSTREAM-ID="CC1"',
        '#EXT-X-STREAM-INF:BANDWIDTH=800000,CODECS="avc1.64001f,mp4a.40.2",SUBTITLES="s",CLOSED-CAPTIONS="cc"',
        "v",
      ].join("\n"),
      url,
    );
    expect(master.contentTypes).toEqual([
      "VIDEO",
      "AUDIO",
      "SUBTITLES",
      "CLOSED-CAPTIONS",
    ]);
    const audio = parse(
      '#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=96000,CODECS="mp4a.40.2"\na',
      url,
    );
    expect(audio.contentTypes).toEqual(["AUDIO"]);
    const media = parse(
      "#EXTM3U\n#EXT-X-TARGETDURATION:5\n#EXTINF:5,\na.ts\n#EXT-X-ENDLIST",
      url,
    );
    expect(media.contentTypes).toEqual([]);
  });
});
