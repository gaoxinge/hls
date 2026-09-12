import { sha256 } from "@noble/hashes/sha256";
import { bytesToHex } from "@noble/hashes/utils";
import type { Playlist, Segment } from "../domain/hls/model";
export interface DownloadPlan {
  id: string;
  schemaVersion: 2;
  playlistUrl: string;
  format: "ts" | "mp4";
  resources: Segment[];
}
export function createPlan(playlist: Playlist): DownloadPlan {
  const resources = playlist.segments.map((s) => ({
    ...s,
    source: { line: 0, tag: "segment" },
  }));
  const semantic = {
    version: 2,
    url: playlist.url,
    resources: resources.map((s) => ({
      sequence: s.sequence,
      url: s.url,
      duration: s.duration,
      timeline: s.timeline,
      range: s.range ?? null,
      init: s.init ?? null,
      key: s.key ?? null,
    })),
  };
  return {
    id: bytesToHex(sha256(new TextEncoder().encode(JSON.stringify(semantic)))),
    schemaVersion: 2,
    playlistUrl: playlist.url,
    format: resources[0].init ? "mp4" : "ts",
    resources,
  };
}
