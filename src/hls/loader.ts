import { parse } from "../adapters/parser/m3u8";
import { createResolver } from "../planning/resolver";
import { request } from "../network/http";
import type { Playlist } from "../domain/hls/model";
export function loadPlaylist(
  url: string,
  signal: AbortSignal,
  quality: "best" | "lowest",
  selected?: string,
  probe?: (playlist: Playlist, signal: AbortSignal) => Promise<void>,
) {
  return createResolver(
    (uri, limit, abort) => request(uri, limit, abort),
    parse,
    probe,
  )(url, signal, quality, selected);
}
