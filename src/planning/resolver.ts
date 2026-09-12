import type { PlaylistParser } from "../ports/parser";
import { assertExecutable } from "../protocol/capabilities";
import { HlsError, type Playlist } from "../domain/hls/model";
import type { ReadResource } from "../ports/transport";
export function createResolver(
  read: ReadResource,
  parse: PlaylistParser,
  probe?: (playlist: Playlist, signal: AbortSignal) => Promise<void>,
) {
  return async (
    url: string,
    signal: AbortSignal,
    quality: "best" | "lowest",
    selected?: string,
  ): Promise<Playlist> => {
    let budget = 8;
    async function visit(
      uri: string,
      path: string[],
      definitions: Record<string, string>,
      manual?: string,
    ): Promise<Playlist> {
      signal.throwIfAborted();
      if (path.includes(uri))
        throw new HlsError("PLAYLIST_CYCLE", "播放列表循环引用");
      if (path.length >= 5)
        throw new HlsError("PLAYLIST_DEPTH", "播放列表嵌套超过 5 层");
      const response = await read(uri, 2 * 1024 * 1024, signal);
      const playlist = parse(
        new TextDecoder("utf-8", { fatal: true }).decode(response.bytes),
        response.url,
        definitions,
      );
      assertExecutable(playlist);
      if (playlist.kind === "media") {
        await probe?.(playlist, signal);
        return playlist;
      }
      const ordered = [...playlist.variants].sort(
        (a, b) => b.height - a.height || b.bandwidth - a.bandwidth,
      );
      if (quality === "lowest") ordered.reverse();
      const candidates = manual
        ? ordered.filter((v) => v.url === manual)
        : ordered;
      if (!candidates.length)
        throw new HlsError("VARIANT_NOT_FOUND", "指定变体不在主列表中");
      const reasons: string[] = [];
      for (const candidate of candidates) {
        if (!candidate.supported) {
          reasons.push(candidate.reason ?? "独立音轨");
          if (manual) throw new HlsError("UNSUPPORTED_VARIANT", reasons[0]);
          continue;
        }
        if (budget-- <= 0)
          throw new HlsError("PROBE_BUDGET", "已达到 8 个候选的探测上限");
        try {
          return await visit(
            candidate.url,
            [...path, uri],
            playlist.definitions,
          );
        } catch (error) {
          signal.throwIfAborted();
          if (manual) throw error;
          reasons.push(error instanceof Error ? error.message : "资源不可用");
        }
      }
      throw new HlsError(
        "NO_EXECUTABLE_VARIANT",
        `没有可用变体：${reasons.join("；")}`,
      );
    }
    return visit(url, [], {}, selected);
  };
}
