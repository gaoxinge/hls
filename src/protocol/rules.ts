import { HlsError, type Playlist } from "../domain/hls/model";
export function validateProtocol(playlist: Playlist) {
  if (playlist.kind === "media") {
    if (!playlist.segments.length)
      throw new HlsError("EMPTY_PLAYLIST", "仅支持已结束且非空的点播列表");
    if (playlist.targetDuration === undefined)
      throw new HlsError(
        "MISSING_TARGET_DURATION",
        "媒体列表缺少 TARGETDURATION",
      );
    for (const s of playlist.segments)
      if (Math.round(s.duration) > playlist.targetDuration)
        throw new HlsError(
          "INVALID_DURATION",
          "分片时长超过目标时长",
          s.source,
        );
    const required = playlist.segments.some((s) => s.init)
      ? 6
      : playlist.segments.some((s) => s.range)
        ? 4
        : playlist.segments.some((s) => !Number.isInteger(s.duration))
          ? 3
          : playlist.segments.some((s) => s.key?.iv)
            ? 2
            : 1;
    if (playlist.version < required)
      throw new HlsError(
        "INVALID_VERSION",
        `当前特性要求 EXT-X-VERSION 至少为 ${required}`,
      );
  }
  const mediaTags = [
    "EXTINF",
    "EXT-X-TARGETDURATION",
    "EXT-X-MEDIA-SEQUENCE",
    "EXT-X-KEY",
    "EXT-X-MAP",
    "EXT-X-BYTERANGE",
    "EXT-X-ENDLIST",
    "EXT-X-PLAYLIST-TYPE",
  ];
  const masterTags = [
    "EXT-X-STREAM-INF",
    "EXT-X-MEDIA",
    "EXT-X-SESSION-KEY",
    "EXT-X-SESSION-DATA",
  ];
  const conflict = playlist.tags.find((s) =>
    (playlist.kind === "master" ? mediaTags : masterTags).includes(s.tag),
  );
  if (conflict)
    throw new HlsError("MIXED_PLAYLIST", "主列表和媒体标签不能混用", conflict);
  const keyFormat = playlist.tags.find((s) =>
    /KEYFORMAT(?:VERSIONS)?=/.test(s.raw ?? ""),
  );
  if (keyFormat && playlist.version < 5)
    throw new HlsError(
      "INVALID_VERSION",
      "KEYFORMAT 要求版本至少为 5",
      keyFormat,
    );
}
