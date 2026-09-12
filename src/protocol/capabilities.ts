import { HlsError, type Playlist } from "../domain/hls/model";
const blocked: Record<string, string> = {
  "EXT-X-PART": "低延迟分片",
  "EXT-X-PART-INF": "低延迟分片",
  "EXT-X-PRELOAD-HINT": "预加载",
  "EXT-X-SERVER-CONTROL": "低延迟控制",
  "EXT-X-SKIP": "增量更新",
  "EXT-X-GAP": "存在缺失分片",
  "EXT-X-I-FRAMES-ONLY": "仅 I 帧播放列表",
  "EXT-X-CONTENT-STEERING": "动态内容导向",
  "EXT-X-DISCONTINUITY": "不连续时间线需要独立输出确认",
};
export function assertExecutable(playlist: Playlist) {
  for (const source of playlist.tags)
    if (blocked[source.tag])
      throw new HlsError(
        "UNSUPPORTED_FEATURE",
        `暂不支持 ${source.tag}：${blocked[source.tag]}`,
        source,
      );
  if (playlist.kind === "media" && !playlist.ended)
    throw new HlsError("UNSUPPORTED_LIVE", "仅支持已结束且非空的点播列表");
  const inits = new Set(
    playlist.segments.map((s) => JSON.stringify(s.init ?? null)),
  );
  if (inits.size > 1)
    throw new HlsError(
      "UNSUPPORTED_INIT_CHANGE",
      "初始化资源发生变化，需要重新封装",
    );
}
