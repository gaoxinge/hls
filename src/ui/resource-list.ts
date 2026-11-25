import type { ResourceRow } from "../application/resource-library";
import { active, type Task } from "../shared/model";
import type { MediaType } from "../domain/hls/content-types";

export function primaryTask(row: ResourceRow) {
  return row.tasks.find((t) => active(t.status)) ?? row.tasks[0];
}

export function playlistName(url: string) {
  const path = new URL(url);
  const name = path.pathname.split("/").at(-1) || path.hostname;
  try {
    return decodeURIComponent(name);
  } catch {
    return name;
  }
}

const labels: Record<MediaType, string> = {
  VIDEO: "视频",
  AUDIO: "音频",
  SUBTITLES: "字幕",
  "CLOSED-CAPTIONS": "隐藏字幕",
};
export function contentLabel(row: ResourceRow) {
  const types = row.candidate?.contentTypes?.length
    ? row.candidate.contentTypes
    : row.tasks.find((t) => t.contentTypes?.length)?.contentTypes;
  return types?.map((t) => labels[t]).join("、") || "未声明";
}
export const statuses = {
  queued: "排队中",
  resolving: "解析中",
  downloading: "下载中",
  merging: "合并中",
  exporting: "保存中",
  completed: "已下载",
  failed: "失败",
  cancelled: "已取消",
  interrupted: "已中断",
} satisfies Record<Task["status"], string>;
export const progressText = (t: Task) => t.done + " / " + (t.total || "待定");
