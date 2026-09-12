export { active } from "../domain/task/model";
export type { Task, Status } from "../domain/task/model";
export interface Candidate {
  id: string;
  tabId: number;
  epoch: string;
  url: string;
  firstSeen: number;
  lastSeen: number;
  status: number;
}
export type {
  KeyContext,
  Segment,
  Variant,
  Playlist,
} from "../domain/hls/model";
export function httpUrl(value: string, base?: string): string {
  const url = new URL(value, base);
  if (!["http:", "https:"].includes(url.protocol))
    throw new Error("资源地址必须是 HTTP(S)");
  return url.href;
}
export function safeError(error: unknown): string {
  const message = error instanceof Error ? error.message : "操作失败，请重试";
  return message.replace(/https?:\/\/[^\s]+/g, "[资源地址]").slice(0, 240);
}
