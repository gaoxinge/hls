import type { Status } from "./model";
const next: Record<Status, readonly Status[]> = {
  queued: ["resolving", "exporting", "cancelled", "failed", "interrupted"],
  resolving: ["downloading", "failed", "cancelled", "interrupted"],
  downloading: ["merging", "failed", "cancelled", "interrupted"],
  merging: ["exporting", "failed", "cancelled", "interrupted"],
  exporting: ["completed", "failed", "cancelled", "interrupted"],
  failed: ["queued"],
  interrupted: ["queued"],
  completed: [],
  cancelled: [],
};
export function assertTransition(from: Status, to: Status) {
  if (from !== to && !next[from].includes(to))
    throw new Error(`无效任务状态转换：${from} → ${to}`);
}
