import type { Task, VisibleCandidate } from "../shared/model";
import { tasks as taskStore } from "../storage/db";

export const listResourceTasks = taskStore.all;

export interface ResourceRow {
  url: string;
  candidate?: VisibleCandidate;
  tasks: Task[];
}

export function resourceRows(
  candidates: VisibleCandidate[],
  tasks: Task[],
  includeHistory: boolean,
): ResourceRow[] {
  const rows = new Map<string, ResourceRow>();
  // Both observation IDs and exact URLs can identify a rediscovered child.
  // A shared child has no unique parent unless the task recorded its root.
  const parents = new Map<string, string | null>();
  for (const c of candidates) {
    // Identical full URLs represent the same resource; keep signed URLs distinct.
    const row = rows.get(c.url);
    if (!row || c.lastSeen > row.candidate!.lastSeen)
      rows.set(c.url, { url: c.url, candidate: c, tasks: [] });
    for (const child of c.groupedCandidates ?? []) {
      for (const key of [child.id, child.url]) {
        if (!parents.has(key)) parents.set(key, c.url);
        else if (parents.get(key) !== c.url) parents.set(key, null);
      }
    }
  }
  for (const task of [...tasks].sort((a, b) => b.createdAt - a.createdAt)) {
    let url = task.resourceUrl ?? task.playlistUrl;
    if (url === task.playlistUrl && !rows.get(url)?.candidate) {
      url = parents.get(task.candidateId) ?? parents.get(url) ?? url;
    }
    let row = rows.get(url);
    if (!row) {
      if (!includeHistory) continue;
      row = { url, tasks: [] };
      rows.set(url, row);
    }
    row.tasks.push(task);
  }
  return [...rows.values()];
}
