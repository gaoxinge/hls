import { active, type Task, type VisibleCandidate } from "../shared/model";
import { commitCommand, tasks } from "../storage/db";

export async function queueDownload(
  candidate: { id: string; url: string },
  fileHandle: FileSystemFileHandle,
  chosenVariant?: Task["chosenVariant"],
  root?: Pick<VisibleCandidate, "url" | "contentTypes">,
) {
  const resourceUrl = root?.url ?? candidate.url;
  await navigator.locks.request(`hls-start-${resourceUrl}`, async () => {
    if (
      (await tasks.all()).some(
        (t) =>
          (t.candidateId === candidate.id ||
            (t.resourceUrl ?? t.playlistUrl) === resourceUrl) &&
          active(t.status),
      )
    )
      throw new Error("此资源已有下载任务，请在管理页查看");
    const id = crypto.randomUUID();
    const now = Date.now();
    const task: Task = {
      id,
      candidateId: candidate.id,
      playlistUrl: candidate.url,
      resourceUrl,
      contentTypes: root?.contentTypes,
      chosenVariant,
      selectedUrl: chosenVariant?.url,
      fileHandle,
      status: "queued",
      createdAt: now,
      updatedAt: now,
      total: 0,
      done: 0,
      bytes: 0,
      quality: "best",
      filename: fileHandle.name,
    };
    await commitCommand(id, id, "start", () => task);
  });
}
