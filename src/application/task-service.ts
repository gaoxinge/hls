import { DownloadEngine } from "../bootstrap/manager";
import { tasks, commands, commitCommand } from "../storage/db";
import { cleanFiles } from "../storage/files";
import { active } from "../shared/model";
export const taskService = {
  list: () => tasks.all(),
  async retry(id: string, commandId = crypto.randomUUID()) {
    await commitCommand(commandId, id, "retry", (task) => {
      if (!task || !["failed", "interrupted"].includes(task.status)) return;
      if (task.legacy) {
        task.fingerprint = undefined;
        task.finalSize = undefined;
        task.finalDigest = undefined;
        task.artifactId = undefined;
        task.done = 0;
        task.bytes = 0;
        task.legacy = false;
      }
      return { ...task, status: "queued", error: undefined };
    });
  },
  async cancel(id: string) {
    await commands.put(`cancel:${id}`, id, "cancel");
  },
  async clean(id: string) {
    await navigator.locks.request(
      "hls-engine",
      { ifAvailable: true },
      async (lock) => {
        if (!lock) throw new Error("请等待当前下载结束后清理");
        const task = await tasks.get(id);
        if (task && active(task.status)) throw new Error("请先取消任务");
        await cleanFiles(id);
        await tasks.remove(id);
      },
    );
  },
};

export const createTaskRuntime = (changed: () => void) =>
  new DownloadEngine(changed);
