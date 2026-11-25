import { DownloadEngine } from "../bootstrap/manager";
import {
  tasks,
  plans,
  artifacts,
  commands,
  commitCommand,
} from "../storage/db";
import { cleanFiles } from "../storage/files";
import { active } from "../shared/model";
import { suggestDownloadName } from "./download-filename";
import { outputFilename, outputBaseName } from "../domain/output/artifact";
export const taskService = {
  async retryFilename(id: string) {
    const task = await tasks.get(id);
    if (!task) throw new Error("任务已不存在");
    const base = outputBaseName(task.filename);
    if (!task.legacy) {
      const artifact = task.artifactId
        ? await artifacts.get(task.artifactId)
        : undefined;
      const format = artifact?.formatId ?? (await plans.get(id))?.format;
      if (format) return outputFilename(base, format);
      if (task.finalSize)
        throw new Error("旧输出缺少格式信息，请重新创建下载任务");
    }
    return suggestDownloadName(
      task.playlistUrl,
      task.chosenVariant?.url,
      task.quality,
      base,
    );
  },
  async retry(
    id: string,
    fileHandle: FileSystemFileHandle,
    commandId = crypto.randomUUID(),
  ) {
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
      return {
        ...task,
        directory: undefined,
        fileHandle,
        filename: fileHandle.name,
        downloadId: undefined,
        status: "queued",
        error: undefined,
      };
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
