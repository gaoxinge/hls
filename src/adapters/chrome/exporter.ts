import {
  artifactFilename,
  outputFormats,
  type OutputArtifact,
} from "../../domain/output/artifact";
import type { Task } from "../../shared/model";
export async function exportFile(
  task: Task,
  file: File,
  artifact: OutputArtifact,
  signal: AbortSignal,
  save: (task: Task) => Promise<void>,
) {
  const filename = artifactFilename(artifact);
  if (
    artifact.taskId !== task.id ||
    artifact.planId !== task.fingerprint ||
    file.size !== artifact.size
  )
    throw new Error("导出产物与任务或文件不匹配");
  const blob = file.slice(
    0,
    file.size,
    outputFormats[artifact.formatId].mimeType,
  );
  const url = URL.createObjectURL(blob);
  let id: number | undefined;
  try {
    signal.throwIfAborted();
    id = await chrome.downloads.download({
      url,
      filename,
      saveAs: task.saveAs,
      conflictAction: "uniquify",
    });
    task.downloadId = id;
    await save(task);
    await new Promise<void>((resolve, reject) => {
      let finished = false;
      const finish = (error?: Error) => {
        if (finished) return;
        finished = true;
        chrome.downloads.onChanged.removeListener(change);
        signal.removeEventListener("abort", abort);
        if (error) reject(error);
        else resolve();
      };
      const change = (delta: chrome.downloads.DownloadDelta) => {
        if (delta.id !== id) return;
        if (delta.state?.current === "complete") finish();
        if (delta.state?.current === "interrupted")
          finish(new Error("文件保存被中断，可重试导出"));
      };
      const abort = () => {
        if (id !== undefined) void chrome.downloads.cancel(id).catch(() => {});
        finish(new Error("下载已取消"));
      };
      chrome.downloads.onChanged.addListener(change);
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort();
      void chrome.downloads.search({ id }).then(
        ([item]) => {
          if (!item || item.state === "interrupted")
            finish(new Error("文件保存被中断，可重试导出"));
          else if (item.state === "complete") finish();
        },
        () => finish(new Error("无法确认文件保存状态")),
      );
    });
    const [item] = await chrome.downloads.search({ id });
    if (!item || item.state !== "complete")
      throw new Error("无法确认文件保存结果");
    return { id, filename: item.filename, mime: item.mime };
  } finally {
    URL.revokeObjectURL(url);
  }
}
