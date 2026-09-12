import {
  OUTPUT_STORAGE_KEY,
  outputBaseName,
  validateArtifact,
  type OutputArtifact,
} from "../domain/output/artifact";
import type { Task } from "../domain/task/model";
import type { DownloadPlan } from "../planning/plan";
export function describeOutput(
  task: Task,
  plan: DownloadPlan,
  size: number,
  sha256: string,
  storageKey = OUTPUT_STORAGE_KEY,
): OutputArtifact {
  if (plan.schemaVersion !== 2 || task.fingerprint !== plan.id)
    throw new Error("输出产物与下载计划不匹配");
  const artifact: OutputArtifact = {
    schemaVersion: 1,
    id: `${task.id}/main`,
    taskId: task.id,
    planId: plan.id,
    formatId: plan.format,
    storageKey,
    baseName: outputBaseName(task.filename),
    size,
    sha256,
  };
  validateArtifact(artifact);
  return artifact;
}
export function restoreLegacyOutput(
  task: Task,
  plan: DownloadPlan | undefined,
): OutputArtifact {
  if (!plan || !task.finalSize || !task.finalDigest)
    throw new Error("旧输出缺少格式或完整性信息，请重新验证或创建下载任务");
  // Historical v2 outputs used video.ts for BOTH containers. The persisted plan
  // determines the format; the old filename and File.type are never authoritative.
  return describeOutput(
    task,
    plan,
    task.finalSize,
    task.finalDigest,
    "video.ts",
  );
}
