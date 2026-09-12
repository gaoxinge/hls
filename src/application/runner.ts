import {
  artifactFilename,
  outputBaseName,
  outputFilename,
  validateArtifact,
} from "../domain/output/artifact";
import { describeOutput, restoreLegacyOutput } from "./output-artifacts";
import { active, safeError, type Task } from "../shared/model";
import { createResourceExecutor } from "../execution/resource-executor";
import { parallel } from "../execution/queue";
import { createPlan } from "../planning/plan";
import type { RunnerPorts } from "../ports/runner";
export function createRunner(deps: RunnerPorts, changed: () => void) {
  const { tasks, commands, plans, artifacts, loadPlaylist } = deps;
  const {
    cleanFiles,
    ensureSpace,
    finalFile,
    assemble,
    digestFile,
    preflight,
  } = deps.files;
  class DownloadEngine {
    private current?: {
      id: string;
      controller: AbortController;
      cancelled: boolean;
    };
    busy = false;
    constructor(private changed: () => void) {}
    async recover() {
      await deps.ownership.request(
        "hls-engine",
        { ifAvailable: true },
        async (lock) => {
          if (!lock) return;
          for (const task of await tasks.all()) {
            if (task.status !== "queued" && active(task.status)) {
              if (
                task.status === "exporting" &&
                task.downloadId !== undefined
              ) {
                const [item] = await deps.exporter.search({
                  id: task.downloadId,
                });
                if (item?.state === "complete") {
                  if (task.artifactId) {
                    const artifact = await artifacts.get(task.artifactId);
                    if (artifact) {
                      artifact.download = {
                        id: item.id,
                        filename: item.filename,
                        mime: item.mime,
                      };
                      await artifacts.put(artifact);
                    }
                  }
                  task.status = "completed";
                  await cleanFiles(task.id);
                  await tasks.put(task);
                  continue;
                }
                if (item?.state === "in_progress")
                  await deps.exporter.cancel(item.id).catch(() => {});
              }
              task.status = "interrupted";
              await tasks.put(task);
            }
            if (
              !task.legacy &&
              !active(task.status) &&
              Date.now() - task.updatedAt > 7 * 86400000
            ) {
              await cleanFiles(task.id);
              await tasks.remove(task.id);
            }
          }
        },
      );
    }
    async cancel(id: string) {
      await commands.put(`cancel:${id}`, id, "cancel");
      if (this.current?.id === id) {
        this.current.cancelled = true;
        this.current.controller.abort();
        return;
      }
      // This also supports a second manager cancelling the owner via BroadcastChannel.
      const channel = deps.control();
      channel.postMessage({ cancel: id });
      channel.close();
      await deps.ownership.request(
        `hls-task-${id}`,
        { ifAvailable: true },
        async (lock) => {
          if (!lock) return;
          const task = await tasks.get(id);
          if (!task || !active(task.status)) return;
          task.status = "cancelled";
          await tasks.put(task);
          await cleanFiles(id);
          this.changed();
        },
      );
    }
    listen() {
      const channel = deps.control();
      channel.onmessage = (event) => {
        const current = this.current;
        if (current && current.id === event.data?.cancel) {
          current.cancelled = true;
          current.controller.abort();
        }
      };
      return channel;
    }
    async pump() {
      if (this.busy) return;
      this.busy = true;
      try {
        await deps.ownership.request(
          "hls-engine",
          { ifAvailable: true },
          async (lock) => {
            if (!lock) return;
            const task = (await tasks.all())
              .filter((t) => t.status === "queued")
              .sort((a, b) => a.createdAt - b.createdAt)[0];
            if (task)
              await deps.ownership.request(`hls-task-${task.id}`, async () => {
                const current = await tasks.get(task.id);
                if (current?.status === "queued") await this.run(current);
              });
          },
        );
      } finally {
        this.busy = false;
      }
    }
    private async run(task: Task) {
      task.ownerGeneration = crypto.randomUUID();
      const controller = new AbortController();
      const running = { id: task.id, controller, cancelled: false };
      this.current = running;
      const { signal } = controller;
      const cancellationPoll = setInterval(() => {
        void commands
          .get(`cancel:${task.id}`)
          .then((command) => {
            if (command) {
              running.cancelled = true;
              controller.abort();
            }
          })
          .catch(() => {});
      }, 500);
      const update = async () => {
        if (await commands.get(`cancel:${task.id}`)) {
          running.cancelled = true;
          if (!["cancelled", "failed"].includes(task.status))
            throw new Error("下载已取消");
        }
        await tasks.put(task);
        this.changed();
      };
      try {
        if (task.finalSize) {
          const artifact = task.artifactId
            ? await artifacts.get(task.artifactId)
            : restoreLegacyOutput(task, await plans.get(task.id));
          if (!artifact) throw new Error("输出产物记录丢失，请重新验证任务");
          validateArtifact(artifact);
          if (
            artifact.taskId !== task.id ||
            artifact.planId !== task.fingerprint
          )
            throw new Error("输出产物与任务不匹配");
          const file = await finalFile(task.id, artifact.storageKey);
          if (
            file.size !== artifact.size ||
            (await digestFile(file)) !== artifact.sha256
          )
            throw new Error("缓存文件不完整，请清理任务后重新下载");
          await artifacts.put(artifact);
          task.artifactId = artifact.id;
          task.filename = artifactFilename(artifact);
          task.status = "exporting";
          await update();
          artifact.download = await deps.exporter.exportFile(
            task,
            file,
            artifact,
            signal,
            tasks.put,
          );
          await artifacts.put(artifact);
        } else {
          task.status = "resolving";
          task.error = undefined;
          await update();
          const executeResource = createResourceExecutor(deps, task.id, signal);
          const savedPlan = task.fingerprint
            ? await plans.get(task.id)
            : undefined;
          if (
            task.fingerprint &&
            (!savedPlan ||
              savedPlan.schemaVersion !== 2 ||
              savedPlan.id !== task.fingerprint)
          )
            throw new Error("下载计划缺失或版本不兼容，请重新创建任务");
          const plan =
            savedPlan ??
            createPlan(
              await loadPlaylist(
                task.playlistUrl,
                signal,
                task.quality,
                task.selectedUrl,
                async (playlist) => {
                  const candidate = createPlan(playlist);
                  const first = playlist.segments[0];
                  if (first.init)
                    await executeResource(
                      {
                        ...first,
                        ...first.init,
                        init: undefined,
                        index: -1,
                        sequence: "0",
                      },
                      candidate.id,
                    );
                  await executeResource(first, candidate.id);
                  await preflight(task.id, candidate, signal);
                },
              ),
            );
          const playlist = { url: plan.playlistUrl, segments: plan.resources };
          const fingerprint = plan.id;
          if (task.fingerprint && task.fingerprint !== fingerprint)
            throw new Error("播放列表已变化，请清理此任务并重新下载");
          await plans.put(task.id, plan);
          task.filename = outputFilename(
            outputBaseName(task.filename),
            plan.format,
          );
          task.fingerprint = fingerprint;
          task.selectedUrl = playlist.url;
          task.total = playlist.segments.length;
          task.done = 0;
          task.bytes = 0;
          task.status = "downloading";
          await update();
          // Serialize counters/records; network and decryption remain concurrent.
          let commits = Promise.resolve();
          const init = playlist.segments[0].init;
          const resources = init
            ? [
                {
                  ...playlist.segments[0],
                  ...init,
                  init: undefined,
                  index: -1,
                  sequence: "0",
                },
                ...playlist.segments,
              ]
            : playlist.segments;
          const downloadResource = async (
            segment: (typeof resources)[number],
          ) => {
            const size = await executeResource(segment, fingerprint);
            const length = size;
            commits = commits.then(async () => {
              if (segment.index >= 0) task.done++;
              task.bytes += length;
              await update();
            });
            await commits;
          };
          for (const resource of resources.filter((s) => s.index <= 0))
            await downloadResource(resource);
          await preflight(task.id, plan, signal);
          await parallel(
            resources.filter((s) => s.index > 0),
            downloadResource,
            controller,
            4,
          );

          await commits;
          await ensureSpace(task.bytes);
          task.status = "merging";
          await update();
          const file = await assemble(task.id, plan, signal);
          const finalDigest = await digestFile(file);
          const artifact = describeOutput(task, plan, file.size, finalDigest);
          await artifacts.put(artifact);
          task.finalSize = artifact.size;
          task.finalDigest = artifact.sha256;
          task.artifactId = artifact.id;
          task.filename = artifactFilename(artifact);
          task.status = "exporting";
          await update();
          artifact.download = await deps.exporter.exportFile(
            task,
            file,
            artifact,
            signal,
            tasks.put,
          );
          await artifacts.put(artifact);
        }
        task.status = "completed";
        task.error = undefined;
        await update();
        await cleanFiles(task.id).catch(() => {
          /* Cleanup can be retried from the manager. */
        });
      } catch (error) {
        task.status = running.cancelled ? "cancelled" : "failed";
        task.error = running.cancelled ? undefined : safeError(error);
        await update();
        if (running.cancelled) await cleanFiles(task.id);
      } finally {
        clearInterval(cancellationPoll);
        this.current = undefined;
        this.changed();
      }
    }
  }

  return new DownloadEngine(changed);
}
