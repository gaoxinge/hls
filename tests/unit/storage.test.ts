import "fake-indexeddb/auto";
import { it, expect } from "vitest";
import { tasks, segments } from "../../src/storage/db";
import type { Task } from "../../src/shared/model";
it("persists tasks and isolates per-task cache records", async () => {
  const task: Task = {
    id: "test",
    candidateId: "c",
    playlistUrl: "https://a/list",
    status: "queued",
    createdAt: 1,
    updatedAt: 1,
    total: 0,
    done: 0,
    bytes: 0,
    filename: "video.ts",
    quality: "best",
    saveAs: false,
  };
  await tasks.put(task);
  expect((await tasks.get("test"))?.status).toBe("queued");
  await segments.put("test/hash/0", 188);
  await segments.put("other/hash/0", 376);
  await segments.clear("test");
  expect(await segments.get("test/hash/0")).toBeUndefined();
  expect((await segments.get("other/hash/0"))?.size).toBe(376);
  await tasks.remove("test");
  expect(await tasks.get("test")).toBeUndefined();
});
it("persists only entity validators, never a transport response body", async () => {
  const response = {
    etag: '"v2"',
    lastModified: "Sat, 12 Sep 2026 00:00:00 GMT",
    url: "https://a/segment",
    bytes: new Uint8Array([1, 2, 3]),
  };
  await segments.put("whitelist/hash/0", 3, "hash", response);
  const record = await segments.get("whitelist/hash/0");
  expect(record?.etag).toBe('"v2"');
  expect(record).not.toHaveProperty("bytes");
  expect(record).not.toHaveProperty("url");
  await segments.clear("whitelist");
});
it("removes a task's artifact records while preserving other tasks", async () => {
  const { artifacts } = await import("../../src/storage/db");
  const common = {
    schemaVersion: 1 as const,
    planId: "plan",
    formatId: "mp4" as const,
    storageKey: "output.bin",
    baseName: "video",
    size: 3,
    sha256: "a".repeat(64),
  };
  await artifacts.put({
    ...common,
    id: "output-task/main",
    taskId: "output-task",
  });
  await artifacts.put({
    ...common,
    id: "other-task/main",
    taskId: "other-task",
  });
  expect((await artifacts.get("output-task/main"))?.formatId).toBe("mp4");
  await tasks.remove("output-task");
  expect(await artifacts.get("output-task/main")).toBeUndefined();
  expect(await artifacts.get("other-task/main")).toBeDefined();
  await tasks.remove("other-task");
});
