import { IDBFactory } from "fake-indexeddb";
import { it, expect, vi } from "vitest";
it("upgrades v1 in place and never starts old queued jobs; commands are atomic and idempotent", async () => {
  vi.stubGlobal("indexedDB", new IDBFactory());
  vi.resetModules();
  const legacy = {
    id: "legacy",
    status: "queued",
    createdAt: 1,
    updatedAt: 1,
    candidateId: "c",
    playlistUrl: "https://a/list",
    quality: "best",
    saveAs: false,
    filename: "old.ts",
    done: 0,
    total: 0,
    bytes: 0,
  };
  await new Promise<void>((resolve, reject) => {
    const req = indexedDB.open("hls-v1", 1);
    req.onupgradeneeded = () => {
      const store = req.result.createObjectStore("tasks", { keyPath: "id" });
      store.put(legacy);
      req.result
        .createObjectStore("segments", { keyPath: "id" })
        .put({ id: "legacy/old/0", size: 188 });
    };
    req.onsuccess = () => {
      req.result.close();
      resolve();
    };
    req.onerror = () => reject(req.error);
  });
  const { tasks, segments, commitCommand, database } = await import(
    "../../src/storage/db"
  );
  const old = await tasks.get("legacy");
  expect(old?.legacy).toBe(true);
  expect(old?.status).toBe("interrupted");
  expect((await segments.get("legacy/old/0"))?.size).toBe(188);
  await commitCommand("retry-1", "legacy", "retry", (current) => ({
    ...current!,
    status: "queued",
  }));
  await commitCommand("retry-1", "legacy", "retry", () => {
    throw new Error("must not execute again");
  });
  const task = await tasks.get("legacy");
  expect(task?.revision).toBe(1);
  expect(task?.status).toBe("queued");
  await expect(tasks.put({ ...task!, revision: 0 })).rejects.toThrow(
    "其他页面",
  );
  await expect(
    commitCommand("bad", "legacy", "retry", () => {
      throw new Error("rollback");
    }),
  ).rejects.toThrow("rollback");
  expect((await tasks.get("legacy"))?.revision).toBe(1);
  (await database()).close();
  vi.unstubAllGlobals();
});
