import {
  validateArtifact,
  type OutputArtifact,
} from "../domain/output/artifact";
import { assertTransition } from "../domain/task/transitions";
import { active, type Task } from "../shared/model";
import type { DownloadPlan } from "../planning/plan";
let opened: Promise<IDBDatabase> | undefined;
export function database() {
  return (opened ??= new Promise((resolve, reject) => {
    const req = indexedDB.open("hls-v1", 2);
    req.onupgradeneeded = () => {
      for (const name of [
        "tasks",
        "segments",
        "plans",
        "resources",
        "artifacts",
        "commands",
      ])
        if (!req.result.objectStoreNames.contains(name))
          req.result.createObjectStore(name, { keyPath: "id" });
      const cursor = req.transaction!.objectStore("tasks").openCursor();
      cursor.onsuccess = () => {
        const c = cursor.result;
        if (!c) return;
        const task = c.value as Task;
        if (task.schemaVersion !== 2) {
          c.update({
            ...task,
            schemaVersion: 2,
            revision: 0,
            legacy: true,
            status: active(task.status) ? "interrupted" : task.status,
            error: active(task.status)
              ? "v1 任务已保留，请重试后按 v2 规则重新验证资源"
              : task.error,
          });
        }
        c.continue();
      };
    };
    req.onsuccess = () => {
      req.result.onversionchange = () => {
        req.result.close();
        opened = undefined;
      };
      resolve(req.result);
    };
    req.onerror = () => {
      opened = undefined;
      reject(req.error);
    };
    req.onblocked = () => reject(new Error("请关闭其他旧版本管理页后重试升级"));
  }));
}
async function operation<T>(
  store: string,
  mode: IDBTransactionMode,
  perform: (s: IDBObjectStore) => IDBRequest<T>,
): Promise<T> {
  const db = await database();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, mode),
      req = perform(tx.objectStore(store));
    tx.oncomplete = () => resolve(req.result);
    tx.onerror = () => reject(tx.error ?? req.error);
    tx.onabort = () => reject(tx.error ?? new Error("存储事务已中断"));
  });
}
export const tasks = {
  get: (id: string) =>
    operation<Task | undefined>("tasks", "readonly", (s) => s.get(id)),
  all: () => operation<Task[]>("tasks", "readonly", (s) => s.getAll()),
  put: async (task: Task) => {
    const db = await database();
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction("tasks", "readwrite"),
        store = tx.objectStore("tasks"),
        req = store.get(task.id);
      let revision = 0;
      let conflict = false;
      let stateError: unknown;
      req.onsuccess = () => {
        const current = req.result as Task | undefined;
        if (current && (current.revision ?? 0) !== (task.revision ?? 0)) {
          conflict = true;
          tx.abort();
          return;
        }
        try {
          if (current) assertTransition(current.status, task.status);
        } catch (error) {
          stateError = error;
          tx.abort();
          return;
        }
        revision = (current?.revision ?? 0) + 1;
        store.put({
          ...task,
          schemaVersion: 2,
          revision,
          updatedAt: Date.now(),
        });
      };
      tx.oncomplete = () => {
        task.revision = revision;
        task.schemaVersion = 2;
        resolve();
      };
      tx.onabort = () =>
        reject(
          stateError ??
            new Error(
              conflict ? "任务已被其他页面修改，请刷新后重试" : "任务存储失败",
            ),
        );
      tx.onerror = () => reject(tx.error);
    });
  },
  remove: async (id: string) => {
    const db = await database();
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction(
        ["tasks", "plans", "artifacts", "commands"],
        "readwrite",
      );
      for (const name of ["tasks", "plans", "artifacts"])
        tx.objectStore(name).delete(id);
      const artifactCursor = tx.objectStore("artifacts").openCursor();
      artifactCursor.onsuccess = () => {
        const c = artifactCursor.result;
        if (!c) return;
        if (c.value.taskId === id) c.delete();
        c.continue();
      };
      const req = tx.objectStore("commands").openCursor();
      req.onsuccess = () => {
        const c = req.result;
        if (!c) return;
        if (c.value.taskId === id) c.delete();
        c.continue();
      };
      tx.oncomplete = () => resolve();
      tx.onabort = () => reject(tx.error);
      tx.onerror = () => reject(tx.error);
    });
  },
};
export interface SegmentRecord {
  id: string;
  size: number;
  digest?: string;
  etag?: string;
  lastModified?: string;
}
export const segments = {
  get: (id: string) =>
    operation<SegmentRecord | undefined>("segments", "readonly", (s) =>
      s.get(id),
    ),
  put: async (
    id: string,
    size: number,
    digest?: string,
    validators?: { etag?: string; lastModified?: string },
  ) => {
    await operation("segments", "readwrite", (s) =>
      s.put({
        id,
        size,
        digest,
        etag: validators?.etag,
        lastModified: validators?.lastModified,
      }),
    );
  },
  clear: async (taskId: string) => {
    await operation("segments", "readwrite", (s) =>
      s.delete(IDBKeyRange.bound(`${taskId}/`, `${taskId}/\uffff`)),
    );
  },
};
export const plans = {
  async get(id: string) {
    return (
      await operation<{ id: string; plan: DownloadPlan } | undefined>(
        "plans",
        "readonly",
        (s) => s.get(id),
      )
    )?.plan;
  },
  async put(id: string, plan: DownloadPlan) {
    await operation("plans", "readwrite", (s) => s.put({ id, plan }));
  },
};
export const commands = {
  get: (id: string) =>
    operation<{ id: string; taskId: string; kind: string } | undefined>(
      "commands",
      "readonly",
      (s) => s.get(id),
    ),
  put: async (id: string, taskId: string, kind: string) => {
    await operation("commands", "readwrite", (s) =>
      s.put({ id, taskId, kind }),
    );
  },
  remove: async (id: string) => {
    await operation("commands", "readwrite", (s) => s.delete(id));
  },
};
/** Persist command receipt and state transition in the SAME IndexedDB transaction. */
export async function commitCommand(
  commandId: string,
  taskId: string,
  kind: string,
  change: (current: Task | undefined) => Task | undefined,
): Promise<void> {
  const db = await database();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(["tasks", "commands"], "readwrite"),
      taskStore = tx.objectStore("tasks"),
      commandStore = tx.objectStore("commands");
    let failure: unknown;
    const seen = commandStore.get(commandId);
    seen.onsuccess = () => {
      if (seen.result) return;
      const req = taskStore.get(taskId);
      req.onsuccess = () => {
        try {
          const current = req.result as Task | undefined;
          const next = change(current);
          if (current && next) assertTransition(current.status, next.status);
          if (next)
            taskStore.put({
              ...next,
              schemaVersion: 2,
              revision: (current?.revision ?? 0) + 1,
              updatedAt: Date.now(),
            });
          if (kind === "retry") commandStore.delete(`cancel:${taskId}`);
          commandStore.put({ id: commandId, taskId, kind });
        } catch (error) {
          failure = error;
          tx.abort();
        }
      };
    };
    tx.oncomplete = () => resolve();
    tx.onabort = () => reject(failure ?? tx.error ?? new Error("命令事务失败"));
    tx.onerror = () => reject(tx.error);
  });
}

export const artifacts = {
  get: (id: string) =>
    operation<OutputArtifact | undefined>("artifacts", "readonly", (s) =>
      s.get(id),
    ),
  async put(artifact: OutputArtifact) {
    validateArtifact(artifact);
    await operation("artifacts", "readwrite", (s) => s.put(artifact));
  },
};
