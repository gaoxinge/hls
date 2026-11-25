import "./style.css";
import { taskService, createTaskRuntime } from "../application/task-service";
import { pickDownloadFile } from "./save-file-picker";
import { createResourceTable, report } from "./resource-table";

const engine = createTaskRuntime(() => {
  void table.refreshTasks().catch(report);
});
const tab = new URL(location.href).searchParams.get("tab");
async function queued() {
  await table.refreshTasks();
  void engine.pump().catch(report);
}
const table = createResourceTable({
  tabId: tab === null ? undefined : Number(tab),
  includeHistory: tab === null,
  onQueued: queued,
  actions: {
    cancel: (id) => engine.cancel(id),
    async retry(id) {
      const file = await pickDownloadFile(await taskService.retryFilename(id));
      await taskService.retry(id, file);
      await queued();
    },
    async clean(id) {
      await taskService.clean(id);
      await table.refreshTasks();
    },
  },
});
const channel = engine.listen();
window.addEventListener("pagehide", () => channel.close());
const refresh = document.querySelector<HTMLButtonElement>("#refresh")!;
refresh.onclick = async () => {
  refresh.disabled = true;
  refresh.textContent = "刷新中…";
  try {
    await table.refreshCandidates();
  } catch (error) {
    report(error);
  } finally {
    refresh.disabled = false;
    refresh.textContent = "刷新资源";
  }
};
const notifications =
  document.querySelector<HTMLInputElement>("#notifications")!;
notifications.onchange = () => {
  void chrome.storage.local.set({ notifications: notifications.checked });
};
async function start() {
  notifications.checked =
    (await chrome.storage.local.get({ notifications: true })).notifications ===
    true;
  await engine.recover();
  void table.refreshCandidates().catch(report);
  setInterval(() => {
    void queued().catch(report);
  }, 1000);
  await queued();
}
void start().catch(report);
