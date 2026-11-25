import "./style.css";
import { send } from "./common";
import { createResourceTable, report } from "./resource-table";
const source = new URL(location.href).searchParams.get("tab");
const tabId = source !== null && /^\d+$/.test(source) ? Number(source) : -1;
async function openManager() {
  await send<void>({ type: "OPEN_MANAGER", tabId });
  window.close();
}
document.querySelector<HTMLButtonElement>("#manager")!.onclick = () => {
  void openManager().catch(report);
};
async function start() {
  const table = createResourceTable({
    tabId,
    includeHistory: false,
    onQueued: openManager,
  });
  const timer = setInterval(() => {
    void table.refreshTasks().catch(report);
  }, 1000);
  window.addEventListener("pagehide", () => clearInterval(timer), {
    once: true,
  });
  await Promise.all([table.refreshCandidates(), table.refreshTasks()]);
}
void start().catch(report);
