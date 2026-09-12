import "./style.css";
import { candidateCard, element, resourceAddress, send } from "./common";
import {
  active,
  safeError,
  type Candidate,
  type Status,
} from "../shared/model";
import { taskService, createTaskRuntime } from "../application/task-service";
const statuses: Record<Status, string> = {
  queued: "等待下载",
  resolving: "解析播放列表",
  downloading: "下载中",
  merging: "合并中",
  exporting: "保存到本地",
  completed: "已完成",
  failed: "失败",
  cancelled: "已取消",
  interrupted: "已中断",
};
const report = (e: unknown) => {
  document.querySelector("#error")!.textContent = safeError(e);
};
const engine = createTaskRuntime(() => {
  void renderTasks().catch(report);
});
const channel = engine.listen();
window.addEventListener("pagehide", () => channel.close());
let rendering = false;
const cards = new Map<string, { status: Status; node: HTMLElement }>();
async function renderTasks() {
  if (rendering) return;
  rendering = true;
  try {
    const list = (await taskService.list()).sort(
      (a, b) => b.createdAt - a.createdAt,
    );
    const nodes = list.map((t) => {
      const existing = cards.get(t.id);
      if (existing?.status === t.status) {
        existing.node.querySelector("progress")!.value = t.done;
        existing.node.querySelector("p")!.textContent =
          `${t.done} / ${t.total} 个分片 · ${(t.bytes / 1048576).toFixed(1)} MiB`;
        return existing.node;
      }
      const card = element("article", "", "card");
      card.dataset.taskId = t.id;
      card.dataset.status = t.status;
      card.append(
        element("h3", t.filename),
        element("span", statuses[t.status], "pill"),
      );
      const bar = element("progress");
      bar.max = t.total || 1;
      bar.value = t.done;
      bar.setAttribute("aria-label", "已完成分片");
      card.append(
        bar,
        element(
          "p",
          `${t.done} / ${t.total} 个分片 · ${(t.bytes / 1048576).toFixed(1)} MiB`,
        ),
      );
      if (t.selectedUrl)
        card.append(resourceAddress(t.selectedUrl, "small", "来源："));
      if (t.legacy)
        card.append(
          element("small", "旧版任务保留原始数据；重试将按 v2 重新下载。"),
        );
      if (t.status === "interrupted")
        card.append(
          element(
            "small",
            "恢复检查本地摘要及可用的源站校验器；无校验器时无法保证源站内容未变。",
          ),
        );
      if (t.error) card.append(element("p", t.error, "error"));
      const buttons = element("div", "", "actions");
      function button(text: string, action: () => Promise<void>) {
        const b = element("button", text, "secondary");
        b.onclick = async () => {
          b.disabled = true;
          try {
            await action();
            await renderTasks();
          } catch (e) {
            report(e);
          }
        };
        buttons.append(b);
      }
      if (active(t.status)) button("取消", () => engine.cancel(t.id));
      if (["failed", "interrupted"].includes(t.status))
        button(t.finalSize ? "重新保存" : "恢复 / 重试", async () => {
          await taskService.retry(t.id);
          void engine.pump().catch(report);
        });
      if (!active(t.status))
        button("清理任务", async () => {
          await taskService.clean(t.id);
        });
      card.append(buttons);
      cards.set(t.id, { status: t.status, node: card });
      return card;
    });
    for (const id of cards.keys())
      if (!list.some((t) => t.id === id)) cards.delete(id);
    document
      .querySelector("#tasks")!
      .replaceChildren(
        ...(nodes.length
          ? nodes
          : [element("p", "暂无任务。选择下方候选，点击下载。", "empty")]),
      );
  } finally {
    rendering = false;
  }
}
async function renderCandidates() {
  const value = new URL(location.href).searchParams.get("tab");
  const list = await send<Candidate[]>({
    type: "LIST_CANDIDATES",
    ...(value === null ? {} : { tabId: Number(value) }),
  });
  document.querySelector("#candidates")!.replaceChildren(
    ...(list.length
      ? list.map((c) =>
          candidateCard(c, async (quality, saveAs, selectedUrl) => {
            await send({
              type: "START_TASK",
              id: c.id,
              quality,
              saveAs,
              selectedUrl,
              commandId: crypto.randomUUID(),
            });
            await renderTasks();
            void engine.pump().catch(report);
          }),
        )
      : [element("p", "尚未发现候选，请在已授权网页中播放视频。", "empty")]),
  );
}
document.querySelector<HTMLButtonElement>("#refresh")!.onclick = () => {
  void renderCandidates().catch(report);
};
const notificationInput =
  document.querySelector<HTMLInputElement>("#notifications")!;
notificationInput.onchange = () => {
  void chrome.storage.local.set({ notifications: notificationInput.checked });
};
async function start() {
  notificationInput.checked =
    (await chrome.storage.local.get({ notifications: true })).notifications ===
    true;
  await engine.recover();
  await renderTasks();
  await renderCandidates();
  setInterval(() => {
    void renderTasks().catch(report);
    void engine.pump().catch(report);
  }, 1000);
  await engine.pump();
}
void start().catch(report);
