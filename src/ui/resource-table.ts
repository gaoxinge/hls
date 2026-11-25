import {
  active,
  safeError,
  type Task,
  type VisibleCandidate,
} from "../shared/model";
import {
  listResourceTasks,
  resourceRows,
  type ResourceRow,
} from "../application/resource-library";
import { element, send } from "./common";
import { downloadActions } from "./download-actions";
import {
  contentLabel,
  playlistName,
  primaryTask,
  progressText,
  statuses,
} from "./resource-list";
import { createTaskDialog, type TaskActions } from "./task-dialog";

function resourceLine(
  row: ResourceRow,
  show: (url: string, task: Task) => void,
  onQueued: () => Promise<void>,
) {
  const tr = element("tr");
  const name = element("span", playlistName(row.url), "playlist-name");
  name.title = row.url;
  const nameCell = element("th");
  nameCell.scope = "row";
  nameCell.append(name);
  const type = element("td");
  const status = element("td");
  const statusLabel = element("span", "", "status-label");
  status.append(statusLabel);
  const progress = element("td");
  const open = element("button", "", "progress-link");
  open.setAttribute("aria-haspopup", "dialog");
  open.title = "查看任务详情";
  const empty = element("span", "—", "progress-empty");
  progress.append(open, empty);
  const action = element("td");
  tr.append(nameCell, type, status, progress, action);
  let controls: HTMLFieldSetElement;
  let current = row;
  open.onclick = () => {
    const task = primaryTask(current);
    if (task) show(current.url, task);
  };
  function update(next: ResourceRow) {
    const task = primaryTask(next);
    if (
      !controls ||
      current.candidate !== next.candidate ||
      (!next.candidate &&
        current.tasks.map((t) => t.id).join() !==
          next.tasks.map((t) => t.id).join())
    ) {
      controls = downloadActions(next, onQueued);
      action.replaceChildren(controls);
    }
    controls.disabled = Boolean(task && active(task.status));
    tr.toggleAttribute("data-active", controls.disabled);
    current = next;
    type.textContent = contentLabel(next);
    type.title = "播放列表中声明的内容类型";
    tr.dataset.status = task?.status ?? "available";
    if (task) tr.dataset.taskId = task.id;
    else delete tr.dataset.taskId;
    statusLabel.textContent = task ? statuses[task.status] : "未下载";
    if (
      task &&
      task.status !== "completed" &&
      next.tasks.some((t) => t.status === "completed")
    )
      statusLabel.textContent += " · 已下载";
    open.hidden = !task;
    empty.hidden = !!task;
    if (task) open.textContent = progressText(task);
  }
  update(row);
  return { tr, update };
}

export function createResourceTable(options: {
  tabId?: number;
  includeHistory: boolean;
  onQueued: () => Promise<void>;
  actions?: TaskActions;
}) {
  const target = document.querySelector("#resources")!;
  const table = element("table");
  table.setAttribute("aria-label", "HLS 资源");
  const headings = table.createTHead().insertRow();
  for (const label of ["m3u8名称", "类型", "状态", "进度", "操作"]) {
    const th = element("th", label);
    th.scope = "col";
    headings.append(th);
  }
  const body = table.createTBody();
  target.replaceChildren(table);
  const dialog = createTaskDialog(options.actions);
  const lines = new Map<string, ReturnType<typeof resourceLine>>();
  let candidates: VisibleCandidate[] = [];
  let tasks: Task[] = [];
  let loading = true;
  const empty = element("tr");
  const message = element("td", "", "empty");
  const emptyTitle = element("strong", "暂无 HLS 资源");
  const emptyHelp = element(
    "p",
    "在来源网页播放媒体。仍未发现资源时，请允许扩展访问该网站并刷新网页。",
  );
  message.append(emptyTitle, emptyHelp);
  message.colSpan = 5;
  empty.append(message);
  function render() {
    const rows = resourceRows(candidates, tasks, options.includeHistory);
    const keys = new Set(rows.map((r) => r.url));
    for (const [key, line] of lines) {
      if (!keys.has(key)) {
        line.tr.remove();
        lines.delete(key);
      }
    }
    if (rows.length) empty.remove();
    else {
      emptyTitle.textContent = loading ? "正在加载资源…" : "暂无 HLS 资源";
      emptyHelp.hidden = loading;
      if (!empty.isConnected) body.append(empty);
    }
    rows.forEach((row, i) => {
      let line = lines.get(row.url);
      if (!line) {
        line = resourceLine(row, dialog.show, options.onQueued);
        lines.set(row.url, line);
      } else line.update(row);
      if (body.children[i] !== line.tr)
        body.insertBefore(line.tr, body.children[i] ?? null);
    });
    dialog.sync(tasks);
  }
  render();
  let pending: Promise<void> | undefined;
  const refreshTasks = () =>
    (pending ??= listResourceTasks()
      .then((list) => {
        tasks = list;
        render();
      })
      .finally(() => {
        pending = undefined;
      }));
  let generation = 0;
  async function refreshCandidates() {
    const request = ++generation;
    try {
      const list = await send<VisibleCandidate[]>({
        type: "LIST_CANDIDATES",
        tabId: options.tabId,
      });
      if (request === generation) candidates = list;
    } finally {
      if (request === generation) {
        loading = false;
        render();
      }
    }
  }
  return { refreshTasks, refreshCandidates };
}
export function report(error: unknown) {
  document.querySelector("#error")!.textContent = safeError(error);
}
