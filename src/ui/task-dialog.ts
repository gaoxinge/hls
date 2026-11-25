import { active, type Task } from "../shared/model";
import { element } from "./common";
import { pickerCancelled } from "./save-file-picker";
import { playlistName, statuses, progressText } from "./resource-list";

export interface TaskActions {
  cancel(id: string): Promise<void>;
  retry(id: string): Promise<void>;
  clean(id: string): Promise<void>;
}

function taskPanel(t: Task, actions?: TaskActions) {
  const panel = element("section", "", "task-panel");
  panel.dataset.taskId = t.id;
  const title = element("h3", t.filename);
  const info = element("p");
  const bar = element("progress");
  bar.setAttribute("aria-label", "已完成分片");
  const meta = element("dl");
  for (const [label, value] of [
    ["创建时间", new Date(t.createdAt).toLocaleString()],
    ["档位", t.chosenVariant?.label],
    ["播放列表", t.playlistUrl],
    ["所选媒体列表", t.selectedUrl],
  ]) {
    if (value) meta.append(element("dt", label!), element("dd", value));
  }
  const feedback = element("p", "", "error");
  if (t.error) feedback.textContent = t.error;
  const buttons = element("div", "", "actions");
  function button(label: string, run: (id: string) => Promise<void>) {
    const b = element("button", label, "secondary");
    b.onclick = async () => {
      b.disabled = true;
      try {
        await run(t.id);
      } catch (e) {
        if (!pickerCancelled(e))
          feedback.textContent = e instanceof Error ? e.message : "操作失败";
      } finally {
        b.disabled = false;
      }
    };
    buttons.append(b);
  }
  if (actions) {
    if (active(t.status)) button("取消", actions.cancel);
    if (t.status === "failed" || t.status === "interrupted")
      button(t.finalSize ? "重新保存" : "恢复 / 重试", actions.retry);
    if (!active(t.status)) button("清理任务", actions.clean);
  }
  panel.append(title, info, bar, meta);
  if (t.legacy) panel.append(element("p", "旧版任务重试时将重新验证资源。"));
  if (t.status === "interrupted")
    panel.append(element("p", "恢复时将校验缓存及可用的源站校验器。"));
  panel.append(feedback, buttons);
  return {
    panel,
    id: t.id,
    status: t.status,
    update(task: Task) {
      info.textContent =
        statuses[task.status] +
        " · " +
        progressText(task) +
        " 个分片 · " +
        (task.bytes / 1048576).toFixed(1) +
        " MiB";
      bar.max = task.total || 1;
      if (task.total) bar.value = task.done;
      else bar.removeAttribute("value");
    },
  };
}

export function createTaskDialog(actions?: TaskActions) {
  const dialog = element("dialog");
  dialog.setAttribute("aria-labelledby", "task-dialog-title");
  const header = element("header");
  const title = element("h2", "任务详情");
  title.id = "task-dialog-title";
  const close = element("button", "关闭", "secondary");
  close.onclick = () => dialog.close();
  header.append(title, close);
  const name = element("p", "", "dialog-resource");
  const body = element("div");
  dialog.append(header, name, body);
  document.body.append(dialog);
  let view: ReturnType<typeof taskPanel> | undefined;
  dialog.onclose = () => {
    if (dialog.open) return;
    view = undefined;
    body.replaceChildren();
  };
  function update(task: Task) {
    if (!view || view.id !== task.id || view.status !== task.status) {
      view = taskPanel(task, actions);
      body.replaceChildren(view.panel);
    }
    view.update(task);
  }
  return {
    show(url: string, task: Task) {
      name.textContent = playlistName(url);
      name.title = url;
      update(task);
      dialog.showModal();
    },
    sync(tasks: Task[]) {
      if (!dialog.open) return;
      const task = tasks.find((t) => t.id === view?.id);
      if (task) update(task);
      else dialog.close();
    },
  };
}
