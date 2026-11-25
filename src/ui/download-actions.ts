import {
  type Task,
  type VariantOption,
  type VisibleCandidate,
} from "../shared/model";
import { queueDownload } from "../application/start-download";
import { element, send } from "./common";
import { pickDownloadFile, pickerCancelled } from "./save-file-picker";
import { suggestDownloadName } from "../application/download-filename";
import type { ResourceRow } from "../application/resource-library";
import { playlistName } from "./resource-list";

function controls(
  variants: Pick<VariantOption, "supported" | "label" | "reason">[] | undefined,
  available: boolean,
  start: (index: number) => Promise<void>,
) {
  const wrap = element("div");
  const options = element("div", "", "actions");
  const quality = element("select");
  quality.setAttribute("aria-label", "下载档位");
  if (variants?.length) {
    variants.forEach((v, index) => {
      const option = new Option(
        v.supported ? v.label : v.label + "（" + v.reason + "）",
        String(index),
      );
      option.disabled = !v.supported;
      quality.add(option);
    });
    quality.selectedIndex = variants.findIndex((v) => v.supported);
  } else {
    quality.add(new Option("默认", "default"));
    quality.disabled = true;
  }
  const button = element("button", "下载");
  button.disabled = !available || quality.selectedIndex < 0;
  if (quality.selectedIndex < 0) button.title = "此播放列表没有可下载的档位";
  else if (!variants) quality.title = "暂未读取到档位，下载时将重新解析";
  const error = element("small", "", "error");
  error.setAttribute("role", "alert");
  button.onclick = async () => {
    button.disabled = true;
    error.textContent = "";
    try {
      await start(quality.selectedIndex);
    } catch (e) {
      if (!pickerCancelled(e))
        error.textContent = e instanceof Error ? e.message : "操作失败";
    } finally {
      button.disabled = false;
    }
  };
  options.append(quality, button);
  wrap.append(options, error);
  return wrap;
}

function candidateActions(
  root: VisibleCandidate,
  candidate: VisibleCandidate,
  onQueued: () => Promise<void>,
) {
  return controls(
    candidate.variants,
    candidate.status >= 200 && candidate.status < 300,
    async (index) => {
      const current = await send<{ id: string; url: string }>({
        type: "GET_DOWNLOAD_CANDIDATE",
        id: candidate.id,
      });
      const variant = candidate.variants?.[index];
      const file = await pickDownloadFile(
        await suggestDownloadName(current.url, variant?.url),
      );
      await queueDownload(
        current,
        file,
        variant && { url: variant.url, label: variant.label },
        root,
      );
      await onQueued();
    },
  );
}

function historyActions(row: ResourceRow, onQueued: () => Promise<void>) {
  const byVariant = new Map<string, Task>();
  for (const task of row.tasks) {
    const key = JSON.stringify([task.playlistUrl, task.chosenVariant?.url]);
    if (!byVariant.has(key)) byVariant.set(key, task);
  }
  const attempts = [...byVariant.values()];
  const options = attempts.map((t) => ({
    label:
      t.chosenVariant?.label ??
      (t.playlistUrl === row.url ? "默认" : playlistName(t.playlistUrl)),
    supported: true,
  }));
  const wrap = controls(
    options.length === 1 && !attempts[0].chosenVariant ? [] : options,
    true,
    async (index) => {
      const t = attempts[index];
      const file = await pickDownloadFile(
        await suggestDownloadName(t.playlistUrl, t.chosenVariant?.url),
      );
      await queueDownload(
        { id: t.candidateId, url: t.playlistUrl },
        file,
        t.chosenVariant,
        { url: row.url, contentTypes: t.contentTypes },
      );
      await onQueued();
    },
  );
  wrap.title = "历史资源，下载时将重新验证播放列表";
  return wrap;
}

export function downloadActions(
  row: ResourceRow,
  onQueued: () => Promise<void>,
) {
  const field = element("fieldset");
  field.setAttribute("aria-label", "下载操作");
  const root = row.candidate;
  if (!root) {
    field.append(historyActions(row, onQueued));
    return field;
  }
  field.append(candidateActions(root, root, onQueued));
  if (root.children?.length) {
    const more = element("details");
    more.append(element("summary", "查看已发现的子列表"));
    for (const child of root.children) {
      const name = element("small", playlistName(child.url));
      name.title = child.url;
      more.append(name, candidateActions(root, child, onQueued));
    }
    field.append(more);
  }
  return field;
}
