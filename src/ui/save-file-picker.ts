import { outputFormats } from "../domain/output/artifact";
import { element } from "./common";

declare global {
  interface Window {
    showSaveFilePicker(options: {
      id: string;
      suggestedName: string;
      excludeAcceptAllOption: boolean;
      types: { description: string; accept: Record<string, string[]> }[];
    }): Promise<FileSystemFileHandle>;
  }
}

export const pickerCancelled = (error: unknown) =>
  error instanceof DOMException && error.name === "AbortError";

export async function pickDownloadFile(suggestedName: string) {
  const format = suggestedName.toLowerCase().endsWith(".mp4") ? "mp4" : "ts";
  const extension = "." + format;
  const open = () =>
    window.showSaveFilePicker({
      id: "hls-download",
      suggestedName,
      excludeAcceptAllOption: true,
      types: [
        {
          description: format === "ts" ? "MPEG-TS" : "MP4",
          accept: { [outputFormats[format].mimeType]: [extension] },
        },
      ],
    });
  const handle = await (navigator.userActivation.isActive
    ? open()
    : withFreshClick(suggestedName, open));
  if (
    !handle.name.toLowerCase().endsWith(extension) ||
    handle.name.length <= extension.length
  )
    throw new Error(`请使用带 ${extension} 后缀的文件名`);
  return handle;
}

// Slow manifest requests can outlive the original click's transient activation.
function withFreshClick(
  name: string,
  open: () => Promise<FileSystemFileHandle>,
) {
  return new Promise<FileSystemFileHandle>((resolve, reject) => {
    const dialog = element("dialog");
    dialog.setAttribute("aria-label", "选择保存位置");
    const choose = element("button", "选择保存位置");
    const cancel = element("button", "取消", "secondary");
    choose.onclick = () => {
      resolve(open());
      dialog.close();
    };
    cancel.onclick = () => dialog.close();
    dialog.onclose = () => {
      dialog.remove();
      reject(new DOMException("已取消", "AbortError"));
    };
    const actions = element("div", "", "actions");
    actions.append(choose, cancel);
    dialog.append(element("p", name), actions);
    document.body.append(dialog);
    dialog.showModal();
  });
}
