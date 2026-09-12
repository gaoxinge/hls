import type { Variant } from "../domain/hls/model";
import type { Candidate } from "../shared/model";
export function element<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  text = "",
  className = "",
) {
  const el = document.createElement(tag);
  el.textContent = text;
  el.className = className;
  return el;
}
export async function send<T>(message: object): Promise<T> {
  const result = await chrome.runtime.sendMessage(message);
  if (!result?.ok) throw new Error(result?.error ?? "扩展后台暂不可用");
  return result.data;
}
export function resourceAddress(
  value: string,
  tag: "h3" | "small" = "h3",
  prefix = "",
) {
  const url = new URL(value);
  const address = element(
    tag,
    prefix + url.host + url.pathname,
    "resource-address",
  );
  address.title = address.textContent ?? "";
  return address;
}
export function candidateCard(
  candidate: Candidate,
  onStart: (
    quality: string,
    saveAs: boolean,
    selectedUrl?: string,
  ) => Promise<void>,
) {
  const card = element("article", "", "card");
  card.append(resourceAddress(candidate.url));
  const options = element("div", "", "actions");
  const quality = element("select");
  quality.setAttribute("aria-label", "清晰度策略");
  for (const [value, label] of [
    ["best", "最高可用清晰度"],
    ["lowest", "最低可用清晰度"],
  ]) {
    const o = element("option", label);
    o.value = value;
    quality.append(o);
  }
  const label = element("label", "另存为 ");
  const saveAs = element("input");
  saveAs.type = "checkbox";
  label.append(saveAs);
  const button = element("button", "下载");
  button.disabled = candidate.status < 200 || candidate.status >= 300;
  const error = element("p", "", "error");
  button.onclick = async () => {
    button.disabled = true;
    try {
      await onStart(
        quality.value === "lowest" ? "lowest" : "best",
        saveAs.checked,
        quality.value.startsWith("http") ? quality.value : undefined,
      );
      button.textContent = "已加入任务";
    } catch (e) {
      error.textContent = e instanceof Error ? e.message : "操作失败";
      button.disabled = false;
    }
  };
  const inspect = element("button", "查看清晰度", "secondary");
  inspect.onclick = async () => {
    inspect.disabled = true;
    try {
      const variants = await send<Variant[]>({
        type: "INSPECT_CANDIDATE",
        id: candidate.id,
      });
      for (const variant of variants) {
        const option = element(
          "option",
          variant.label +
            (variant.supported ? "" : "（" + variant.reason + "）"),
        );
        option.value = variant.url;
        option.disabled = !variant.supported;
        quality.append(option);
      }
      inspect.textContent = variants.length ? "已加载清晰度" : "单一媒体列表";
    } catch (e) {
      error.textContent = e instanceof Error ? e.message : "读取失败";
      inspect.disabled = false;
    }
  };
  options.append(quality, label, button, inspect);
  card.append(options, error);
  return card;
}
