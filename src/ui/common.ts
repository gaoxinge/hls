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
