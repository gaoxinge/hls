import "./style.css";
import { candidateCard, element, send } from "./common";
import type { Candidate } from "../shared/model";
document.querySelector<HTMLButtonElement>("#manager")!.onclick = () => {
  void chrome.tabs.create({ url: chrome.runtime.getURL("/manager.html") });
};
async function refresh() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  const list = await send<Candidate[]>({
    type: "LIST_CANDIDATES",
    tabId: tab?.id ?? -1,
  });
  const target = document.querySelector("#candidates")!;
  target.replaceChildren(
    ...list.map((c) =>
      candidateCard(c, async (quality, saveAs, selectedUrl) => {
        await send({
          type: "START_TASK",
          id: c.id,
          quality,
          saveAs,
          selectedUrl,
          commandId: crypto.randomUUID(),
        });
      }),
    ),
  );
  if (!list.length)
    target.append(element("p", "尚未发现 HLS 播放列表", "empty"));
}
void refresh().catch((e) => {
  document.querySelector("#candidates")!.textContent = String(e);
});
