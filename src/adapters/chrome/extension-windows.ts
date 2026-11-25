export async function openResourceWindow(tab: chrome.tabs.Tab) {
  if (tab.id === undefined) return;
  const url = chrome.runtime.getURL(`/resources.html?tab=${tab.id}`);
  const [existing] = await chrome.runtime.getContexts({
    documentUrls: [url],
    incognito: tab.incognito,
  });
  if (existing && existing.windowId >= 0) {
    await chrome.windows.update(existing.windowId, { focused: true });
    return;
  }
  await chrome.windows.create({
    url,
    type: "popup",
    focused: true,
    incognito: tab.incognito,
    width: 760,
    height: 440,
  });
}

export async function openDownloadManager(tabId: number, incognito: boolean) {
  const url = chrome.runtime.getURL("/manager.html");
  const contexts = await chrome.runtime.getContexts({
    contextTypes: [chrome.runtime.ContextType.TAB],
    documentUrls: [url, url + "?tab=" + tabId],
    incognito,
  });
  const manager = contexts.find((c) => c.tabId >= 0 && c.windowId >= 0);
  if (manager) {
    await chrome.tabs.update(manager.tabId, { active: true });
    await chrome.windows.update(manager.windowId, { focused: true });
    return;
  }
  // The last focused window may be the resource picker, which closes after handoff.
  const source =
    tabId >= 0
      ? await chrome.tabs.get(tabId).catch(() => undefined)
      : undefined;
  const windows = await chrome.windows.getAll({ windowTypes: ["normal"] });
  const target =
    windows.find(
      (w) => w.id === source?.windowId && w.incognito === incognito,
    ) ?? windows.find((w) => w.incognito === incognito);
  if (target?.id !== undefined) {
    await chrome.tabs.create({ url, windowId: target.id, active: true });
    await chrome.windows.update(target.id, { focused: true });
  } else {
    await chrome.windows.create({
      url,
      type: "normal",
      focused: true,
      incognito,
    });
  }
}
