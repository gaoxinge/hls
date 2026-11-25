import { afterEach, expect, it, vi } from "vitest";
import {
  openResourceWindow,
  openDownloadManager,
} from "../../src/adapters/chrome/extension-windows";

function mockChrome() {
  const api = {
    runtime: {
      ContextType: { TAB: "TAB" },
      getURL: (path: string) => "chrome-extension://test" + path,
      getContexts: vi.fn().mockResolvedValue([]),
    },
    windows: {
      create: vi.fn(),
      update: vi.fn(),
      getAll: vi.fn().mockResolvedValue([{ id: 2, incognito: false }]),
    },
    tabs: {
      get: vi.fn().mockResolvedValue({ windowId: 2 }),
      create: vi.fn(),
      update: vi.fn(),
    },
  };
  vi.stubGlobal("chrome", api);
  return api;
}
afterEach(() => vi.unstubAllGlobals());

it("opens a persistent chooser for the clicked tab and reuses it on another click", async () => {
  const api = mockChrome();
  const tab = { id: 17, incognito: false } as chrome.tabs.Tab;
  await openResourceWindow(tab);
  expect(api.windows.create).toHaveBeenCalledWith(
    expect.objectContaining({
      url: "chrome-extension://test/resources.html?tab=17",
      type: "popup",
    }),
  );
  api.runtime.getContexts.mockResolvedValue([{ windowId: 3 }]);
  await openResourceWindow(tab);
  expect(api.windows.create).toHaveBeenCalledTimes(1);
  expect(api.windows.update).toHaveBeenCalledWith(3, { focused: true });
});

it("opens the runner in the source window or activates an existing runner", async () => {
  const api = mockChrome();
  await openDownloadManager(17, false);
  expect(api.tabs.create).toHaveBeenCalledWith({
    url: "chrome-extension://test/manager.html",
    windowId: 2,
    active: true,
  });
  api.runtime.getContexts.mockResolvedValue([{ windowId: 4, tabId: 21 }]);
  await openDownloadManager(17, false);
  expect(api.tabs.create).toHaveBeenCalledTimes(1);
  expect(api.tabs.update).toHaveBeenCalledWith(21, { active: true });
  expect(api.windows.update).toHaveBeenCalledWith(4, { focused: true });
});
