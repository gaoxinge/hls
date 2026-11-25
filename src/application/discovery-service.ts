import {
  createCandidateInspector,
  needsInspection,
  trimInspections,
} from "./candidate-inspection";
import {
  selectVisibleCandidates,
  type ObservedCandidate,
} from "../detection/candidate-groups";
import { addCandidate, isHls, TTL } from "../detection/candidates";
import { safeError, type VisibleCandidate } from "../shared/model";
import {
  openResourceWindow,
  openDownloadManager,
} from "../adapters/chrome/extension-windows";

export function startBackground() {
  let queue = Promise.resolve();
  function enqueue<T>(fn: () => Promise<T>): Promise<T> {
    const result = queue.then(fn);
    queue = result.then(
      () => {},
      () => {},
    );
    return result;
  }
  const run = (fn: () => Promise<unknown>) => {
    void enqueue(fn).catch(() => console.warn("HLS 后台操作失败"));
    return undefined;
  };
  interface State {
    candidates: ObservedCandidate[];
    epochs: Record<string, string>;
    notices: Record<string, number>;
    requests: Record<
      string,
      { url?: string; epoch: string; tabId: number; redirects?: string[] }
    >;
  }
  const state = async (): Promise<State> => {
    const data = await chrome.storage.session.get("discovery");
    const s = data.discovery as State | undefined;
    return s ?? { candidates: [], epochs: {}, notices: {}, requests: {} };
  };
  const save = (s: State) => chrome.storage.session.set({ discovery: s });
  const badge = async (tabId: number, count: number) => {
    await chrome.action
      .setBadgeText({ tabId, text: count ? String(count) : "" })
      .catch(() => {});
    await chrome.action
      .setBadgeBackgroundColor({ tabId, color: "#22745c" })
      .catch(() => {});
  };
  const inspect = createCandidateInspector();
  const refreshing = new Map<number, Promise<VisibleCandidate[]>>();
  async function resolveTab(tabId: number): Promise<VisibleCandidate[]> {
    for (;;) {
      const snapshot = await enqueue(state);
      const now = Date.now();
      const pending = snapshot.candidates.filter(
        (c) =>
          c.tabId === tabId &&
          now - c.lastSeen < TTL &&
          needsInspection(c, now),
      );
      const deadline = now + 4000;
      const updates = new Map(
        await Promise.all(
          pending.map(async (c) => [c.id, await inspect(c, deadline)] as const),
        ),
      );
      const rows = await enqueue(async () => {
        const s = await state();
        const now = Date.now();
        s.candidates = s.candidates.filter((c) => now - c.lastSeen < TTL);
        const page = s.candidates.filter((c) => c.tabId === tabId);
        for (const c of page) {
          const result = updates.get(c.id);
          if (result && needsInspection(c, now)) c.inspection = result;
        }
        // A new possible parent arrived during a read. Inspect it before publishing.
        const more = page.some(
          (c) => needsInspection(c, now) && !updates.has(c.id),
        );
        trimInspections(s.candidates, now);
        await save(s);
        if (more) return;
        const visible = selectVisibleCandidates(s.candidates, tabId, now);
        await badge(tabId, visible.length);
        return visible;
      });
      if (rows) return rows;
    }
  }
  function refreshTab(tabId: number) {
    const current = refreshing.get(tabId);
    if (current) return current;
    const job = resolveTab(tabId).finally(() => refreshing.delete(tabId));
    refreshing.set(tabId, job);
    return job;
  }
  async function listCandidates(tabId?: number) {
    if (tabId !== undefined) return refreshTab(tabId);
    const snapshot = await enqueue(state);
    const tabs = new Set(snapshot.candidates.map((c) => c.tabId));
    return (await Promise.all(Array.from(tabs, refreshTab))).flat();
  }
  async function discover(d: chrome.webRequest.OnHeadersReceivedDetails) {
    if (
      d.tabId < 0 ||
      d.initiator?.startsWith(`chrome-extension://${chrome.runtime.id}`)
    )
      return;
    const s = await state();
    const tracked = s.requests[d.requestId];
    const mime =
      d.responseHeaders?.find((h) => h.name.toLowerCase() === "content-type")
        ?.value ?? "";
    if (!isHls(d.url, mime) && !(tracked?.url && isHls(tracked.url))) return;
    if (d.statusCode >= 300 && d.statusCode < 400) return;
    const epoch = s.epochs[d.tabId] ?? "initial";
    if (tracked && tracked.epoch !== epoch) return;
    const now = Date.now();
    const alreadySeen = s.candidates.some(
      (c) =>
        c.tabId === d.tabId &&
        c.epoch === epoch &&
        c.url === d.url &&
        now - c.lastSeen < TTL,
    );
    // Only the final URL is offered; a redirect does not produce duplicate candidates.
    s.candidates = addCandidate(s.candidates, {
      id: crypto.randomUUID(),
      tabId: d.tabId,
      epoch,
      url: d.url,
      aliases: tracked?.redirects,
      status: d.statusCode,
      firstSeen: now,
      lastSeen: now,
    });
    const noticeKey = `${d.tabId}:${epoch}`;
    const settings = await chrome.storage.local.get({
      notifications: true,
      cooldown: 60000,
    });
    const notify =
      !alreadySeen &&
      d.statusCode >= 200 &&
      d.statusCode < 300 &&
      now - (s.notices[noticeKey] ?? 0) >=
        Math.max(60000, Number(settings.cooldown) || 60000);
    if (notify) s.notices[noticeKey] = now;
    await save(s);
    const page = s.candidates.filter((c) => c.tabId === d.tabId);
    if (page.length > 1 && page.some((c) => needsInspection(c, now))) {
      // Manifest reads run outside the event queue so navigation and redirects can proceed.
      void refreshTab(d.tabId).catch(() => console.warn("HLS 清单归并失败"));
    } else {
      await badge(d.tabId, selectVisibleCandidates(page, d.tabId, now).length);
    }
    if (notify && settings.notifications)
      await chrome.notifications
        .create(`hls:${d.tabId}`, {
          type: "basic",
          iconUrl: chrome.runtime.getURL("/icon/128.png"),
          title: "发现 HLS 视频",
          message: "点击查看候选列表，确认后开始下载。",
        })
        .catch(() => {});
  }
  chrome.webRequest.onBeforeRequest.addListener(
    (d) =>
      run(async () => {
        if (
          d.tabId < 0 ||
          d.initiator?.startsWith(`chrome-extension://${chrome.runtime.id}`)
        )
          return;
        if (d.type !== "main_frame" && !isHls(d.url)) return;
        const s = await state();
        if (d.type === "main_frame" && !s.requests[d.requestId]) {
          s.epochs[d.tabId] = d.requestId;
          s.candidates = s.candidates.filter((c) => c.tabId !== d.tabId);
          for (const key of Object.keys(s.notices))
            if (key.startsWith(`${d.tabId}:`)) delete s.notices[key];
          await badge(d.tabId, 0);
        }
        if (isHls(d.url) || d.type === "main_frame") {
          s.requests[d.requestId] = {
            ...s.requests[d.requestId],
            url:
              s.requests[d.requestId]?.url ??
              (isHls(d.url) ? d.url : undefined),
            epoch:
              s.requests[d.requestId]?.epoch ?? s.epochs[d.tabId] ?? "initial",
            tabId: d.tabId,
          };
          const ids = Object.keys(s.requests);
          for (const id of ids.slice(0, Math.max(0, ids.length - 1000)))
            delete s.requests[id];
        }
        await save(s);
      }),
    { urls: ["http://*/*", "https://*/*"] },
  );
  chrome.webRequest.onHeadersReceived.addListener(
    (d) => run(() => discover(d)),
    { urls: ["http://*/*", "https://*/*"] },
    ["responseHeaders"],
  );
  chrome.webRequest.onBeforeRedirect.addListener(
    (d) => {
      if (
        d.tabId < 0 ||
        !isHls(d.url) ||
        d.initiator?.startsWith(chrome.runtime.getURL(""))
      )
        return;
      return run(async () => {
        const s = await state();
        const record = s.requests[d.requestId];
        if (!record || record.epoch !== (s.epochs[d.tabId] ?? "initial"))
          return;
        record.redirects = [...(record.redirects ?? []), d.url].slice(-8);
        await save(s);
      });
    },
    { urls: ["http://*/*", "https://*/*"] },
  );
  const finish = (d: chrome.webRequest.WebRequestDetails) =>
    run(async () => {
      const s = await state();
      if (!s.requests[d.requestId]) return;
      delete s.requests[d.requestId];
      await save(s);
    });
  chrome.webRequest.onCompleted.addListener(finish, {
    urls: ["http://*/*", "https://*/*"],
  });
  chrome.webRequest.onErrorOccurred.addListener(finish, {
    urls: ["http://*/*", "https://*/*"],
  });
  chrome.tabs.onRemoved.addListener((tabId) =>
    run(async () => {
      const s = await state();
      s.candidates = s.candidates.filter((c) => c.tabId !== tabId);
      delete s.epochs[tabId];
      for (const k of Object.keys(s.notices))
        if (k.startsWith(`${tabId}:`)) delete s.notices[k];
      for (const [k, r] of Object.entries(s.requests))
        if (r.tabId === tabId) delete s.requests[k];
      await save(s);
    }),
  );
  chrome.action.onClicked.addListener((tab) =>
    run(() => openResourceWindow(tab)),
  );
  chrome.notifications.onClicked.addListener((id) => {
    if (/^hls:\d+$/.test(id))
      void chrome.tabs.create({
        url: chrome.runtime.getURL(`/manager.html?tab=${id.slice(4)}`),
      });
  });
  chrome.runtime.onMessage.addListener((message, sender, respond) => {
    if (
      sender.id !== chrome.runtime.id ||
      !sender.url?.startsWith(chrome.runtime.getURL("")) ||
      !message ||
      typeof message.type !== "string"
    )
      return false;
    async function handle() {
      if (message.type === "LIST_CANDIDATES") {
        return listCandidates(
          Number.isInteger(message.tabId) ? message.tabId : undefined,
        );
      }
      if (message.type === "GET_DOWNLOAD_CANDIDATE") {
        return enqueue(async () => {
          if (typeof message.id !== "string") throw new Error("候选无效");
          const candidate = (await state()).candidates.find(
            (c) => c.id === message.id && Date.now() - c.lastSeen < TTL,
          );
          if (!candidate || candidate.status < 200 || candidate.status >= 300)
            throw new Error("候选已失效，请刷新源网页");
          return { id: candidate.id, url: candidate.url };
        });
      }
      if (message.type === "OPEN_MANAGER") {
        const tabId = Number.isInteger(message.tabId) ? message.tabId : -1;
        return enqueue(() =>
          openDownloadManager(tabId, sender.tab?.incognito ?? false),
        );
      }
      throw new Error("未知操作");
    }
    void handle().then(
      (data) => respond({ ok: true, data }),
      (error) => respond({ ok: false, error: safeError(error) }),
    );
    return true;
  });
}
