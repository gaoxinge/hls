import {
  createCandidateInspector,
  readCandidate,
  trimInspections,
} from "./candidate-inspection";
import {
  pageCandidates,
  selectVisibleCandidates,
  type ObservedCandidate,
} from "../detection/candidate-groups";
import { addCandidate, isHls, TTL } from "../detection/candidates";
import { active, safeError, type Task } from "../shared/model";
import { tasks, commands, commitCommand } from "../storage/db";

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
  const badge = async (s: State, tabId: number) => {
    const n = selectVisibleCandidates(s.candidates, tabId).length;
    await chrome.action
      .setBadgeText({ tabId, text: n ? String(n) : "" })
      .catch(() => {});
    await chrome.action
      .setBadgeBackgroundColor({ tabId, color: "#22745c" })
      .catch(() => {});
  };
  const inspect = createCandidateInspector();
  async function listCandidates(tabId?: number) {
    const snapshot = await enqueue(state);
    const pages = pageCandidates(snapshot.candidates, tabId);
    const pending = Array.from(pages.values())
      .filter((page) => page.length > 1)
      .flat()
      .filter(
        (c) =>
          c.status >= 200 &&
          c.status < 300 &&
          (!c.inspection || c.inspection.until <= Date.now()),
      );
    const deadline = Date.now() + 4000;
    const updates = new Map(
      await Promise.all(
        pending.map(async (c) => [c.id, await inspect(c, deadline)] as const),
      ),
    );
    return enqueue(async () => {
      const s = await state();
      const now = Date.now();
      s.candidates = s.candidates.filter((c) => now - c.lastSeen < TTL);
      // Navigation creates new candidate IDs. Only commit to a still-live observation.
      for (const c of s.candidates) {
        const result = updates.get(c.id);
        if (result && c.status >= 200 && c.status < 300) c.inspection = result;
      }
      trimInspections(s.candidates, now);
      await save(s);
      const tabs =
        tabId === undefined
          ? new Set(snapshot.candidates.map((c) => c.tabId))
          : [tabId];
      await Promise.all(Array.from(tabs, (id) => badge(s, id)));
      return selectVisibleCandidates(s.candidates, tabId, now);
    });
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
    await badge(s, d.tabId);
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
          await badge(s, d.tabId);
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
      if (message.type === "INSPECT_CANDIDATE") {
        const candidate = (await enqueue(state)).candidates.find(
          (c) => c.id === message.id && Date.now() - c.lastSeen < TTL,
        );
        if (!candidate) throw new Error("候选已失效");
        return (
          await readCandidate(candidate.url, new AbortController().signal)
        ).variants;
      }
      if (message.type === "START_TASK") {
        return enqueue(async () => {
          if (typeof message.id !== "string") throw new Error("候选无效");
          const candidate = (await state()).candidates.find(
            (c) => c.id === message.id && Date.now() - c.lastSeen < TTL,
          );
          if (!candidate || candidate.status < 200 || candidate.status >= 300)
            throw new Error("候选已失效，请刷新源网页");
          const receipt =
            typeof message.commandId === "string"
              ? await commands.get(message.commandId)
              : undefined;
          let task = receipt ? await tasks.get(receipt.taskId) : undefined;
          task ??= (await tasks.all()).find(
            (t) => t.candidateId === candidate.id && active(t.status),
          );
          if (!task) {
            const now = Date.now();
            task = {
              id: crypto.randomUUID(),
              candidateId: candidate.id,
              playlistUrl: candidate.url,
              status: "queued",
              createdAt: now,
              updatedAt: now,
              total: 0,
              done: 0,
              bytes: 0,
              filename: `HLS-${new Date(now).toISOString().replace(/[:.]/g, "-")}.ts`,
              quality: message.quality === "lowest" ? "lowest" : "best",
              saveAs: message.saveAs === true,
              selectedUrl:
                typeof message.selectedUrl === "string"
                  ? message.selectedUrl
                  : undefined,
            } satisfies Task;
            const next = task;
            await commitCommand(
              typeof message.commandId === "string"
                ? message.commandId
                : crypto.randomUUID(),
              task.id,
              "start",
              (current) => current ?? next,
            );
          }
          const managers = await chrome.runtime.getContexts({
            contextTypes: [chrome.runtime.ContextType.TAB],
          });
          const manager = managers.find(
            (context) =>
              context.documentUrl &&
              new URL(context.documentUrl).pathname.replace(/^\/+/, "") ===
                "manager.html",
          );
          if (manager && manager.tabId >= 0)
            await chrome.tabs.update(manager.tabId, { active: true });
          else
            await chrome.tabs.create({
              url: chrome.runtime.getURL("manager.html"),
            });
          return task.id;
        });
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
