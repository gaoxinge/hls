import { installSaveFilePicker, readSavedFile } from "./downloads.mjs";
import { chromium, expect } from "@playwright/test";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, mkdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { createCipheriv, createHash } from "node:crypto";
import assert from "node:assert/strict";

const extension = resolve(".output/chrome-mv3");
const profile = await mkdtemp(join(tmpdir(), "hls-e2e-"));
const hits = [];
const requestMetadata = [];
const media = await Promise.all(
  [0, 1].map((i) => readFile("tests/fixtures/media/ts/segment" + i + ".ts")),
);
const key = Buffer.alloc(16, 3);
function encrypted(index) {
  const iv = Buffer.alloc(16);
  iv.writeUInt32BE(index, 12);
  const c = createCipheriv("aes-128-cbc", key, iv);
  return Buffer.concat([c.update(media[index - 5]), c.final()]);
}
let cdnPort;
let releaseInspection;
const inspectionGate = new Promise((resolve) => {
  releaseInspection = resolve;
});
let inspectingMaster = false;
const cdn = createServer(async (req, res) => {
  hits.push(req.url);
  const fromSource =
    req.headers.origin?.startsWith("http://127.0.0.1:") === true;
  requestMetadata.push({
    fromSource,
    hasCookie: Boolean(req.headers.cookie),
    hasReferrer: Boolean(req.headers.referer),
  });
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (req.url.startsWith("/master")) {
    if (!fromSource) {
      inspectingMaster = true;
      await inspectionGate;
    }
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.end(
      '#EXTM3U\r\n#EXT-X-STREAM-INF:BANDWIDTH=1000,RESOLUTION=640x360\r\n/v/low.m3u8\r\n#EXT-X-STREAM-INF:BANDWIDTH=2000,RESOLUTION=1280x720\r\nvideo.m3u8\r\n#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH=500,URI="preview.m3u8"\r\n',
    );
  } else if (req.url === "/video.m3u8") {
    res.writeHead(302, { Location: "/v/high.m3u8" });
    res.end();
  } else if (req.url === "/preview.m3u8") {
    res.end(
      "#EXTM3U\n#EXT-X-VERSION:4\n#EXT-X-TARGETDURATION:1\n#EXT-X-I-FRAMES-ONLY\n#EXTINF:1,\n/segments/iframe\n#EXT-X-ENDLIST",
    );
  } else if (req.url.startsWith("/v/")) {
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    const base =
      "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:5\n";
    res.end(
      base +
        '#EXT-X-KEY:METHOD=AES-128,URI="../key?x=a=b,c"\n#EXTINF:1,\n../segments/5?fixture=segment\n#EXTINF:1,\n../segments/6?fixture=segment\n#EXT-X-ENDLIST',
    );
  } else if (req.url.startsWith("/key")) res.end(key);
  else if (req.url.startsWith("/segments/")) {
    const index = Number(req.url.split("/")[2].split("?")[0]);
    setTimeout(() => res.end(encrypted(index)), index === 5 ? 100 : 0);
  } else {
    res.statusCode = 404;
    res.end();
  }
});
await new Promise((r) => cdn.listen(0, "127.0.0.1", r));
cdnPort = cdn.address().port;
const web = createServer((req, res) => {
  if (req.url.startsWith("/ordinary-redirect")) {
    res.writeHead(302, { Location: "/landing" });
    res.end();
    return;
  }
  res.setHeader("Content-Type", "text/html");
  res.end(
    `<html><title>HLS fixture</title><button id="play">播放样例</button><script>document.querySelector('button').onclick=()=>fetch('http://127.0.0.1:${cdnPort}/master.m3u8?fixture=master');</script></html>`,
  );
});
await new Promise((r) => web.listen(0, "127.0.0.1", r));
let context;
try {
  context = await chromium.launchPersistentContext(profile, {
    channel: "chromium",
    headless: true,
    acceptDownloads: true,
    downloadsPath: join(profile, "downloads"),
    args: [
      `--disable-extensions-except=${extension}`,
      `--load-extension=${extension}`,
      "--no-sandbox",
    ],
  });
  await installSaveFilePicker(context);
  let [worker] = context.serviceWorkers();
  if (!worker) worker = await context.waitForEvent("serviceworker");
  const id = new URL(worker.url()).host;
  const errors = [];
  context.on("page", (page) =>
    page.on("pageerror", (error) => errors.push(error.message)),
  );
  const source = await context.newPage();
  await source.goto(`http://127.0.0.1:${web.address().port}/`);
  await context.addCookies([
    {
      name: "fixture_session",
      value: "synthetic",
      url: `http://127.0.0.1:${cdnPort}`,
    },
  ]);
  await worker.evaluate(() => {
    globalThis.recordedOrdinaryRedirect = false;
    chrome.storage.onChanged.addListener((changes, area) => {
      if (
        area === "session" &&
        JSON.stringify(changes.discovery?.newValue ?? {}).includes(
          "/ordinary-redirect",
        )
      )
        globalThis.recordedOrdinaryRedirect = true;
    });
  });
  await source.evaluate(() =>
    fetch("/ordinary-redirect?example=local").then((r) => r.text()),
  );
  const sourceTab = await worker.evaluate(
    async () =>
      (await chrome.tabs.query({ active: true, currentWindow: true }))[0].id,
  );
  const badge = () =>
    worker.evaluate(
      (tabId) => chrome.action.getBadgeText({ tabId }),
      sourceTab,
    );
  const beforeInspection = requestMetadata.length;
  const observe = (path) =>
    source.evaluate(
      ([port, path]) =>
        fetch("http://127.0.0.1:" + port + "/" + path).then((r) => r.text()),
      [cdnPort, path],
    );
  await observe("video.m3u8");
  await expect.poll(badge).toBe("1");
  await source.click("#play");
  await expect.poll(() => inspectingMaster).toBe(true);
  await observe("preview.m3u8");
  assert.equal(
    await badge(),
    "1",
    "do not publish a raw count during inspection",
  );
  releaseInspection();
  await expect
    .poll(() =>
      worker.evaluate(async (tabId) => {
        const { discovery } = await chrome.storage.session.get("discovery");
        return discovery.candidates.filter(
          (c) =>
            c.tabId === tabId &&
            c.inspection?.kind &&
            c.inspection.kind !== "unknown",
        ).length;
      }, sourceTab),
    )
    .toBe(3);
  assert.equal(await badge(), "1", "group before opening the extension");
  const beforeOpen = requestMetadata.length;
  let manager = await context.newPage();
  await manager.goto(`chrome-extension://${id}/manager.html`);

  await manager.waitForSelector("#resources fieldset button");
  await expect(manager.locator(".activity-hint")).toBeHidden();
  const formats = manager.getByText("支持的格式", { exact: true });
  await formats.click();
  await expect(manager.locator(".preferences details p")).toBeVisible();
  await formats.click();
  assert.equal(
    hits.filter((x) => x.startsWith("/segments")).length,
    0,
    "no media before confirmation",
  );
  assert.equal(
    await manager.locator("#resources tbody tr").count(),
    1,
    "only the master remains visible",
  );
  assert.match(
    await manager.locator("#resources .playlist-name").innerText(),
    /master\.m3u8$/,
  );
  assert.deepEqual(await manager.getByRole("columnheader").allTextContents(), [
    "m3u8名称",
    "类型",
    "状态",
    "进度",
    "操作",
  ]);
  await expect(manager.locator(".playlist-name")).toHaveAttribute(
    "title",
    `http://127.0.0.1:${cdnPort}/master.m3u8?fixture=master`,
  );
  await expect(
    manager.getByRole("cell", { name: "视频", exact: true }),
  ).toHaveCount(1);
  assert.equal(await badge(), "1");
  const tiers = manager.getByRole("combobox", { name: "下载档位" });
  assert.deepEqual(await tiers.locator("option").allTextContents(), [
    "720p",
    "360p",
  ]);
  assert.equal(await tiers.locator("option:checked").innerText(), "720p");
  assert.equal(
    await manager.getByRole("button", { name: "查看清晰度" }).count(),
    0,
  );
  assert.equal(
    await worker.evaluate(() => globalThis.recordedOrdinaryRedirect),
    false,
  );
  assert.equal(
    requestMetadata.length,
    beforeOpen,
    "opening uses the discovery inspection cache",
  );
  const inspectionRequests = requestMetadata
    .slice(beforeInspection)
    .filter((r) => !r.fromSource);
  assert.ok(inspectionRequests.length > 0);
  assert.ok(inspectionRequests.every((r) => !r.hasCookie && !r.hasReferrer));
  assert.ok(!hits.some((x) => x.startsWith("/key") || x.startsWith("/v/low")));
  const highReads = hits.filter((x) => x === "/v/high.m3u8").length;
  // A repeat request must not create another row.
  await source.click("#play");
  await manager.click("#refresh");
  assert.equal(await manager.locator("#resources tbody tr").count(), 1);
  const startButton = manager
    .locator("#resources")
    .getByRole("button", { name: "下载", exact: true });
  await expect(manager.getByText("另存为")).toHaveCount(0);
  await manager.evaluate(() => {
    window.cancelNextPicker = true;
  });
  await startButton.click();
  await expect(startButton).toBeEnabled();
  await expect(manager.locator("tr[data-task-id]")).toHaveCount(0);
  assert.match(
    await manager.evaluate(() => window.suggestedName),
    /^HLS-.*\.ts$/,
  );
  await manager.evaluate(() => {
    window.nextSaveName = "课程.第一节.ts";
  });
  assert.ok(
    !hits.some((x) => x.startsWith("/key") || x.startsWith("/segments")),
  );
  await manager
    .locator("#resources fieldset button")
    .filter({ hasText: /^下载$/ })
    .click();
  try {
    await manager.waitForSelector('[data-status="completed"]', {
      timeout: 60000,
    });
  } catch (error) {
    console.error(await manager.locator("body").innerText());
    console.error(errors);
    throw error;
  }
  const progressButton = manager.getByRole("button", {
    name: "2 / 2",
    exact: true,
  });
  await progressButton.click();
  let dialog = manager.getByRole("dialog", { name: "任务详情" });
  await expect(dialog.getByRole("progressbar")).toHaveAttribute("value", "2");
  const firstId = await dialog
    .locator(".task-panel")
    .getAttribute("data-task-id");
  const name = await dialog.locator("h3").innerText();
  await manager.keyboard.press("Escape");
  assert.equal(name, "课程.第一节.ts");
  const output = await readFile(await readSavedFile(manager, name, profile));
  assert.deepEqual(
    output,
    Buffer.concat(media),
    "decrypted output matches reference bytes",
  );
  assert.ok(hits.filter((x) => x === "/v/high.m3u8").length > highReads);
  assert.ok(!hits.includes("/v/low.m3u8"));
  assert.equal(hits.filter((x) => x.startsWith("/key")).length, 1);
  assert.equal(await manager.locator('[data-status="completed"]').count(), 1);
  assert.deepEqual(errors, [], "no page exceptions");
  // No manager is running when the user starts this download. Exercise a real
  // Chrome popup-type window, including loss of focus while the chooser is open.
  await manager.close();
  const opened = context.waitForEvent("page");
  await worker.evaluate(async (tabId) => {
    if (
      chrome.runtime.getManifest().action.default_popup ||
      !chrome.action.onClicked.hasListeners()
    )
      throw new Error("The toolbar must open a persistent window");
    await chrome.windows.create({
      type: "popup",
      url: chrome.runtime.getURL(`/resources.html?tab=${tabId}`),
    });
  }, sourceTab);
  const popup = await opened;
  await popup.setViewportSize({ width: 720, height: 460 });
  await popup
    .getByRole("combobox", { name: "下载档位" })
    .selectOption({ label: "360p" });
  await expect(popup.getByRole("button", { name: "下载管理" })).toBeVisible();
  await mkdir("test-results", { recursive: true });
  await popup.locator("body").screenshot({ path: "test-results/popup.png" });
  const startFromWindow = popup.getByRole("button", {
    name: "下载",
    exact: true,
  });
  await popup.evaluate(() => {
    window.cancelNextPicker = true;
  });
  await startFromWindow.click();
  await expect(startFromWindow).toBeEnabled();
  assert.ok(!context.pages().some((p) => p.url().endsWith("/manager.html")));
  await popup.evaluate(() => {
    window.nextSaveName = "资源窗口.360p.ts";
    const pick = window.showSaveFilePicker;
    window.showSaveFilePicker = async (options) => {
      const handle = await pick(options);
      await new Promise((resolve) => {
        window.confirmSelection = resolve;
      });
      return handle;
    };
  });
  await popup
    .locator("#resources")
    .getByRole("button", { name: "下载", exact: true })
    .click();
  await expect
    .poll(() => popup.evaluate(() => Boolean(window.confirmSelection)))
    .toBe(true);
  await source.bringToFront();
  const managerOpened = context.waitForEvent("page");
  const windowClosed = popup.waitForEvent("close");
  await popup.evaluate(() => window.confirmSelection());
  manager = await managerOpened;
  await manager.waitForURL(`chrome-extension://${id}/manager.html`);
  await windowClosed;
  dialog = manager.getByRole("dialog", { name: "任务详情" });
  const row = manager.locator("tr[data-task-id]");
  await expect(row).toHaveCount(1);
  await expect(row).not.toHaveAttribute("data-task-id", firstId);
  const secondId = await row.getAttribute("data-task-id");
  await row.locator(".progress-link").click();
  await expect(dialog.locator(".task-panel")).toHaveCount(1);
  await expect(dialog.locator(".task-panel")).toHaveAttribute(
    "data-task-id",
    secondId,
  );
  await expect(dialog.getByText("360p", { exact: true })).toBeVisible();
  await expect(dialog.locator("h3")).toHaveText("资源窗口.360p.ts");
  await expect(
    dialog.locator(".task-panel p").filter({ hasText: "已下载" }),
  ).toHaveCount(1, { timeout: 60000 });
  await dialog.getByRole("button", { name: "关闭" }).click();
  await expect(manager.locator("tbody tr")).toHaveCount(1);
  await observe("v/standalone.m3u8");
  await expect.poll(badge).toBe("2");
  await manager.getByRole("button", { name: "刷新资源" }).click();
  await expect(manager.locator("tbody tr")).toHaveCount(2);
  await source.close();
  await manager.getByRole("button", { name: "刷新资源" }).click();
  const history = manager
    .locator("fieldset")
    .getByTitle("历史资源，下载时将重新验证播放列表");
  await expect(history).toBeVisible();
  await history.getByRole("combobox").selectOption({ label: "720p" });
  await history.getByRole("button", { name: "下载", exact: true }).click();
  await expect(row).not.toHaveAttribute("data-task-id", secondId);
  await row.locator(".progress-link").click();
  await expect(dialog.locator(".task-panel")).toHaveCount(1);
  await expect(dialog.getByText("720p", { exact: true })).toBeVisible();
  await expect(
    dialog.locator(".task-panel p").filter({ hasText: "已下载" }),
  ).toHaveCount(1, { timeout: 60000 });
  await dialog.getByRole("button", { name: "关闭" }).click();
  assert.ok(
    hits.includes("/v/low.m3u8"),
    "explicit tier overrides automatic highest",
  );
  await manager.screenshot({
    path: "test-results/manager.png",
    fullPage: true,
  });
  const version = await (
    await context.newCDPSession(manager)
  ).send("Browser.getVersion");
  const result = {
    browser: version.product,
    bytes: output.length,
    sha256: createHash("sha256").update(output).digest("hex"),
    requests: hits.map((x) => x.split("?")[0]),
    checks: [
      "cross-origin discovery",
      "badge groups a child-first variant and I-frame before opening, and counts an independent playlist separately",
      "no pre-confirm downloads",
      "deduplication",
      "highest variant",
      "automatically loaded tier options and explicit variant download",
      "AES sequence IV",
      "ordered OPFS merge",
      "cancelling file selection creates no task or media traffic",
      "renamed TS output matches expected bytes and the chosen name reaches the manager",
    ],
  };
  await writeFile("test-results/e2e.json", JSON.stringify(result, null, 2));
  console.log(JSON.stringify(result, null, 2));
} finally {
  await context?.close();
  await new Promise((r) => cdn.close(r));
  await new Promise((r) => web.close(r));
  await rm(profile, { recursive: true, force: true });
}
