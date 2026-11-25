import {
  installSaveFilePicker,
  readSavedFile,
  savedNames,
} from "./downloads.mjs";
import { chromium, expect } from "@playwright/test";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, mkdir, writeFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";
async function until(check, timeout = 30000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error("Timed out waiting for task state");
}
const profile = await mkdtemp(join(tmpdir(), "hls-resilience-"));
const small = await Promise.all(
  Array.from({ length: 12 }, (_, i) =>
    readFile("test-results/media/ts/segment" + i + ".ts"),
  ),
);
const large = process.env.HLS_LARGE === "1" || process.argv.includes("--large");
const largeParts = large
  ? await Promise.all(
      Array.from({ length: 128 }, (_, i) =>
        readFile("test-results/media/ts/segment" + i + ".ts"),
      ),
    )
  : [];
function largePart(index) {
  const media = largeParts[index];
  const padding = Buffer.alloc(8460000 - media.length, 255);
  for (let p = 0; p < padding.length; p += 188)
    padding.set([0x47, 0x1f, 0xff, 0x10], p);
  return Buffer.concat([media, padding]);
}
const hits = new Map();
const server = createServer((req, res) => {
  const url = new URL(req.url, "http://localhost");
  const path = url.pathname;
  hits.set(path, (hits.get(path) ?? 0) + 1);
  if (path === "/") {
    res.setHeader("Content-Type", "text/html");
    res.end(
      '<button id="play">play</button><script>document.querySelector("button").onclick=()=>fetch(location.hash.slice(1)||"/slow.m3u8")</script>',
    );
  } else if (path.endsWith(".m3u8")) {
    const name = path.slice(1, -5);
    const count = name === "large" ? 128 : 12;
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.end(
      "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n" +
        Array.from(
          { length: count },
          (_, i) => `#EXTINF:1,\n/${name}/${i}`,
        ).join("\n") +
        "\n#EXT-X-ENDLIST",
    );
  } else {
    const [, name, index] = path.split("/");
    if (name === "large" && large) res.end(largePart(Number(index)));
    else
      setTimeout(
        () => res.end(small[Number(index)]),
        Number(index) < 4 ? 20 : 1500,
      );
  }
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
let context;
let metricsTimer;
try {
  context = await chromium.launchPersistentContext(profile, {
    channel: "chromium",
    headless: true,
    acceptDownloads: true,
    downloadsPath: join(profile, "downloads"),
    args: [
      `--disable-extensions-except=${resolve(".output/chrome-mv3")}`,
      `--load-extension=${resolve(".output/chrome-mv3")}`,
      "--no-sandbox",
    ],
  });
  await installSaveFilePicker(context);
  let [worker] = context.serviceWorkers();
  if (!worker) worker = await context.waitForEvent("serviceworker");
  const id = new URL(worker.url()).host;
  const managerUrl = `chrome-extension://${id}/manager.html`;
  const source = await context.newPage();
  await source.goto(`http://127.0.0.1:${server.address().port}/`);
  await source.click("#play");
  let manager = await context.newPage();
  await manager.goto(managerUrl);
  await manager.waitForSelector("#resources fieldset button");
  const tiers = manager.getByRole("combobox", { name: "下载档位" });
  assert.deepEqual(await tiers.locator("option").allTextContents(), ["默认"]);
  assert.ok(await tiers.isDisabled());
  await manager
    .locator("#resources fieldset button")
    .filter({ hasText: /^下载$/ })
    .click();
  await until(
    async () =>
      parseInt(
        await manager
          .locator("#resources .progress-link")
          .first()
          .textContent(),
      ) >= 4,
  );
  const taskId = await manager
    .locator("tr[data-task-id]")
    .first()
    .getAttribute("data-task-id");
  const before = Array.from({ length: 4 }, (_, i) => hits.get(`/slow/${i}`));
  await manager.close();
  manager = await context.newPage();
  await manager.goto(managerUrl);
  await manager.waitForSelector(
    `[data-task-id="${taskId}"][data-status="interrupted"]`,
  );
  await manager.evaluate(async (id) => {
    const root = await navigator.storage.getDirectory();
    const dir = await (
      await root.getDirectoryHandle(id)
    ).getDirectoryHandle("v2");
    const handle = await dir.getFileHandle("1.part");
    const bytes = new Uint8Array(await (await handle.getFile()).arrayBuffer());
    bytes[bytes.length - 1] ^= 1;
    const writer = await handle.createWritable();
    await writer.write(bytes);
    await writer.close();
  }, taskId);
  await manager.locator(`tr[data-task-id="${taskId}"] .progress-link`).click();
  await manager
    .locator(`dialog [data-task-id="${taskId}"]`)
    .getByRole("button", { name: "恢复 / 重试" })
    .click();
  await manager.waitForSelector(
    `[data-task-id="${taskId}"][data-status="completed"]`,
    { timeout: 60000 },
  );
  assert.deepEqual(
    Array.from({ length: 4 }, (_, i) => hits.get(`/slow/${i}`)),
    before.map((n, i) => (i === 1 ? n + 1 : n)),
    "valid committed segments reused; same-size corruption redownloaded",
  );
  await manager.getByRole("button", { name: "关闭" }).click();
  const [name] = await savedNames(manager);
  assert.match(name, /\.ts$/);
  assert.deepEqual(
    await readFile(await readSavedFile(manager, name, profile)),
    Buffer.concat(small),
  );
  // Queue a new candidate, then cancel its owner from the same manager.
  await source.goto(`http://127.0.0.1:${server.address().port}/#/cancel.m3u8`);
  await source.click("#play");
  await manager.click("#refresh");
  await manager
    .locator("#resources tbody tr")
    .filter({ hasText: "cancel.m3u8" })
    .getByRole("button", { name: "下载", exact: true })
    .click();
  await manager.waitForSelector('[data-status="downloading"]');
  assert.ok(await manager.locator(".activity-hint").isVisible());
  const observer = await context.newPage();
  await observer.goto(managerUrl);
  await observer.locator('[data-status="downloading"] .progress-link').click();
  await observer
    .getByRole("dialog")
    .getByRole("button", { name: "取消", exact: true })
    .click();
  await manager.waitForSelector('[data-status="cancelled"]');
  await observer.close();
  assert.equal((await savedNames(manager)).length, 1, "cancel does not export");
  if (!large) {
    const row = manager.getByRole("row").filter({ hasText: "cancel.m3u8" });
    const cancelledId = await row.getAttribute("data-task-id");
    await row.getByRole("button", { name: "下载", exact: true }).click();
    await expect(row).not.toHaveAttribute("data-task-id", cancelledId);
    const currentId = await row.getAttribute("data-task-id");
    await row.locator(".progress-link").click();
    const dialog = manager.getByRole("dialog");
    const panel = dialog.locator(".task-panel");
    await expect(panel).toHaveCount(1);
    await expect(panel).toHaveAttribute("data-task-id", currentId);
    await expect(panel).toContainText("已下载", { timeout: 60000 });
    await expect(dialog.getByRole("progressbar")).toHaveCount(1);
    await dialog.getByRole("button", { name: "关闭" }).click();
  }
  const cdp = await context.newCDPSession(manager);
  const version = await cdp.send("Browser.getVersion");
  const result = {
    browser: version.product,
    checks: [
      "plain TS",
      "page-close interruption",
      "resume revalidates and reuses segments",
      "cross-page durable cancel prevents export",
      "same-size corrupted cache is redownloaded",
    ],
  };
  if (large) {
    let peakHeap = 0,
      peakStorage = 0,
      peakBackingStorage = 0,
      peakBrowserRss = 0;
    await cdp.send("Performance.enable");
    metricsTimer = setInterval(() => {
      void cdp
        .send("Runtime.getHeapUsage")
        .then((v) => {
          peakBackingStorage = Math.max(
            peakBackingStorage,
            v.backingStorageSize ?? 0,
          );
        })
        .catch(() => {});
      void cdp
        .send("Performance.getMetrics")
        .then(({ metrics }) => {
          peakHeap = Math.max(
            peakHeap,
            metrics.find((m) => m.name === "JSHeapUsedSize")?.value ?? 0,
          );
        })
        .catch(() => {});
      void manager
        .evaluate(() => navigator.storage.estimate())
        .then((v) => {
          peakStorage = Math.max(peakStorage, v.usage ?? 0);
        })
        .catch(() => {});
      if (process.platform === "linux") {
        try {
          const lines = execFileSync("ps", ["-eo", "pid,ppid,rss,args"], {
            encoding: "utf8",
          })
            .trim()
            .split("\n")
            .slice(1)
            .map((line) => {
              const m = line.trim().match(/^(\d+)\s+(\d+)\s+(\d+)\s+(.*)$/);
              return m
                ? { pid: +m[1], ppid: +m[2], rss: +m[3], args: m[4] }
                : null;
            })
            .filter(Boolean);
          const pids = new Set(
            lines
              .filter((p) => p.args.includes(`--user-data-dir=${profile}`))
              .map((p) => p.pid),
          );
          for (let n = 0; n < 5; n++)
            for (const p of lines) if (pids.has(p.ppid)) pids.add(p.pid);
          peakBrowserRss = Math.max(
            peakBrowserRss,
            lines
              .filter((p) => pids.has(p.pid))
              .reduce((a, p) => a + p.rss * 1024, 0),
          );
        } catch {
          /* RSS metric unavailable on this platform. */
        }
      }
    }, 500);
    await source.goto(`http://127.0.0.1:${server.address().port}/#/large.m3u8`);
    await source.click("#play");
    await manager.click("#refresh");
    await manager
      .locator("#resources tbody tr")
      .filter({ hasText: "large.m3u8" })
      .getByRole("button", { name: "下载", exact: true })
      .click();
    await until(
      async () =>
        (await manager.locator('[data-status="completed"]').count()) === 2,
      300000,
    );
    clearInterval(metricsTimer);
    metricsTimer = undefined;
    await manager
      .getByRole("row")
      .filter({ hasText: "large.m3u8" })
      .locator(".progress-link")
      .click();
    const largeName = await manager.locator("dialog h3").first().innerText();
    const filename = await readSavedFile(manager, largeName, profile);
    assert.match(filename, /\.ts$/);
    const expected = createHash("sha256");
    for (let i = 0; i < 128; i++) expected.update(largePart(i));
    const actual = createHash("sha256");
    for await (const chunk of createReadStream(filename)) actual.update(chunk);
    assert.equal(actual.digest("hex"), expected.digest("hex"));
    const mediaInfo = JSON.parse(
      execFileSync(
        process.env.FFPROBE ?? "ffprobe",
        [
          "-v",
          "error",
          "-count_frames",
          "-show_entries",
          "stream=codec_type,nb_read_frames",
          "-of",
          "json",
          filename,
        ],
        { encoding: "utf8" },
      ),
    );
    assert.equal(
      mediaInfo.streams.find((s) => s.codec_type === "video").nb_read_frames,
      "3200",
    );
    execFileSync(
      process.env.FFMPEG ?? "ffmpeg",
      ["-v", "error", "-i", filename, "-f", "null", "-"],
      { stdio: "pipe" },
    );
    Object.assign(result, {
      decoded: true,
      streams: mediaInfo.streams,
      largeBytes: 8460000 * 128,
      peakJsHeapBytes: peakHeap,
      peakBackingStorageBytes: peakBackingStorage,
      peakOpfsUsageBytes: peakStorage,
      peakBrowserRssBytes: peakBrowserRss,
    });
    result.checks.push(
      "1 GiB+ sequential merge and real export",
      "large file SHA-256 equality",
    );
  }
  await mkdir("test-results", { recursive: true });
  await writeFile(
    large ? "test-results/large.json" : "test-results/resilience.json",
    JSON.stringify(result, null, 2),
  );
  console.log(JSON.stringify(result, null, 2));
} catch (error) {
  for (const page of context?.pages() ?? [])
    if (page.url().includes("manager.html"))
      console.error(
        await page
          .locator("body")
          .innerText()
          .catch(() => ""),
      );
  throw error;
} finally {
  if (metricsTimer) clearInterval(metricsTimer);
  await context?.close();
  await new Promise((r) => server.close(r));
  await rm(profile, { recursive: true, force: true });
}
