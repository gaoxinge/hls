import { prepareDownloads, useNativeDownloads } from "./downloads.mjs";
import { chromium } from "@playwright/test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createHash } from "node:crypto";
import assert from "node:assert/strict";
// Export-only fixtures exercise both persisted artifacts and pre-fix v2 caches.
const bytes = Buffer.concat(
  await Promise.all(
    ["init.mp4", ...Array.from({ length: 4 }, (_, i) => `segment${i}.m4s`)].map(
      (n) => readFile(`tests/fixtures/media/fmp4/${n}`),
    ),
  ),
);
const digest = createHash("sha256").update(bytes).digest("hex");
const profile = await mkdtemp(join(tmpdir(), "hls-export-"));
let context;
try {
  await prepareDownloads(profile);
  context = await chromium.launchPersistentContext(profile, {
    channel: "chromium",
    headless: true,
    args: [
      `--disable-extensions-except=${resolve(".output/chrome-mv3")}`,
      `--load-extension=${resolve(".output/chrome-mv3")}`,
      "--no-sandbox",
    ],
  });
  let [worker] = context.serviceWorkers();
  worker ??= await context.waitForEvent("serviceworker");
  const manager = await context.newPage();
  await manager.goto(
    `chrome-extension://${new URL(worker.url()).host}/manager.html`,
  );
  await useNativeDownloads(context, manager);
  const network = [];
  context.on("request", (req) => {
    if (/^https?:/.test(req.url())) network.push(req.url());
  });
  for (const mode of ["legacy", "artifact", "missing-plan"]) {
    await manager.evaluate(
      async ({ mode, data, digest }) => {
        const storageKey = mode === "artifact" ? "output.bin" : "video.ts";
        const root = await navigator.storage.getDirectory();
        const dir = await (
          await root.getDirectoryHandle(mode, { create: true })
        ).getDirectoryHandle("v2", { create: true });
        const writer = await (
          await dir.getFileHandle(storageKey, { create: true })
        ).createWritable();
        await writer.write(new Uint8Array(data));
        await writer.close();
        const req = indexedDB.open("hls-v1", 2);
        const db = await new Promise((resolve, reject) => {
          req.onsuccess = () => resolve(req.result);
          req.onerror = () => reject(req.error);
        });
        await new Promise((resolve, reject) => {
          const tx = db.transaction(
            ["tasks", "plans", "artifacts"],
            "readwrite",
          );
          tx.objectStore("tasks").put({
            id: mode,
            candidateId: mode,
            playlistUrl: "https://example.invalid/list.m3u8",
            schemaVersion: 2,
            status: "failed",
            createdAt: Date.now(),
            updatedAt: Date.now(),
            total: 4,
            done: 4,
            bytes: data.length,
            filename: `${mode}.ts`,
            quality: "best",
            saveAs: false,
            fingerprint: "fixture-plan",
            finalSize: data.length,
            finalDigest: digest,
            ...(mode === "artifact" ? { artifactId: `${mode}/main` } : {}),
          });
          if (mode !== "missing-plan")
            tx.objectStore("plans").put({
              id: mode,
              plan: {
                id: "fixture-plan",
                schemaVersion: 2,
                format: "mp4",
                playlistUrl: "https://example.invalid/list.m3u8",
                resources: [],
              },
            });
          if (mode === "artifact")
            tx.objectStore("artifacts").put({
              id: `${mode}/main`,
              taskId: mode,
              planId: "fixture-plan",
              schemaVersion: 1,
              formatId: "mp4",
              storageKey,
              baseName: mode,
              size: data.length,
              sha256: digest,
            });
          tx.oncomplete = resolve;
          tx.onerror = () => reject(tx.error);
        });
        db.close();
      },
      { mode, data: Array.from(bytes), digest },
    );
    await manager.reload();
    const card = manager
      .locator("#tasks .card")
      .filter({ has: manager.locator("h3", { hasText: `${mode}.ts` }) });
    await card.getByRole("button", { name: "重新保存", exact: true }).click();
    const deadline = Date.now() + 30000;
    let record;
    while (Date.now() < deadline) {
      record = await manager.evaluate(async (mode) => {
        const req = indexedDB.open("hls-v1", 2);
        const db = await new Promise((resolve) => {
          req.onsuccess = () => resolve(req.result);
        });
        const result = await new Promise((resolve) => {
          const r = db.transaction("tasks").objectStore("tasks").get(mode);
          r.onsuccess = () => resolve(r.result);
        });
        db.close();
        return result;
      }, mode);
      if (["completed", "failed"].includes(record.status)) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    if (mode === "missing-plan") {
      assert.equal(record.status, "failed");
      assert.match(record.error, /缺少格式/);
      assert.equal(record.downloadId, undefined);
      continue;
    }
    assert.equal(record.status, "completed", record.error);
    const [download] = await worker.evaluate(
      (id) => chrome.downloads.search({ id }),
      record.downloadId,
    );
    assert.ok(download.filename.endsWith(`/${mode}.mp4`));
    assert.equal(download.mime, "video/mp4");
    assert.deepEqual(await readFile(download.filename), bytes);
    const artifact = await manager.evaluate(async (id) => {
      const req = indexedDB.open("hls-v1", 2);
      const db = await new Promise((resolve) => {
        req.onsuccess = () => resolve(req.result);
      });
      const result = await new Promise((resolve) => {
        const r = db.transaction("artifacts").objectStore("artifacts").get(id);
        r.onsuccess = () => resolve(r.result);
      });
      db.close();
      return result;
    }, record.artifactId);
    assert.equal(artifact.formatId, "mp4");
    assert.equal(
      artifact.storageKey,
      mode === "legacy" ? "video.ts" : "output.bin",
    );
    assert.equal(artifact.download.filename, download.filename);
    assert.equal(artifact.download.mime, download.mime);
  }
  assert.deepEqual(network, [], "cached exports must not re-download media");
  console.log(
    "Export recovery: legacy/new MP4 caches, actual extension/MIME, persisted receipt, byte identity, missing-plan rejection, zero network requests passed",
  );
} finally {
  await context?.close();
  await rm(profile, { recursive: true, force: true });
}
