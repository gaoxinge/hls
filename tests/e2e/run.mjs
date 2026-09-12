import { prepareDownloads, useNativeDownloads } from "./downloads.mjs";
import { chromium } from "@playwright/test";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, mkdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { createCipheriv, createHash } from "node:crypto";
import assert from "node:assert/strict";

const extension = resolve(".output/chrome-mv3");
const profile = await mkdtemp(join(tmpdir(), "hls-e2e-"));
const hits = [];
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
const cdn = createServer((req, res) => {
  hits.push(req.url);
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (req.url.startsWith("/master")) {
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.end(
      "#EXTM3U\r\n#EXT-X-STREAM-INF:BANDWIDTH=1000,RESOLUTION=640x360\r\n/v/low.m3u8\r\n#EXT-X-STREAM-INF:BANDWIDTH=2000,RESOLUTION=1280x720\r\n/v/high.m3u8\r\n",
    );
  } else if (req.url.startsWith("/v/")) {
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    const base =
      "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:5\n";
    res.end(
      base +
        '#EXT-X-KEY:METHOD=AES-128,URI="../key?x=a=b,c"\n#EXTINF:1,\n../segments/5?token=yes\n#EXTINF:1,\n../segments/6?token=yes\n#EXT-X-ENDLIST',
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
  res.setHeader("Content-Type", "text/html");
  res.end(
    `<html><title>HLS fixture</title><button id="play">播放样例</button><script>document.querySelector('button').onclick=()=>fetch('http://127.0.0.1:${cdnPort}/master.m3u8?token=private');</script></html>`,
  );
});
await new Promise((r) => web.listen(0, "127.0.0.1", r));
let context;
try {
  await prepareDownloads(profile);
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
  let [worker] = context.serviceWorkers();
  if (!worker) worker = await context.waitForEvent("serviceworker");
  const id = new URL(worker.url()).host;
  const errors = [];
  context.on("page", (page) =>
    page.on("pageerror", (error) => errors.push(error.message)),
  );
  const source = await context.newPage();
  await source.goto(`http://127.0.0.1:${web.address().port}/`);
  await source.click("#play");
  const manager = await context.newPage();
  await manager.goto(`chrome-extension://${id}/manager.html`);
  await useNativeDownloads(context, manager);

  await manager.waitForSelector("#candidates button");
  assert.equal(
    hits.filter((x) => x.startsWith("/segments")).length,
    0,
    "no media before confirmation",
  );
  assert.equal(
    await manager.locator("#candidates .card").count(),
    1,
    "normal HTML is not a candidate",
  );
  // A repeat request must not create another row.
  await source.click("#play");
  await manager.click("#refresh");
  assert.equal(await manager.locator("#candidates .card").count(), 1);
  await manager
    .locator("#candidates button")
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
  const [download] = await worker.evaluate(() =>
    chrome.downloads.search({ state: "complete" }),
  );
  assert.ok(download?.filename, "download must really complete");
  assert.match(download.filename, /\.ts$/);
  const output = await readFile(download.filename);
  assert.deepEqual(
    output,
    Buffer.concat(media),
    "decrypted output matches reference bytes",
  );
  assert.ok(hits.includes("/v/high.m3u8"));
  assert.ok(!hits.includes("/v/low.m3u8"));
  assert.equal(hits.filter((x) => x.startsWith("/key")).length, 1);
  assert.equal(await manager.locator('[data-status="completed"]').count(), 1);
  assert.deepEqual(errors, [], "no page exceptions");
  await mkdir("test-results", { recursive: true });
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
      "no pre-confirm downloads",
      "deduplication",
      "highest variant",
      "AES sequence IV",
      "ordered OPFS merge",
      "Chrome export completed",
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
