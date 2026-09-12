import { prepareDownloads, useNativeDownloads } from "./downloads.mjs";
import { chromium } from "@playwright/test";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, writeFile, mkdir } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createHash, createCipheriv } from "node:crypto";
import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";
const large = process.argv.includes("--large");
const folder = large ? "test-results/media/fmp4" : "tests/fixtures/media/fmp4";
const init = await readFile(`${folder}/init.mp4`),
  parts = await Promise.all(
    Array.from({ length: large ? 128 : 4 }, (_, i) =>
      readFile(`${folder}/segment${i}.m4s`),
    ),
  );
const plain = Buffer.concat([init, ...parts]);
const key = Buffer.alloc(16, 9);
function encrypt(bytes, sequence) {
  const iv = Buffer.alloc(16);
  iv.writeUInt32BE(sequence, 12);
  const cipher = createCipheriv("aes-128-cbc", key, iv);
  return Buffer.concat([cipher.update(bytes), cipher.final()]);
}
const encrypted = [encrypt(init, 99), ...parts.map((p, i) => encrypt(p, i))];
function largePart(i) {
  const part = parts[i],
    padding = Buffer.alloc(8388608 - part.length);
  padding.writeUInt32BE(padding.length);
  padding.write("free", 4);
  return Buffer.concat([part, padding]);
}
const hits = [];
const server = createServer((req, res) => {
  const url = new URL(req.url, "http://localhost");
  hits.push({ path: url.pathname, range: req.headers.range });
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (url.pathname === "/") {
    res.setHeader("Content-Type", "text/html");
    res.end('<button onclick="fetch(location.hash.slice(1))">play</button>');
    return;
  }
  if (url.pathname === "/master.m3u8") {
    res.end(
      '#EXTM3U\n#EXT-X-DEFINE:NAME="file",VALUE="bundle"\n#EXT-X-STREAM-INF:BANDWIDTH=2\n/bad.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=1\n/range.m3u8?token=abc',
    );
    return;
  }
  if (url.pathname.endsWith(".m3u8")) {
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    if (url.pathname === "/bad.m3u8") {
      res.end(
        "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-GAP\n#EXTINF:1,\ns\n#EXT-X-ENDLIST",
      );
      return;
    }
    let text = "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:1\n";
    if (url.pathname === "/encrypted.m3u8") {
      text +=
        '#EXT-X-KEY:METHOD=AES-128,URI="/key",IV=0x63\n#EXT-X-MAP:URI="/enc/init"\n#EXT-X-KEY:METHOD=AES-128,URI="/key"\n' +
        parts.map((_, i) => `#EXTINF:1,\n/enc/${i}`).join("\n");
    } else if (url.pathname === "/large.m3u8") {
      text +=
        '#EXT-X-MAP:URI="/init"\n' +
        parts.map((_, i) => `#EXTINF:1,\n/large/${i}`).join("\n");
    } else {
      text +=
        '#EXT-X-DEFINE:IMPORT="file"\n#EXT-X-DEFINE:QUERYPARAM="token"\n' +
        `#EXT-X-MAP:URI="/{$file}?token={$token}",BYTERANGE="${init.length}@0"\n`;
      let offset = init.length;
      text += parts
        .map((p, i) => {
          const tag = `#EXTINF:1,\n#EXT-X-BYTERANGE:${p.length}${i === 0 ? "@" + offset : ""}\n/{$file}?token={$token}`;
          offset += p.length;
          return tag;
        })
        .join("\n");
    }
    res.end(text + "\n#EXT-X-ENDLIST");
    return;
  }
  if (url.pathname === "/bundle") {
    const match = /^bytes=(\d+)-(\d+)$/.exec(req.headers.range ?? "");
    if (!match) {
      res.statusCode = 400;
      res.end();
      return;
    }
    const a = +match[1],
      b = +match[2];
    res.writeHead(206, {
      "Content-Range": `bytes ${a}-${b}/${plain.length}`,
      "Content-Length": b - a + 1,
      ETag: '"fixture-v2"',
    });
    res.end(plain.subarray(a, b + 1));
    return;
  }
  if (url.pathname === "/key") {
    res.end(key);
    return;
  }
  if (url.pathname.startsWith("/enc/")) {
    const part = url.pathname.split("/")[2];
    res.end(encrypted[part === "init" ? 0 : Number(part) + 1]);
    return;
  }
  if (url.pathname === "/init") {
    res.end(init);
    return;
  }
  if (url.pathname.startsWith("/large/")) {
    res.end(largePart(Number(url.pathname.split("/")[2])));
    return;
  }
  res.statusCode = 404;
  res.end();
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const profile = await mkdtemp(join(tmpdir(), "hls-v2-"));
let context, timer;
const result = {
  checks: [],
  peakJsHeapBytes: 0,
  peakBackingStorageBytes: 0,
  peakOpfsBytes: 0,
  peakBrowserRssBytes: 0,
};
async function until(check, timeout = 300000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error("task timeout");
}
try {
  await prepareDownloads(profile);
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
  let [worker] = context.serviceWorkers();
  worker ??= await context.waitForEvent("serviceworker");
  const id = new URL(worker.url()).host;
  const source = await context.newPage(),
    manager = await context.newPage();
  await manager.goto(`chrome-extension://${id}/manager.html`);
  const cdp = await context.newCDPSession(manager);
  await useNativeDownloads(context, manager);
  await cdp.send("Performance.enable");
  timer = setInterval(() => {
    void cdp
      .send("Runtime.getHeapUsage")
      .then((v) => {
        result.peakBackingStorageBytes = Math.max(
          result.peakBackingStorageBytes,
          v.backingStorageSize ?? 0,
        );
      })
      .catch(() => {});
    void cdp
      .send("Performance.getMetrics")
      .then(({ metrics }) => {
        result.peakJsHeapBytes = Math.max(
          result.peakJsHeapBytes,
          metrics.find((m) => m.name === "JSHeapUsedSize")?.value ?? 0,
        );
      })
      .catch(() => {});
    void manager
      .evaluate(() => navigator.storage.estimate())
      .then((v) => {
        result.peakOpfsBytes = Math.max(result.peakOpfsBytes, v.usage ?? 0);
      })
      .catch(() => {});
    try {
      const rows = execFileSync("ps", ["-eo", "pid,ppid,rss,args"], {
        encoding: "utf8",
      })
        .split("\n")
        .map((l) => l.trim().match(/^(\d+)\s+(\d+)\s+(\d+)\s+(.*)$/))
        .filter(Boolean);
      const pids = new Set(
        rows
          .filter((r) => r[4].includes("--user-data-dir=" + profile))
          .map((r) => +r[1]),
      );
      for (let i = 0; i < 8; i++)
        for (const r of rows) if (pids.has(+r[2])) pids.add(+r[1]);
      result.peakBrowserRssBytes = Math.max(
        result.peakBrowserRssBytes,
        rows
          .filter((r) => pids.has(+r[1]))
          .reduce((n, r) => n + Number(r[3]) * 1024, 0),
      );
    } catch {
      /* Optional on non-POSIX hosts. */
    }
  }, 500);
  let completed = 0;
  for (const name of large ? ["large"] : ["master", "encrypted"]) {
    await source.goto(
      `http://127.0.0.1:${server.address().port}/#/${name}.m3u8`,
    );
    await source.getByRole("button").click();
    await manager.click("#refresh");
    await manager
      .locator("#candidates .card")
      .filter({ hasText: `/${name}.m3u8` })
      .getByRole("button", { name: "下载", exact: true })
      .click();
    await until(async () => {
      const failed = manager.locator('[data-status="failed"]');
      if (await failed.count()) throw new Error(await failed.innerText());
      return (
        (await manager.locator('[data-status="completed"]').count()) ===
        completed + 1
      );
    });
    completed++;
    const downloads = await worker.evaluate(() =>
      chrome.downloads.search({ state: "complete" }),
    );
    const latest = downloads.sort((a, b) => b.id - a.id)[0];
    assert.ok(
      (await manager.locator("#tasks h3").first().innerText()).endsWith(".mp4"),
    );
    assert.ok(
      latest.filename.endsWith(
        "/" + (await manager.locator("#tasks h3").first().innerText()),
      ),
    );
    assert.equal(latest.mime, "video/mp4");
    const expected = createHash("sha256").update(init);
    if (large) for (let i = 0; i < 128; i++) expected.update(largePart(i));
    else for (const p of parts) expected.update(p);
    const actual = createHash("sha256");
    for await (const chunk of createReadStream(latest.filename))
      actual.update(chunk);
    assert.equal(actual.digest("hex"), expected.digest("hex"));
    const ffprobe = JSON.parse(
      execFileSync(
        process.env.FFPROBE ?? "ffprobe",
        [
          "-v",
          "error",
          "-count_frames",
          "-show_entries",
          "stream=codec_name,codec_type,duration,nb_read_frames",
          "-of",
          "json",
          latest.filename,
        ],
        { encoding: "utf8" },
      ),
    );
    assert.deepEqual(ffprobe.streams.map((s) => s.codec_type).sort(), [
      "audio",
      "video",
    ]);
    const reference = JSON.parse(
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
          folder + "/list.m3u8",
        ],
        { encoding: "utf8" },
      ),
    );
    assert.deepEqual(
      ffprobe.streams.map((s) => [s.codec_type, s.nb_read_frames]),
      reference.streams.map((s) => [s.codec_type, s.nb_read_frames]),
    );
    execFileSync(
      process.env.FFMPEG ?? "ffmpeg",
      ["-v", "error", "-i", latest.filename, "-f", "null", "-"],
      { stdio: "pipe" },
    );
    result.checks.push({
      name,
      bytes: latest.fileSize,
      decoded: true,
      streams: ffprobe.streams,
    });
  }
  if (!large) {
    assert.ok(hits.some((h) => h.path === "/bad.m3u8"));
    assert.equal(hits.filter((h) => h.path === "/bundle").length, 5);
    assert.ok(hits.filter((h) => h.path === "/bundle").every((h) => h.range));
  }
  result.browser = (await cdp.send("Browser.getVersion")).product;
  await mkdir("test-results", { recursive: true });
  await writeFile(
    `test-results/${large ? "v2-large" : "v2-e2e"}.json`,
    JSON.stringify(result, null, 2),
  );
  console.log(JSON.stringify(result, null, 2));
} catch (error) {
  for (const page of context?.pages() ?? [])
    if (page.url().includes("manager.html"))
      console.error(await page.locator("body").innerText());
  throw error;
} finally {
  clearInterval(timer);
  await context?.close();
  await new Promise((r) => server.close(r));
  await rm(profile, { recursive: true, force: true });
}
