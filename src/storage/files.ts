import { request } from "../network/http";
import type { Segment } from "../domain/hls/model";
import { sha256 } from "@noble/hashes/sha256";
import { bytesToHex } from "@noble/hashes/utils";
import { segments } from "./db";
export const directory = async (id: string) =>
  (
    await (
      await navigator.storage.getDirectory()
    ).getDirectoryHandle(id, { create: true })
  ).getDirectoryHandle("v2", { create: true });
export async function ensureSpace(bytes: number) {
  const { usage, quota } = await navigator.storage.estimate();
  if (
    quota !== undefined &&
    usage !== undefined &&
    quota - usage < bytes + 8 * 1024 * 1024
  )
    throw new Error("浏览器存储空间不足，请清理缓存");
}
export async function cached(
  id: string,
  index: number,
  fingerprint: string,
  resource?: Segment,
  signal?: AbortSignal,
): Promise<number | undefined> {
  const record = await segments.get(`${id}/${fingerprint}/${index}`);
  if (!record) return undefined;
  if (resource && (record.etag || record.lastModified)) {
    const response = await fetch(resource.url, {
      method: "HEAD",
      credentials: "include",
      signal: AbortSignal.any([
        signal ?? new AbortController().signal,
        AbortSignal.timeout(30000),
      ]),
    });
    if (
      !response.ok ||
      (record.etag
        ? response.headers.get("etag") !== record.etag
        : response.headers.get("last-modified") !== record.lastModified)
    )
      return undefined;
  }
  try {
    const f = await (
      await (await directory(id)).getFileHandle(`${index}.part`)
    ).getFile();
    if (f.size !== record.size || !record.digest) return undefined;
    return (await digestFile(f)) === record.digest ? record.size : undefined;
  } catch (error) {
    if (error instanceof DOMException && error.name === "NotFoundError")
      return undefined;
    throw error;
  }
}
export async function writeSegment(
  id: string,
  index: number,
  fingerprint: string,
  bytes: Uint8Array<ArrayBuffer>,
  validators?: { etag?: string; lastModified?: string },
) {
  await ensureSpace(bytes.byteLength);
  // A file is committed only after close AND a completion record. Orphans are never reused.
  const handle = await (
    await directory(id)
  ).getFileHandle(`${index}.part`, { create: true });
  const writer = await handle.createWritable();
  try {
    await writer.write(bytes);
    await writer.close();
  } catch (e) {
    await writer.abort().catch(() => {});
    throw e;
  }
  await segments.put(
    `${id}/${fingerprint}/${index}`,
    bytes.length,
    bytesToHex(sha256(bytes)),
    validators,
  );
}
export async function merge(
  id: string,
  count: number,
  signal: AbortSignal,
): Promise<File> {
  const dir = await directory(id);
  const writer = await (
    await dir.getFileHandle("video.ts", { create: true })
  ).createWritable();
  try {
    for (let i = 0; i < count; i++) {
      const file = await (await dir.getFileHandle(`${i}.part`)).getFile();
      const reader = file.stream().getReader();
      try {
        while (true) {
          signal.throwIfAborted();
          const chunk = await reader.read();
          if (chunk.done) break;
          await writer.write(chunk.value);
        }
      } finally {
        await reader.cancel();
      }
    }
    signal.throwIfAborted();
    await writer.close();
  } catch (e) {
    await writer.abort().catch(() => {});
    throw e;
  }
  return finalFile(id);
}
export const finalFile = async (id: string, storageKey = "video.ts") =>
  (await (await directory(id)).getFileHandle(storageKey)).getFile();
export async function cleanFiles(id: string) {
  try {
    await (
      await navigator.storage.getDirectory()
    ).removeEntry(id, { recursive: true });
  } catch (e) {
    if (!(e instanceof DOMException && e.name === "NotFoundError")) throw e;
  }
  await segments.clear(id);
}

export async function digestFile(file: File) {
  const hash = sha256.create();
  const reader = file.stream().getReader();
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      hash.update(chunk.value);
    }
  } finally {
    reader.releaseLock();
  }
  return bytesToHex(hash.digest());
}

export async function streamResource(
  id: string,
  segment: Segment,
  fingerprint: string,
  signal: AbortSignal,
) {
  await ensureSpace(32 * 1024 * 1024);
  const handle = await (
    await directory(id)
  ).getFileHandle(segment.index + ".part", { create: true });
  let writer: FileSystemWritableFileStream | undefined;
  let hash = sha256.create(),
    size = 0;
  try {
    const response = await request(
      segment.url,
      (segment.index < 0 ? 2 : 32) * 1024 * 1024,
      signal,
      fetch,
      30000,
      segment.range,
      {
        async reset() {
          await writer?.abort().catch(() => {});
          writer = await handle.createWritable();
          hash = sha256.create();
          size = 0;
        },
        async write(bytes) {
          signal.throwIfAborted();
          hash.update(bytes);
          size += bytes.length;
          await writer!.write(bytes);
        },
      },
    );
    signal.throwIfAborted();
    await writer!.close();
    await segments.put(
      id + "/" + fingerprint + "/" + segment.index,
      size,
      bytesToHex(hash.digest()),
      response,
    );
    return size;
  } catch (error) {
    await writer?.abort().catch(() => {});
    throw error;
  }
}
