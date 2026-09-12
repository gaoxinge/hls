import type { ByteRange } from "../domain/hls/model";
import { httpUrl } from "../shared/model";
export class HttpError extends Error {
  constructor(public status: number) {
    super(
      status === 401 || status === 403
        ? "访问已失效，请刷新源网页后重新发现视频"
        : `资源请求失败（HTTP ${status}）`,
    );
  }
}
export function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, ms);
    signal.addEventListener("abort", abort, { once: true });
  });
}
export async function request(
  url: string,
  limit: number,
  signal: AbortSignal,
  fetcher: typeof fetch = fetch,
  timeout = 30000,
  range?: ByteRange,
  sink?: {
    reset(): Promise<void>;
    write(bytes: Uint8Array<ArrayBuffer>): Promise<void>;
  },
): Promise<{
  bytes: Uint8Array<ArrayBuffer>;
  url: string;
  etag?: string;
  lastModified?: string;
}> {
  httpUrl(url);
  for (let attempt = 0; ; attempt++) {
    signal.throwIfAborted();
    const controller = new AbortController();
    const abort = () => controller.abort(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    const timer = setTimeout(
      () => controller.abort(new Error("资源请求超时")),
      timeout,
    );
    let retryAfter = 0;
    try {
      const response = await fetcher(url, {
        signal: controller.signal,
        credentials: "include",
        headers: range
          ? {
              Range: `bytes=${range.offset}-${BigInt(range.offset) + BigInt(range.length) - 1n}`,
              "Accept-Encoding": "identity",
            }
          : undefined,
      });
      httpUrl(response.url || url);
      if (!response.ok) {
        const value = response.headers.get("retry-after");
        retryAfter = value
          ? /^\d+$/.test(value)
            ? Number(value) * 1000
            : Date.parse(value) - Date.now()
          : 0;
        await response.body?.cancel();
        throw new HttpError(response.status);
      }
      if (range) {
        const match = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(
          response.headers.get("content-range") ?? "",
        );
        const offset = BigInt(range.offset),
          length = BigInt(range.length);
        if (
          response.status !== 206 ||
          !match ||
          BigInt(match[1]) !== offset ||
          BigInt(match[2]) !== offset + length - 1n ||
          BigInt(match[3]) <= BigInt(match[2]) ||
          (response.headers.get("content-encoding") &&
            response.headers.get("content-encoding") !== "identity")
        ) {
          await response.body?.cancel();
          throw new RangeError(
            "范围响应无效，必须返回精确的 206 Content-Range",
          );
        }
        if (length > BigInt(limit)) {
          await response.body?.cancel();
          throw new RangeError("范围超出资源大小限制");
        }
      } else if (response.status === 206) {
        await response.body?.cancel();
        throw new RangeError("未请求范围却收到部分响应");
      }
      if (!response.body) throw new Error("响应内容为空");
      await sink?.reset();
      const reader = response.body.getReader();
      const parts: Uint8Array[] = [];
      let size = 0;
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        size += value.length;
        if (size > limit) {
          await reader.cancel();
          throw new RangeError("资源超出大小限制");
        }
        if (sink) await sink.write(value);
        else parts.push(value);
      }
      const bytes = new Uint8Array(sink ? 0 : size);
      let offset = 0;
      for (const part of parts) {
        bytes.set(part, offset);
        offset += part.length;
      }
      if (range && BigInt(size) !== BigInt(range.length))
        throw new RangeError("范围响应长度不匹配");
      const contentLength = response.headers.get("content-length");
      if (
        contentLength &&
        !response.headers.get("content-encoding") &&
        Number(contentLength) !== size
      )
        throw new RangeError("响应长度不匹配");
      return {
        bytes,
        url: response.url || url,
        etag: response.headers.get("etag") ?? undefined,
        lastModified: response.headers.get("last-modified") ?? undefined,
      };
    } catch (error) {
      signal.throwIfAborted();
      const retryable =
        error instanceof HttpError
          ? [408, 429, 500, 502, 503, 504].includes(error.status)
          : !(error instanceof RangeError) &&
            (error instanceof TypeError || controller.signal.aborted);
      if (!retryable || attempt >= 3) throw error;
    } finally {
      controller.abort();
      clearTimeout(timer);
      signal.removeEventListener("abort", abort);
    }
    await sleep(
      Math.min(
        30000,
        Math.max(
          Number.isFinite(retryAfter) ? retryAfter : 0,
          500 * 2 ** attempt + Math.random() * 250,
        ),
      ),
      signal,
    );
  }
}
