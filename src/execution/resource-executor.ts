import { BufferBudget } from "./budget";
import { decryptSegment } from "../media/aes128";
import type { Segment } from "../domain/hls/model";
import type { RunnerPorts } from "../ports/runner";
/** One executor per run: keys never reach persistent storage and a validated
 * resource probed during selection is not downloaded twice. */
export function createResourceExecutor(
  deps: Pick<RunnerPorts, "files" | "request">,
  taskId: string,
  signal: AbortSignal,
) {
  const keys = new Map<string, Promise<Uint8Array<ArrayBuffer>>>(),
    ready = new Map<string, number>();
  const budget = new BufferBudget();
  return async (segment: Segment, fingerprint: string) => {
    const identity = fingerprint + "/" + segment.index;
    if (ready.has(identity)) return ready.get(identity)!;
    let size = await deps.files.cached(
      taskId,
      segment.index,
      fingerprint,
      segment,
      signal,
    );
    if (size === undefined && !segment.key)
      size = await deps.files.streamResource(
        taskId,
        segment,
        fingerprint,
        signal,
      );
    if (size === undefined)
      size = await budget.run(signal, async () => {
        const response = await deps.request(
          segment.url,
          (segment.index < 0 ? 2 : 32) * 1024 * 1024,
          signal,
          undefined,
          30000,
          segment.range,
        );
        const key = segment.key!;
        if (!keys.has(key.id))
          keys.set(
            key.id,
            deps.request(key.url, 16, signal).then((r) => r.bytes),
          );
        const bytes = await decryptSegment(
          response.bytes,
          await keys.get(key.id)!,
          segment,
        );
        signal.throwIfAborted();
        await deps.files.writeSegment(
          taskId,
          segment.index,
          fingerprint,
          bytes,
          response,
        );
        return bytes.length;
      });
    ready.set(identity, size);
    return size;
  };
}
