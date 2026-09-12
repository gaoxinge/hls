import { OUTPUT_STORAGE_KEY } from "../domain/output/artifact";
import { directory, finalFile } from "../storage/files";
import { Fmp4Inspector } from "./fmp4";
import { inspectTs } from "./ts";
import type { DownloadPlan } from "../planning/plan";
/** Validate in order with bounded per-resource buffers; copy to OPFS incrementally. */
export async function assemble(
  id: string,
  plan: DownloadPlan,
  signal: AbortSignal,
): Promise<File> {
  const dir = await directory(id);
  let mp4: Fmp4Inspector | undefined;
  const writer = await (
    await dir.getFileHandle(OUTPUT_STORAGE_KEY, { create: true })
  ).createWritable();
  try {
    if (plan.format === "mp4") {
      const init = await (await dir.getFileHandle("-1.part")).getFile();
      const bytes = new Uint8Array(await init.arrayBuffer());
      mp4 = new Fmp4Inspector(bytes);
      await writer.write(bytes);
    }
    let previousVideo: number | undefined;
    for (const resource of plan.resources) {
      signal.throwIfAborted();
      const file = await (
        await dir.getFileHandle(`${resource.index}.part`)
      ).getFile();
      if (file.size > 32 * 1024 * 1024)
        throw new Error("分片超过 32 MiB 检查预算");
      const bytes = new Uint8Array(await file.arrayBuffer());
      if (mp4) mp4.inspect(bytes);
      else {
        const info = inspectTs(bytes);
        const first = info.video?.[0]?.dts;
        if (
          first !== undefined &&
          previousVideo !== undefined &&
          first < previousVideo &&
          previousVideo - first < 2 ** 32
        )
          throw new Error("TS 视频时间戳倒退，需要独立时间线输出");
        previousVideo = info.video?.at(-1)?.dts ?? previousVideo;
      }
      await writer.write(bytes);
    }
    signal.throwIfAborted();
    await writer.close();
    return finalFile(id, OUTPUT_STORAGE_KEY);
  } catch (error) {
    await writer.abort().catch(() => {});
    throw error;
  }
}
export async function preflight(
  id: string,
  plan: DownloadPlan,
  signal: AbortSignal,
) {
  signal.throwIfAborted();
  const dir = await directory(id);
  const bytes = new Uint8Array(
    await (await (await dir.getFileHandle("0.part")).getFile()).arrayBuffer(),
  );
  if (plan.format === "mp4") {
    const init = new Uint8Array(
      await (
        await (await dir.getFileHandle("-1.part")).getFile()
      ).arrayBuffer(),
    );
    new Fmp4Inspector(init).inspect(bytes);
  } else inspectTs(bytes);
}
