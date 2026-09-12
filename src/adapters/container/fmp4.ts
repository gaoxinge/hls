import { createFile, type Movie, type Sample } from "mp4box";
import { HlsError } from "../../domain/hls/model";
interface Box {
  type: string;
  start: number;
  end: number;
  payload: number;
}
function boxes(bytes: Uint8Array, start = 0, end = bytes.length): Box[] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const result: Box[] = [];
  while (start < end) {
    if (start + 8 > end) throw new HlsError("INVALID_MP4", "截断的 box");
    let size = view.getUint32(start);
    const type = String.fromCharCode(...bytes.subarray(start + 4, start + 8));
    let header = 8;
    if (size === 1) {
      if (start + 16 > end) throw new HlsError("INVALID_MP4", "截断的大 box");
      const n = view.getBigUint64(start + 8);
      if (n > BigInt(Number.MAX_SAFE_INTEGER))
        throw new HlsError("INVALID_MP4", "box 过大");
      size = Number(n);
      header = 16;
    }
    if (size < header || start + size > end)
      throw new HlsError("INVALID_MP4", "box 边界无效");
    result.push({ type, start, end: start + size, payload: start + header });
    start += size;
  }
  return result;
}
function append(
  file: ReturnType<typeof createFile>,
  bytes: Uint8Array,
  offset: number,
) {
  const buffer = Object.assign(Uint8Array.from(bytes).buffer, {
    fileStart: offset,
  });
  file.appendBuffer(buffer);
}
/** Each fragment uses a fresh public MP4Box parser. Released samples and discarded
 * parser metadata bound retention to one fragment, rather than the whole movie. */
export class Fmp4Inspector {
  private next = new Map<number, number>();
  private info: Movie;
  constructor(private init: Uint8Array) {
    const top = boxes(init);
    if (
      top.filter((b) => b.type === "moov").length !== 1 ||
      !top.some((b) => b.type === "ftyp") ||
      top.some((b) => b.type === "mdat" || b.type === "moof")
    )
      throw new HlsError("INVALID_INIT", "初始化段必须包含 ftyp 和一个 moov");
    const parser = createFile();
    let info: Movie | undefined;
    parser.onReady = (v) => {
      info = v;
    };
    parser.onError = () => {
      throw new HlsError("INVALID_INIT", "MP4 初始化解析失败");
    };
    append(parser, init, 0);
    if (
      !info ||
      !info.isFragmented ||
      !info.tracks.length ||
      info.tracks.some((t) => !t.video && !t.audio)
    )
      throw new HlsError("UNSUPPORTED_MP4", "仅支持分片 MP4 音视频轨道");
    if (info.tracks.some((t) => /^enc/.test(t.codec)))
      throw new HlsError("UNSUPPORTED_ENCRYPTION", "不支持 MP4 样本加密");
    this.info = info;
  }
  inspect(bytes: Uint8Array) {
    const top = boxes(bytes);
    if (
      top.some(
        (b) =>
          !["styp", "sidx", "emsg", "prft", "free", "moof", "mdat"].includes(
            b.type,
          ),
      )
    )
      throw new HlsError("UNSUPPORTED_MP4", "分片包含不支持的顶层 box");
    const moofs = top.filter((b) => b.type === "moof"),
      mdats = top.filter((b) => b.type === "mdat");
    if (!moofs.length || !mdats.length)
      throw new HlsError("INVALID_MP4", "分片缺少 moof/mdat");
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    let expected = 0;
    for (const moof of moofs)
      for (const traf of boxes(bytes, moof.payload, moof.end).filter(
        (b) => b.type === "traf",
      )) {
        const child = boxes(bytes, traf.payload, traf.end),
          tfhd = child.find((b) => b.type === "tfhd");
        if (!tfhd || !child.some((b) => b.type === "tfdt"))
          throw new HlsError("INVALID_MP4", "分片缺少 tfhd/tfdt");
        const flags = view.getUint32(tfhd.payload) & 0xffffff;
        if (!(flags & 0x020000) || flags & 1)
          throw new HlsError(
            "UNSUPPORTED_MP4_OFFSET",
            "仅支持 default-base-is-moof 的相对数据偏移",
          );
        const id = view.getUint32(tfhd.payload + 4);
        if (!this.info.tracks.some((t) => t.id === id))
          throw new HlsError("INVALID_MP4_TRACK", "分片引用未知轨道");
        for (const trun of child.filter((b) => b.type === "trun")) {
          if (!(view.getUint32(trun.payload) & 1))
            throw new HlsError(
              "UNSUPPORTED_MP4_OFFSET",
              "每个 trun 必须显式声明数据偏移",
            );
          expected += view.getUint32(trun.payload + 4);
        }
      }
    const parser = createFile();
    let count = 0;
    const ends = new Map(this.next),
      seen = new Set<number>();
    parser.onError = () => {
      throw new HlsError("INVALID_MP4", "MP4 分片解析失败");
    };
    parser.onReady = (info) => {
      for (const track of info.tracks)
        parser.setExtractionOptions(track.id, undefined, { nbSamples: 1 });
      parser.start();
    };
    parser.onSamples = (id: number, _user: unknown, samples: Sample[]) => {
      for (const sample of samples) {
        if (
          "type" in sample.description &&
          /^enc/.test(String(sample.description.type))
        )
          throw new HlsError("UNSUPPORTED_ENCRYPTION", "不支持 MP4 样本加密");
        if (
          !Number.isSafeInteger(sample.dts) ||
          sample.duration <= 0 ||
          !Number.isSafeInteger(sample.dts + sample.duration)
        )
          throw new HlsError("UNSUPPORTED_MP4_TIME", "解码时间超出支持范围");
        if (ends.has(id) && sample.dts !== ends.get(id))
          throw new HlsError(
            "MP4_TIMELINE_GAP",
            "轨道解码时间不连续，需要重新封装",
          );
        const start = sample.offset - this.init.length;
        if (
          !mdats.some(
            (b) => start >= b.payload && start + sample.size <= b.end,
          ) ||
          sample.data?.length !== sample.size
        )
          throw new HlsError("INVALID_MP4_OFFSET", "样本数据不在 mdat 范围内");
        if (
          !this.next.has(id) &&
          !seen.has(id) &&
          this.info.tracks.find((t) => t.id === id)?.video &&
          !sample.is_sync
        )
          throw new HlsError("MP4_NOT_INDEPENDENT", "首个视频样本不能独立解码");
        ends.set(id, sample.dts + sample.duration);
        seen.add(id);
        count++;
        parser.releaseUsedSamples(id, sample.number + 1);
      }
    };
    append(parser, this.init, 0);
    append(parser, bytes, this.init.length);
    parser.flush();
    if (
      !expected ||
      count !== expected ||
      this.info.tracks.some((t) => !seen.has(t.id))
    )
      throw new HlsError("INVALID_MP4_SAMPLES", "分片样本不完整或缺少轨道");
    this.next = ends;
  }
}
