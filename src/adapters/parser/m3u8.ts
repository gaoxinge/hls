import { validateProtocol } from "../../protocol/rules";
import { Parser } from "m3u8-parser";
import { httpUrl } from "../../shared/model";
import { attributes, expand, integer } from "../../protocol/lexical";
import {
  HlsError,
  type Playlist,
  type KeyContext,
  type InitResource,
  type ByteRange,
  type Source,
} from "../../domain/hls/model";

/** Preserve source lexemes where the vendor parser uses lossy JavaScript numbers.
 * Segment/variant structure comes from m3u8-parser; this pass supplies scoped raw metadata. */
export function parse(
  text: string,
  base: string,
  inherited: Record<string, string> = {},
): Playlist {
  if (text.length > 2 * 1024 * 1024)
    throw new HlsError("PLAYLIST_SIZE", "播放列表超过 2 MiB 限制");
  const lines = text.replace(/^\uFEFF/, "").split(/\r?\n/);
  if (lines[0]?.trim() !== "#EXTM3U")
    throw new HlsError("INVALID_HEADER", "响应不是有效的 HLS 播放列表");
  const result: Playlist = {
    kind: "media",
    url: base,
    segments: [],
    variants: [],
    definitions: {},
    diagnostics: [],
    tags: [],
    ended: false,
    version: 1,
    sessionKeys: [],
  };
  let sequence = 0n,
    key: KeyContext | undefined,
    init: InitResource | undefined,
    rangeText: string | undefined;
  let duration: number | undefined,
    pending: Record<string, string> | undefined,
    timeline = 0,
    keyIndex = 0;
  const normalized: string[] = [];
  const singletons = new Set<string>();
  const groups = new Map<string, boolean>();
  const referenced: Array<{ index: number; group: string }> = [];
  const unique = new Set([
    "EXT-X-VERSION",
    "EXT-X-TARGETDURATION",
    "EXT-X-MEDIA-SEQUENCE",
    "EXT-X-DISCONTINUITY-SEQUENCE",
    "EXT-X-PLAYLIST-TYPE",
    "EXT-X-ENDLIST",
  ]);
  const range = (
    value: string,
    url: string,
    previous?: { url: string; range?: ByteRange },
  ): ByteRange => {
    const parts = value.split("@");
    if (parts.length > 2)
      throw new HlsError("INVALID_RANGE", "字节范围格式无效");
    const length = integer(parts[0]);
    if (length === 0n)
      throw new HlsError("INVALID_RANGE", "字节范围长度必须大于零");
    const offset =
      parts[1] !== undefined
        ? integer(parts[1])
        : previous?.url === url && previous.range
          ? BigInt(previous.range.offset) + BigInt(previous.range.length)
          : undefined;
    if (offset === undefined)
      throw new HlsError("INVALID_RANGE", "隐式范围必须紧接同一资源的范围");
    integer((offset + length - 1n).toString());
    return { offset: offset.toString(), length: length.toString() };
  };
  const encryption = (
    a: Record<string, string>,
    source: Source,
  ): KeyContext | undefined => {
    if (a.METHOD === "NONE") {
      if (Object.keys(a).length !== 1)
        throw new HlsError("INVALID_KEY", "METHOD=NONE 不能含其他属性", source);
      return;
    }
    if (a.METHOD !== "AES-128" || (a.KEYFORMAT && a.KEYFORMAT !== "identity"))
      throw new HlsError(
        "UNSUPPORTED_ENCRYPTION",
        "仅支持标准 AES-128 identity 加密",
        source,
      );
    if (!a.URI) throw new HlsError("INVALID_KEY", "缺少密钥地址", source);
    if (a.IV && !/^0x[0-9a-f]{1,32}$/i.test(a.IV))
      throw new HlsError("INVALID_IV", "IV 格式无效", source);
    if (a.KEYFORMATVERSIONS && a.KEYFORMATVERSIONS !== "1")
      throw new HlsError(
        "UNSUPPORTED_ENCRYPTION",
        "不支持此密钥格式版本",
        source,
      );
    return {
      id: `${base}#key-${keyIndex++}`,
      url: httpUrl(expand(a.URI, result.definitions), base),
      iv: a.IV,
    };
  };
  for (let i = 0; i < lines.length; i++) {
    let line = lines[i].trim();
    if (!line) continue;
    const tag = line.startsWith("#") ? line.slice(1).split(":")[0] : "URI";
    const source = { tag, line: i + 1, raw: line };
    const value = line.slice(line.indexOf(":") + 1);
    if (tag.startsWith("EXT")) result.tags.push(source);
    if (unique.has(tag)) {
      if (singletons.has(tag))
        throw new HlsError("DUPLICATE_TAG", `${tag} 重复`, source);
      singletons.add(tag);
    }
    if (tag === "EXT-X-DEFINE") {
      const a = attributes(value);
      const name = a.NAME ?? a.IMPORT ?? a.QUERYPARAM;
      if (
        !name ||
        !/^[a-zA-Z0-9_-]+$/.test(name) ||
        Object.hasOwn(result.definitions, name)
      )
        throw new HlsError("INVALID_DEFINE", "变量声明无效或重复", source);
      if (
        a.NAME !== undefined &&
        a.VALUE !== undefined &&
        Object.keys(a).length === 2
      )
        result.definitions[name] = a.VALUE;
      else if (a.IMPORT && Object.keys(a).length === 1) {
        if (!Object.hasOwn(inherited, name))
          throw new HlsError("INVALID_IMPORT", "父列表未定义导入变量", source);
        result.definitions[name] = inherited[name];
      } else if (a.QUERYPARAM && Object.keys(a).length === 1) {
        const values = new URL(base).searchParams.getAll(name);
        if (values.length !== 1)
          throw new HlsError(
            "INVALID_QUERY_VARIABLE",
            "查询变量必须有唯一值",
            source,
          );
        result.definitions[name] = values[0];
      } else
        throw new HlsError("INVALID_DEFINE", "DEFINE 属性组合无效", source);
      continue;
    }
    line = expand(line, result.definitions);
    normalized.push(line);
    const v = line.slice(line.indexOf(":") + 1);
    if (tag === "EXT-X-VERSION") {
      result.version = Number(integer(v, source));
      if (result.version < 1)
        throw new HlsError("INVALID_VERSION", "版本必须大于零", source);
    } else if (tag === "EXT-X-TARGETDURATION") {
      result.targetDuration = Number(integer(v, source));
      if (!result.targetDuration)
        throw new HlsError("INVALID_DURATION", "目标时长无效", source);
    } else if (tag === "EXT-X-MEDIA-SEQUENCE") {
      if (result.segments.length)
        throw new HlsError("INVALID_SEQUENCE", "序号必须位于分片之前", source);
      sequence = integer(v, source);
    } else if (tag === "EXT-X-DISCONTINUITY-SEQUENCE") {
      if (result.segments.length || timeline)
        throw new HlsError("INVALID_SEQUENCE", "不连续序号位置无效", source);
      integer(v, source);
    } else if (tag === "EXT-X-DISCONTINUITY") timeline++;
    else if (tag === "EXTINF") {
      if (duration !== undefined)
        throw new HlsError("INVALID_SEGMENT", "分片缺少地址", source);
      duration = Number(v.split(",")[0]);
      if (!Number.isFinite(duration) || duration <= 0)
        throw new HlsError("INVALID_DURATION", "分片时长无效", source);
    } else if (tag === "EXT-X-BYTERANGE") {
      if (rangeText) throw new HlsError("INVALID_RANGE", "范围重复", source);
      rangeText = v;
    } else if (tag === "EXT-X-KEY") key = encryption(attributes(v), source);
    else if (tag === "EXT-X-SESSION-KEY") {
      const k = encryption(attributes(v), source);
      if (!k)
        throw new HlsError(
          "INVALID_SESSION_KEY",
          "会话密钥不能为 NONE",
          source,
        );
      result.sessionKeys.push(k);
    } else if (tag === "EXT-X-MAP") {
      const a = attributes(v);
      if (!a.URI)
        throw new HlsError("INVALID_MAP", "初始化资源缺少 URI", source);
      if (key && !key.iv)
        throw new HlsError(
          "INVALID_MAP_IV",
          "加密初始化资源必须有显式 IV",
          source,
        );
      const url = httpUrl(a.URI, base);
      init = {
        url,
        key,
        range: a.BYTERANGE ? range(a.BYTERANGE, url, init) : undefined,
      };
    } else if (tag === "EXT-X-STREAM-INF") {
      if (pending)
        throw new HlsError("INVALID_VARIANT", "变体缺少地址", source);
      pending = attributes(v);
    } else if (tag === "EXT-X-MEDIA") {
      const a = attributes(v);
      if (!a.TYPE || !a["GROUP-ID"] || !a.NAME)
        throw new HlsError("INVALID_GROUP", "媒体组缺少必要属性", source);
      groups.set(
        `${a.TYPE}:${a["GROUP-ID"]}`,
        (groups.get(`${a.TYPE}:${a["GROUP-ID"]}`) ?? false) || Boolean(a.URI),
      );
    } else if (tag === "EXT-X-ENDLIST") result.ended = true;
    else if (tag === "URI") {
      const url = httpUrl(line, base);
      if (pending) {
        const bandwidth = Number(integer(pending.BANDWIDTH ?? "", source));
        const height = Number(pending.RESOLUTION?.split("x")[1] ?? 0);
        if (bandwidth <= 0 || !Number.isFinite(height))
          throw new HlsError("INVALID_VARIANT", "变体带宽无效", source);
        const index = result.variants.length;
        result.variants.push({
          url,
          bandwidth,
          height,
          label: height ? `${height}p` : `${Math.round(bandwidth / 1000)} kbps`,
          supported: true,
          codecs: pending.CODECS,
          audio: pending.AUDIO,
        });
        for (const type of ["AUDIO", "VIDEO", "SUBTITLES"])
          if (pending[type])
            referenced.push({ index, group: `${type}:${pending[type]}` });
        pending = undefined;
      } else {
        if (duration === undefined)
          throw new HlsError("INVALID_SEGMENT", "分片缺少 EXTINF", source);
        const seq = integer(
          (sequence + BigInt(result.segments.length)).toString(),
          source,
        );
        result.segments.push({
          index: result.segments.length,
          sequence: seq.toString(),
          url,
          duration,
          key,
          init,
          timeline,
          source,
          range: rangeText
            ? range(rangeText, url, result.segments.at(-1))
            : undefined,
        });
        duration = undefined;
        rangeText = undefined;
      }
    }
  }
  if (pending || duration !== undefined || rangeText)
    throw new HlsError("INCOMPLETE_PLAYLIST", "播放列表不完整");
  if (result.variants.length && result.segments.length)
    throw new HlsError("MIXED_PLAYLIST", "不能混合主列表和媒体列表");
  result.kind = result.variants.length ? "master" : "media";
  validateProtocol(result);
  for (const ref of referenced) {
    if (!groups.has(ref.group))
      throw new HlsError("INVALID_GROUP", "引用了不存在的媒体组");
    if (groups.get(ref.group)) {
      result.variants[ref.index].supported = false;
      result.variants[ref.index].reason = "独立音视频或字幕轨道尚未支持";
    }
  }
  const known = new Set([
    "EXTM3U",
    "EXTINF",
    "EXT-X-VERSION",
    "EXT-X-TARGETDURATION",
    "EXT-X-MEDIA-SEQUENCE",
    "EXT-X-DISCONTINUITY-SEQUENCE",
    "EXT-X-DISCONTINUITY",
    "EXT-X-ENDLIST",
    "EXT-X-PLAYLIST-TYPE",
    "EXT-X-KEY",
    "EXT-X-SESSION-KEY",
    "EXT-X-SESSION-DATA",
    "EXT-X-MAP",
    "EXT-X-BYTERANGE",
    "EXT-X-STREAM-INF",
    "EXT-X-I-FRAME-STREAM-INF",
    "EXT-X-I-FRAMES-ONLY",
    "EXT-X-MEDIA",
    "EXT-X-DEFINE",
    "EXT-X-INDEPENDENT-SEGMENTS",
    "EXT-X-START",
    "EXT-X-PROGRAM-DATE-TIME",
    "EXT-X-DATERANGE",
    "EXT-X-GAP",
    "EXT-X-PART",
    "EXT-X-PART-INF",
    "EXT-X-PRELOAD-HINT",
    "EXT-X-SERVER-CONTROL",
    "EXT-X-SKIP",
    "EXT-X-CONTENT-STEERING",
  ]);
  for (const source of result.tags)
    if (!known.has(source.tag))
      result.diagnostics.push({
        ...source,
        code: "UNKNOWN_TAG",
        message: "未知标签已保留：" + source.tag,
        severity: "warning",
      });
  const vendor = new Parser({ uri: base });
  vendor.on("warn", (event) =>
    result.diagnostics.push({
      line: 0,
      tag: "PARSER",
      code: "PARSER_WARNING",
      message: event.message,
      severity: "warning",
    }),
  );
  vendor.push(
    normalized
      .map((line) => {
        if (!/^#EXT-X-(?:SESSION-)?KEY:/.test(line)) return line;
        const colon = line.indexOf(":");
        const attrs = attributes(line.slice(colon + 1));
        if (attrs.IV) attrs.IV = "0x" + attrs.IV.slice(2).padStart(32, "0");
        return (
          line.slice(0, colon + 1) +
          Object.entries(attrs)
            .map(
              ([name, value]) =>
                name +
                "=" +
                (["METHOD", "IV"].includes(name)
                  ? value
                  : JSON.stringify(value)),
            )
            .join(",")
        );
      })
      .join("\n") + "\n",
  );
  vendor.end();
  if (
    (vendor.manifest.playlists?.length ?? 0) !== result.variants.length ||
    vendor.manifest.segments.length !== result.segments.length
  )
    throw new HlsError("PARSER_MISMATCH", "解析器结构与源文件不一致");
  // Take structural URI/duration from the mature parser, keeping lossless scope metadata.
  result.segments.forEach((s, i) => {
    const parsed = vendor.manifest.segments[i];
    s.url = httpUrl(parsed.uri, base);
    s.duration = parsed.duration;
  });
  for (const name of Object.keys(result.definitions))
    result.definitions[name] = expand(
      result.definitions[name],
      result.definitions,
      [name],
    );
  return result;
}
