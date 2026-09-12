import inspector from "mux.js/lib/tools/ts-inspector.js";
export function checkTs(bytes: Uint8Array) {
  if (!bytes.length || bytes.length % 188 !== 0)
    throw new Error("分片不是支持的 MPEG-TS 格式");
  for (let i = 0; i < bytes.length; i += 188)
    if (bytes[i] !== 0x47) throw new Error("分片 TS 同步字节无效");
}
export function inspectTs(bytes: Uint8Array) {
  checkTs(bytes);
  const info = inspector.inspect(bytes);
  if (!info?.video && !info?.audio)
    throw new Error("TS 分片缺少可识别的 PAT/PMT 或音视频时间戳");
  return info;
}
