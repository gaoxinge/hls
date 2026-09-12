import { ivBytes } from "../hls/parser";
import type { KeyContext } from "../domain/hls/model";
export async function decryptSegment(
  bytes: Uint8Array<ArrayBuffer>,
  key: Uint8Array<ArrayBuffer>,
  segment: { sequence: string; key?: KeyContext },
) {
  if (key.length !== 16) throw new Error("AES-128 密钥长度无效");
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    key,
    "AES-CBC",
    false,
    ["decrypt"],
  );
  try {
    return new Uint8Array(
      await crypto.subtle.decrypt(
        { name: "AES-CBC", iv: ivBytes(segment) },
        cryptoKey,
        bytes,
      ),
    );
  } catch {
    throw new Error("分片解密失败，密钥或 IV 无效");
  }
}
