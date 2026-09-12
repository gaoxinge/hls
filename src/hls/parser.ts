import { parse } from "../adapters/parser/m3u8";
import { assertExecutable } from "../protocol/capabilities";
import type { KeyContext } from "../domain/hls/model";
export { attributes } from "../protocol/lexical";
export function parsePlaylist(
  text: string,
  base: string,
  definitions: Record<string, string> = {},
) {
  const result = parse(text, base, definitions);
  assertExecutable(result);
  return result;
}
export function ivBytes(segment: {
  sequence: string;
  key?: KeyContext;
}): Uint8Array<ArrayBuffer> {
  const value = BigInt(segment.key?.iv ?? segment.sequence);
  if (value < 0n || value >= 1n << 128n) throw new Error("IV 超出范围");
  return Uint8Array.from(
    value.toString(16).padStart(32, "0").match(/../g)!,
    (x) => parseInt(x, 16),
  );
}
