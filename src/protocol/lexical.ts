import { HlsError, type Source } from "../domain/hls/model";
export function attributes(text: string): Record<string, string> {
  const out: Record<string, string> = {};
  let rest = text;
  while (rest) {
    const m = /^\s*([A-Z0-9-]+)=(?:"([^"]*)"|([^,"]*))(?:,|$)/.exec(rest);
    if (!m || Object.hasOwn(out, m[1]))
      throw new HlsError("INVALID_ATTRIBUTES", "播放列表属性格式无效");
    out[m[1]] = m[2] ?? m[3].trim();
    rest = rest.slice(m[0].length);
  }
  return out;
}
export function integer(value: string, source?: Source): bigint {
  if (!/^\d+$/.test(value) || BigInt(value) > (1n << 64n) - 1n)
    throw new HlsError(
      "INVALID_INTEGER",
      "整数必须处于无符号 64 位范围内",
      source,
    );
  return BigInt(value);
}
export function expand(
  value: string,
  definitions: Record<string, string>,
  path: string[] = [],
): string {
  if (path.length > 16 || value.length > 65536)
    throw new HlsError("VARIABLE_BUDGET", "变量展开超过深度或长度限制");
  let total = 0;
  const result = value.replace(/\{\$([a-zA-Z0-9_-]+)\}/g, (_, name: string) => {
    if (path.includes(name))
      throw new HlsError("VARIABLE_CYCLE", "变量循环引用");
    if (!Object.hasOwn(definitions, name))
      throw new HlsError("UNDEFINED_VARIABLE", `变量 ${name} 未定义`);
    const replacement = expand(definitions[name], definitions, [...path, name]);
    total += replacement.length;
    if (total > 65536)
      throw new HlsError("VARIABLE_BUDGET", "变量展开超过长度限制");
    return replacement;
  });
  if (result.length > 65536)
    throw new HlsError("VARIABLE_BUDGET", "变量展开超过长度限制");
  return result;
}
