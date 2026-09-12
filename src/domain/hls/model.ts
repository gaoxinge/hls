export interface Source {
  line: number;
  tag: string;
  raw?: string;
}
export interface Diagnostic extends Source {
  code: string;
  message: string;
  severity: "error" | "warning";
}
export interface ByteRange {
  offset: string;
  length: string;
}
export interface KeyContext {
  id: string;
  url: string;
  iv?: string;
}
export interface InitResource {
  url: string;
  range?: ByteRange;
  key?: KeyContext;
}
export interface Segment {
  index: number;
  sequence: string;
  url: string;
  duration: number;
  key?: KeyContext;
  range?: ByteRange;
  init?: InitResource;
  timeline: number;
  source: Source;
}
export interface Variant {
  url: string;
  bandwidth: number;
  height: number;
  label: string;
  supported: boolean;
  codecs?: string;
  audio?: string;
  reason?: string;
}
export interface Playlist {
  kind: "master" | "media";
  url: string;
  segments: Segment[];
  variants: Variant[];
  definitions: Record<string, string>;
  diagnostics: Diagnostic[];
  tags: Source[];
  ended: boolean;
  version: number;
  targetDuration?: number;
  sessionKeys: KeyContext[];
}
export class HlsError extends Error {
  constructor(
    public code: string,
    message: string,
    public source?: Source,
  ) {
    super(`${code}${source ? `（第 ${source.line} 行）` : ""}：${message}`);
  }
}
