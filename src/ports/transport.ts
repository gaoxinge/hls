import type { ByteRange } from "../domain/hls/model";
export interface ResourceResponse {
  bytes: Uint8Array<ArrayBuffer>;
  url: string;
  etag?: string;
  lastModified?: string;
}
export type ReadResource = (
  url: string,
  limit: number,
  signal: AbortSignal,
  range?: ByteRange,
) => Promise<ResourceResponse>;
