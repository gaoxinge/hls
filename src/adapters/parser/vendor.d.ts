declare module "m3u8-parser" {
  export class Parser {
    constructor(options?: {
      uri?: string;
      mainDefinitions?: Record<string, string>;
    });
    on(type: string, listener: (event: { message: string }) => void): void;
    push(text: string): void;
    end(): void;
    manifest: {
      segments: Array<{ uri: string; duration: number }>;
      playlists?: Array<{ uri: string; attributes: Record<string, unknown> }>;
    };
  }
}
declare module "mux.js/lib/tools/ts-inspector.js" {
  const tools: {
    inspect(bytes: Uint8Array): {
      video?: Array<{ dts: number; pts: number }>;
      audio?: Array<{ dts: number; pts: number }>;
    } | null;
  };
  export default tools;
}
