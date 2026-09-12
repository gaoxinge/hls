import type { Playlist } from "../domain/hls/model";
export type PlaylistParser = (
  text: string,
  url: string,
  definitions?: Record<string, string>,
) => Playlist;
