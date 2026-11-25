import type { Playlist } from "./model";

export function playlistOutputFormat(playlist: Playlist) {
  return playlist.segments[0].init ? "mp4" : "ts";
}
