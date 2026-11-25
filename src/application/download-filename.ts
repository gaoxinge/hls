import { loadPlaylist } from "../hls/loader";
import { outputFilename } from "../domain/output/artifact";
import { playlistOutputFormat } from "../domain/hls/output-format";

export async function suggestDownloadName(
  url: string,
  selected?: string,
  quality: "best" | "lowest" = "best",
  baseName = `HLS-${new Date().toISOString().replace(/[:.]/g, "-")}`,
) {
  // A master can mix containers across variants. Only read manifests here.
  const playlist = await loadPlaylist(
    url,
    AbortSignal.timeout(30000),
    quality,
    selected,
  );
  return outputFilename(baseName, playlistOutputFormat(playlist));
}
