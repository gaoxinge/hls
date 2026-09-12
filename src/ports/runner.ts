import type { tasks, commands, plans, artifacts } from "../storage/db";
import type * as files from "../storage/files";
import type { assemble, preflight } from "../media/output";
import type { loadPlaylist } from "../hls/loader";
import type { request } from "../network/http";
import type { exportFile } from "../adapters/chrome/exporter";
/** All browser effects are supplied at the composition root. */
export interface RunnerPorts {
  artifacts: typeof artifacts;
  tasks: typeof tasks;
  commands: typeof commands;
  plans: typeof plans;
  files: Pick<
    typeof files,
    | "cached"
    | "cleanFiles"
    | "ensureSpace"
    | "finalFile"
    | "streamResource"
    | "writeSegment"
    | "digestFile"
  > & { assemble: typeof assemble; preflight: typeof preflight };
  loadPlaylist: typeof loadPlaylist;
  request: typeof request;
  ownership: LockManager;
  exporter: {
    exportFile: typeof exportFile;
    search: typeof chrome.downloads.search;
    cancel: typeof chrome.downloads.cancel;
  };
  control: () => BroadcastChannel;
}
