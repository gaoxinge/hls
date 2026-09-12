import { createRunner } from "../application/runner";
import { tasks, commands, plans, artifacts } from "../storage/db";
import * as files from "../storage/files";
import { assemble, preflight } from "../media/output";
import { loadPlaylist } from "../hls/loader";
import { request } from "../network/http";
import { exportFile } from "../adapters/chrome/exporter";
export class DownloadEngine {
  private runner: ReturnType<typeof createRunner>;
  constructor(changed: () => void) {
    this.runner = createRunner(
      {
        artifacts,
        tasks,
        commands,
        plans,
        files: { ...files, assemble, preflight },
        loadPlaylist,
        request,
        ownership: navigator.locks,
        exporter: {
          exportFile,
          search: chrome.downloads.search.bind(chrome.downloads),
          cancel: chrome.downloads.cancel.bind(chrome.downloads),
        },
        control: () => new BroadcastChannel("hls-control"),
      },
      changed,
    );
  }
  recover() {
    return this.runner.recover();
  }
  cancel(id: string) {
    return this.runner.cancel(id);
  }
  listen() {
    return this.runner.listen();
  }
  pump() {
    return this.runner.pump();
  }
  get busy() {
    return this.runner.busy;
  }
}
