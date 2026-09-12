import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
/** CDP allow/allowAndName bypass Chrome's normal filename determiner. */
export async function prepareDownloads(profile) {
  await mkdir(join(profile, "Default"), { recursive: true });
  await mkdir(join(profile, "downloads"), { recursive: true });
  await writeFile(
    join(profile, "Default", "Preferences"),
    JSON.stringify({
      download: {
        default_directory: join(profile, "downloads"),
        prompt_for_download: false,
        directory_upgrade: true,
      },
    }),
  );
}
export async function useNativeDownloads(context, page) {
  await (
    await context.newCDPSession(page)
  ).send("Browser.setDownloadBehavior", { behavior: "default" });
}
