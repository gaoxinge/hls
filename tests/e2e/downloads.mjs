import { join } from "node:path";

// Headless Chromium has no OS chooser. Supply a real browser file handle
// and exercise the same structured clone and transactional stream as production.
export async function installSaveFilePicker(context) {
  await context.addInitScript(() => {
    if (location.protocol !== "chrome-extension:") return;
    window.showSaveFilePicker = async ({ suggestedName, types }) => {
      if (!navigator.userActivation.isActive)
        throw new Error("Save picker requires an active click");
      window.suggestedName = suggestedName;
      if (
        !Object.values(types[0].accept)
          .flat()
          .some((ext) => suggestedName.toLowerCase().endsWith(ext))
      )
        throw new Error("Suggested filename and accepted extensions differ");
      if (window.cancelNextPicker) {
        window.cancelNextPicker = false;
        throw new DOMException("Cancelled", "AbortError");
      }
      const root = await navigator.storage.getDirectory();
      const dir = await root.getDirectoryHandle("chosen-directory", {
        create: true,
      });
      const name = window.nextSaveName ?? suggestedName;
      delete window.nextSaveName;
      return dir.getFileHandle(name, { create: true });
    };
  });
}

export function savedNames(page) {
  return page.evaluate(async () => {
    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle("chosen-directory");
    const names = [];
    // The chooser may reserve an empty file before a task is cancelled.
    for await (const [name, handle] of dir.entries())
      if ((await handle.getFile()).size > 0) names.push(name);
    return names;
  });
}

// Stream the saved bytes out of the browser for the hash and FFmpeg assertions.
export async function readSavedFile(page, name, profile) {
  const url = await page.evaluate(async (name) => {
    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle("chosen-directory");
    const file = await (await dir.getFileHandle(name)).getFile();
    return URL.createObjectURL(file);
  }, name);
  const event = page.waitForEvent("download");
  await page.evaluate(
    ({ url, name }) => {
      const link = document.createElement("a");
      link.href = url;
      link.download = name;
      link.click();
    },
    { url, name },
  );
  const filename = join(profile, name);
  await (await event).saveAs(filename);
  await page.evaluate((url) => URL.revokeObjectURL(url), url);
  return filename;
}
