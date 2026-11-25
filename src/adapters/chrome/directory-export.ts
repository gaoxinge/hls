/** The runner serializes exports. Skip names already used by files or directories. */
export async function writeToDirectory(
  directory: FileSystemDirectoryHandle,
  filename: string,
  file: File,
  signal: AbortSignal,
) {
  const extension = filename.lastIndexOf(".");
  for (let suffix = 0; ; suffix++) {
    signal.throwIfAborted();
    const name = suffix
      ? `${filename.slice(0, extension)} (${suffix})${filename.slice(extension)}`
      : filename;
    try {
      await directory.getFileHandle(name);
      continue;
    } catch (error) {
      if (!(error instanceof DOMException)) throw error;
      if (error.name === "TypeMismatchError") continue;
      if (error.name !== "NotFoundError") throw error;
    }
    const handle = await directory.getFileHandle(name, { create: true });
    try {
      const writable = await handle.createWritable();
      await file.stream().pipeTo(writable, { signal });
    } catch (error) {
      // A failed transactional write can leave the newly created empty entry.
      const current = await directory
        .getFileHandle(name)
        .catch(() => undefined);
      if (
        current &&
        (await current.isSameEntry(handle)) &&
        (await current.getFile()).size === 0
      )
        await directory.removeEntry(name).catch(() => {});
      throw error;
    }
    return name;
  }
}
