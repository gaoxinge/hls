import { afterEach, expect, it, vi } from "vitest";
import { exportFile } from "../../src/adapters/chrome/exporter";
import {
  describeOutput,
  restoreLegacyOutput,
} from "../../src/application/output-artifacts";
import {
  artifactFilename,
  type OutputArtifact,
} from "../../src/domain/output/artifact";
import type { Task } from "../../src/domain/task/model";
import type { DownloadPlan } from "../../src/planning/plan";
const task = {
  id: "task",
  fingerprint: "plan",
  filename: "HLS-test.ts",
  saveAs: false,
  finalSize: 3,
  finalDigest: "a".repeat(64),
} as Task;
const plan = {
  id: "plan",
  schemaVersion: 2,
  format: "mp4",
  resources: [],
  playlistUrl: "https://example.org/list",
} as DownloadPlan;
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
it("restores legacy format from the matching plan, not video.ts or the suggested extension", () => {
  const artifact = restoreLegacyOutput(task, plan);
  expect(artifact.storageKey).toBe("video.ts");
  expect(artifactFilename(artifact)).toBe("HLS-test.mp4");
  expect(describeOutput(task, plan, 3, task.finalDigest!).storageKey).toBe(
    "output.bin",
  );
  expect(() => restoreLegacyOutput(task, undefined)).toThrow();
  expect(() =>
    restoreLegacyOutput(task, { ...plan, id: "different" }),
  ).toThrow();
});
it.each([
  ["mp4", "video/mp4"],
  ["ts", "video/mp2t"],
] as const)(
  "exports %s with explicit MIME and unchanged bytes despite wrong storage MIME",
  async (format, mime) => {
    const artifact = describeOutput(
      task,
      { ...plan, format },
      3,
      task.finalDigest!,
    );
    let exported: Blob | undefined;
    const url = vi.spyOn(URL, "createObjectURL").mockImplementation((blob) => {
      if (!(blob instanceof Blob)) throw new Error("Expected Blob");
      exported = blob;
      return "blob:test";
    });
    const revoke = vi
      .spyOn(URL, "revokeObjectURL")
      .mockImplementation(() => {});
    const download = vi.fn().mockResolvedValue(42);
    const item = {
      id: 42,
      state: "complete",
      filename: `/downloads/HLS-test.${format}`,
      mime,
    };
    vi.stubGlobal("chrome", {
      downloads: {
        download,
        search: vi.fn().mockResolvedValue([item]),
        onChanged: { addListener: vi.fn(), removeListener: vi.fn() },
      },
    });
    const bytes = new Uint8Array([0, 128, 255]);
    const receipt = await exportFile(
      { ...task },
      new File([bytes], "video.ts", { type: "text/plain" }),
      artifact,
      new AbortController().signal,
      vi.fn(),
    );
    expect(exported?.type).toBe(mime);
    expect(new Uint8Array(await exported!.arrayBuffer())).toEqual(bytes);
    expect(download).toHaveBeenCalledWith({
      url: "blob:test",
      filename: `HLS-test.${format}`,
      saveAs: false,
      conflictAction: "uniquify",
    });
    expect(receipt).toEqual({ id: 42, filename: item.filename, mime });
    expect(url).toHaveBeenCalledOnce();
    expect(revoke).toHaveBeenCalledWith("blob:test");
  },
);
it("rejects unknown format before creating a Blob URL", async () => {
  const artifact = {
    ...describeOutput(task, plan, 3, task.finalDigest!),
    formatId: "unknown",
  } as unknown as OutputArtifact;
  const url = vi.spyOn(URL, "createObjectURL");
  await expect(
    exportFile(
      task,
      new File(["abc"], "x"),
      artifact,
      new AbortController().signal,
      vi.fn(),
    ),
  ).rejects.toThrow();
  expect(url).not.toHaveBeenCalled();
});
