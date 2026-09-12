export const outputFormats = {
  ts: { extension: "ts", mimeType: "video/mp2t" },
  mp4: { extension: "mp4", mimeType: "video/mp4" },
} as const;
export type OutputFormatId = keyof typeof outputFormats;
export interface OutputArtifact {
  schemaVersion: 1;
  id: string;
  taskId: string;
  planId: string;
  formatId: OutputFormatId;
  storageKey: string;
  baseName: string;
  size: number;
  sha256: string;
  download?: { id: number; filename: string; mime: string };
}
export const OUTPUT_STORAGE_KEY = "output.bin";
export function validateArtifact(artifact: OutputArtifact) {
  if (
    artifact.schemaVersion !== 1 ||
    !Object.hasOwn(outputFormats, artifact.formatId)
  )
    throw new Error("输出产物版本或格式不受支持");
  if (
    !/^[\w.-]+$/.test(artifact.storageKey) ||
    [".", ".."].includes(artifact.storageKey)
  )
    throw new Error("输出产物存储位置无效");
  if (
    !artifact.baseName ||
    /[\\/]/.test(artifact.baseName) ||
    !Number.isSafeInteger(artifact.size) ||
    artifact.size <= 0 ||
    !/^[a-f0-9]{64}$/.test(artifact.sha256)
  )
    throw new Error("输出产物信息不完整");
}
export function outputFilename(baseName: string, formatId: OutputFormatId) {
  if (!Object.hasOwn(outputFormats, formatId))
    throw new Error("输出格式不受支持");
  return `${baseName}.${outputFormats[formatId].extension}`;
}
export function artifactFilename(artifact: OutputArtifact) {
  validateArtifact(artifact);
  return outputFilename(artifact.baseName, artifact.formatId);
}
export const outputBaseName = (filename: string) =>
  filename.replace(/\.[^./\\]+$/, "");
