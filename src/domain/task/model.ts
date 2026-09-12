export type Status =
  | "queued"
  | "resolving"
  | "downloading"
  | "merging"
  | "exporting"
  | "completed"
  | "failed"
  | "cancelled"
  | "interrupted";
export interface Task {
  revision?: number;
  ownerGeneration?: string;
  schemaVersion?: number;
  cancelRequested?: boolean;
  legacy?: boolean;
  id: string;
  candidateId: string;
  playlistUrl: string;
  status: Status;
  createdAt: number;
  updatedAt: number;
  total: number;
  done: number;
  bytes: number;
  filename: string;
  fingerprint?: string;
  selectedUrl?: string;
  quality: "best" | "lowest";
  saveAs: boolean;
  downloadId?: number;
  artifactId?: string;
  finalSize?: number;
  finalDigest?: string;
  error?: string;
}
export const active = (s: Status) =>
  ["queued", "resolving", "downloading", "merging", "exporting"].includes(s);
