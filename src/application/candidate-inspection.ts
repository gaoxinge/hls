import { parse } from "../adapters/parser/m3u8";
import { request } from "../network/http";
import type {
  Inspection,
  ObservedCandidate,
} from "../detection/candidate-groups";

export async function readCandidate(
  url: string,
  signal: AbortSignal,
  credentials: RequestCredentials = "include",
) {
  const response = await request(url, 2 * 1024 * 1024, signal, (input, init) =>
    fetch(input, { ...init, credentials, referrerPolicy: "no-referrer" }),
  );
  return parse(
    new TextDecoder("utf-8", { fatal: true }).decode(response.bytes),
    response.url,
  );
}

// Popup and manager share the same three workers and in-flight reads.
export function createCandidateInspector(read = readCandidate) {
  const lanes = Array.from({ length: 3 }, () => Promise.resolve());
  const pending = new Map<string, Promise<Inspection | undefined>>();
  let next = 0;
  return function inspect(candidate: ObservedCandidate, deadline: number) {
    const existing = pending.get(candidate.id);
    if (existing) return existing;
    const lane = next++ % lanes.length;
    const job = lanes[lane]
      .then(async (): Promise<Inspection | undefined> => {
        const remaining = deadline - Date.now();
        if (remaining <= 0) return;
        try {
          const playlist = await read(
            candidate.url,
            AbortSignal.timeout(remaining),
            "omit",
          );
          return {
            until: Date.now() + 5 * 60_000,
            kind: playlist.kind,
            references: [...new Set(playlist.references.map((ref) => ref.url))],
            showChildren:
              playlist.kind === "master" &&
              (!playlist.variants.length ||
                playlist.variants.some((v) => !v.supported)),
          };
        } catch {
          return {
            until: Date.now() + 15_000,
            kind: "unknown",
            references: [],
          };
        }
      })
      .finally(() => pending.delete(candidate.id));
    pending.set(candidate.id, job);
    lanes[lane] = job.then(() => {});
    return job;
  };
}

// Keep the extra session data bounded even for very large variant lists.
export function trimInspections(candidates: ObservedCandidate[], now: number) {
  let chars = 0;
  for (const candidate of [...candidates].sort(
    (a, b) => b.lastSeen - a.lastSeen,
  )) {
    const inspection = candidate.inspection;
    if (!inspection) continue;
    const size = inspection.references.reduce(
      (n, url) => n + url.length + 4,
      0,
    );
    if (inspection.until <= now || chars + size > 1024 * 1024)
      delete candidate.inspection;
    else chars += size;
  }
}
