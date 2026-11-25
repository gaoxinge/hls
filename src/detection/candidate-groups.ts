import type {
  Candidate,
  VariantOption,
  VisibleCandidate,
} from "../shared/model";
import { TTL } from "./candidates";

export interface Inspection {
  contentTypes?: VisibleCandidate["contentTypes"];
  until: number;
  kind: "master" | "media" | "unknown";
  references: string[];
  variants?: VariantOption[];
  showChildren?: boolean;
}

export interface ObservedCandidate extends Candidate {
  aliases?: string[];
  inspection?: Inspection;
}

function pageCandidates(
  candidates: ObservedCandidate[],
  tabId?: number,
  now = Date.now(),
) {
  const pages = new Map<string, ObservedCandidate[]>();
  for (const c of candidates) {
    if (now - c.lastSeen >= TTL || (tabId !== undefined && c.tabId !== tabId))
      continue;
    const key = JSON.stringify([c.tabId, c.epoch]);
    const page = pages.get(key);
    if (page) page.push(c);
    else pages.set(key, [c]);
  }
  return pages;
}

function asCandidate(c: ObservedCandidate, now: number): VisibleCandidate {
  const { id, tabId, epoch, url, firstSeen, lastSeen, status } = c;
  const inspection =
    c.inspection && c.inspection.until > now ? c.inspection : undefined;
  return {
    id,
    tabId,
    epoch,
    url,
    firstSeen,
    lastSeen,
    status,
    variants: inspection?.variants,
    contentTypes: inspection?.contentTypes,
  };
}

function visibleOnPage(
  page: ObservedCandidate[],
  now: number,
): VisibleCandidate[] {
  const masters = page.filter(
    (c) =>
      c.status >= 200 &&
      c.status < 300 &&
      c.inspection?.kind === "master" &&
      c.inspection.until > now,
  );
  const masterIds = new Set(masters.map((c) => c.id));
  const byUrl = new Map<string, ObservedCandidate | null>();
  for (const c of page) {
    for (const url of new Set([c.url, ...(c.aliases ?? [])])) {
      // A redirect alias shared by different responses is ambiguous.
      byUrl.set(url, byUrl.has(url) ? null : c);
    }
  }
  const hidden = new Set<string>();
  const children = new Map<string, VisibleCandidate[]>();
  const grouped = new Map<string, Pick<Candidate, "id" | "url">[]>();
  for (const parent of masters) {
    const matches = new Set<ObservedCandidate>();
    for (const url of parent.inspection!.references) {
      const child = byUrl.get(url);
      if (child && !masterIds.has(child.id)) {
        hidden.add(child.id);
        matches.add(child);
      }
    }
    if (matches.size)
      grouped.set(
        parent.id,
        Array.from(matches, ({ id, url }) => ({ id, url })),
      );
    if (parent.inspection!.showChildren && matches.size)
      children.set(
        parent.id,
        Array.from(matches, (c) => asCandidate(c, now)),
      );
  }
  return page
    .filter((c) => !hidden.has(c.id))
    .map((c) => ({
      ...asCandidate(c, now),
      groupedCandidates: grouped.get(c.id),
      ...(children.has(c.id) ? { children: children.get(c.id) } : {}),
    }));
}

export function selectVisibleCandidates(
  candidates: ObservedCandidate[],
  tabId?: number,
  now = Date.now(),
) {
  return Array.from(pageCandidates(candidates, tabId, now).values()).flatMap(
    (page) => visibleOnPage(page, now),
  );
}
