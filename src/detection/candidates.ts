import type { Candidate } from "../shared/model";
export const TTL = 30 * 60 * 1000;
export function isHls(url: string, mime = "") {
  try {
    return (
      /\.m3u8$/i.test(new URL(url).pathname) ||
      ["application/vnd.apple.mpegurl", "application/x-mpegurl"].includes(
        mime.split(";")[0].trim().toLowerCase(),
      )
    );
  } catch {
    return false;
  }
}
export function addCandidate(
  list: Candidate[],
  item: Candidate,
  now = Date.now(),
) {
  const fresh = list.filter(
    (c) =>
      now - c.lastSeen < TTL &&
      (c.tabId !== item.tabId || c.epoch === item.epoch),
  );
  const previous = fresh.find(
    (c) =>
      c.tabId === item.tabId && c.epoch === item.epoch && c.url === item.url,
  );
  if (previous) {
    previous.lastSeen = item.lastSeen;
    previous.status = item.status;
  } else fresh.push(item);
  const mine = fresh.filter((c) => c.tabId === item.tabId).slice(-100);
  return [...fresh.filter((c) => c.tabId !== item.tabId), ...mine].slice(-1000);
}
