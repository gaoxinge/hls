import type { ObservedCandidate } from "./candidate-groups";
export const TTL = 30 * 60 * 1000;
export function isHls(url: string, mime = "") {
  try {
    return (
      /\.m3u8?$/i.test(new URL(url).pathname) ||
      [
        "application/vnd.apple.mpegurl",
        "application/x-mpegurl",
        "audio/mpegurl",
      ].includes(mime.split(";")[0].trim().toLowerCase())
    );
  } catch {
    return false;
  }
}
export function addCandidate(
  list: ObservedCandidate[],
  item: ObservedCandidate,
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
    if (previous.status !== item.status) delete previous.inspection;
    if (item.aliases?.length)
      previous.aliases = [
        ...new Set([...(previous.aliases ?? []), ...item.aliases]),
      ].slice(-8);
    previous.lastSeen = item.lastSeen;
    previous.status = item.status;
  } else fresh.push(item);
  const mine = fresh.filter((c) => c.tabId === item.tabId).slice(-100);
  return [...fresh.filter((c) => c.tabId !== item.tabId), ...mine].slice(-1000);
}
