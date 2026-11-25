export const mediaTypes = [
  "VIDEO",
  "AUDIO",
  "SUBTITLES",
  "CLOSED-CAPTIONS",
] as const;
export type MediaType = (typeof mediaTypes)[number];

/** Evidence declared by HLS, independent of filename, container and download support. */
export function declaredMediaTypes(
  tag: string,
  attrs: Record<string, string>,
): MediaType[] {
  if (tag === "EXT-X-MEDIA")
    return mediaTypes.filter((type) => attrs.TYPE === type);
  const types = new Set<MediaType>();
  if (
    tag === "EXT-X-I-FRAMES-ONLY" ||
    tag === "EXT-X-I-FRAME-STREAM-INF" ||
    attrs.RESOLUTION ||
    attrs["FRAME-RATE"]
  )
    types.add("VIDEO");
  for (const codec of (attrs.CODECS ?? "").toLowerCase().split(",")) {
    const family = codec.trim().split(".")[0];
    if (
      [
        "avc1",
        "avc3",
        "hvc1",
        "hev1",
        "av01",
        "vp09",
        "dvh1",
        "dvhe",
        "dva1",
        "dvav",
      ].includes(family)
    )
      types.add("VIDEO");
    if (
      [
        "mp4a",
        "ac-3",
        "ec-3",
        "ac-4",
        "opus",
        "flac",
        "alac",
        "mha1",
        "mhm1",
      ].includes(family)
    )
      types.add("AUDIO");
    if (["wvtt", "stpp"].includes(family)) types.add("SUBTITLES");
  }
  return [...types];
}
