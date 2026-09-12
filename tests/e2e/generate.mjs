import { mkdir } from "node:fs/promises";
import { execFileSync } from "node:child_process";
const ffmpeg = process.env.FFMPEG ?? "ffmpeg";
const count = process.argv.includes("--large") ? 128 : 12;
for (const format of ["ts", "fmp4"]) {
  const dir = `test-results/media/${format}`;
  await mkdir(dir, { recursive: true });
  execFileSync(
    ffmpeg,
    [
      "-y",
      "-hide_banner",
      "-loglevel",
      "error",
      "-f",
      "lavfi",
      "-i",
      "testsrc=size=160x90:rate=25",
      "-f",
      "lavfi",
      "-i",
      "sine=frequency=1000:sample_rate=48000",
      "-t",
      String(count),
      "-c:v",
      "libx264",
      "-pix_fmt",
      "yuv420p",
      "-preset",
      "ultrafast",
      "-g",
      "25",
      "-sc_threshold",
      "0",
      "-c:a",
      "aac",
      "-b:a",
      "64k",
      "-f",
      "hls",
      "-hls_time",
      "1",
      "-hls_playlist_type",
      "vod",
      ...(format === "fmp4" ? ["-hls_segment_type", "fmp4"] : []),
      "-hls_segment_filename",
      `${dir}/segment%d.${format === "ts" ? "ts" : "m4s"}`,
      `${dir}/list.m3u8`,
    ],
    { stdio: "inherit" },
  );
}
console.log(`Generated ${count}s H.264/AAC fixtures with ${ffmpeg}`);
