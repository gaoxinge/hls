import { it, expect } from "vitest";
import { assertTransition } from "../../src/domain/task/transitions";
import { parse } from "../../src/adapters/parser/m3u8";
import { createPlan } from "../../src/planning/plan";
it("rejects completion without export and revival of terminal tasks", () => {
  expect(() => assertTransition("downloading", "completed")).toThrow();
  expect(() => assertTransition("completed", "queued")).toThrow();
  expect(() => assertTransition("exporting", "completed")).not.toThrow();
  expect(() => assertTransition("interrupted", "queued")).not.toThrow();
});
it("plan identity ignores source positions but includes ranges", () => {
  const text =
    "#EXTM3U\n#EXT-X-VERSION:4\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\n#EXT-X-BYTERANGE:188@0\ns.ts\n#EXT-X-ENDLIST";
  const url = "https://a/list";
  expect(createPlan(parse(text, url)).id).toBe(
    createPlan(parse(text.replace("#EXTINF", "# comment\n#EXTINF"), url)).id,
  );
  expect(createPlan(parse(text, url)).id).not.toBe(
    createPlan(parse(text.replace("188@0", "188@188"), url)).id,
  );
});
