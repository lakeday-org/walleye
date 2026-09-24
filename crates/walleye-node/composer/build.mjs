// Bundles the composer into the single file a walleye worker must be: one
// module, no imports. The engine embeds the result, so building the engine
// never needs Node; this runs only when something here changes.
//
// The first line records a hash of every source this was built from, and an
// engine test recomputes it, so a bundle left stale after an edit fails the
// build rather than shipping the old composer.
import { build } from "esbuild";
import { createHash } from "node:crypto";
import { readFileSync, readdirSync, writeFileSync } from "node:fs";

export function sourceHash() {
  const hash = createHash("sha256");
  const files = [
    "package-lock.json",
    ...readdirSync("src")
      .filter((name) => name.endsWith(".js"))
      .sort()
      .map((name) => `src/${name}`),
  ];
  for (const file of files) {
    hash.update(file);
    hash.update("\0");
    hash.update(readFileSync(file));
    hash.update("\0");
  }
  return hash.digest("hex");
}

const result = await build({
  entryPoints: ["src/worker.js"],
  bundle: true,
  format: "esm",
  minify: true,
  legalComments: "none",
  write: false,
});
const out = "../src/see/composer.js";
writeFileSync(out, `// source-sha256: ${sourceHash()}\n${result.outputFiles[0].text}`);
console.log(`wrote ${out}`);
