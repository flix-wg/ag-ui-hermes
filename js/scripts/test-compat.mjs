// Runs typecheck and tests against each supported @ag-ui/client version, then restores the lockfile install.
import { execSync } from "node:child_process";

const versions = process.argv.slice(2).length ? process.argv.slice(2) : ["1.0.3", "1.0.4"];
const run = (cmd) => execSync(cmd, { stdio: "inherit" });
let failed = false;
try {
  for (const v of versions) {
    console.log(`\n== @ag-ui/client@${v} ==`);
    try {
      run(`npm install --no-save --no-audit --no-fund @ag-ui/client@${v}`);
      run("npx tsc --noEmit && npx vitest run");
    } catch {
      failed = true;
    }
  }
} finally {
  run("npm ci --no-audit --no-fund");
}
process.exit(failed ? 1 : 0);
