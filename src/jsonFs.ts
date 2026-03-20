import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

/**
 * Write JSON atomically: temp file in the same directory, fsync, then rename.
 * Avoids truncated files if the process dies mid-write.
 */
export function writeJsonAtomic(filePath: string, value: unknown, pretty = true): void {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
  const body = (pretty ? JSON.stringify(value, null, 2) : JSON.stringify(value)) + "\n";
  const tmp = path.join(dir, `.${path.basename(filePath)}.${crypto.randomUUID()}.tmp`);
  try {
    const fd = fs.openSync(tmp, "w");
    try {
      fs.writeFileSync(fd, body, "utf-8");
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(tmp, filePath);
  } catch (err) {
    try {
      if (fs.existsSync(tmp)) fs.unlinkSync(tmp);
    } catch {
      // ignore cleanup errors
    }
    throw err;
  }
}
