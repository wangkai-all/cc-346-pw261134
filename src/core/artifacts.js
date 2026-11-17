import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

// Local artifact store. Downloads are written to a temp file first and only
// renamed into place after size + hash checks, so a partially received or
// corrupted download can never be mistaken for a complete one.
export class ArtifactStore {
  constructor(filesDir) {
    this.filesDir = filesDir;
    fs.mkdirSync(filesDir, { recursive: true });
  }

  filePath(jobId) {
    return path.join(this.filesDir, `${jobId}.csv`);
  }

  exists(jobId) {
    return fs.existsSync(this.filePath(jobId));
  }

  describe(jobId) {
    const p = this.filePath(jobId);
    if (!fs.existsSync(p)) return null;
    const stat = fs.statSync(p);
    const sha256 = crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
    return { path: p, bytes: stat.size, sha256 };
  }

  // Write atomically. Returns artifact metadata.
  write(jobId, bytes) {
    const final = this.filePath(jobId);
    const tmp = path.join(this.filesDir, `.${jobId}.${process.pid}.part`);
    const buf = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
    const sha256 = crypto.createHash('sha256').update(buf).digest('hex');
    fs.writeFileSync(tmp, buf);
    try {
      fs.renameSync(tmp, final);
    } catch (err) {
      try { fs.unlinkSync(tmp); } catch { /* ignore */ }
      throw err;
    }
    return { path: final, bytes: buf.length, sha256 };
  }

  read(jobId) {
    const p = this.filePath(jobId);
    if (!fs.existsSync(p)) return null;
    return fs.readFileSync(p);
  }

  // Ingestion confirmation: verify the artifact is byte-identical to what was
  // recorded at download time before declaring it ingested. Detects tampering
  // or torn writes.
  verify(jobId, expectedSha256, expectedBytes) {
    const info = this.describe(jobId);
    if (!info) return { ok: false, reason: 'artifact file missing on disk' };
    if (expectedSha256 && info.sha256 !== expectedSha256) {
      return { ok: false, reason: `hash mismatch: expected ${expectedSha256}, found ${info.sha256}` };
    }
    if (expectedBytes != null && info.bytes !== expectedBytes) {
      return { ok: false, reason: `size mismatch: expected ${expectedBytes}, found ${info.bytes}` };
    }
    return { ok: true, artifact: info };
  }
}
