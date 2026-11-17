import crypto from 'node:crypto';

// Append-only, hash-chained audit log. Every operator action (reconcile,
// manual resolve, cancel, retry, ingest) and every automatic state transition
// that affects quota is recorded. The chain makes accidental edits detectable;
// it is not a security boundary against someone with the database file.
export class AuditLog {
  constructor(db, now = () => Date.now()) {
    this.db = db;
    this.now = now;
    this.last = db.prepare('SELECT hash FROM events ORDER BY id DESC LIMIT 1').get()?.hash ?? 'genesis';
  }

  append({ kind, actor = 'system', tenant = null, jobId = null, detail = {} }) {
    const at = this.now();
    const prevHash = this.last;
    const payload = JSON.stringify({ at, kind, actor, tenant, jobId, detail, prevHash });
    const hash = crypto.createHash('sha256').update(payload).digest('hex');
    this.db.prepare(`INSERT INTO events(at, kind, actor, tenant, job_id, detail, prev_hash, hash)
      VALUES (?,?,?,?,?,?,?,?)`).run(at, kind, actor, tenant, jobId, JSON.stringify(detail), prevHash, hash);
    this.last = hash;
    return { at, kind, actor, tenant, jobId, detail, prevHash, hash };
  }

  list({ tenant = null, jobId = null, limit = 100, before = null } = {}) {
    const clauses = [];
    const args = [];
    if (tenant) { clauses.push('tenant = ?'); args.push(tenant); }
    if (jobId) { clauses.push('job_id = ?'); args.push(jobId); }
    if (before) { clauses.push('id < ?'); args.push(before); }
    const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
    const rows = this.db.prepare(`SELECT * FROM events ${where} ORDER BY id DESC LIMIT ?`).all(...args, limit);
    return rows.map((r) => ({
      id: r.id, at: r.at, kind: r.kind, actor: r.actor, tenant: r.tenant,
      jobId: r.job_id, detail: JSON.parse(r.detail), hash: r.hash, prevHash: r.prev_hash,
    }));
  }

  verifyChain(limit = 1000) {
    const rows = this.db.prepare('SELECT * FROM events ORDER BY id ASC LIMIT ?').all(limit);
    let prev = 'genesis';
    for (const r of rows) {
      const payload = JSON.stringify({
        at: r.at, kind: r.kind, actor: r.actor, tenant: r.tenant,
        jobId: r.job_id, detail: JSON.parse(r.detail), prevHash: prev,
      });
      const expected = crypto.createHash('sha256').update(payload).digest('hex');
      if (r.prev_hash !== prev || expected !== r.hash) return { ok: false, brokenAt: r.id };
      prev = r.hash;
    }
    return { ok: true, checked: rows.length, head: prev };
  }
}
