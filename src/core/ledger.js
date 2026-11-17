import { RESERVATION } from '../domain/states.js';
import { rowToReservation, tx } from './db.js';

// Local booking record of source quota. The source charges on acceptance; the
// console keeps a ledger so it can (a) never oversubscribe its own limit and
// (b) distinguish "we spent it" from "we merely hoped to".
//
// Rules:
//   pending   – held locally. Counts against local budget because a create may
//               be in flight. Never reused until resolved.
//   uncertain – a create may have been accepted. Counts against budget until
//               reconciled. This is the conservative choice: pretending it was
//               not charged is how quota silently leaks.
//   committed – definitely charged. Counts.
//   released  – definitely not charged. Returns budget.
//
// Local budget for a UTC day = source limit − charged − held(pending+uncertain).
// Never reuse quota on optimism; only a definite reject or a remote check that
// finds no job may release a reservation.
export class QuotaLedger {
  constructor(db, audit, now = () => Date.now()) {
    this.db = db;
    this.audit = audit;
    this.now = now;
  }

  #get(jobId) {
    return rowToReservation(this.db.prepare('SELECT * FROM reservations WHERE job_id = ?').get(jobId));
  }

  #set(jobId, patch) {
    const res = this.#get(jobId);
    if (!res) throw new Error(`job ${jobId} has no reservation`);
    const at = this.now();
    const next = {
      state: patch.state ?? res.state,
      remoteId: patch.remoteId ?? res.remoteId,
      note: patch.note ?? res.note,
      resolveAfter: patch.resolveAfter ?? res.resolveAfter,
    };
    this.db.prepare(`UPDATE reservations SET state = ?, remote_id = ?, note = ?, resolve_after = ?,
      updated_at = ?, version = version + 1 WHERE id = ?`)
      .run(next.state, next.remoteId, next.note, next.resolveAfter, at, res.id);
    return { ...res, ...next, updatedAt: at };
  }

  // A job may be re-queued after a definitive reject or a reconciliation that
  // proved nothing was charged. Its previous reservation is history, but the
  // table has one row per job (UNIQUE job_id), so start a fresh reservation
  // group by replacing the terminal row rather than inserting a second one.
  // Only a terminal reservation may be replaced; anything still consuming
  // budget means a second create would double-charge.
  reserve({ jobId, tenant, day, ttlMs }) {
    return tx(this.db, () => {
      const existing = this.db.prepare('SELECT * FROM reservations WHERE job_id = ?').get(jobId);
      if (existing && !['released', 'committed'].includes(existing.state)) {
        const err = new Error(`job ${jobId} already has an open reservation (${existing.state})`);
        err.code = 'RESERVATION_EXISTS';
        throw err;
      }
      // A committed reservation is never replaced: the source already charged.
      if (existing?.state === 'committed') {
        const err = new Error(`job ${jobId} already spent quota; refusing to reserve again`);
        err.code = 'RESERVATION_COMMITTED';
        throw err;
      }
      if (existing) {
        this.db.prepare('DELETE FROM reservations WHERE job_id = ?').run(jobId);
      }
      const id = `rsv_${jobId.slice(-12)}_${this.now().toString(36)}_${this.db.prepare('SELECT COUNT(*) AS n FROM reservations').get().n + 1}`;
      const at = this.now();
      this.db.prepare(`INSERT INTO reservations(id, job_id, tenant, state, day, resolve_after, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?)`)
        .run(id, jobId, tenant, RESERVATION.PENDING, day, at + ttlMs, at, at);
      this.db.prepare('UPDATE jobs SET reservation_id = ?, updated_at = ? WHERE id = ?').run(id, at, jobId);
      this.audit.append({
        kind: 'quota.reserved', tenant, jobId,
        detail: { reservationId: id, day, ttlMs, replaces: existing ? existing.state : null },
      });
      return this.#get(jobId);
    });
  }

  getForJob(jobId) {
    return this.#get(jobId);
  }

  markAttempted(jobId) {
    const at = this.now();
    this.db.prepare('UPDATE reservations SET attempt_started_at = COALESCE(attempt_started_at, ?), updated_at = ? WHERE job_id = ?')
      .run(at, at, jobId);
  }

  // Source definitively accepted the create. Quota is spent.
  commit(jobId, { remoteId, note = null }) {
    return tx(this.db, () => {
      const current = this.#get(jobId);
      if (!current) throw new Error(`job ${jobId} has no reservation`);
      if (current.state === RESERVATION.RELEASED) {
        const err = new Error('cannot commit a reservation that was released');
        err.code = 'RESERVATION_RELEASED';
        throw err;
      }
      const next = this.#set(jobId, { state: RESERVATION.COMMITTED, remoteId, note });
      this.audit.append({ kind: 'quota.committed', tenant: next.tenant, jobId, detail: { remoteId } });
      return next;
    });
  }

  // Source definitively rejected before charging, or the create provably never
  // left the console.
  release(jobId, { reason, note = null }) {
    return tx(this.db, () => {
      const current = this.#get(jobId);
      if (!current) throw new Error(`job ${jobId} has no reservation`);
      if (current.state === RESERVATION.RELEASED) return current;
      const next = this.#set(jobId, { state: RESERVATION.RELEASED, note: note ?? reason });
      this.audit.append({ kind: 'quota.released', tenant: next.tenant, jobId, detail: { reason, note } });
      return next;
    });
  }

  // Create timed out / errored ambiguously. Keep the budget held and require
  // reconciliation before anything else may use it.
  markUncertain(jobId, { reason, resolveAfter }) {
    return tx(this.db, () => {
      const current = this.#get(jobId);
      if (!current || current.state === RESERVATION.RELEASED) return current;
      const next = this.#set(jobId, { state: RESERVATION.UNCERTAIN, note: reason, resolveAfter });
      this.audit.append({ kind: 'quota.uncertain', tenant: next.tenant, jobId, detail: { reason } });
      return next;
    });
  }

  // Manual operator decision after checking remote state.
  resolveManually(jobId, { decision, actor, note, remoteId = null }) {
    if (!['committed', 'released'].includes(decision)) throw new Error('decision must be committed or released');
    return tx(this.db, () => {
      const current = this.#get(jobId);
      if (!current) throw new Error(`job ${jobId} has no reservation`);
      const next = this.#set(jobId, {
        state: decision,
        remoteId: remoteId ?? current.remoteId,
        note: note ?? `manual ${decision}`,
      });
      this.audit.append({
        kind: 'quota.manual_resolve', actor, tenant: next.tenant, jobId,
        detail: { decision, note, previousState: current.state, remoteId },
      });
      return next;
    });
  }

  breakdown(day) {
    const rows = this.db.prepare('SELECT state, COUNT(*) AS n FROM reservations WHERE day = ? GROUP BY state').all(day);
    const out = { pending: 0, committed: 0, uncertain: 0, released: 0 };
    for (const r of rows) out[r.state] = r.n;
    return out;
  }

  // Conservative local budget view.
  snapshot(sourceQuota) {
    const day = sourceQuota.day;
    const b = this.breakdown(day);
    const held = b.pending + b.uncertain;
    const charged = b.committed;
    // Whichever number is larger wins: the source may have been used outside
    // this console (external use), and we must never oversubscribe on optimism.
    const effectiveUsed = Math.max(sourceQuota.used, charged + held);
    const localRemaining = Math.max(0, sourceQuota.limit - effectiveUsed);
    return {
      day,
      sourceLimit: sourceQuota.limit,
      sourceUsed: sourceQuota.used,
      sourceRemaining: sourceQuota.remaining,
      charged,
      held,
      pending: b.pending,
      uncertain: b.uncertain,
      released: b.released,
      localRemaining,
      // A discrepancy is not itself an error; it is the signal to reconcile.
      discrepancy: sourceQuota.used !== charged ? {
        amount: sourceQuota.used - charged,
        explanation: sourceQuota.used > charged
          ? 'source used more than we can prove — possible accepted-but-unrecorded create or external use'
          : 'our ledger has charges the source does not — reconcile before trusting the ledger',
      } : null,
      resetsAt: sourceQuota.resetsAt,
    };
  }
}
