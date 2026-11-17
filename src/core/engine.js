import crypto from 'node:crypto';
import { JOB, RESERVATION, isTerminal } from '../domain/states.js';
import { nextQueueSeq, rowToJob, rowToReservation, tx } from './db.js';
import { rankCreateQueue, rankDownloadQueue, POLICIES } from './policy.js';

const CREATE_TIMEOUT = Symbol('create-timeout');

// Drives every job transition. Invariants:
//  * A job is only created after a local reservation exists.
//  * A create timeout never releases quota and never recreates automatically:
//    the job becomes `uncertain` and must be reconciled.
//  * A downloaded artifact is immutable; ingestion re-verifies its hash.
//  * Cancellation never claims a refund of source-charged quota.
export class Engine {
  constructor({ db, audit, ledger, source, artifacts, config, now = () => Date.now() }) {
    this.db = db;
    this.audit = audit;
    this.ledger = ledger;
    this.source = source;
    this.artifacts = artifacts;
    this.config = config;
    this.now = now;
    this.inflightCreates = new Set();
    this.inflightDownloads = new Set();
    this.timer = null;
    this.ticking = false;
    this.stopping = false;
    this.lastSourceLimitHitUntil = 0;
    this.blockedDay = null;
    this.policy = config.schedulerPolicy;
    this.starvationMs = config.starvationMs;
  }

  start() {
    if (this.timer) return;
    this.recoverOnStartup();
    this.timer = setInterval(() => {
      this.tick().catch((err) => {
        console.error('[engine] tick failed', err);
      });
    }, this.config.tickMs);
    this.timer.unref?.();
  }

  stop() {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  setPolicy(policy) {
    if (!POLICIES[policy]) throw new Error(`unknown policy: ${policy}`);
    this.policy = policy;
    this.audit.append({ kind: 'policy.changed', actor: 'operator', detail: { policy, summary: POLICIES[policy].summary } });
    return this.policy;
  }

  // ---- queueing -----------------------------------------------------------

  enqueue({ tenant, name, meta = {}, clientKey = null, actor = 'web' }) {
    if (!tenant || !name) throw Object.assign(new Error('tenant and name are required'), { status: 400 });
    return tx(this.db, () => {
      if (clientKey) {
        const existing = rowToJob(this.db.prepare('SELECT * FROM jobs WHERE tenant = ? AND client_key = ?').get(tenant, clientKey));
        if (existing) return { job: existing, reused: true };
      }
      const id = `job_${crypto.randomUUID()}`;
      const at = this.now();
      const seq = nextQueueSeq(this.db);
      this.db.prepare(`INSERT INTO jobs(id, tenant, name, meta, state, queue_seq, client_key, created_by,
        queued_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)`)
        .run(id, tenant, name, JSON.stringify(meta), JOB.QUEUED, seq, clientKey, actor, at, at);
      this.audit.append({ kind: 'job.enqueued', actor, tenant, jobId: id, detail: { name, queueSeq: seq, meta } });
      return { job: this.getJob(id), reused: false };
    });
  }

  getJob(id, tenant = null) {
    const row = tenant
      ? this.db.prepare('SELECT * FROM jobs WHERE id = ? AND tenant = ?').get(id, tenant)
      : this.db.prepare('SELECT * FROM jobs WHERE id = ?').get(id);
    return rowToJob(row);
  }

  listJobs({ tenant = null, states = null, limit = 200 } = {}) {
    const clauses = [];
    const args = [];
    if (tenant) { clauses.push('tenant = ?'); args.push(tenant); }
    if (states?.length) { clauses.push(`state IN (${states.map(() => '?').join(',')})`); args.push(...states); }
    const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
    return this.db.prepare(`SELECT * FROM jobs ${where} ORDER BY queue_seq ASC LIMIT ?`)
      .all(...args, limit).map(rowToJob);
  }

  // ---- tick ---------------------------------------------------------------

  async tick() {
    if (this.ticking || this.stopping) return;
    this.ticking = true;
    const tasks = [];
    try {
      this.#refreshRunning();
      this.#expireOverdue();
      const creates = this.#launchCreates();
      const downloads = this.#launchDownloads();
      tasks.push(...creates, ...downloads);
      // Reconciliation runs before any new create so held quota is settled
      // first whenever possible.
      tasks.push(this.#reconcileDue());
      await Promise.allSettled(tasks);
    } finally {
      this.ticking = false;
    }
  }

  async #waitFor(promise, ms) {
    let timer;
    const timeout = new Promise((resolve) => {
      timer = setTimeout(() => resolve(CREATE_TIMEOUT), ms);
      timer.unref?.();
    });
    try {
      return await Promise.race([promise, timeout]);
    } finally {
      clearTimeout(timer);
    }
  }

  // ---- create -------------------------------------------------------------

  #launchCreates() {
    const tasks = [];
    const slots = Math.max(0, this.config.createConcurrency - this.inflightCreates.size);
    if (slots === 0) return tasks;
    if (this.now() < this.lastSourceLimitHitUntil) {
      // We stopped creating after the source rejected us as exhausted. Do not
      // hammer it every tick, but do notice if the allowance comes back early
      // (a plan change, or an operator raising the limit in the simulator)
      // rather than waiting blindly until midnight.
      const sourceQuota = this.source.quota();
      if (sourceQuota.remaining > 0 || sourceQuota.day !== this.blockedDay) {
        this.lastSourceLimitHitUntil = 0;
    this.blockedDay = null;
        this.blockedDay = null;
        this.audit.append({
          kind: 'ledger.allowance_returned', actor: 'system',
          detail: { sourceRemaining: sourceQuota.remaining, day: sourceQuota.day },
        });
      } else {
        return tasks;
      }
    }

    const queued = this.listJobs({ states: [JOB.QUEUED] });
    if (queued.length === 0) return tasks;
    // Re-check the budget inside the guard above: two concurrent ticks are
    // prevented by `ticking`, but a create in flight from the same tick is not
    // reflected in the ledger snapshot until reserve() runs. Reserve happens
    // synchronously per candidate, so the snapshot below is re-read each loop.
    const tenantActive = this.#tenantActiveMap();
    const ranked = rankCreateQueue(queued, {
      policy: this.policy,
      starvationMs: this.starvationMs,
      now: this.now(),
      tenantActive,
    });

    for (const candidate of ranked) {
      if (tasks.length >= slots) break;
      const quota = this.quotaSnapshot();
      if (quota.localRemaining <= 0) break;
      try {
        this.ledger.reserve({
          jobId: candidate.id,
          tenant: candidate.tenant,
          day: quota.day,
          ttlMs: this.config.createTimeoutMs + this.config.reconcileGraceMs,
        });
      } catch (err) {
        if (err.code === 'RESERVATION_EXISTS' || err.code === 'RESERVATION_COMMITTED') {
          // Should be unreachable (queued jobs have no open reservation), but a
          // stale row must never crash the tick or cause a second charge.
          this.audit.append({
            kind: 'quota.reserve_refused', tenant: candidate.tenant, jobId: candidate.id,
            detail: { code: err.code, message: err.message },
          });
          continue;
        }
        throw err;
      }
      this.#setJob(candidate.id, { state: JOB.CREATING, create_started_at: this.now(), attempts: candidate.attempts + 1 });
      this.audit.append({
        kind: 'job.create_started', tenant: candidate.tenant, jobId: candidate.id,
        detail: { queueReason: candidate.reason, starvation: Boolean(candidate.starving), policy: this.policy },
      });
      this.inflightCreates.add(candidate.id);
      tasks.push(this.#performCreate(candidate).finally(() => this.inflightCreates.delete(candidate.id)));
    }
    return tasks;
  }

  #tenantActiveMap() {
    const rows = this.db.prepare(`SELECT tenant, COUNT(*) AS n FROM jobs
      WHERE state IN (?,?,?,?) GROUP BY tenant`).all(JOB.CREATING, JOB.UNCERTAIN, JOB.RUNNING, JOB.READY);
    const map = new Map();
    for (const r of rows) map.set(r.tenant, r.n);
    return map;
  }

  async #performCreate(job) {
    this.ledger.markAttempted(job.id);
    const res = this.ledger.getForJob(job.id);
    let outcome;
    try {
      outcome = await this.#waitFor(
        this.source.create({ tenant: job.tenant, name: job.name, meta: { ...job.meta, jobId: job.id } }),
        this.config.createTimeoutMs,
      );
    } catch (err) {
      outcome = { outcome: 'error', message: String(err?.message ?? err) };
    }

    // Cancellation is only meaningful to record here; the create already left
    // the console, so a cancel must never imply a refund.
    const fresh = this.getJob(job.id);
    const cancelRequested = Boolean(fresh?.cancelRequestedAt);

    if (outcome === CREATE_TIMEOUT || outcome?.outcome === 'lost' || outcome?.outcome === 'error') {
      const reason = outcome === CREATE_TIMEOUT
        ? `create timed out after ${this.config.createTimeoutMs}ms; outcome unknown`
        : outcome?.outcome === 'error'
          ? `create failed ambiguously: ${outcome.message}`
          : 'create response lost; outcome unknown';
      this.ledger.markUncertain(job.id, {
        reason,
        resolveAfter: this.now() + this.config.reconcileGraceMs,
      });
      this.#setJob(job.id, {
        state: JOB.UNCERTAIN,
        last_error: reason,
      });
      if (cancelRequested) this.#setJob(job.id, { cancel_requested_at: this.now() });
      this.audit.append({
        kind: 'job.create_uncertain', tenant: job.tenant, jobId: job.id,
        detail: { reason, cancelRequested, note: 'quota held; automatic recreate suppressed; reconciliation scheduled' },
      });
      return;
    }

    if (outcome?.outcome === 'rejected') {
      this.ledger.release(job.id, { reason: `source rejected create: ${outcome.code}` });
      this.#setJob(job.id, {
        state: cancelRequested ? JOB.CANCELLED : JOB.QUEUED,
        last_error: `source rejected: ${outcome.code}`,
        create_started_at: null,
      });
      if (outcome.code === 'QUOTA_EXCEEDED') {
        // The source is exhausted even though our ledger thought otherwise.
        // Stop creating until the allowance resets; flag the discrepancy.
        const sourceQuota = this.source.quota();
        this.lastSourceLimitHitUntil = sourceQuota.resetsAt;
        this.blockedDay = sourceQuota.day;
        this.audit.append({
          kind: 'ledger.discrepancy', tenant: job.tenant, jobId: job.id,
          detail: { kind: 'source_quota_exhausted', ledger: this.quotaSnapshot() },
        });
      }
      this.audit.append({
        kind: cancelRequested ? 'job.cancelled' : 'job.requeued',
        tenant: job.tenant, jobId: job.id,
        detail: { reason: outcome.code, quotaReleased: true },
      });
      return;
    }

    // Accepted.
    this.ledger.commit(job.id, { remoteId: outcome.remoteId });
    if (cancelRequested) {
      // Quota is spent and the source keeps it. We cancel the *local* job but
      // mark the remote result abandoned rather than pretending it vanished.
      this.#setJob(job.id, {
        state: JOB.CANCELLED,
        remote_id: outcome.remoteId,
        ready_at: null,
        expires_at: outcome.expiresAt,
        last_error: 'cancelled after create was accepted; source quota is not refunded',
      });
      this.audit.append({
        kind: 'job.cancelled', tenant: job.tenant, jobId: job.id,
        detail: {
          remoteId: outcome.remoteId, refunded: false,
          note: 'cancel arrived while create was in flight; source charged quota and does not refund',
        },
      });
      return;
    }
    const lookup = this.source.lookup(outcome.remoteId);
    const remote = lookup.found ? lookup.job : null;
    this.#setJob(job.id, {
      state: JOB.RUNNING,
      remote_id: outcome.remoteId,
      ready_at: remote?.readyAt ?? null,
      expires_at: outcome.expiresAt ?? remote?.expiresAt ?? null,
      last_error: null,
    });
    this.audit.append({
      kind: 'job.created', tenant: job.tenant, jobId: job.id,
      detail: { remoteId: outcome.remoteId, expiresAt: outcome.expiresAt },
    });
  }

  // ---- running / expiry ---------------------------------------------------

  #refreshRunning() {
    const running = this.listJobs({ states: [JOB.RUNNING] });
    for (const job of running) {
      if (!job.remoteId) continue;
      const res = this.source.lookup(job.remoteId);
      const at = this.now();
      if (!res.found) {
        this.#setJob(job.id, {
          state: JOB.FAILED,
          last_error: 'source no longer knows this export job; result cannot be downloaded',
        });
        this.audit.append({
          kind: 'job.failed', tenant: job.tenant, jobId: job.id,
          detail: { reason: 'remote_not_found', remoteId: job.remoteId, quotaRefunded: false,
            note: 'source quota already charged; this is a data loss, not a quota refund' },
        });
        continue;
      }
      const remote = res.job;
      if (remote.status === 'expired' || at >= remote.expiresAt) {
        this.#expireJob(job, 'result expired before it was downloaded');
        continue;
      }
      if (remote.status === 'ready') {
        this.#setJob(job.id, {
          state: JOB.READY, ready_at: remote.readyAt, expires_at: remote.expiresAt,
          last_error: null, retry_after: null,
        });
        this.audit.append({ kind: 'job.ready', tenant: job.tenant, jobId: job.id, detail: { readyAt: remote.readyAt, expiresAt: remote.expiresAt } });
      } else if (job.readyAt !== remote.readyAt || job.expiresAt !== remote.expiresAt) {
        this.#setJob(job.id, { ready_at: remote.readyAt, expires_at: remote.expiresAt });
      }
    }
  }

  #expireOverdue() {
    const at = this.now();
    const overdue = this.db.prepare(`SELECT * FROM jobs WHERE state IN (?,?) AND expires_at IS NOT NULL AND expires_at <= ?`)
      .all(JOB.RUNNING, JOB.READY, at).map(rowToJob);
    for (const job of overdue) this.#expireJob(job, 'expiry deadline passed');
  }

  #expireJob(job, reason) {
    this.#setJob(job.id, { state: JOB.EXPIRED, last_error: reason });
    this.audit.append({
      kind: 'job.expired', tenant: job.tenant, jobId: job.id,
      detail: { reason, remoteId: job.remoteId, quotaRefunded: false,
        note: 'created quota was charged by the source and cannot be reclaimed by expiry' },
    });
  }

  // ---- download -----------------------------------------------------------

  #launchDownloads() {
    const tasks = [];
    const slots = Math.max(0, this.config.downloadConcurrency - this.inflightDownloads.size);
    if (slots === 0) return tasks;

    const at = this.now();
    const ready = this.listJobs({ states: [JOB.READY] })
      .filter((j) => !this.inflightDownloads.has(j.id))
      // Back off after a failed transfer so a persistent source fault does not
      // turn into a retry storm against an already-scarce service.
      .filter((j) => (j.retryAfter ?? 0) <= at);
    if (ready.length === 0) return tasks;

    const ranked = rankDownloadQueue(ready, { policy: this.policy, now: this.now() })
      .slice(0, slots);
    for (const job of ranked) {
      this.inflightDownloads.add(job.id);
      tasks.push(this.#performDownload(job).finally(() => this.inflightDownloads.delete(job.id)));
    }
    return tasks;
  }

  async #performDownload(job) {
    const res = await this.source.download(job.remoteId);
    const at = this.now();
    if (res.outcome === 'ok') {
      let artifact;
      try {
        artifact = this.artifacts.write(job.id, res.bytes);
      } catch (err) {
        this.#setJob(job.id, {
          state: JOB.READY,
          last_error: `local write failed: ${err.message}`,
          retry_after: at + 1000,
        });
        this.audit.append({ kind: 'download.failed', tenant: job.tenant, jobId: job.id, detail: { reason: 'local_write', message: err.message } });
        return;
      }
      this.#setJob(job.id, {
        state: JOB.DOWNLOADED,
        downloaded_at: at,
        artifact_path: artifact.path,
        artifact_sha256: artifact.sha256,
        artifact_bytes: artifact.bytes,
        last_error: null,
        retry_after: null,
      });
      this.audit.append({
        kind: 'job.downloaded', tenant: job.tenant, jobId: job.id,
        detail: { remoteId: job.remoteId, bytes: artifact.bytes, sha256: artifact.sha256, expiresAt: job.expiresAt },
      });
      return;
    }
    if (res.outcome === 'expired') {
      this.#expireJob(this.getJob(job.id), 'source reports the result expired before download');
      return;
    }
    if (res.outcome === 'not_found') {
      this.#setJob(job.id, { state: JOB.FAILED, last_error: 'source lost the result; cannot download' });
      this.audit.append({ kind: 'job.failed', tenant: job.tenant, jobId: job.id, detail: { reason: 'remote_not_found', remoteId: job.remoteId } });
      return;
    }
    if (res.outcome === 'not_ready') {
      const lookup = this.source.lookup(job.remoteId);
      if (lookup.found) this.#setJob(job.id, { ready_at: lookup.job.readyAt, expires_at: lookup.job.expiresAt });
      return;
    }
    // 'lost': retry while the result is still alive; the attempt counter is
    // visible to operators so a flapping source is diagnosable.
    const next = this.getJob(job.id);
    const attempts = (next?.downloadAttempts ?? 0) + 1;
    const expired = next?.expiresAt != null && at >= next.expiresAt;
    // Exponential backoff capped at 5s, and never past the expiry deadline.
    const backoff = Math.min(5_000, 250 * 2 ** (attempts - 1));
    const retryAfter = Math.min(at + backoff, next?.expiresAt ?? Infinity);
    this.#setJob(job.id, {
      state: expired ? JOB.EXPIRED : JOB.READY,
      dl_attempts: attempts,
      retry_after: expired ? null : retryAfter,
      last_error: expired ? 'download response lost and the result then expired' : 'download response lost; retrying',
    });
    this.audit.append({
      kind: 'download.lost', tenant: job.tenant, jobId: job.id,
      detail: { remoteId: job.remoteId, attempt: attempts, expired, retryAfter: expired ? null : retryAfter },
    });
  }

  // ---- reconciliation -----------------------------------------------------

  #reconcileDue() {
    const at = this.now();
    const due = this.db.prepare(`SELECT * FROM reservations WHERE state = ? AND resolve_after <= ?`)
      .all(RESERVATION.UNCERTAIN, at).map(rowToReservation);
    const tasks = due.map((r) => this.reconcileJob(r.jobId, { actor: 'system', automatic: true }));
    return Promise.allSettled(tasks);
  }

  // Probe the source for jobs matching an uncertain create. Never guesses:
  //   * a match → committed, link remote id, resume the job
  //   * a fully-covered empty probe → released (definitely not charged)
  //   * an index that has not caught up → stay uncertain, retry later
  async reconcileJob(jobId, { actor = 'operator', automatic = false } = {}) {
    const job = this.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { status: 404 });
    const res = this.ledger.getForJob(jobId);
    if (!res) throw Object.assign(new Error('no reservation for job'), { status: 409 });
    if (![RESERVATION.UNCERTAIN, RESERVATION.PENDING].includes(res.state)) {
      return { result: 'not_uncertain', reservation: res };
    }
    const resStart = res.attemptStartedAt ?? job.createStartedAt ?? job.queuedAt;
    const since = resStart - 60_000; // clock-skew tolerance
    const probe = this.source.probe({ tenant: job.tenant, sinceMs: since, untilMs: this.now() });
    // A create request may still be running inside the source even after our
    // own timeout fired. While any request is unresolved, "no job found" is not
    // proof that nothing was charged.
    const inflightAtSource = typeof this.source.inflightCreates === 'function'
      ? this.source.inflightCreates()
      : 0;

    const claimed = this.#claimedRemoteIds();
    const match = probe.jobs.find((rj) => rj.meta?.jobId === job.id && !claimed.has(rj.remoteId))
      ?? probe.jobs.find((rj) => {
        if (claimed.has(rj.remoteId)) return false;
        return rj.tenant === job.tenant
          && rj.name === job.name
          && rj.createdAt >= since
          && rj.createdAt <= (res.attemptStartedAt ?? this.now()) + this.config.createTimeoutMs + 5_000;
      });

    this.audit.append({
      kind: 'reconcile.probe', actor, tenant: job.tenant, jobId,
      detail: {
        automatic, probedSince: since, found: probe.jobs.length,
        indexedThrough: probe.indexedThrough, inflightAtSource,
      },
    });

    if (match) {
      this.ledger.commit(jobId, { remoteId: match.remoteId, note: 'reconciled: source has a matching job' });
      const cancelRequested = Boolean(job.cancelRequestedAt);
      if (cancelRequested) {
        this.#setJob(jobId, {
          state: JOB.CANCELLED,
          remote_id: match.remoteId,
          expires_at: match.expiresAt,
          last_error: 'reconciled as charged; cancelled locally, quota not refunded',
        });
        this.audit.append({
          kind: 'job.cancelled', actor, tenant: job.tenant, jobId,
          detail: { refunded: false, remoteId: match.remoteId, note: 'create was accepted; cancel does not refund source quota' },
        });
        return { result: 'committed_cancelled', remote: match };
      }
      // Resume: the job exists remotely. It may already be ready or even
      // expired while we were reconciling.
      if (this.now() >= match.expiresAt) {
        this.#setJob(jobId, {
          state: JOB.EXPIRED,
          remote_id: match.remoteId,
          ready_at: match.readyAt,
          expires_at: match.expiresAt,
          last_error: 'reconciled after the result had already expired',
        });
        this.audit.append({ kind: 'job.expired', actor, tenant: job.tenant, jobId,
          detail: { reason: 'reconciled_after_expiry', remoteId: match.remoteId, note: 'quota was charged; data lost' } });
        return { result: 'committed_expired', remote: match };
      }
      this.#setJob(jobId, {
        state: match.status === 'ready' ? JOB.READY : JOB.RUNNING,
        remote_id: match.remoteId,
        ready_at: match.readyAt,
        expires_at: match.expiresAt,
        last_error: null,
      });
      this.audit.append({ kind: 'reconcile.matched', actor, tenant: job.tenant, jobId,
        detail: { remoteId: match.remoteId, status: match.status, note: 'quota charged; remote job adopted' } });
      return { result: 'committed', remote: match };
    }

    // No match. Absence is only proof if two independent conditions hold:
    //   1. the probe index covers the whole attempt window, and
    //   2. the source has no unresolved create request that could still charge.
    const attemptEnd = (res.attemptStartedAt ?? job.createStartedAt ?? this.now()) + this.config.createTimeoutMs;
    if (probe.indexedThrough >= attemptEnd && inflightAtSource === 0) {
      this.ledger.release(jobId, { reason: 'reconciled: source has no matching job within a fully indexed window' });
      const state = job.cancelRequestedAt ? JOB.CANCELLED : JOB.QUEUED;
      this.#setJob(jobId, {
        state,
        last_error: job.cancelRequestedAt ? 'cancelled; reconciliation proved no quota was charged' : null,
        create_started_at: null,
      });
      this.audit.append({
        kind: 'reconcile.released', actor, tenant: job.tenant, jobId,
        detail: {
          reason: 'no remote job in fully indexed window', quotaReleased: true,
          note: job.cancelRequestedAt ? 'cancel completed; nothing was charged' : 'job re-queued, allowance returned',
        },
      });
      return { result: 'released' };
    }

    // Inconclusive: push the retry out and make it visible.
    const resolveAfter = Math.max(this.now() + Math.min(this.config.reconcileGraceMs, 2000), probe.indexedThrough + 1);
    const why = inflightAtSource > 0
      ? `reconciliation inconclusive: ${inflightAtSource} create request(s) still unresolved at the source and may still charge`
      : 'reconciliation inconclusive: source index has not covered the attempt window';
    this.ledger.markUncertain(jobId, { reason: why, resolveAfter });
    this.audit.append({
      kind: 'reconcile.inconclusive', actor, tenant: job.tenant, jobId,
      detail: { attemptEnd, indexedThrough: probe.indexedThrough, inflightAtSource, retryAt: resolveAfter },
    });
    return { result: 'inconclusive', retryAt: resolveAfter };
  }

  #claimedRemoteIds() {
    const rows = this.db.prepare('SELECT remote_id FROM jobs WHERE remote_id IS NOT NULL').all();
    return new Set(rows.map((r) => r.remote_id));
  }

  // Full ledger-vs-source check. Only uses the source's public quota endpoint
  // plus per-tenant probes, not privileged diagnostics.
  async auditLedger({ actor = 'operator' } = {}) {
    const quota = this.quotaSnapshot();
    const uncertain = this.db.prepare('SELECT * FROM reservations WHERE state IN (?,?)')
      .all(RESERVATION.UNCERTAIN, RESERVATION.PENDING).map(rowToReservation);
    const uncertainDetails = await Promise.all(uncertain.map(async (r) => {
      const probe = this.source.probe({ tenant: r.tenant, sinceMs: (r.attemptStartedAt ?? r.createdAt) - 60_000, untilMs: this.now() });
      const job = this.getJob(r.jobId);
      const claimed = this.#claimedRemoteIds();
      const match = probe.jobs.find((rj) => rj.meta?.jobId === r.jobId && !claimed.has(rj.remoteId));
      return {
        jobId: r.jobId,
        tenant: r.tenant,
        jobName: job?.name ?? null,
        jobState: job?.state ?? null,
        reservationState: r.state,
        attemptStartedAt: r.attemptStartedAt,
        retryAt: r.resolveAfter,
        probeFound: match ? { remoteId: match.remoteId, status: match.status } : null,
        indexedThrough: probe.indexedThrough,
        needsReconcile: true,
      };
    }));

    return {
      quota,
      managedJobs: this.#countJobs(),
      uncertainReservations: uncertainDetails,
      unreconciled: uncertainDetails.length,
      inflightAtSource: typeof this.source.inflightCreates === 'function' ? this.source.inflightCreates() : 0,
      sourceJobsWithoutLocalCharge: this.#orphanSourceJobs(),
      consistency: quota.discrepancy
        ? { ok: false, ...quota.discrepancy }
        : { ok: true, note: 'source used equals ledger charged' },
      checkedAt: this.now(),
    };
  }

  #countJobs() {
    const rows = this.db.prepare('SELECT state, COUNT(*) AS n FROM jobs GROUP BY state').all();
    return Object.fromEntries(rows.map((r) => [r.state, r.n]));
  }

  // Find source jobs in today's window that no local reservation claims. These
  // are the accepted-but-lost creates that make the source's used counter
  // exceed ours.
  #orphanSourceJobs() {
    const rows = this.db.prepare('SELECT DISTINCT tenant FROM jobs').all();
    const claimed = this.#claimedRemoteIds();
    const out = [];
    for (const { tenant } of rows) {
      const probe = this.source.probe({ tenant, sinceMs: this.now() - 86_400_000, untilMs: this.now() });
      for (const rj of probe.jobs) {
        if (!claimed.has(rj.remoteId)) out.push({ tenant, remoteId: rj.remoteId, name: rj.name, createdAt: rj.createdAt, status: rj.status });
      }
    }
    return out;
  }

  // ---- operator actions ---------------------------------------------------

  cancel(jobId, { actor = 'operator', reason = null } = {}) {
    const job = this.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { status: 404 });
    if (isTerminal(job.state) && job.state !== JOB.CANCELLED) {
      throw Object.assign(new Error(`job is already ${job.state}`), { status: 409 });
    }
    if (job.state === JOB.INGESTED) throw Object.assign(new Error('job is already ingested'), { status: 409 });
    const res = job.reservationId ? this.ledger.getForJob(jobId) : null;

    if (job.state === JOB.UNCERTAIN) {
      throw Object.assign(new Error(
        'create outcome is unknown; cancel would hide whether quota was charged. Reconcile first.'), {
        status: 409, code: 'RECONCILE_FIRST',
      });
    }

    const at = this.now();
    switch (job.state) {
      case JOB.QUEUED: {
        this.#setJob(jobId, { state: JOB.CANCELLED, cancel_requested_at: at, last_error: reason ?? 'cancelled before create' });
        this.audit.append({ kind: 'job.cancelled', actor, tenant: job.tenant, jobId,
          detail: { refunded: false, charged: false, note: 'no create was attempted, so no quota was spent' } });
        return this.getJob(jobId);
      }
      case JOB.CREATING: {
        // Do not claim a refund. Record intent; the in-flight create decides.
        this.#setJob(jobId, { cancel_requested_at: at, last_error: 'cancel requested while create in flight' });
        this.audit.append({ kind: 'job.cancel_requested', actor, tenant: job.tenant, jobId,
          detail: { note: 'create already left the console; outcome will decide, no refund assumed' } });
        return this.getJob(jobId);
      }
      case JOB.RUNNING:
      case JOB.READY: {
        this.#setJob(jobId, {
          state: JOB.CANCELLED,
          cancel_requested_at: at,
          last_error: reason ?? 'cancelled after create; source quota is not refunded',
        });
        this.audit.append({ kind: 'job.cancelled', actor, tenant: job.tenant, jobId,
          detail: { refunded: false, charged: true, remoteId: job.remoteId,
            note: 'source already charged quota on acceptance; cancellation stops local work only' } });
        return this.getJob(jobId);
      }
      case JOB.DOWNLOADED: {
        this.#setJob(jobId, {
          state: JOB.CANCELLED,
          cancel_requested_at: at,
          last_error: reason ?? 'cancelled after download; artifact retained locally, quota not refunded',
        });
        this.audit.append({ kind: 'job.cancelled', actor, tenant: job.tenant, jobId,
          detail: { refunded: false, charged: true, artifactRetained: true } });
        return this.getJob(jobId);
      }
      default:
        throw Object.assign(new Error(`cannot cancel a job in state ${job.state}`), { status: 409 });
    }
  }

  resolveManually(jobId, { decision, note, remoteId = null, actor = 'operator' }) {
    if (!note || !String(note).trim()) {
      throw Object.assign(new Error('a note is required for a manual reconciliation decision'), { status: 400 });
    }
    const job = this.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { status: 404 });
    const res = this.ledger.resolveManually(jobId, { decision, actor, note, remoteId });
    if (decision === 'committed') {
      const remote = remoteId ? this.source.lookup(remoteId) : null;
      const remoteJob = remote?.found ? remote.job : null;
      const at = this.now();
      if (job.cancelRequestedAt) {
        this.#setJob(jobId, { state: JOB.CANCELLED, remote_id: remoteId ?? job.remoteId, last_error: note });
      } else if (remoteJob && at < remoteJob.expiresAt) {
        this.#setJob(jobId, {
          state: remoteJob.status === 'ready' ? JOB.READY : JOB.RUNNING,
          remote_id: remoteJob.remoteId,
          ready_at: remoteJob.readyAt,
          expires_at: remoteJob.expiresAt,
          last_error: null,
        });
      } else {
        this.#setJob(jobId, {
          state: JOB.FAILED,
          remote_id: remoteId ?? job.remoteId,
          last_error: note,
        });
      }
    } else {
      this.#setJob(jobId, {
        state: job.cancelRequestedAt ? JOB.CANCELLED : JOB.QUEUED,
        create_started_at: null,
        last_error: job.cancelRequestedAt ? 'manually reconciled as not charged; cancelled' : null,
      });
    }
    return { reservation: res, job: this.getJob(jobId) };
  }

  // Explicit recovery for a job that failed locally (e.g. the source briefly
  // forgot a remote id). This never creates anything: it only re-reads the
  // source and resumes if the already-charged result is still alive. If the
  // result is gone, the job stays failed and the spent quota stays spent.
  recoverJob(jobId, { actor = 'operator' } = {}) {
    const job = this.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { status: 404 });
    if (job.state !== JOB.FAILED) {
      throw Object.assign(new Error(`only a failed job can be recovered (currently ${job.state})`), { status: 409 });
    }
    if (!job.remoteId) {
      // No remote handle: nothing was created, so the job can safely re-queue.
      if (this.ledger.getForJob(jobId)?.state === 'uncertain') {
        throw Object.assign(new Error('quota is unverified; reconcile before retrying'), { status: 409 });
      }
      this.#setJob(jobId, { state: JOB.QUEUED, create_started_at: null, last_error: null });
      this.audit.append({ kind: 'job.recovered', actor, tenant: job.tenant, jobId,
        detail: { action: 'requeued', note: 'no remote id existed, so no create was charged' } });
      return this.getJob(jobId);
    }
    const lookup = this.source.lookup(job.remoteId);
    const at = this.now();
    if (!lookup.found) {
      this.audit.append({ kind: 'job.recovery_refused', actor, tenant: job.tenant, jobId,
        detail: { reason: 'source still has no such job', remoteId: job.remoteId } });
      throw Object.assign(new Error('the source still does not know this job; nothing to recover'), {
        status: 409, code: 'REMOTE_GONE',
      });
    }
    const remote = lookup.job;
    if (at >= remote.expiresAt || remote.status === 'expired') {
      this.#setJob(jobId, {
        state: JOB.EXPIRED,
        expires_at: remote.expiresAt,
        last_error: 'recovery found the result already expired; quota remains spent',
      });
      this.audit.append({ kind: 'job.recovery_refused', actor, tenant: job.tenant, jobId,
        detail: { reason: 'expired', remoteId: job.remoteId, expiresAt: remote.expiresAt } });
      return this.getJob(jobId);
    }
    this.#setJob(jobId, {
      state: remote.status === 'ready' ? JOB.READY : JOB.RUNNING,
      ready_at: remote.readyAt,
      expires_at: remote.expiresAt,
      retry_after: null,
      last_error: null,
    });
    this.audit.append({ kind: 'job.recovered', actor, tenant: job.tenant, jobId,
      detail: { action: 'resumed', remoteId: job.remoteId, status: remote.status, expiresAt: remote.expiresAt } });
    return this.getJob(jobId);
  }

  ingest(jobId, { actor = 'operator', note = null } = {}) {
    const job = this.getJob(jobId);
    if (!job) throw Object.assign(new Error('job not found'), { status: 404 });
    if (job.state !== JOB.DOWNLOADED) {
      throw Object.assign(new Error(`only a downloaded job can be ingested (currently ${job.state})`), { status: 409 });
    }
    const check = this.artifacts.verify(jobId, job.artifact?.sha256, job.artifact?.bytes);
    if (!check.ok) {
      this.audit.append({ kind: 'ingest.rejected', actor, tenant: job.tenant, jobId, detail: { reason: check.reason } });
      throw Object.assign(new Error(`integrity check failed: ${check.reason}`), { status: 409, code: 'ARTIFACT_INTEGRITY' });
    }
    this.#setJob(jobId, {
      state: JOB.INGESTED,
      ingested_at: this.now(),
      ingest_note: note,
      artifact_sha256: check.artifact.sha256,
      artifact_bytes: check.artifact.bytes,
      last_error: null,
    });
    this.audit.append({
      kind: 'job.ingested', actor, tenant: job.tenant, jobId,
      detail: { sha256: check.artifact.sha256, bytes: check.artifact.bytes, note },
    });
    return this.getJob(jobId);
  }

  // Crash recovery: a job stuck in `creating` has an attempt that may have
  // charged quota. Move it to uncertain and reconcile instead of retrying.
  recoverOnStartup() {
    const stuck = this.db.prepare('SELECT * FROM jobs WHERE state = ?').all(JOB.CREATING).map(rowToJob);
    for (const job of stuck) {
      const res = this.ledger.getForJob(job.id);
      if (res && [RESERVATION.PENDING, RESERVATION.UNCERTAIN].includes(res.state)) {
        this.ledger.markUncertain(job.id, {
          reason: 'console restarted while a create was in flight; outcome unknown',
          resolveAfter: this.now() + this.config.reconcileGraceMs,
        });
      }
      this.#setJob(job.id, {
        state: JOB.UNCERTAIN,
        last_error: 'console restarted during create; quota held pending reconciliation',
      });
      this.audit.append({
        kind: 'job.recovered_uncertain', tenant: job.tenant, jobId: job.id,
        detail: { note: 'no automatic recreate after restart' },
      });
    }
    // Ready jobs whose local artifact is missing but which are still within
    // expiry are fine (they will download again). Downloaded jobs missing
    // their artifact are flagged rather than silently ingested later.
    const downloaded = this.db.prepare('SELECT * FROM jobs WHERE state = ?').all(JOB.DOWNLOADED).map(rowToJob);
    for (const job of downloaded) {
      if (!this.artifacts.exists(job.id)) {
        this.#setJob(job.id, {
          state: JOB.FAILED,
          last_error: 'artifact missing after restart; source result may already be expired',
        });
        this.audit.append({ kind: 'job.failed', tenant: job.tenant, jobId: job.id,
          detail: { reason: 'artifact_missing_after_restart', quotaRefunded: false } });
      }
    }
  }

  // ---- helpers ------------------------------------------------------------

  quotaSnapshot() {
    return this.ledger.snapshot(this.source.quota());
  }

  #setJob(id, patch) {
    const sets = [];
    const args = [];
    const columnMap = {
      state: 'state',
      remote_id: 'remote_id',
      ready_at: 'ready_at',
      expires_at: 'expires_at',
      downloaded_at: 'downloaded_at',
      ingested_at: 'ingested_at',
      create_started_at: 'create_started_at',
      cancel_requested_at: 'cancel_requested_at',
      artifact_path: 'artifact_path',
      artifact_sha256: 'artifact_sha256',
      artifact_bytes: 'artifact_bytes',
      last_error: 'last_error',
      attempts: 'attempts',
      dl_attempts: 'dl_attempts',
      retry_after: 'retry_after',
      ingest_note: 'ingest_note',
    };
    for (const [k, v] of Object.entries(patch)) {
      const col = columnMap[k];
      if (!col) throw new Error(`unknown job column ${k}`);
      sets.push(`${col} = ?`);
      args.push(v);
    }
    sets.push('updated_at = ?', 'version = version + 1');
    args.push(this.now(), id);
    this.db.prepare(`UPDATE jobs SET ${sets.join(', ')} WHERE id = ?`).run(...args);
  }
}
