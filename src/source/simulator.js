import fs from 'node:fs';
import path from 'node:path';

// Local stand-in for the upstream export service. It owns three pieces of
// truth the console must never assume:
//   * daily create allowance (resets on the source's UTC day)
//   * whether a create was actually accepted (even when the caller saw a
//     timeout / connection reset)
//   * when a produced file expires and becomes undownloadable
//
// Faults are toggles the console can read through the diagnostics endpoint and
// an operator can change from the web UI. They are deliberately *not* exposed
// to the console's own scheduling code so the console cannot "know" it should
// retry more or less.
export class SourceService {
  constructor({ statePath, dailyLimit = 12, runMs = 4000, expireMs = 60_000,
    downloadMs = 900, now = () => Date.now(), persist = true } = {}) {
    this.statePath = statePath;
    this.persist = persist && Boolean(statePath);
    this.now = now;
    this.dailyLimit = dailyLimit;
    this.runMs = runMs;
    this.expireMs = expireMs;
    this.downloadMs = downloadMs;
    this.faults = {
      // Probability that a create is accepted by the source but the response
      // never reaches the console (client sees a timeout).
      lostCreateResponseRate: 0,
      // Probability that a create request is *rejected before charging* but the
      // console sees a network error.
      droppedBeforeChargeRate: 0,
      // Probability that a download errors after the file was partially sent.
      lostDownloadRate: 0,
      // Additional per-UTC-day limit delta, to simulate a moveable limit.
      limitAdjustment: 0,
      // Delay before the create response is delivered. The charge is applied
      // up-front, so a client timeout after this delay is genuinely ambiguous.
      createDelayMs: 0,
      // Probe index lag: recently created jobs are invisible to probe() until
      // this much time has passed. Makes reconciliation genuinely inconclusive
      // instead of instantly authoritative.
      probeLagMs: 0,
    };
    this.state = this.#load() ?? {
      day: null,
      usedToday: 0,
      seq: 0,
      jobs: {},
      events: [],
      inflight: {},
    };
    this.state.inflight ??= {};
    this.#rollDay();
  }

  #load() {
    if (!this.persist) return null;
    try {
      return JSON.parse(fs.readFileSync(this.statePath, 'utf8'));
    } catch {
      return null;
    }
  }

  #save() {
    if (!this.persist) return;
    fs.mkdirSync(path.dirname(this.statePath), { recursive: true });
    const tmp = `${this.statePath}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(this.state, null, 2));
    fs.renameSync(tmp, this.statePath);
  }

  #log(type, detail = {}) {
    this.state.events.push({ at: this.now(), type, ...detail });
    if (this.state.events.length > 500) this.state.events.splice(0, this.state.events.length - 500);
  }

  #rollDay() {
    const day = new Date(this.now()).toISOString().slice(0, 10);
    if (this.state.day === day) return;
    this.state.day = day;
    this.state.usedToday = 0;
    this.#log('day.roll', { day });
    this.#save();
  }

  effectiveLimit() {
    this.#rollDay();
    return Math.max(0, this.dailyLimit + this.faults.limitAdjustment);
  }

  quota() {
    this.#rollDay();
    const limit = this.effectiveLimit();
    // Create requests that have arrived but whose acceptance decision has not
    // been made yet. Their quota effect is genuinely unknown — they may charge
    // after the caller gives up. Callers must treat this as a lower bound on
    // possible charges, not as noise.
    const inflight = Object.keys(this.state.inflight ?? {}).length;
    return {
      day: this.state.day,
      limit,
      used: this.state.usedToday,
      inflight,
      remaining: Math.max(0, limit - this.state.usedToday),
      resetsAt: Date.parse(`${this.state.day}T24:00:00.000Z`),
    };
  }

  // How many create requests are still unresolved at the source. Reconciliation
  // cannot declare "not charged" while any of them could still charge.
  inflightCreates() {
    this.#rollDay();
    return Object.keys(this.state.inflight ?? {}).length;
  }

  setFaults(patch) {
    const rates = ['lostCreateResponseRate', 'droppedBeforeChargeRate', 'lostDownloadRate'];
    const numbers = ['limitAdjustment', 'createDelayMs', 'probeLagMs'];
    for (const key of [...rates, ...numbers]) {
      if (patch[key] === undefined) continue;
      const n = Number(patch[key]);
      if (!Number.isFinite(n)) throw new Error(`invalid fault value for ${key}`);
      this.faults[key] = rates.includes(key) ? Math.min(1, Math.max(0, n)) : Math.max(0, Math.trunc(n));
    }
    this.#log('faults.set', { faults: { ...this.faults } });
    this.#save();
    return { ...this.faults };
  }

  // Create an export job. Returns one of:
  //   { outcome: 'accepted', remoteId, expiresAt }
  //   { outcome: 'rejected', code }             – definitive, not charged
  //   { outcome: 'lost', maybeRemoteId }        – caller cannot tell; source
  //                                               may or may not have charged
  // A lost response is produced *after* the acceptance decision (response lost)
  // or *before* charging (request dropped), so a console timeout has genuinely
  // ambiguous quota consequences and cannot be treated as a free retry.
  async create({ tenant, name, meta = {} }) {
    this.#rollDay();
    const requestId = meta.jobId ?? `req_${this.state.seq}_${Date.now()}`;
    this.state.inflight[requestId] = { tenant, since: this.now() };
    this.#save();
    const settle = () => {
      delete this.state.inflight[requestId];
      this.#save();
    };
    if (this.faults.createDelayMs > 0) {
      // Connection-level delay applies whether or not the request is charged.
      await new Promise((r) => setTimeout(r, this.faults.createDelayMs));
    }
    if (Math.random() < this.faults.droppedBeforeChargeRate) {
      // Request never reached the charging layer: nothing was charged.
      this.#log('create.dropped_before_charge', { tenant, requestId });
      settle();
      return { outcome: 'lost', maybeRemoteId: null };
    }
    if (this.state.usedToday >= this.effectiveLimit()) {
      this.#log('create.rejected', { tenant, reason: 'QUOTA_EXCEEDED' });
      settle();
      return { outcome: 'rejected', code: 'QUOTA_EXCEEDED' };
    }
    this.state.seq += 1;
    const remoteId = `exp_${this.state.day.replaceAll('-', '')}_${String(this.state.seq).padStart(5, '0')}`;
    const createdAt = this.now();
    const job = {
      remoteId,
      tenant,
      name,
      meta,
      createdAt,
      readyAt: createdAt + this.runMs,
      expiresAt: createdAt + this.runMs + this.expireMs,
      status: 'running',
      bytes: null,
    };
    this.state.jobs[remoteId] = job;
    this.state.usedToday += 1;
    this.#log('create.accepted', { tenant, remoteId });
    settle();

    if (Math.random() < this.faults.lostCreateResponseRate) {
      return { outcome: 'lost', maybeRemoteId: null };
    }
    return { outcome: 'accepted', remoteId, expiresAt: job.expiresAt };
  }

  #refresh(job) {
    if (job.status === 'running' && this.now() >= job.readyAt) {
      job.status = 'ready';
      // Deterministic filename/content derived from the remote id.
      job.bytes = Buffer.from(`remoteId,tenant,name,createdAt\n${job.remoteId},${job.tenant},${JSON.stringify(job.name)},${new Date(job.createdAt).toISOString()}\n`);
    }
    return job;
  }

  lookup(remoteId) {
    this.#rollDay();
    const job = this.state.jobs[remoteId];
    if (!job) return { found: false };
    this.#refresh(job);
    this.#save();
    return { found: true, job: { ...job } };
  }

  // Probe used by reconciliation: list remote jobs created for a tenant in a
  // time window. This costs no quota.
  probe({ tenant, sinceMs, untilMs }) {
    this.#rollDay();
    const indexedThrough = this.now() - (this.faults.probeLagMs ?? 0);
    const matches = Object.values(this.state.jobs)
      .filter((j) => j.tenant === tenant && j.createdAt >= sinceMs && j.createdAt <= Math.min(untilMs, indexedThrough))
      .map((j) => ({ ...this.#refresh(j) }));
    this.#save();
    // completeThrough tells the caller how far the index is trustworthy; an
    // empty result only proves "no job" for creates that started before it.
    return { jobs: matches, indexedThrough, probedAt: this.now() };
  }

  // Downloads take real time (downloadMs) so that a single download slot is
  // genuinely contended when several results are ready. The expiry decision is
  // made when the transfer *starts*: a request made before the deadline is
  // served even if it finishes after it. That mirrors a source that streams an
  // already-opened file handle.
  async download(remoteId, { now = this.now() } = {}) {
    this.#rollDay();
    const job = this.state.jobs[remoteId];
    if (!job) return { outcome: 'not_found' };
    this.#refresh(job);
    if (now >= job.expiresAt || job.status === 'expired') {
      job.status = 'expired';
      this.#save();
      return { outcome: 'expired' };
    }
    if (job.status !== 'ready') return { outcome: 'not_ready' };
    if (this.downloadMs > 0) {
      await new Promise((r) => setTimeout(r, this.downloadMs));
    }
    if (Math.random() < this.faults.lostDownloadRate) {
      this.#log('download.lost', { remoteId });
      this.#save();
      return { outcome: 'lost' };
    }
    this.#log('download.served', { remoteId, bytes: job.bytes.length });
    this.#save();
    return { outcome: 'ok', bytes: job.bytes, expiresAt: job.expiresAt };
  }

  diagnostics() {
    this.#rollDay();
    return {
      quota: this.quota(),
      faults: { ...this.faults },
      jobs: Object.values(this.state.jobs).map((j) => this.#refresh(j)).map((j) => ({
        remoteId: j.remoteId, tenant: j.tenant, name: j.name,
        createdAt: j.createdAt, readyAt: j.readyAt, expiresAt: j.expiresAt, status: j.status,
      })),
      events: this.state.events.slice(-100),
    };
  }
}
