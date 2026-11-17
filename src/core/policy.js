// Scheduling policy for who gets the next scarce create allowance.
//
// The console generally knows, per queued job, whether the *job's own* result
// would expire soon — but a queued job has no result yet, so expiry cannot
// apply before creation. Two different scarce resources are competing:
//
//   1. Create allowance (quota): needed to turn queued → created.
//   2. Download throughput and result lifetime: needed to turn ready →
//      downloaded before expiry.
//
// Therefore the decision has two layers:
//   * Admit (create): FIFO with per-tenant fairness. Starvation relief: a job
//     older than starvationMs temporarily outranks newer jobs, capped so it
//     cannot monopolise. Expiry is irrelevant here because nothing exists to
//     expire — so "save the nearly-expiring result" is not a create decision.
//   * Download/claim: strictly expiring-first. When a ready result is close to
//     expiry, spending download capacity on it returns more value than
//     preserving strict queue order. This is where the expiring-vs-FIFO trade
//     is real, and the tiebreak is deadline, then queue order.
//
// The policy reports the reason for every choice so the UI can explain it and
// an operator can audit why a later job jumped ahead.

export const POLICIES = {
  'expiring-first': {
    key: 'expiring-first',
    label: 'Expiring results first (recommended)',
    summary:
      'Downloads always go to the ready result with the nearest expiry. Creates follow FIFO per tenant with ' +
      'bounded starvation relief. Rationale: a created result is a sunk quota cost already charged; letting it ' +
      'expire loses both the quota and the data, while a queued job that waits costs nothing but time.',
    createOrder: 'fifo-fair',
    downloadOrder: 'expiry',
  },
  fifo: {
    key: 'fifo',
    label: 'Strict queue order',
    summary:
      'Everything follows global queue order. Predictable and explainable, but a nearly-expired result can be ' +
      'lost behind an older queued job if download capacity is scarce. Use when fairness matters more than salvage.',
    createOrder: 'fifo',
    downloadOrder: 'fifo',
  },
};

// Rank queued jobs for the next create. Returns ordered list with reasons.
//
// jobs: [{ id, tenant, queueSeq, queuedAt }]
// opts: { policy, starvationMs, now, tenantsWaiting }
export function rankCreateQueue(jobs, opts = {}) {
  const policy = POLICIES[opts.policy] ?? POLICIES['expiring-first'];
  const now = opts.now ?? Date.now();
  const starvationMs = opts.starvationMs ?? 30_000;

  const decorated = jobs.map((j) => {
    const ageMs = Math.max(0, now - j.queuedAt);
    const starving = ageMs >= starvationMs;
    return { ...j, ageMs, starving };
  });

  if (policy.createOrder === 'fifo') {
    return decorated
      .sort((a, b) => a.queueSeq - b.queueSeq)
      .map((j) => ({ ...j, reason: 'strict FIFO: lowest queue sequence first' }));
  }

  // Per-tenant fairness: within a tenant FIFO. Across tenants, a tenant with
  // already-created or in-flight jobs yields to tenants with none — prevents a
  // bulk submitter from occupying the whole daily allowance.
  const tenantActive = opts.tenantActive ?? new Map(); // tenant -> count of created/running/ready today
  const sorted = decorated.sort((a, b) => {
    // Starvation relief first, but only among jobs that are actually old.
    if (a.starving !== b.starving) return a.starving ? -1 : 1;
    const aActive = tenantActive.get(a.tenant) ?? 0;
    const bActive = tenantActive.get(b.tenant) ?? 0;
    if (aActive !== bActive) return aActive - bActive; // less-progressed tenant first
    return a.queueSeq - b.queueSeq;
  });

  return sorted.map((j) => {
    const active = tenantActive.get(j.tenant) ?? 0;
    let reason;
    if (j.starving) {
      reason = `starvation relief: queued ${Math.round(j.ageMs / 1000)}s (≥ ${Math.round(starvationMs / 1000)}s) ` +
        'so it outranks newer jobs; queued work has no expiry risk, this only bounds wait time';
    } else if (active === 0) {
      reason = `tenant ${j.tenant} has no in-flight work yet; FIFO within tenant`;
    } else {
      reason = `FIFO within tenant ${j.tenant} (${active} in-flight); other tenants with less in-flight work go first`;
    }
    return { ...j, reason };
  });
}

// Rank ready jobs for download. This is where expiry genuinely matters.
//
// ready: [{ id, tenant, queueSeq, expiresAt, readyAt }]
export function rankDownloadQueue(ready, opts = {}) {
  const policy = POLICIES[opts.policy] ?? POLICIES['expiring-first'];
  if (policy.downloadOrder === 'fifo') {
    return [...ready].sort((a, b) => a.queueSeq - b.queueSeq)
      .map((j) => ({ ...j, reason: 'strict FIFO: oldest job downloads first even if a newer result expires sooner' }));
  }
  return [...ready].sort((a, b) => (a.expiresAt - b.expiresAt) || (a.queueSeq - b.queueSeq))
    .map((j, i) => {
      const minutes = Math.max(0, Math.round((j.expiresAt - (opts.now ?? Date.now())) / 1000));
      return {
        ...j,
        reason: i === 0
          ? `nearest expiry (${minutes}s left): created quota is already spent, downloading salvages it; ` +
            'queued jobs lose nothing by waiting'
          : `expiry order position ${i + 1}`,
      };
    });
}

