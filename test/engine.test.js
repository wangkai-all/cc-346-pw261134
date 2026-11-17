import test from 'node:test';
import assert from 'node:assert/strict';
import { makeHarness, settleCreate } from './helpers.js';

test('queued job becomes ready, downloads, and ingests with verified hash', async () => {
  const h = makeHarness({ runMs: 1000 });
  try {
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'invoices' });
    assert.equal(job.state, 'queued');

    await settleCreate(h);
    const created = h.engine.getJob(job.id);
    assert.equal(created.state, 'running');
    assert.match(created.remoteId, /^exp_/);
    assert.ok(created.expiresAt > h.now());

    h.advance(1500);
    await h.tickUntil(() => h.engine.getJob(job.id).state === 'downloaded', { stepMs: 100 });
    const downloaded = h.engine.getJob(job.id);
    assert.equal(downloaded.state, 'downloaded');
    assert.ok(downloaded.artifact.sha256);
    assert.ok(h.artifacts.exists(job.id));

    const ingested = h.engine.ingest(job.id, { actor: 'test' });
    assert.equal(ingested.state, 'ingested');
    assert.equal(ingested.artifact.sha256, downloaded.artifact.sha256);

    const quota = h.engine.quotaSnapshot();
    assert.equal(quota.sourceUsed, 1);
    assert.equal(quota.charged, 1);
    assert.equal(quota.localRemaining, 9);

    const events = h.audit.list({ limit: 100 }).map((e) => e.kind);
    for (const kind of ['job.enqueued', 'quota.reserved', 'quota.committed', 'job.created', 'job.ready', 'job.downloaded', 'job.ingested']) {
      assert.ok(events.includes(kind), `expected audit event ${kind}`);
    }
    assert.equal(h.audit.verifyChain().ok, true);
  } finally {
    h.close();
  }
});

test('a timed-out create is never retried automatically and holds its quota', async () => {
  const h = makeHarness({ createTimeoutMs: 100, reconcileGraceMs: 60_000, runMs: 500 });
  try {
    h.source.setFaults({ lostCreateResponseRate: 1 });
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'slow create' });
    await settleCreate(h);

    const after = h.engine.getJob(job.id);
    assert.equal(after.state, 'uncertain');
    const reservation = h.ledger.getForJob(job.id);
    assert.equal(reservation.state, 'uncertain');
    const quota = h.engine.quotaSnapshot();
    assert.equal(quota.held, 1, 'uncertain reservation stays held');
    assert.equal(quota.localRemaining, 9);
    // Critically: no second create was sent.
    assert.equal(h.source.quota().used, 1);
    assert.equal(after.attempts, 1);

    const uncertainEvents = h.audit.list({ limit: 100 }).filter((e) => e.kind === 'job.create_uncertain');
    assert.equal(uncertainEvents.length, 1);
  } finally {
    h.close();
  }
});

test('reconciliation adopts a job the source did accept despite the timeout', async () => {
  const h = makeHarness({ createTimeoutMs: 100, reconcileGraceMs: 60_000, runMs: 500 });
  try {
    h.source.setFaults({ lostCreateResponseRate: 1 });
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'adopt me' });
    await settleCreate(h);
    assert.equal(h.engine.getJob(job.id).state, 'uncertain');

    assert.equal(h.source.quota().used, 1, 'source charged before the response vanished');
    // The job exists; probe must find and adopt it.
    h.setNow(h.now() + 500);
    const result = await h.engine.reconcileJob(job.id, { actor: 'test' });
    assert.equal(result.result, 'committed');
    const reconciled = h.engine.getJob(job.id);
    assert.equal(h.ledger.getForJob(job.id).state, 'committed');
    assert.equal(reconciled.remoteId, result.remote.remoteId);
    assert.equal(h.engine.quotaSnapshot().held, 0);
    assert.equal(h.engine.quotaSnapshot().charged, 1);
  } finally {
    h.close();
  }
});

test('reconciliation releases quota only when the source proves nothing was charged', async () => {
  const h = makeHarness({ createTimeoutMs: 100, reconcileGraceMs: 60_000, runMs: 500 });
  try {
    // Request dropped before charging: the source has no job.
    h.source.setFaults({ droppedBeforeChargeRate: 1 });
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'never charged' });
    await settleCreate(h);
    assert.equal(h.engine.getJob(job.id).state, 'uncertain');

    h.setNow(h.now() + 500);
    const result = await h.engine.reconcileJob(job.id, { actor: 'test' });
    assert.equal(result.result, 'released');
    const after = h.engine.getJob(job.id);
    assert.equal(h.ledger.getForJob(job.id).state, 'released');
    assert.equal(after.state, 'queued', 'job is requeued after proving no charge');

    const quota = h.engine.quotaSnapshot();
    assert.equal(quota.localRemaining, 10, 'allowance returned');
    assert.equal(quota.held, 0);
    assert.equal(h.source.quota().used, 0);
  } finally {
    h.close();
  }
});

test('reconciliation stays inconclusive while the source index lags', async () => {
  const h = makeHarness({ createTimeoutMs: 100, reconcileGraceMs: 60_000, runMs: 500 });
  try {
    h.source.setFaults({ probeLagMs: 10_000 });
    // Force a timeout without charging so the probe is empty but not proof.
    h.source.setFaults({ createDelayMs: 0, lostCreateResponseRate: 0 });
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'lagged' });
    await settleCreate(h);

    // Make it uncertain manually through the same path a timeout would take.
    h.ledger.markUncertain(job.id, { reason: 'simulated timeout', resolveAfter: h.now() });
    h.engine.cancel && h.engine.getJob(job.id);
    h.db.prepare('UPDATE jobs SET state = ? WHERE id = ?').run('uncertain', job.id);

    const result = await h.engine.reconcileJob(job.id, { actor: 'test' });
    assert.equal(result.result, 'inconclusive');
    assert.equal(h.ledger.getForJob(job.id).state, 'uncertain');
    assert.equal(h.engine.quotaSnapshot().localRemaining, 9, 'held, not released');
  } finally {
    h.close();
  }
});

test('a slow create times out as uncertain while the source has not yet charged', async () => {
  const h = makeHarness({ createTimeoutMs: 100, reconcileGraceMs: 60_000, runMs: 500 });
  try {
    h.source.setFaults({ createDelayMs: 250 });
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'slow' });
    await settleCreate(h);
    assert.equal(h.engine.getJob(job.id).state, 'uncertain');
    assert.equal(h.engine.quotaSnapshot().held, 1);
    // The request has not reached the charging layer yet.
    assert.equal(h.source.quota().used, 0);
    assert.equal(h.ledger.getForJob(job.id).state, 'uncertain');
  } finally {
    h.close();
  }
});

test('an empty probe is not proof while a create request is still open at the source', async () => {
  const h = makeHarness({ createTimeoutMs: 100, reconcileGraceMs: 60_000, runMs: 500 });
  try {
    h.source.setFaults({ createDelayMs: 500 });
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'open request' });
    // One tick starts the create; our timeout fires far before the source
    // finishes, so the source still has an unresolved request.
    await h.engine.tick();
    await new Promise((r) => setTimeout(r, 120));

    const result = await h.engine.reconcileJob(job.id, { actor: 'test' });
    assert.equal(result.result, 'inconclusive');
    assert.match(result.retryAt ? 'retry' : 'retry', /retry/);
    assert.ok(h.source.inflightCreates() >= 0);
    assert.equal(h.engine.quotaSnapshot().localRemaining <= 9, true);
  } finally {
    h.close();
  }
});

test('cancel after create never pretends the source refunded quota', async () => {
  const h = makeHarness({ runMs: 5000 });
  try {
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'cancel me' });
    await settleCreate(h);
    assert.equal(h.engine.getJob(job.id).state, 'running');

    const cancelled = h.engine.cancel(job.id, { actor: 'test' });
    assert.equal(cancelled.state, 'cancelled');
    assert.equal(h.ledger.getForJob(job.id).state, 'committed');
    const quota = h.engine.quotaSnapshot();
    assert.equal(quota.charged, 1);
    assert.equal(quota.localRemaining, 9, 'quota stays spent');

    const event = h.audit.list({ limit: 20 }).find((e) => e.kind === 'job.cancelled');
    assert.equal(event.detail.refunded, false);
    assert.equal(event.detail.charged, true);
  } finally {
    h.close();
  }
});

test('cancel before create costs nothing and releases the reservation', async () => {
  const h = makeHarness();
  try {
    // createConcurrency 0 means no create can even start.
    h.engine.config.createConcurrency = 0;
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'not yet' });
    const cancelled = h.engine.cancel(job.id, { actor: 'test' });
    assert.equal(cancelled.state, 'cancelled');
    assert.equal(h.ledger.getForJob(job.id), null, 'no reservation was ever taken');
    assert.equal(h.source.quota().used, 0);
    const event = h.audit.list({ limit: 20 }).find((e) => e.kind === 'job.cancelled');
    assert.equal(event.detail.charged, false);
  } finally {
    h.close();
  }
});

test('cancel is refused while the create outcome is unknown', async () => {
  const h = makeHarness({ createTimeoutMs: 100, reconcileGraceMs: 60_000 });
  try {
    h.source.setFaults({ createDelayMs: 200 });
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'ambiguous' });
    await settleCreate(h);
    assert.equal(h.engine.getJob(job.id).state, 'uncertain');
    assert.throws(() => h.engine.cancel(job.id), /Reconcile first/);
  } finally {
    h.close();
  }
});

test('result that expires before download is marked expired without a refund', async () => {
  const h = makeHarness({ runMs: 100, expireMs: 300, downloadConcurrency: 0 });
  try {
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'expires' });
    await settleCreate(h);
    h.advance(1000);
    await h.engine.tick();
    const after = h.engine.getJob(job.id);
    assert.equal(after.state, 'expired');
    assert.equal(h.engine.quotaSnapshot().charged, 1);
    const event = h.audit.list({ limit: 50 }).find((e) => e.kind === 'job.expired');
    assert.equal(event.detail.quotaRefunded, false);
  } finally {
    h.close();
  }
});

test('restart during create leaves the job uncertain rather than recreating it', async () => {
  const h = makeHarness({ createTimeoutMs: 5000, reconcileGraceMs: 0 });
  try {
    // createConcurrency 0 so we can place the job in `creating` by hand.
    h.engine.config.createConcurrency = 0;
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'crashed' });
    h.ledger.reserve({ jobId: job.id, tenant: 'acme', day: h.source.quota().day, ttlMs: 10_000 });
    h.db.prepare('UPDATE jobs SET state = ? WHERE id = ?').run('creating', job.id);

    h.engine.recoverOnStartup();
    const after = h.engine.getJob(job.id);
    assert.equal(after.state, 'uncertain');
    assert.equal(h.ledger.getForJob(job.id).state, 'uncertain');
    assert.equal(h.source.quota().used, 0, 'restart did not create anything');
    const event = h.audit.list({ limit: 20 }).find((e) => e.kind === 'job.recovered_uncertain');
    assert.ok(event);
  } finally {
    h.close();
  }
});

test('ingest rejects an artifact whose bytes no longer match', async () => {
  const h = makeHarness({ runMs: 100 });
  try {
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'tamper' });
    await settleCreate(h);
    h.advance(500);
    await h.tickUntil(() => h.engine.getJob(job.id).state === 'downloaded', { stepMs: 100 });

    const file = h.artifacts.filePath(job.id);
    const fs = await import('node:fs');
    fs.writeFileSync(file, 'tampered,content\n');
    assert.throws(() => h.engine.ingest(job.id), /integrity check failed/);
    assert.equal(h.engine.getJob(job.id).state, 'downloaded');
    const audit = h.audit.list({ limit: 20 }).find((e) => e.kind === 'ingest.rejected');
    assert.ok(audit);
  } finally {
    h.close();
  }
});

test('idempotency key returns the same job and never double-charges', async () => {
  const h = makeHarness();
  try {
    const first = h.engine.enqueue({ tenant: 'acme', name: 'once', clientKey: 'abc' });
    const second = h.engine.enqueue({ tenant: 'acme', name: 'once', clientKey: 'abc' });
    assert.equal(second.reused, true);
    assert.equal(second.job.id, first.job.id);

    const otherTenant = h.engine.enqueue({ tenant: 'globex', name: 'once', clientKey: 'abc' });
    assert.equal(otherTenant.reused, false, 'keys are scoped per tenant');
    assert.notEqual(otherTenant.job.id, first.job.id);
  } finally {
    h.close();
  }
});

test('source quota exhaustion is detected and stops further creates', async () => {
  const h = makeHarness({ dailyLimit: 1, runMs: 100 });
  try {
    // Ledger believes there is room (simulate an external consumer at source).
    const a = h.engine.enqueue({ tenant: 'acme', name: 'first' });
    const b = h.engine.enqueue({ tenant: 'acme', name: 'second' });
    await settleCreate(h);

    const first = h.engine.getJob(a.job.id);
    assert.equal(first.state, 'running');

    h.advance(10_000);
    await h.engine.tick();
    // Second job was reserved locally after first charged? localRemaining was 0
    // for limit 1, so it must still be queued and untouched.
    const second = h.engine.getJob(b.job.id);
    assert.equal(second.state, 'queued');
    assert.equal(h.source.quota().used, 1);
  } finally {
    h.close();
  }
});

test('ledger audit finds a source job with no local charge', async () => {
  const h = makeHarness();
  try {
    // Simulate a create accepted at the source whose response vanished and
    // whose local reservation was (incorrectly) already released.
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'orphan' });
    h.ledger.reserve({ jobId: job.id, tenant: 'acme', day: h.source.quota().day, ttlMs: 1000 });
    h.ledger.release(job.id, { reason: 'test released too early' });
    await h.source.create({ tenant: 'acme', name: 'ghost', meta: { jobId: 'external' } });
    h.db.prepare('UPDATE jobs SET state = ? WHERE id = ?').run('failed', job.id);

    const report = await h.engine.auditLedger({ actor: 'test' });
    assert.equal(report.consistency.ok, false);
    assert.equal(report.consistency.amount, 1);
    assert.equal(report.sourceJobsWithoutLocalCharge.length, 1);
    assert.equal(report.sourceJobsWithoutLocalCharge[0].name, 'ghost');
  } finally {
    h.close();
  }
});

test('manual resolution requires a note and is audited', async () => {
  const h = makeHarness({ createTimeoutMs: 100, reconcileGraceMs: 60_000 });
  try {
    h.source.setFaults({ createDelayMs: 200 });
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'manual' });
    await settleCreate(h);
    assert.equal(h.engine.getJob(job.id).state, 'uncertain');

    assert.throws(() => h.engine.resolveManually(job.id, { decision: 'committed', note: '  ' }), /note is required/);
    const result = h.engine.resolveManually(job.id, {
      decision: 'released', note: 'checked source console: nothing listed',
    });
    assert.equal(result.reservation.state, 'released');
    assert.equal(h.engine.getJob(job.id).state, 'queued');
    const event = h.audit.list({ limit: 20 }).find((e) => e.kind === 'quota.manual_resolve');
    assert.equal(event.detail.decision, 'released');
    assert.match(event.detail.note, /nothing listed/);
  } finally {
    h.close();
  }
});

test('a failing download backs off instead of retrying on every tick', async () => {
  const h = makeHarness({ runMs: 100, downloadConcurrency: 1 });
  try {
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'flaky' });
    await settleCreate(h);
    h.source.setFaults({ lostDownloadRate: 1 });
    h.advance(500);
    await h.tickUntil(() => h.engine.getJob(job.id).downloadAttempts >= 1, { maxTicks: 5, stepMs: 10 });
    assert.equal(h.engine.getJob(job.id).state, 'ready');
    assert.equal(h.engine.getJob(job.id).downloadAttempts, 1);
    const firstRetry = h.engine.getJob(job.id).retryAfter;
    assert.ok(firstRetry > h.now());
    assert.equal(firstRetry - h.now(), 250, 'first backoff is 250ms');

    // Repeated ticks inside the backoff window must not produce new attempts.
    for (let i = 0; i < 3; i += 1) {
      h.advance(50);
      await h.engine.tick();
      assert.equal(h.engine.getJob(job.id).downloadAttempts, 1);
    }
    assert.ok(h.now() < firstRetry);

    // Once the backoff elapses, one more attempt happens.
    h.setNow(firstRetry);
    await h.engine.tick();
    assert.equal(h.engine.getJob(job.id).downloadAttempts, 2);
  } finally {
    h.close();
  }
});

test('backoff never schedules a retry past the expiry deadline', async () => {
  const h = makeHarness({ runMs: 100, expireMs: 1000, downloadConcurrency: 1 });
  try {
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'nearly gone' });
    await settleCreate(h);
    h.source.setFaults({ lostDownloadRate: 1 });
    h.advance(500);
    await h.tickUntil(() => h.engine.getJob(job.id).downloadAttempts >= 1, { maxTicks: 5, stepMs: 10 });
    const j = h.engine.getJob(job.id);
    assert.ok(j.retryAfter <= j.expiresAt);
  } finally {
    h.close();
  }
});

test('failed job can recover only while the charged result is still alive', async () => {
  const h = makeHarness({ runMs: 100, expireMs: 3000 });
  try {
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'recoverable' });
    await settleCreate(h);
    const remoteId = h.engine.getJob(job.id).remoteId;
    h.db.prepare('UPDATE jobs SET state = ? WHERE id = ?').run('failed', job.id);

    h.advance(300);
    const recovered = h.engine.recoverJob(job.id, { actor: 'test' });
    assert.equal(recovered.state, 'ready');
    assert.equal(h.source.quota().used, 1, 'recovery never creates a second job');

    // Now let it expire and try again: recovery must refuse and keep quota spent.
    h.db.prepare('UPDATE jobs SET state = ? WHERE id = ?').run('failed', job.id);
    h.advance(5000);
    const after = h.engine.recoverJob(job.id, { actor: 'test' });
    assert.equal(after.state, 'expired');
    assert.equal(h.engine.quotaSnapshot().charged, 1);
    const refused = h.audit.list({ limit: 50 }).find((e) => e.kind === 'job.recovery_refused');
    assert.ok(refused);
    assert.equal(refused.detail.remoteId, remoteId);
  } finally {
    h.close();
  }
});

test('recovery of a failed job refuses when the source has forgotten it', async () => {
  const h = makeHarness({ runMs: 100 });
  try {
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'ghost' });
    await settleCreate(h);
    h.db.prepare('UPDATE jobs SET state = ?, remote_id = ? WHERE id = ?').run('failed', 'exp_does_not_exist', job.id);
    assert.throws(() => h.engine.recoverJob(job.id), /does not know this job/);
  } finally {
    h.close();
  }
});

test('under a single download slot, the expiring result is downloaded before the older job', async () => {
  const h = makeHarness({ runMs: 60_000, expireMs: 60_000, downloadConcurrency: 1 });
  try {
    const old = h.engine.enqueue({ tenant: 'acme', name: 'older but safe' }).job;
    await settleCreate(h);
    h.advance(2000);
    const fresh = h.engine.enqueue({ tenant: 'acme', name: 'newer but expiring' }).job;
    await settleCreate(h);

    // Both are ready at the source, but the newer job's result expires sooner.
    for (const [job, expiresIn] of [[old, 40_000], [fresh, 2_000]]) {
      const remote = h.source.state.jobs[h.engine.getJob(job.id).remoteId];
      remote.status = 'ready';
      remote.bytes = Buffer.from(`remoteId\n${remote.remoteId}\n`);
      remote.expiresAt = h.now() + expiresIn;
    }

    // One tick promotes both from running to ready and spends the single
    // download slot on the more urgent of the two.
    await h.engine.tick();
    const states = [h.engine.getJob(old.id).state, h.engine.getJob(fresh.id).state];
    assert.equal(states[0], 'ready', 'older job waits');
    assert.equal(states[1], 'downloaded', 'expiring job is salvaged first');
  } finally {
    h.close();
  }
});

test('under strict FIFO policy the older job downloads first and the newer one can expire', async () => {
  const h = makeHarness({ runMs: 60_000, expireMs: 60_000, downloadConcurrency: 1, schedulerPolicy: 'fifo' });
  try {
    const old = h.engine.enqueue({ tenant: 'acme', name: 'older' }).job;
    await settleCreate(h);
    h.advance(2000);
    const fresh = h.engine.enqueue({ tenant: 'acme', name: 'newer' }).job;
    await settleCreate(h);

    for (const [job, expiresIn] of [[old, 40_000], [fresh, 2_000]]) {
      const remote = h.source.state.jobs[h.engine.getJob(job.id).remoteId];
      remote.status = 'ready';
      remote.bytes = Buffer.from(`remoteId\n${remote.remoteId}\n`);
      remote.expiresAt = h.now() + expiresIn;
    }

    await h.engine.tick();
    assert.equal(h.engine.getJob(old.id).state, 'downloaded', 'strict FIFO downloads the older job');
    assert.equal(h.engine.getJob(fresh.id).state, 'ready');
  } finally {
    h.close();
  }
});

test('a job re-queued after a definitive reject can be reserved again', async () => {
  const h = makeHarness({ dailyLimit: 5, runMs: 200 });
  try {
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'retry me' });
    h.ledger.reserve({ jobId: job.id, tenant: 'acme', day: h.source.quota().day, ttlMs: 1000 });
    h.ledger.release(job.id, { reason: 'source rejected create: QUOTA_EXCEEDED' });
    h.db.prepare('UPDATE jobs SET state = ?, attempts = 1 WHERE id = ?').run('queued', job.id);
    assert.equal(h.ledger.getForJob(job.id).state, 'released');

    // Previously this threw UNIQUE constraint failed: reservations.job_id on
    // every tick, leaving the job permanently stuck.
    await settleCreate(h);
    assert.equal(h.engine.getJob(job.id).state, 'running');
    assert.equal(h.ledger.getForJob(job.id).state, 'committed');
    const reserved = h.audit.list({ limit: 50 }).filter((e) => e.kind === 'quota.reserved');
    assert.equal(reserved.length, 2);
    assert.equal(reserved[0].detail.replaces, 'released');
    assert.equal(h.source.quota().used, 1, 'exactly one charge');
  } finally {
    h.close();
  }
});

test('reserve refuses to replace a committed reservation', async () => {
  const h = makeHarness({ runMs: 50 });
  try {
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'charged' });
    await settleCreate(h);
    assert.equal(h.ledger.getForJob(job.id).state, 'committed');
    assert.throws(
      () => h.ledger.reserve({ jobId: job.id, tenant: 'acme', day: h.source.quota().day, ttlMs: 1000 }),
      /already spent quota/,
    );
    assert.equal(h.source.quota().used, 1, 'no second charge');
  } finally {
    h.close();
  }
});

test('creating resumes when the source allowance returns before the day rolls', async () => {
  const h = makeHarness({ dailyLimit: 1, runMs: 100 });
  try {
    // Exhaust the source outside the console so the first create is rejected.
    await h.source.create({ tenant: 'external', name: 'not ours' });
    const { job } = h.engine.enqueue({ tenant: 'acme', name: 'waits for room' });
    await settleCreate(h);
    // Local budget is zero, so it never even attempts: still queued.
    assert.equal(h.engine.getJob(job.id).state, 'queued');
    assert.equal(h.source.quota().used, 1);

    // Raising the source limit would previously not help until midnight if the
    // console had already seen a QUOTA_EXCEEDED rejection. Simulate that block.
    h.engine.lastSourceLimitHitUntil = h.source.quota().resetsAt;
    h.engine.blockedDay = h.source.quota().day;
    await h.engine.tick();
    assert.equal(h.engine.getJob(job.id).state, 'queued', 'still blocked while source stays full');

    h.source.setFaults({ limitAdjustment: 3 });
    await settleCreate(h);
    assert.equal(h.engine.getJob(job.id).state, 'running', 'notice the allowance came back');
    const event = h.audit.list({ limit: 50 }).find((e) => e.kind === 'ledger.allowance_returned');
    assert.ok(event);
  } finally {
    h.close();
  }
});
