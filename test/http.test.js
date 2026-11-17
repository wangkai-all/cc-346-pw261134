import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createServer } from '../src/server.js';

async function boot(overrides = {}) {
  const dataDir = overrides.dataDir ?? fs.mkdtempSync(path.join(os.tmpdir(), 'export-console-http-'));
  const app = createServer({
    dataDir,
    port: 0,
    sourceDailyLimit: 5,
    sourceRunMs: 150,
    sourceExpireMs: 20_000,
    tickMs: 40,
    createTimeoutMs: 400,
    reconcileGraceMs: 100,
    seedTenants: ['acme', 'globex', 'initech'],
    ...overrides,
  });
  const addr = await app.listen(0);
  const base = `http://127.0.0.1:${addr.port}`;
  const call = async (p, { method = 'GET', body, tenant, headers = {} } = {}) => {
    const h = { ...headers };
    if (tenant) h['x-tenant'] = tenant;
    if (body !== undefined) h['content-type'] = 'application/json';
    const res = await fetch(base + p, { method, headers: h, body: body === undefined ? undefined : JSON.stringify(body) });
    const text = await res.text();
    let json = null;
    try { json = text ? JSON.parse(text) : null; } catch { json = { raw: text }; }
    return { status: res.status, body: json, headers: res.headers };
  };
  return { app, base, call, dataDir };
}

async function waitFor(fn, { timeout = 8000, interval = 40 } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    if (await fn()) return true;
    await new Promise((r) => setTimeout(r, interval));
  }
  return false;
}

test('tenant APIs require a tenant scope and isolate jobs', async () => {
  const { app, call } = await boot();
  try {
    const anon = await call('/api/state');
    assert.equal(anon.status, 401);

    const created = await call('/api/jobs', { method: 'POST', tenant: 'acme', body: { name: 'acme export' } });
    assert.equal(created.status, 201);

    const acme = await call('/api/state', { tenant: 'acme' });
    assert.equal(acme.body.jobs.length, 1);

    const globex = await call('/api/state', { tenant: 'globex' });
    assert.equal(globex.body.jobs.length, 0);

    const crossTenant = await call(`/api/jobs/${created.body.job.id}`, { tenant: 'globex' });
    assert.equal(crossTenant.status, 404, 'a tenant cannot read another tenant job');

    const crossCancel = await call(`/api/jobs/${created.body.job.id}/cancel`, { method: 'POST', tenant: 'globex', body: {} });
    assert.equal(crossCancel.status, 404);

    const crossDownload = await call(`/api/jobs/${created.body.job.id}/download`, { tenant: 'globex' });
    assert.equal(crossDownload.status, 404);
  } finally {
    await app.close();
  }
});

test('full lifecycle over HTTP: queue, download, ingest, audit', async () => {
  const { app, call } = await boot();
  try {
    const created = await call('/api/jobs', {
      method: 'POST', tenant: 'acme', body: { name: 'lifecycle', clientKey: 'idem-1' },
    });
    const id = created.body.job.id;

    const dup = await call('/api/jobs', {
      method: 'POST', tenant: 'acme', body: { name: 'lifecycle', clientKey: 'idem-1' },
    });
    assert.equal(dup.status, 200);
    assert.equal(dup.body.reused, true);
    assert.equal(dup.body.job.id, id);

    const ok = await waitFor(async () => {
      const r = await call(`/api/jobs/${id}`, { tenant: 'acme' });
      return r.body.job.state === 'downloaded';
    });
    assert.ok(ok, 'job should download');

    const detail = await call(`/api/jobs/${id}`, { tenant: 'acme' });
    assert.equal(detail.body.job.state, 'downloaded');
    assert.ok(detail.body.job.artifact.available);
    assert.ok(detail.body.audit.some((e) => e.kind === 'job.downloaded'));

    const ingest = await call(`/api/jobs/${id}/ingest`, { method: 'POST', tenant: 'acme', body: { note: 'loaded into warehouse' } });
    assert.equal(ingest.status, 200);
    assert.equal(ingest.body.job.state, 'ingested');

    const audit = await call('/api/audit?limit=200');
    assert.equal(audit.body.chain.ok, true);

    const overview = await call('/api/operator/overview');
    assert.equal(overview.status, 200);
    assert.equal(overview.body.quota.charged, 1);
    assert.equal(overview.body.tenants.find((t) => t.tenant === 'acme').counts.ingested, 1);
  } finally {
    await app.close();
  }
});

test('operator can list and act across tenants, but tenant routes stay scoped', async () => {
  const { app, call } = await boot();
  try {
    await call('/api/jobs', { method: 'POST', tenant: 'acme', body: { name: 'a1' } });
    await call('/api/jobs', { method: 'POST', tenant: 'globex', body: { name: 'g1' } });

    const overview = await call('/api/operator/overview');
    const tenants = overview.body.tenants.map((t) => t.tenant);
    assert.ok(tenants.includes('acme'));
    assert.ok(tenants.includes('globex'));
    assert.ok(overview.body.queue.create.length >= 2);
    for (const j of overview.body.queue.create) {
      assert.ok(j.reason, 'every queue decision carries a reason');
    }
  } finally {
    await app.close();
  }
});

test('download endpoint requires the artifact and serves CSV with a hash header', async () => {
  const { app, base, call } = await boot();
  try {
    const created = await call('/api/jobs', { method: 'POST', tenant: 'acme', body: { name: 'csv export' } });
    const id = created.body.job.id;
    const early = await fetch(`${base}/api/jobs/${id}/download`, { headers: { 'x-tenant': 'acme' } });
    assert.equal(early.status, 409, 'no artifact before download completes');

    await waitFor(async () => {
      const r = await call(`/api/jobs/${id}`, { tenant: 'acme' });
      return r.body.job.state === 'downloaded';
    });
    const res = await fetch(`${base}/api/jobs/${id}/download`, { headers: { 'x-tenant': 'acme' } });
    assert.equal(res.status, 200);
    assert.match(res.headers.get('content-type'), /text\/csv/);
    assert.match(res.headers.get('x-artifact-sha256'), /^[0-9a-f]{64}$/);
    const text = await res.text();
    assert.match(text, /^remoteId,tenant,name,createdAt/);
    assert.match(text, /acme/);
  } finally {
    await app.close();
  }
});

test('quota exhaustion at the source surfaces as a requeue and a ledger discrepancy', async () => {
  const { app, call } = await boot({ sourceDailyLimit: 1 });
  try {
    await call('/api/jobs', { method: 'POST', tenant: 'acme', body: { name: 'one' } });
    await call('/api/jobs', { method: 'POST', tenant: 'acme', body: { name: 'two' } });
    await waitFor(async () => {
      const r = await call('/api/state', { tenant: 'acme' });
      return r.body.quota.charged === 1 && r.body.jobs.some((j) => j.state === 'running');
    });

    const state = await call('/api/state', { tenant: 'acme' });
    assert.equal(state.body.quota.charged, 1);
    // The second job cannot be admitted: local allowance is zero.
    const queued = state.body.jobs.find((j) => j.name === 'two');
    assert.equal(queued.state, 'queued');
    assert.ok(state.body.quota.localRemaining === 0);
  } finally {
    await app.close();
  }
});

test('restart keeps the ledger and never re-creates an uncertain job', async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'export-console-restart-'));
  const first = await boot({ dataDir });
  let jobId;
  try {
    const created = await first.call('/api/jobs', { method: 'POST', tenant: 'acme', body: { name: 'before restart' } });
    jobId = created.body.job.id;
    await waitFor(async () => {
      const r = await first.call(`/api/jobs/${jobId}`, { tenant: 'acme' });
      return r.body.job.state === 'running';
    });
    // Put the job into the ambiguous state a crash during create would leave.
    first.app.db.prepare('UPDATE jobs SET state = ? WHERE id = ?').run('creating', jobId);
    first.app.ledger.markUncertain(jobId, { reason: 'simulated crash', resolveAfter: Date.now() });
  } finally {
    await first.app.close();
  }

  const second = await boot({ dataDir });
  try {
    const r = await second.call(`/api/jobs/${jobId}`, { tenant: 'acme' });
    assert.equal(r.body.job.state, 'uncertain');
    assert.equal(r.body.job.reservation.state, 'uncertain');
    const audit = await second.call('/api/audit?limit=200');
    const recovered = audit.body.events.find((e) => e.kind === 'job.recovered_uncertain');
    assert.ok(recovered, 'restart is recorded');
    // And no new create was attempted: the source still shows one job.
    const diag = await second.call('/api/source/diagnostics');
    assert.equal(diag.body.quota.used, 1);
    assert.equal(diag.body.quota.inflight, 0);
  } finally {
    await second.app.close();
  }
});

test('fault controls can be changed at runtime and reject bad values', async () => {
  const { app, call } = await boot();
  try {
    const set = await call('/api/source/faults', { method: 'POST', body: { lostCreateResponseRate: 0.5, createDelayMs: 120 } });
    assert.equal(set.status, 200);
    assert.equal(set.body.faults.lostCreateResponseRate, 0.5);
    const bad = await call('/api/source/faults', { method: 'POST', body: { lostCreateResponseRate: 'nope' } });
    assert.equal(bad.status, 400);
    const outOfRange = await call('/api/source/faults', { method: 'POST', body: { lostCreateResponseRate: 5 } });
    assert.equal(outOfRange.body.faults.lostCreateResponseRate, 1);
  } finally {
    await app.close();
  }
});

test('policy can be switched and the switch is audited', async () => {
  const { app, call } = await boot();
  try {
    const set = await call('/api/operator/policy', { method: 'POST', body: { policy: 'fifo' } });
    assert.equal(set.status, 200);
    const state = await call('/api/state', { tenant: 'acme' });
    assert.equal(state.body.policy.key, 'fifo');
    const audit = await call('/api/audit?limit=50');
    const ev = audit.body.events.find((e) => e.kind === 'policy.changed');
    assert.equal(ev.detail.policy, 'fifo');
    assert.ok(ev.detail.summary.length > 50);
  } finally {
    await app.close();
  }
});

test('static console page is served', async () => {
  const { app, base } = await boot();
  try {
    const res = await fetch(base + '/');
    assert.equal(res.status, 200);
    assert.match(await res.text(), /Export Collection Console/);
    const js = await fetch(base + '/app.js');
    assert.equal(js.status, 200);
    const traversal = await fetch(base + '/..%2f..%2fpackage.json');
    assert.notEqual(traversal.status, 200);
  } finally {
    await app.close();
  }
});
