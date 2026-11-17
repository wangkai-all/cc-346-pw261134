import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadConfig } from './config.js';
import { openDatabase } from './core/db.js';
import { AuditLog } from './core/audit.js';
import { QuotaLedger } from './core/ledger.js';
import { ArtifactStore } from './core/artifacts.js';
import { Engine } from './core/engine.js';
import { SourceService } from './source/simulator.js';
import { POLICIES, rankCreateQueue, rankDownloadQueue } from './core/policy.js';
import { JOB } from './domain/states.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.json': 'application/json; charset=utf-8',
};

class HttpError extends Error {
  constructor(status, message, code = null) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

function sendJson(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': Buffer.byteLength(data),
  });
  res.end(data);
}

function readBody(req, limit = 1_000_000) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) {
        reject(new HttpError(413, 'request body too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (chunks.length === 0) return resolve({});
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
        if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
          throw new Error('body must be a JSON object');
        }
        resolve(parsed);
      } catch (err) {
        reject(new HttpError(400, `invalid JSON body: ${err.message}`));
      }
    });
    req.on('error', reject);
  });
}

function tenantOf(req, url) {
  // In production this would come from an authenticated session. Here the demo
  // treats the X-Tenant header (or ?tenant=) as the authenticated tenant scope
  // and rejects requests without one for tenant APIs.
  return (req.headers['x-tenant'] || url.searchParams.get('tenant') || '').trim();
}

function requireTenant(req, url) {
  const tenant = tenantOf(req, url);
  if (!tenant) throw new HttpError(401, 'missing tenant scope (X-Tenant)');
  return tenant;
}

function assertTenantJob(engine, id, tenant) {
  const job = engine.getJob(id, tenant);
  if (!job) throw new HttpError(404, 'job not found in this tenant');
  return job;
}

export function createServer(overrides = {}) {
  const config = loadConfig(overrides);
  const db = openDatabase(config.dbPath);
  const audit = new AuditLog(db);
  const ledger = new QuotaLedger(db, audit);
  const artifacts = new ArtifactStore(config.filesDir);
  const source = new SourceService({
    statePath: config.sourceStatePath,
    dailyLimit: config.sourceDailyLimit,
    runMs: config.sourceRunMs,
    expireMs: config.sourceExpireMs,
    downloadMs: config.sourceDownloadMs,
  });
  const engine = new Engine({ db, audit, ledger, source, artifacts, config });
  engine.start();

  const server = http.createServer(async (req, res) => {
    try {
      await handle(req, res);
    } catch (err) {
      const status = err.status ?? 500;
      if (status >= 500) console.error('[http]', err);
      sendJson(res, status, { error: err.message, code: err.code ?? null });
    }
  });

  async function handle(req, res) {
    const url = new URL(req.url, `http://${req.headers.host ?? 'localhost'}`);
    const p = url.pathname;

    if (!p.startsWith('/api/')) return serveStatic(p, res);

    if (p === '/api/health') {
      return sendJson(res, 200, { ok: true, now: Date.now() });
    }

    if (p === '/api/policies') {
      return sendJson(res, 200, { current: engine.policy, policies: Object.values(POLICIES) });
    }

    // ---- tenant APIs ----
    if (p === '/api/state' && req.method === 'GET') {
      const tenant = requireTenant(req, url);
      return sendJson(res, 200, viewForTenant(tenant));
    }

    if (p === '/api/jobs' && req.method === 'POST') {
      const tenant = requireTenant(req, url);
      const body = await readBody(req);
      const clientKey = typeof body.clientKey === 'string' && body.clientKey ? body.clientKey.slice(0, 200) : null;
      const name = typeof body.name === 'string' ? body.name.trim() : '';
      if (!name) throw new HttpError(400, 'name is required');
      if (name.length > 200) throw new HttpError(400, 'name too long');
      const meta = body.meta && typeof body.meta === 'object' && !Array.isArray(body.meta) ? body.meta : {};
      const { job, reused } = engine.enqueue({ tenant, name, meta, clientKey, actor: `tenant:${tenant}` });
      return sendJson(res, reused ? 200 : 201, { job: presentJob(job, tenant), reused });
    }

    const jobMatch = p.match(/^\/api\/jobs\/([^/]+)(?:\/([a-z-]+))?$/);
    if (jobMatch) {
      const tenant = requireTenant(req, url);
      const id = decodeURIComponent(jobMatch[1]);
      assertTenantJob(engine, id, tenant);
      const action = jobMatch[2] ?? null;

      if (!action && req.method === 'GET') {
        return sendJson(res, 200, detailForJob(id, tenant));
      }
      if (action === 'cancel' && req.method === 'POST') {
        const body = await readBody(req);
        const job = engine.cancel(id, { actor: `tenant:${tenant}`, reason: body.reason ?? null });
        return sendJson(res, 200, { job: presentJob(job, tenant) });
      }
      if (action === 'reconcile' && req.method === 'POST') {
        const result = await engine.reconcileJob(id, { actor: `tenant:${tenant}` });
        return sendJson(res, 200, { ...result, job: presentJob(engine.getJob(id, tenant), tenant) });
      }
      if (action === 'ingest' && req.method === 'POST') {
        const body = await readBody(req);
        const job = engine.ingest(id, { actor: `tenant:${tenant}`, note: body.note ?? null });
        return sendJson(res, 200, { job: presentJob(job, tenant) });
      }
      if (action === 'download' && req.method === 'GET') {
        return serveArtifact(id, tenant, res);
      }
      throw new HttpError(405, 'method not allowed');
    }

    // ---- operator APIs (global, cross-tenant) ----
    if (p === '/api/operator/overview' && req.method === 'GET') {
      const attention = engine.listJobs({ limit: 1000 })
        .filter((j) => [JOB.UNCERTAIN, JOB.FAILED, JOB.EXPIRED].includes(j.state))
        .map((j) => presentJob(j, j.tenant));
      return sendJson(res, 200, {
        now: Date.now(),
        policy: engine.policy,
        policyInfo: POLICIES[engine.policy],
        quota: engine.quotaSnapshot(),
        tenants: tenantSummaries(),
        queue: operatorQueue(),
        attention,
        actionsRequired: engine.listJobs({ limit: 1000 }).filter((j) => j.state === JOB.UNCERTAIN || j.state === JOB.FAILED).length,
      });
    }

    if (p === '/api/operator/ledger' && req.method === 'GET') {
      return sendJson(res, 200, await engine.auditLedger({ actor: 'operator' }));
    }

    if (p === '/api/operator/policy' && req.method === 'POST') {
      const body = await readBody(req);
      const policy = engine.setPolicy(body.policy);
      return sendJson(res, 200, { policy });
    }

    const opMatch = p.match(/^\/api\/operator\/jobs\/([^/]+)\/([a-z-]+)$/);
    if (opMatch) {
      const id = decodeURIComponent(opMatch[1]);
      const action = opMatch[2];
      if (action === 'reconcile' && req.method === 'POST') {
        const result = await engine.reconcileJob(id, { actor: 'operator' });
        return sendJson(res, 200, { ...result, job: engine.getJob(id) });
      }
      if (action === 'resolve' && req.method === 'POST') {
        const body = await readBody(req);
        const result = engine.resolveManually(id, {
          decision: body.decision,
          note: body.note,
          remoteId: body.remoteId ?? null,
          actor: 'operator',
        });
        return sendJson(res, 200, { ...result, job: presentJob(result.job, result.job.tenant) });
      }
      if (action === 'cancel' && req.method === 'POST') {
        const body = await readBody(req);
        const job = engine.cancel(id, { actor: 'operator', reason: body.reason ?? null });
        return sendJson(res, 200, { job: presentJob(job, job.tenant) });
      }
      if (action === 'ingest' && req.method === 'POST') {
        const body = await readBody(req);
        const job = engine.ingest(id, { actor: 'operator', note: body.note ?? null });
        return sendJson(res, 200, { job: presentJob(job, job.tenant) });
      }
      if (action === 'recover' && req.method === 'POST') {
        const job = engine.recoverJob(id, { actor: 'operator' });
        return sendJson(res, 200, { job: presentJob(job, job.tenant) });
      }
      throw new HttpError(405, 'method not allowed');
    }

    if (p.startsWith('/api/operator/jobs/') && req.method === 'GET') {
      const id = decodeURIComponent(p.slice('/api/operator/jobs/'.length));
      const job = engine.getJob(id);
      if (!job) throw new HttpError(404, 'job not found');
      return sendJson(res, 200, detailForJob(id, job.tenant));
    }

    if (p === '/api/audit' && req.method === 'GET') {
      const limit = Math.min(500, Number(url.searchParams.get('limit') ?? 100) || 100);
      return sendJson(res, 200, { events: audit.list({ limit }), chain: audit.verifyChain() });
    }

    // ---- local source control (simulator only) ----
    if (p === '/api/source/diagnostics' && req.method === 'GET') {
      return sendJson(res, 200, source.diagnostics());
    }
    if (p === '/api/source/faults' && req.method === 'POST') {
      const body = await readBody(req);
      let faults;
      try {
        faults = source.setFaults(body);
      } catch (err) {
        throw new HttpError(400, err.message);
      }
      return sendJson(res, 200, { faults });
    }

    throw new HttpError(404, 'not found');
  }

  function tenantSummaries() {
    const tenants = new Set(engine.listJobs({ limit: 5000 }).map((j) => j.tenant));
    for (const t of config.seedTenants) tenants.add(t);
    return [...tenants].map((tenant) => {
      const jobs = engine.listJobs({ tenant, limit: 5000 });
      const counts = {};
      for (const j of jobs) counts[j.state] = (counts[j.state] ?? 0) + 1;
      return {
        tenant,
        jobs: jobs.length,
        counts,
        waiting: (counts.queued ?? 0) + (counts.uncertain ?? 0),
        ready: counts.ready ?? 0,
        expired: counts.expired ?? 0,
        ingested: counts.ingested ?? 0,
      };
    });
  }

  function operatorQueue() {
    const queued = engine.listJobs({ states: [JOB.QUEUED] });
    const ready = engine.listJobs({ states: [JOB.READY] });
    const tenantActive = new Map();
    for (const j of engine.listJobs({ states: [JOB.CREATING, JOB.UNCERTAIN, JOB.RUNNING, JOB.READY], limit: 5000 })) {
      tenantActive.set(j.tenant, (tenantActive.get(j.tenant) ?? 0) + 1);
    }
    return {
      create: rankCreateQueue(queued, { policy: engine.policy, starvationMs: config.starvationMs, now: Date.now(), tenantActive })
        .map((j) => ({ id: j.id, tenant: j.tenant, name: j.name, ageMs: j.ageMs, reason: j.reason })),
      download: rankDownloadQueue(ready, { policy: engine.policy, now: Date.now() })
        .map((j) => ({ id: j.id, tenant: j.tenant, name: j.name, secondsLeft: Math.round((j.expiresAt - Date.now()) / 1000), reason: j.reason })),
    };
  }

  function queueFor(tenant) {
    const queued = engine.listJobs({ tenant, states: [JOB.QUEUED] });
    const ready = engine.listJobs({ tenant, states: [JOB.READY] });
    const tenantActive = new Map();
    for (const j of engine.listJobs({ states: [JOB.CREATING, JOB.UNCERTAIN, JOB.RUNNING, JOB.READY], limit: 5000 })) {
      tenantActive.set(j.tenant, (tenantActive.get(j.tenant) ?? 0) + 1);
    }
    const creates = rankCreateQueue(queued, { policy: engine.policy, starvationMs: config.starvationMs, now: Date.now(), tenantActive });
    const downloads = rankDownloadQueue(ready, { policy: engine.policy, now: Date.now() });
    return {
      create: creates.map((j, i) => ({ id: j.id, name: j.name, position: i + 1, ageMs: j.ageMs, reason: j.reason })),
      download: downloads.map((j, i) => ({ id: j.id, name: j.name, position: i + 1, secondsLeft: Math.round((j.expiresAt - Date.now()) / 1000), reason: j.reason })),
    };
  }

  function viewForTenant(tenant) {
    const jobs = engine.listJobs({ tenant }).map((j) => presentJob(j, tenant));
    const quota = engine.quotaSnapshot();
    // Tenant-level share view: the source limit is global, but a tenant should
    // see whether it is blocked. It cannot see other tenants' job details.
    const ownCharged = jobs.filter((j) => ['running', 'ready', 'downloaded', 'ingested', 'expired'].includes(j.state)).length;
    return {
      tenant,
      now: Date.now(),
      quota,
      ownCharged,
      jobs,
      queue: queueFor(tenant),
      policy: { key: engine.policy, ...POLICIES[engine.policy] },
    };
  }

  function presentJob(job, tenant) {
    if (!job) return null;
    if (job.tenant !== tenant) throw new HttpError(404, 'job not found in this tenant');
    const reservation = job.reservationId ? ledger.getForJob(job.id) : null;
    const now = Date.now();
    return {
      id: job.id,
      tenant: job.tenant,
      name: job.name,
      meta: job.meta,
      state: job.state,
      queueSeq: job.queueSeq,
      remoteId: job.remoteId,
      attempts: job.attempts,
      downloadAttempts: job.downloadAttempts,
      lastError: job.lastError,
      queuedAt: job.queuedAt,
      readyAt: job.readyAt,
      expiresAt: job.expiresAt,
      secondsLeft: job.expiresAt ? Math.max(0, Math.round((job.expiresAt - now) / 1000)) : null,
      downloadedAt: job.downloadedAt,
      ingestedAt: job.ingestedAt,
      artifact: job.artifact ? { bytes: job.artifact.bytes, sha256: job.artifact.sha256, available: artifacts.exists(job.id) } : null,
      reservation: reservation ? {
        state: reservation.state,
        remoteId: reservation.remoteId,
        note: reservation.note,
        resolveAfter: reservation.resolveAfter,
        attemptStartedAt: reservation.attemptStartedAt,
      } : null,
      cancelRequested: Boolean(job.cancelRequestedAt),
      ingestNote: job.ingestNote,
      updatedAt: job.updatedAt,
    };
  }

  function detailForJob(id, tenant) {
    const job = assertTenantJob(engine, id, tenant);
    const auditEvents = audit.list({ jobId: id, limit: 200 });
    return { job: presentJob(job, tenant), audit: auditEvents };
  }

  function serveArtifact(id, tenant, res) {
    const job = assertTenantJob(engine, id, tenant);
    if (!job.artifact) throw new HttpError(409, 'job has no downloaded artifact');
    const bytes = artifacts.read(id);
    if (!bytes) throw new HttpError(410, 'artifact file is no longer available');
    res.writeHead(200, {
      'content-type': 'text/csv; charset=utf-8',
      'content-disposition': `attachment; filename="export-${id.slice(-12)}.csv"`,
      'x-artifact-sha256': job.artifact.sha256,
      'content-length': bytes.length,
    });
    res.end(bytes);
  }

  function serveStatic(p, res) {
    const publicDir = path.join(__dirname, 'web', 'public');
    const rel = p === '/' ? '/index.html' : p;
    const full = path.normalize(path.join(publicDir, rel));
    if (!full.startsWith(publicDir + path.sep) && full !== path.join(publicDir, 'index.html')) {
      return sendJson(res, 404, { error: 'not found' });
    }
    fs.readFile(full, (err, data) => {
      if (err) return sendJson(res, 404, { error: 'not found' });
      res.writeHead(200, { 'content-type': MIME[path.extname(full)] ?? 'application/octet-stream', 'cache-control': 'no-cache' });
      res.end(data);
    });
  }

  return {
    server,
    config,
    db,
    engine,
    source,
    ledger,
    audit,
    artifacts,
    listen(port = config.port) {
      return new Promise((resolve) => {
        server.listen(port, '127.0.0.1', () => resolve(server.address()));
      });
    },
    async close() {
      engine.stop();
      await new Promise((resolve) => server.close(resolve));
      db.close();
    },
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { config, listen, engine } = createServer();
  const addr = await listen();
  console.log(`export console listening on http://127.0.0.1:${addr.port}`);
  console.log(`policy=${engine.policy} source daily limit=${config.sourceDailyLimit} expire after=${config.sourceExpireMs}ms`);
  const shutdown = async () => {
    engine.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
