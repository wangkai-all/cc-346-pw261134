import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { openDatabase } from '../src/core/db.js';
import { AuditLog } from '../src/core/audit.js';
import { QuotaLedger } from '../src/core/ledger.js';
import { ArtifactStore } from '../src/core/artifacts.js';
import { Engine } from '../src/core/engine.js';
import { SourceService } from '../src/source/simulator.js';

// Component-level harness with an injectable clock, so binding rules (expiry,
// deadlines, retry windows) can be tested without sleeping in real time.
// Timers inside the engine are not started; tests call tick() explicitly.
export function makeHarness(overrides = {}) {
  const dataDir = overrides.dataDir ?? fs.mkdtempSync(path.join(os.tmpdir(), 'export-console-test-'));
  fs.mkdirSync(dataDir, { recursive: true });
  const clock = { now: overrides.startAt ?? Date.parse('2026-11-18T09:00:00Z') };
  const now = () => clock.now;

  const db = openDatabase(':memory:');
  const audit = new AuditLog(db, now);
  const ledger = new QuotaLedger(db, audit, now);
  const artifacts = new ArtifactStore(path.join(dataDir, 'files'));
  const source = new SourceService({
    statePath: overrides.sourceStatePath ?? null,
    persist: Boolean(overrides.sourceStatePath),
    dailyLimit: overrides.dailyLimit ?? 10,
    runMs: overrides.runMs ?? 1000,
    expireMs: overrides.expireMs ?? 60_000,
    downloadMs: overrides.downloadMs ?? 0,
    now,
  });
  const config = {
    tickMs: 1000,
    downloadConcurrency: overrides.downloadConcurrency ?? 1,
    createConcurrency: overrides.createConcurrency ?? 1,
    createTimeoutMs: overrides.createTimeoutMs ?? 500,
    reconcileGraceMs: overrides.reconcileGraceMs ?? 0,
    schedulerPolicy: overrides.schedulerPolicy ?? 'expiring-first',
    starvationMs: overrides.starvationMs ?? 30_000,
    seedTenants: ['acme', 'globex'],
  };
  const engine = new Engine({ db, audit, ledger, source, artifacts, config, now });

  const advance = (ms) => { clock.now += ms; };
  const setNow = (ms) => { clock.now = ms; };

  async function tickUntil(predicate, { maxTicks = 200, stepMs = 100 } = {}) {
    for (let i = 0; i < maxTicks; i += 1) {
      await engine.tick();
      if (predicate()) return true;
      advance(stepMs);
    }
    return predicate();
  }

  return {
    dataDir, clock, now, db, audit, ledger, artifacts, source, engine, config,
    advance, setNow, tickUntil,
    close() {
      engine.stop();
      db.close();
      fs.rmSync(dataDir, { recursive: true, force: true });
    },
  };
}

export function jobOf(h, id) {
  return h.engine.getJob(id);
}

export async function settleCreate(h, { maxTicks = 60 } = {}) {
  await h.tickUntil(() => {
    const jobs = h.engine.listJobs();
    return jobs.length > 0 && jobs.every((j) => j.state !== 'creating');
  }, { maxTicks, stepMs: 50 });
}
