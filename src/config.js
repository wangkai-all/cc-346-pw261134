import path from 'node:path';

function intEnv(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) throw new Error(`Invalid ${name}: ${raw}`);
  return Math.floor(n);
}

export function loadConfig(overrides = {}) {
  const dataDir = overrides.dataDir ?? process.env.DATA_DIR ?? path.resolve(process.cwd(), 'data');
  const base = {
    port: intEnv('PORT', 3000),
    dataDir,
    filesDir: path.join(dataDir, 'files'),
    dbPath: process.env.DB_PATH ?? path.join(dataDir, 'console.sqlite'),
    sourceStatePath: process.env.SOURCE_STATE_PATH ?? path.join(dataDir, 'source.json'),
    // Source service constraints.
    sourceDailyLimit: intEnv('SOURCE_DAILY_LIMIT', 12),
    sourceRunMs: intEnv('SOURCE_RUN_MS', 4000),
    sourceExpireMs: intEnv('SOURCE_EXPIRE_MS', 60_000),
    sourceDownloadMs: intEnv('SOURCE_DOWNLOAD_MS', 900),
    // Console behaviour.
    tickMs: intEnv('TICK_MS', 250),
    downloadConcurrency: intEnv('DOWNLOAD_CONCURRENCY', 1),
    createConcurrency: intEnv('CREATE_CONCURRENCY', 2),
    createTimeoutMs: intEnv('CREATE_TIMEOUT_MS', 3000),
    reconcileGraceMs: intEnv('RECONCILE_GRACE_MS', 5000),
    schedulerPolicy: process.env.SCHEDULER_POLICY ?? 'expiring-first',
    starvationMs: intEnv('STARVATION_MS', 30_000),
    auditRetentionMs: intEnv('AUDIT_RETENTION_MS', 6 * 60 * 60 * 1000),
    seedTenants: ['acme', 'globex', 'initech'],
  };
  return { ...base, ...overrides };
}
