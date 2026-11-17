import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

export function openDatabase(dbPath) {
  if (dbPath !== ':memory:') fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  const db = new DatabaseSync(dbPath);
  db.exec('PRAGMA foreign_keys = ON');
  if (dbPath !== ':memory:') db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA busy_timeout = 5000');
  migrate(db);
  return db;
}

function migrate(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id                 TEXT PRIMARY KEY,
      tenant             TEXT NOT NULL,
      name               TEXT NOT NULL,
      meta               TEXT NOT NULL DEFAULT '{}',
      state              TEXT NOT NULL,
      queue_seq          INTEGER NOT NULL,
      reservation_id     TEXT,
      remote_id          TEXT,
      client_key         TEXT,
      created_by         TEXT NOT NULL DEFAULT 'web',
      attempts           INTEGER NOT NULL DEFAULT 0,
      dl_attempts        INTEGER NOT NULL DEFAULT 0,
      last_error         TEXT,
      queued_at          INTEGER NOT NULL,
      create_started_at  INTEGER,
      ready_at           INTEGER,
      expires_at         INTEGER,
      downloaded_at      INTEGER,
      ingested_at        INTEGER,
      artifact_path      TEXT,
      artifact_sha256    TEXT,
      artifact_bytes     INTEGER,
      cancel_requested_at INTEGER,
      retry_after        INTEGER,
      ingest_note        TEXT,
      updated_at         INTEGER NOT NULL,
      version            INTEGER NOT NULL DEFAULT 1
    );
    CREATE UNIQUE INDEX IF NOT EXISTS jobs_tenant_client_key
      ON jobs(tenant, client_key) WHERE client_key IS NOT NULL;
    CREATE INDEX IF NOT EXISTS jobs_state_queue ON jobs(state, queue_seq);
    CREATE INDEX IF NOT EXISTS jobs_tenant ON jobs(tenant, state);
    CREATE INDEX IF NOT EXISTS jobs_expiry ON jobs(state, expires_at);

    CREATE TABLE IF NOT EXISTS reservations (
      id                 TEXT PRIMARY KEY,
      job_id             TEXT NOT NULL UNIQUE REFERENCES jobs(id),
      tenant             TEXT NOT NULL,
      state              TEXT NOT NULL,
      remote_id          TEXT,
      day                TEXT NOT NULL,
      attempt_started_at INTEGER,
      resolve_after      INTEGER,
      note               TEXT,
      created_at         INTEGER NOT NULL,
      updated_at         INTEGER NOT NULL,
      version            INTEGER NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS reservations_day_state ON reservations(day, state);
    CREATE INDEX IF NOT EXISTS reservations_state ON reservations(state);

    CREATE TABLE IF NOT EXISTS events (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      at        INTEGER NOT NULL,
      kind      TEXT NOT NULL,
      actor     TEXT NOT NULL DEFAULT 'system',
      tenant    TEXT,
      job_id    TEXT,
      detail    TEXT NOT NULL DEFAULT '{}',
      prev_hash TEXT,
      hash      TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS events_at ON events(at);

    CREATE TABLE IF NOT EXISTS idempotency (
      tenant       TEXT NOT NULL,
      key          TEXT NOT NULL,
      request_hash TEXT NOT NULL,
      status_code  INTEGER NOT NULL,
      response     TEXT NOT NULL,
      created_at   INTEGER NOT NULL,
      PRIMARY KEY (tenant, key)
    );

    CREATE TABLE IF NOT EXISTS counters (
      name  TEXT PRIMARY KEY,
      value INTEGER NOT NULL
    );
    INSERT OR IGNORE INTO counters(name, value) VALUES ('queue_seq', 0);
  `);
}

export function tx(db, fn) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const result = fn();
    db.exec('COMMIT');
    return result;
  } catch (err) {
    try { db.exec('ROLLBACK'); } catch { /* ignore */ }
    throw err;
  }
}

export function nextQueueSeq(db) {
  const row = db.prepare(`UPDATE counters SET value = value + 1 WHERE name = 'queue_seq' RETURNING value`).get();
  return row.value;
}

export function rowToJob(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenant: row.tenant,
    name: row.name,
    meta: JSON.parse(row.meta || '{}'),
    state: row.state,
    queueSeq: row.queue_seq,
    reservationId: row.reservation_id,
    remoteId: row.remote_id,
    clientKey: row.client_key,
    createdBy: row.created_by,
    attempts: row.attempts,
    downloadAttempts: row.dl_attempts,
    lastError: row.last_error,
    queuedAt: row.queued_at,
    createStartedAt: row.create_started_at,
    readyAt: row.ready_at,
    expiresAt: row.expires_at,
    downloadedAt: row.downloaded_at,
    ingestedAt: row.ingested_at,
    artifact: row.artifact_path ? {
      path: row.artifact_path,
      sha256: row.artifact_sha256,
      bytes: row.artifact_bytes,
    } : null,
    cancelRequestedAt: row.cancel_requested_at,
    retryAfter: row.retry_after,
    ingestNote: row.ingest_note,
    updatedAt: row.updated_at,
    version: row.version,
  };
}

export function rowToReservation(row) {
  if (!row) return null;
  return {
    id: row.id,
    jobId: row.job_id,
    tenant: row.tenant,
    state: row.state,
    remoteId: row.remote_id,
    day: row.day,
    attemptStartedAt: row.attempt_started_at,
    resolveAfter: row.resolve_after,
    note: row.note,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    version: row.version,
  };
}
