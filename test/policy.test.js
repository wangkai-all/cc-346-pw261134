import test from 'node:test';
import assert from 'node:assert/strict';
import { rankCreateQueue, rankDownloadQueue, POLICIES } from '../src/core/policy.js';

const NOW = 1_000_000;

function job(id, tenant, seq, queuedAgoMs = 0, expiresInMs = 10_000) {
  return {
    id, tenant, queueSeq: seq,
    queuedAt: NOW - queuedAgoMs,
    expiresAt: NOW + expiresInMs,
    readyAt: NOW,
  };
}

test('download order is nearest-expiry first under expiring-first', () => {
  const jobs = [
    job('a', 'acme', 1, 0, 30_000),
    job('b', 'globex', 2, 0, 5_000),
    job('c', 'acme', 3, 0, 12_000),
  ];
  const ranked = rankDownloadQueue(jobs, { policy: 'expiring-first', now: NOW });
  assert.deepEqual(ranked.map((j) => j.id), ['b', 'c', 'a']);
  assert.match(ranked[0].reason, /nearest expiry/);
  assert.match(ranked[0].reason, /salvages/);
});

test('download order can be strict FIFO when the operator chooses it', () => {
  const jobs = [
    job('a', 'acme', 1, 0, 30_000),
    job('b', 'globex', 2, 0, 5_000),
  ];
  const ranked = rankDownloadQueue(jobs, { policy: 'fifo', now: NOW });
  assert.deepEqual(ranked.map((j) => j.id), ['a', 'b']);
  assert.match(ranked[0].reason, /even if a newer result expires sooner/);
});

test('expiry tie is broken by queue order', () => {
  const jobs = [
    job('later', 'acme', 2, 0, 5_000),
    job('earlier', 'acme', 1, 0, 5_000),
  ];
  const ranked = rankDownloadQueue(jobs, { policy: 'expiring-first', now: NOW });
  assert.deepEqual(ranked.map((j) => j.id), ['earlier', 'later']);
});

test('create queue is FIFO within tenant and prefers less-in-flight tenants', () => {
  const jobs = [
    job('acme-old', 'acme', 1, 5_000),
    job('globex-new', 'globex', 2, 1_000),
  ];
  const tenantActive = new Map([['acme', 3], ['globex', 1]]);
  const ranked = rankCreateQueue(jobs, { policy: 'expiring-first', now: NOW, tenantActive, starvationMs: 60_000 });
  assert.deepEqual(ranked.map((j) => j.id), ['globex-new', 'acme-old']);
  assert.match(ranked[0].reason, /less in-flight/);
});

test('starvation relief lets a very old job outrank a newer one', () => {
  const jobs = [
    job('old', 'acme', 1, 120_000),
    job('fresh', 'globex', 2, 1_000),
  ];
  const tenantActive = new Map([['acme', 5], ['globex', 0]]);
  const ranked = rankCreateQueue(jobs, { policy: 'expiring-first', now: NOW, tenantActive, starvationMs: 30_000 });
  assert.equal(ranked[0].id, 'old');
  assert.equal(ranked[0].starving, true);
  assert.match(ranked[0].reason, /starvation relief/);
  assert.match(ranked[0].reason, /no expiry risk/);
});

test('strict FIFO policy ignores fairness and starvation', () => {
  const jobs = [
    job('old', 'acme', 1, 120_000),
    job('new', 'globex', 2, 1_000),
  ];
  const tenantActive = new Map([['acme', 5], ['globex', 0]]);
  const ranked = rankCreateQueue(jobs, { policy: 'fifo', now: NOW, tenantActive, starvationMs: 30_000 });
  assert.deepEqual(ranked.map((j) => j.id), ['old', 'new']);
  assert.match(ranked[0].reason, /strict FIFO/);
});

test('policies are documented with an explainable rationale', () => {
  for (const p of Object.values(POLICIES)) {
    assert.ok(p.summary.length > 80, `${p.key} should explain why`);
    assert.ok(p.label);
  }
});

test('ranking is stable when inputs are out of order', () => {
  const jobs = [
    job('c', 'acme', 3, 0, 9_000),
    job('a', 'acme', 1, 0, 9_000),
    job('b', 'acme', 2, 0, 9_000),
  ];
  const ranked = rankDownloadQueue(jobs, { policy: 'expiring-first', now: NOW });
  assert.deepEqual(ranked.map((j) => j.id), ['a', 'b', 'c']);
});
