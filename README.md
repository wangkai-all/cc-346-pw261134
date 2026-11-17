# Export Collection Console

A three-tenant console for a limited export service. It ties together the four
things that go wrong when an upstream service charges a daily create allowance
and deletes results on a timer:

1. **Quota reservation** — reserve before you create; never oversubscribe.
2. **Task creation** — a timeout is ambiguous, not a failure.
3. **File download** — expiry is a hard deadline, and retries must not become a
   storm.
4. **Ingest confirmation** — a downloaded file is not ingested until you say so
   and its hash still matches.

Everything runs against a local source simulator. No real website is contacted.

```
npm start          # http://127.0.0.1:3000
npm test           # 40 tests: unit, engine, HTTP
```

Node ≥ 22.5 is required (the built-in `node:sqlite` module is used; there are no
dependencies to install).

## The hard parts, and how they are handled

### A create timeout is not a failed create

The source charges quota when it *accepts* a create. A client timeout tells you
nothing about whether that happened. So the console never treats a timeout as a
retry:

```
queued ──create accepted──▶ committed ──▶ running ──▶ ready ──▶ downloaded ──▶ ingested
  │                            │
  │                            └─ cancel: quota stays spent (never refunded locally)
  ├──source rejects (definite)──▶ released → back to queued
  └──timeout / lost reply──────▶ uncertain ──reconcile──▶ committed | released
```

* `uncertain` **holds** budget. It is not optimism, it is an unresolved charge.
* No automatic re-create ever happens from `uncertain`.
* A console restart during a create moves the job to `uncertain`
  (`job.recovered_uncertain`), never back to `queued`.

### Reconciliation proves, it does not guess

`probe()` lists the source's jobs for a tenant in a window. It costs no
allowance. The result is only allowed to release budget when **both** hold:

1. the probe index covers the whole attempt window
   (`indexedThrough >= attemptEnd`), and
2. the source has no create request still open (`inflightCreates() === 0`).

Otherwise the job stays `uncertain` with a retry time. This matters because a
slow create can still charge *after* our timeout fired; an empty probe taken
too early is not proof. The simulator exposes `probeLagMs` and `createDelayMs`
so this path can be exercised (`reconciliation stays inconclusive…` and
`an empty probe is not proof…` in the tests).

### Ledger vs. source

`GET /api/operator/ledger` compares the source's used counter with the ledger,
lists uncertain reservations, and lists **orphan source jobs** — jobs the
source created that no local record claims. Those are the accepted-but-lost
creates; they can be adopted (`committed`) or released manually, and both
decisions require a written note that lands in the audit trail.

Local available budget is `max(sourceUsed, charged + held)` against the source
limit: whichever side claims more usage wins.

### Scheduling: expiring-first, with an explanation

Two scarce resources, two different questions:

* **Who gets created next?** A queued job has no result and therefore no expiry.
  Queueing costs nothing but time. Creates are FIFO within a tenant, with
  per-tenant fairness (tenants with less in-flight work go first) and bounded
  starvation relief for jobs waiting longer than `STARVATION_MS`. A created
  result, by contrast, has *already spent quota*.
* **Who gets downloaded next?** Strictly nearest-expiry first. Spending the
  download slot on the result closest to disappearing salvages quota that is
  already gone; a queued job waits without losing anything.

Every decision carries a human-readable `reason` string shown in the UI and
stored in the audit log for create starts. The operator can switch to strict
FIFO (`POST /api/operator/policy`); the trade-off (a newer result may expire
behind an older job) is stated in the policy description and covered by tests.

### Cancel never pretends a refund

| State at cancel | Result |
|---|---|
| `queued` | Cancelled. No quota was spent. |
| `creating` | Cancel is *recorded*; the in-flight create decides. If accepted, quota stays spent. |
| `uncertain` | **Refused** — cancel would hide whether quota was charged. Reconcile first. |
| `running` / `ready` / `downloaded` | Cancelled locally. Quota stays spent; the audit records `refunded: false`. |

The `job.cancelled` event always states `charged` and `refunded` explicitly.

### Expiry and download retries

* Results that expire before download become `expired`; the audit says
  `quotaRefunded: false`.
* A failed download retries with exponential backoff (250 ms → 5 s), and the
  retry is never scheduled past `expiresAt`. Without this, a persistent
  download fault produced 124 attempts in 14 seconds in manual testing.
* Downloads are written to a temp file and renamed into place, so a partial
  transfer is never a completed artifact.

### Ingest confirmation

`POST /api/jobs/:id/ingest` re-hashes the local file and compares it with the
hash recorded at download time. A mismatch is refused
(`ingest.rejected`) and the job stays `downloaded`.

### Recovery

`POST /api/operator/jobs/:id/recover` is the explicit recovery operation for a
failed job:

* no remote id → re-queued (nothing was created, nothing charged);
* remote result alive → resumed to `running`/`ready`;
* remote result gone or expired → stays failed/expired, quota stays spent.

Recovery never creates a new source job.

## Pages

* **Tenant queue** — per-tenant jobs, a live expiry countdown, the create queue
  and download priority with their reasons, and the allowance breakdown
  (charged / held / free).
* **Operator** — cross-tenant allowance, policy switch, everything needing a
  human (uncertain, failed, expired) with *Probe source* / *Decide manually* /
  *Recover*, and a ledger-vs-source check.
* **Local source** — the simulator's own view, with sliders for lost create
  responses, pre-charge drops, lost downloads, limit shifts, create latency and
  probe index lag.
* **Audit** — the hash-chained event log with chain verification.

## API sketch

Tenant routes need `X-Tenant` (the demo's stand-in for an authenticated
session) and are scoped to that tenant; cross-tenant reads return 404.

```
GET  /api/state                       tenant view + queues
POST /api/jobs                        { name, meta?, clientKey? }
GET  /api/jobs/:id                    job + its audit
POST /api/jobs/:id/cancel
POST /api/jobs/:id/reconcile
POST /api/jobs/:id/ingest
GET  /api/jobs/:id/download           CSV + X-Artifact-SHA256

GET  /api/operator/overview
GET  /api/operator/ledger
POST /api/operator/policy             { policy: expiring-first | fifo }
GET  /api/operator/jobs/:id
POST /api/operator/jobs/:id/{reconcile,resolve,cancel,ingest,recover}
GET  /api/audit?limit=
GET  /api/source/diagnostics          (simulator only)
POST /api/source/faults               (simulator only)
```

`POST /api/jobs` accepts `clientKey`; repeated submissions with the same key
and tenant return the original job (`200`, `reused: true`) instead of charging
twice.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `PORT` | 3000 | HTTP port |
| `DATA_DIR` | `./data` | SQLite DB and downloaded artifacts |
| `SOURCE_DAILY_LIMIT` | 12 | Simulator creates per UTC day |
| `SOURCE_RUN_MS` | 4000 | Simulator processing time |
| `SOURCE_EXPIRE_MS` | 60000 | Result lifetime after readiness |
| `SOURCE_DOWNLOAD_MS` | 900 | Simulated transfer time |
| `CREATE_TIMEOUT_MS` | 3000 | Client timeout for create |
| `RECONCILE_GRACE_MS` | 5000 | Wait before automatic reconciliation |
| `DOWNLOAD_CONCURRENCY` | 1 | Parallel downloads (makes expiry contention real) |
| `SCHEDULER_POLICY` | expiring-first | Initial policy |
| `STARVATION_MS` | 30000 | Age at which a queued job gets relief |

## Layout

```
src/domain/states.js     vocabulary and lifecycle sets
src/source/simulator.js  local upstream: quota, jobs, expiry, faults
src/core/db.js           schema + transactions (node:sqlite, WAL)
src/core/ledger.js       reservations, budget snapshot
src/core/policy.js       explainable create/download ordering
src/core/engine.js       orchestration: create, reconcile, download, ingest, recover
src/core/artifacts.js    atomic writes + hash verification
src/core/audit.js        append-only hash-chained log
src/server.js            HTTP API + static files
src/web/public/          UI (vanilla ES modules, no build step)
test/                    40 tests
```

## Known simplifications

* `X-Tenant` stands in for real authentication. Tenant isolation is enforced on
  every route, but the header is caller-supplied.
* The source is in-process. Its state is persisted to JSON so a restart can be
  simulated; the console's SQLite state is also persisted.
* Allowance resets on the source's UTC day. There is no timezone configuration.
