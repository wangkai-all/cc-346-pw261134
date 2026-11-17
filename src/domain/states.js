// Domain vocabulary shared across the source simulator, ledger, scheduler and UI.
//
// Reservation lifecycle (quota is charged by the source the moment a create
// request is *accepted*; a client-side timeout tells us nothing about whether
// that happened):
//
//   pending ──create accepted──────────────→ committed ──job finished──→ committed
//      │                                        │
//      ├──create rejected (definitive)──────────→ released
//      ├──create ambiguous / timeout────────────→ uncertain ──reconcile──→ committed | released
//      └──cancel before create─────────────────→ released
//
// Job lifecycle (local):
//   queued → running → ready → downloaded → ingested
//                     ↘ expired      (source deleted the result before download)
//                     ↘ failed
//   cancel is only possible while the job has not been accepted by the source
//   (queued with no remote side effect, or an uncertain create that is *not*
//   claimed to be refunded). Cancelling a created job charges-are-not-refunded.

export const RESERVATION = Object.freeze({
  PENDING: 'pending',       // reserved locally, create not attempted
  COMMITTED: 'committed',   // source definitively accepted the create
  UNCERTAIN: 'uncertain',   // create may have been accepted but we lack proof
  RELEASED: 'released',     // definitively not charged; budget returned
});

export const QUOTA_STATE = Object.freeze({
  AVAILABLE: 'available',
  HELD: 'held',             // locally reserved but source has not charged yet
  CHARGED: 'charged',       // source charged quota
  UNCERTAIN: 'uncertain',   // source may have charged; cannot reuse
  RELEASED: 'released',     // source never charged / refunded by source itself
});

export const JOB = Object.freeze({
  QUEUED: 'queued',
  CREATING: 'creating',
  UNCERTAIN: 'uncertain',   // create outcome unknown; no automatic recreate
  RUNNING: 'running',
  READY: 'ready',
  DOWNLOADED: 'downloaded',
  INGESTED: 'ingested',
  EXPIRED: 'expired',
  FAILED: 'failed',
  CANCELLED: 'cancelled',
});

export const JOB_TERMINAL = new Set([
  JOB.INGESTED, JOB.EXPIRED, JOB.FAILED, JOB.CANCELLED,
]);

export function isTerminal(jobState) {
  return JOB_TERMINAL.has(jobState);
}
