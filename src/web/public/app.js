const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

const state = {
  view: 'tenant',
  tenant: localStorage.getItem('ec.tenant') || 'acme',
  tenantState: null,
  operator: null,
  source: null,
  audit: null,
  ledgerReport: null,
  errors: 0,
};

// ---- helpers --------------------------------------------------------------

function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k === 'dataset') Object.assign(el.dataset, v);
    else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
    else if (k === 'html') el.innerHTML = v;
    else el.setAttribute(k, v === true ? '' : v);
  }
  for (const child of children.flat()) {
    if (child == null || child === false) continue;
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return el;
}

function pill(state) {
  return h('span', { class: 'pill', dataset: { s: state } }, state);
}

function fmtAge(ms) {
  if (ms == null) return '–';
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

function fmtTime(ms) {
  if (!ms) return '–';
  return new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function fmtBytes(n) {
  if (n == null) return '–';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

function toast(msg, kind = 'ok') {
  const el = $('#toast');
  el.textContent = msg;
  el.className = `toast show${kind === 'error' ? ' error' : ''}`;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { el.className = 'toast'; }, 4200);
}

async function api(path, { method = 'GET', body, tenant } = {}) {
  const headers = {};
  const t = tenant ?? (path.startsWith('/api/state') || path.startsWith('/api/jobs') ? state.tenant : null);
  if (t) headers['x-tenant'] = t;
  if (body !== undefined) headers['content-type'] = 'application/json';
  const res = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  let data = null;
  const text = await res.text();
  if (text) { try { data = JSON.parse(text); } catch { data = { error: text }; } }
  if (!res.ok) throw new Error(data?.error ?? `HTTP ${res.status}`);
  return data;
}

// ---- refresh loop ---------------------------------------------------------

async function refresh() {
  try {
    if (state.view === 'tenant') {
      state.tenantState = await api('/api/state', { tenant: state.tenant });
      renderTenant();
    } else if (state.view === 'operator') {
      state.operator = await api('/api/operator/overview');
      renderOperator();
    } else if (state.view === 'source') {
      state.source = await api('/api/source/diagnostics');
      renderSource();
    } else if (state.view === 'audit') {
      state.audit = await api('/api/audit?limit=120');
      renderAudit();
    }
    state.errors = 0;
    $('#live-dot').classList.remove('stale');
  } catch (err) {
    state.errors += 1;
    $('#live-dot').classList.add('stale');
    if (state.errors === 1) toast(err.message, 'error');
  }
  $('#clock').textContent = new Date().toLocaleTimeString();
}

// ---- tenant view ----------------------------------------------------------

function renderTenant() {
  const s = state.tenantState;
  if (!s) return;
  const q = s.quota;
  const pct = (n) => `${(n / Math.max(1, q.sourceLimit)) * 100}%`;

  $('#quota-card').replaceChildren(
    h('div', { class: 'card-head' }, h('h2', {}, 'Daily export allowance'), h('span', { class: 'badge' }, q.day)),
    h('dl', { class: 'kv' },
      h('dt', {}, 'Source limit'), h('dd', {}, q.sourceLimit),
      h('dt', {}, 'Charged at source'), h('dd', {}, q.sourceUsed),
      h('dt', {}, 'Held for unverified creates'), h('dd', {}, q.held),
      h('dt', { class: 'strong' }, 'Available locally'), h('dd', { class: 'strong' }, q.localRemaining),
    ),
    h('div', { class: 'gauge' },
      h('div', { class: 'gauge-track', title: `${q.charged} charged, ${q.held} held, ${q.localRemaining} free` },
        h('i', { class: 'charged', style: `width:${pct(q.charged)}` }),
        h('i', { class: 'held', style: `width:${pct(q.held)}` }),
        h('i', { class: 'free', style: `width:${pct(q.localRemaining)}` }),
      ),
      h('div', { class: 'legend' },
        h('span', { class: 'l-charged' }, `${q.charged} charged`),
        h('span', { class: 'l-held' }, `${q.held} held`),
        h('span', { class: 'l-free' }, `${q.localRemaining} free`),
      ),
    ),
    q.discrepancy ? h('p', { class: 'hint', style: 'color:var(--bad)' },
      `Source usage differs from our ledger by ${q.discrepancy.amount}. The operator view can reconcile it.`) : null,
    h('p', { class: 'hint' }, `Allowance resets ${new Date(q.resetsAt).toLocaleString()}. Creating is the only thing that consumes it; checking and downloading do not.`),
  );

  const pol = s.policy;
  $('#policy-card').replaceChildren(
    h('div', { class: 'card-head' }, h('h2', {}, 'How this queue is ordered'), h('span', { class: 'badge' }, pol.key)),
    h('p', { style: 'margin:0 0 8px; font-weight:600' }, pol.label),
    h('p', { class: 'hint', style: 'margin:0' }, pol.summary),
    h('p', { class: 'hint' }, 'A queued job has no result yet, so it cannot expire. A created job has already spent quota — that is why ready results are downloaded by nearest expiry, not queue order.'),
  );

  const tbody = $('#jobs-table tbody');
  tbody.replaceChildren(...s.jobs.slice().reverse().map(jobRow));
  $('#jobs-empty').classList.toggle('hidden', s.jobs.length > 0);
  $('#jobs-count').textContent = `${s.jobs.length} job${s.jobs.length === 1 ? '' : 's'}`;

  renderQueueList($('#create-queue'), s.queue.create, 'queued', true);
  $('#create-queue-empty').classList.toggle('hidden', s.queue.create.length > 0);
  $('#create-queue-mode').textContent = s.policy.createOrder === 'fifo-fair' ? 'FIFO + fairness' : 'strict FIFO';

  renderQueueList($('#download-queue'), s.queue.download, 'ready', false);
  $('#download-queue-empty').classList.toggle('hidden', s.queue.download.length > 0);
}

function renderQueueList(root, items, kind, isCreate) {
  root.replaceChildren(...items.map((j) => h('li', { class: j.starving ? 'starving' : '' },
    h('div', { class: 'top' },
      h('strong', {}, j.name),
      h('span', { class: 'mono' }, isCreate ? `waiting ${fmtAge(j.ageMs)}` : `${j.secondsLeft}s left`),
    ),
    h('div', { class: 'why' }, j.reason),
  )));
}

function jobRow(j) {
  const quotaText = j.reservation
    ? ({
        pending: 'held',
        committed: 'charged',
        uncertain: 'held (unverified)',
        released: 'released',
      }[j.reservation.state] ?? j.reservation.state)
    : '—';

  const actions = [];
  if (['queued', 'creating', 'running', 'ready', 'downloaded'].includes(j.state)) {
    actions.push(h('button', { class: 'small', onclick: () => cancelJob(j) }, 'Cancel'));
  }
  if (j.state === 'uncertain') {
    actions.push(h('button', { class: 'small primary', onclick: () => reconcileJob(j, false) }, 'Reconcile'));
  }
  if (j.state === 'failed') {
    actions.push(h('button', { class: 'small', onclick: () => recoverJob(j) }, 'Recover'));
  }
  if (j.state === 'downloaded') {
    actions.push(h('button', { class: 'small primary', onclick: () => ingestJob(j) }, 'Confirm ingest'));
  }
  if (j.artifact?.available) {
    actions.push(h('button', { class: 'small ghost', onclick: () => downloadJob(j) }, 'Download'));
  }
  actions.push(h('button', { class: 'small ghost', onclick: () => openJob(j.id) }, 'Details'));

  return h('tr', {},
    h('td', {},
      h('div', { class: 'title' }, j.name),
      h('div', { class: 'sub mono' }, `${j.id.slice(-10)}${j.remoteId ? ` · ${j.remoteId}` : ''}`),
    ),
    h('td', {}, pill(j.state)),
    h('td', { class: 'mono' }, `#${j.queueSeq}`),
    h('td', {}, expiryCell(j)),
    h('td', { class: 'sub' }, quotaText),
    h('td', {}, resultCell(j)),
    h('td', {}, h('div', { class: 'actions' }, actions)),
  );
}

function expiryCell(j) {
  if (j.expiresAt == null) return h('span', { class: 'muted' }, '—');
  if (['ingested', 'cancelled', 'failed'].includes(j.state)) return h('span', { class: 'muted' }, '—');
  const left = j.secondsLeft ?? 0;
  if (left <= 0 || j.state === 'expired') return h('span', { class: 'countdown critical' }, 'expired');
  const cls = left <= 20 ? 'critical' : left <= 60 ? 'warn' : '';
  return h('div', {},
    h('span', { class: `countdown ${cls}` }, fmtAge(left * 1000)),
    h('div', { class: 'bar' + (cls ? ` ${cls}` : '') },
      h('i', { style: `width:${Math.min(100, (left / 60) * 100)}%` })),
    h('div', { class: 'sub' }, `at ${fmtTime(j.expiresAt)}`),
  );
}

function resultCell(j) {
  if (j.state === 'ingested') return h('span', { class: 'sub' }, `ingested ${fmtTime(j.ingestedAt)}`);
  if (j.state === 'downloaded') return h('span', { class: 'sub' }, `${fmtBytes(j.artifact?.bytes)} · awaiting confirmation`);
  if (j.state === 'expired') return h('span', { class: 'sub', style: 'color:var(--bad)' }, 'result lost to expiry; quota was spent');
  if (j.state === 'failed') return h('span', { class: 'sub', style: 'color:var(--bad)' }, j.lastError ?? 'failed');
  if (j.state === 'uncertain') return h('span', { class: 'sub', style: 'color:var(--bad)' }, j.lastError ?? 'create outcome unknown');
  if (j.state === 'ready') return h('span', { class: 'sub' }, 'result ready at source');
  if (j.lastError) return h('span', { class: 'sub' }, j.lastError);
  return h('span', { class: 'muted' }, '—');
}

// ---- actions --------------------------------------------------------------

let submitting = false;
async function submitJob(ev) {
  ev.preventDefault();
  if (submitting) return;
  const name = $('#job-name').value.trim();
  if (!name) return;
  submitting = true;
  $('#submit-btn').disabled = true;
  try {
    const clientKey = crypto.randomUUID();
    const { job } = await api('/api/jobs', {
      method: 'POST', tenant: state.tenant,
      body: { name, clientKey },
    });
    toast(`Queued “${job.name}” — position ${job.queueSeq}`);
    $('#job-name').value = '';
    await refresh();
  } catch (err) {
    toast(err.message, 'error');
  } finally {
    submitting = false;
    $('#submit-btn').disabled = false;
  }
}

async function cancelJob(j) {
  const charged = ['running', 'ready', 'downloaded'].includes(j.state);
  const msg = charged
    ? `“${j.name}” was already created at the source, so its quota is spent and will NOT be refunded. Cancel local work anyway?`
    : j.state === 'creating'
      ? `The create request for “${j.name}” is already in flight. Cancelling cannot guarantee the source did not charge it. Continue?`
      : `Cancel “${j.name}”? Nothing has been created yet, so no quota is spent.`;
  if (!confirm(msg)) return;
  try {
    const body = j.state === 'creating'
      ? { reason: 'operator cancelled while create in flight; no refund assumed' }
      : {};
    await api(`/api/jobs/${encodeURIComponent(j.id)}/cancel`, { method: 'POST', tenant: state.tenant, body });
    toast('Cancel recorded');
    await refresh();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function reconcileJob(j, operator) {
  try {
    const r = await api(operator
      ? `/api/operator/jobs/${encodeURIComponent(j.id)}/reconcile`
      : `/api/jobs/${encodeURIComponent(j.id)}/reconcile`, { method: 'POST', body: {}, tenant: j.tenant });
    toast(`Reconciliation: ${r.result.replaceAll('_', ' ')}`);
    await refresh();
  } catch (err) {
    toast(err.message, 'error');
  }
}

// The artifact route is tenant-scoped, so a plain <a download> would be
// rejected for lack of an X-Tenant header. Fetch with the header and hand the
// browser a blob URL instead.
async function downloadJob(j) {
  try {
    const res = await fetch(`/api/jobs/${encodeURIComponent(j.id)}/download`, {
      headers: { 'x-tenant': state.tenant },
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.error ?? `HTTP ${res.status}`);
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `export-${j.id.slice(-12)}.csv`;
    document.body.append(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 10_000);
    toast(`Downloaded ${j.name}`);
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function recoverJob(j) {
  try {
    const r = await api(`/api/operator/jobs/${encodeURIComponent(j.id)}/recover`, { method: 'POST', body: {} });
    toast(`Recovered: ${r.job.state}`);
    await refresh();
  } catch (err) {
    toast(`Recovery refused: ${err.message}`, 'error');
  }
}

async function ingestJob(j) {  if (!confirm(`Confirm “${j.name}” was ingested downstream? The local file is re-verified against its recorded hash first.`)) return;
  try {
    await api(`/api/jobs/${encodeURIComponent(j.id)}/ingest`, { method: 'POST', tenant: state.tenant, body: {} });
    toast('Ingest confirmed');
    await refresh();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function resolveManually(j) {
  const dialog = $('#resolve-dialog');
  $('#resolve-context').textContent =
    `Job “${j.name}” (${j.tenant}) · ${j.state} · ${j.lastError ?? 'no detail'}`;
  $('#resolve-remote').value = j.remoteId ?? '';
  $('#resolve-note').value = '';
  $('#resolve-decision').value = 'committed';
  dialog.showModal();
  const confirmBtn = $('#resolve-confirm');
  const handler = async () => {
    confirmBtn.onclick = null;
    const note = $('#resolve-note').value.trim();
    if (!note) { toast('A note is required', 'error'); return; }
    try {
      await api(`/api/operator/jobs/${encodeURIComponent(j.id)}/resolve`, {
        method: 'POST',
        body: {
          decision: $('#resolve-decision').value,
          remoteId: $('#resolve-remote').value.trim() || null,
          note,
        },
      });
      dialog.close();
      toast('Manual decision recorded');
      await refresh();
    } catch (err) {
      toast(err.message, 'error');
    }
  };
  confirmBtn.onclick = handler;
}

async function openJob(id) {
  try {
    const { job, audit } = await api(`/api/operator/jobs/${encodeURIComponent(id)}`);
    $('#job-dialog-title').textContent = job.name;
    $('#job-dialog-body').replaceChildren(
      h('dl', { class: 'kv' },
        ...Object.entries({
          State: job.state,
          Job: job.id,
          Tenant: job.tenant,
          'Remote id': job.remoteId ?? '—',
          'Queue seq': job.queueSeq,
          'Create attempts': job.attempts,
          'Download attempts': job.downloadAttempts,
          'Queued at': fmtTime(job.queuedAt),
          'Ready at': fmtTime(job.readyAt),
          'Expires at': job.expiresAt ? new Date(job.expiresAt).toLocaleString() : '—',
          Artifact: job.artifact ? `${fmtBytes(job.artifact.bytes)} · ${job.artifact.sha256.slice(0, 16)}…` : '—',
          Reservation: job.reservation ? `${job.reservation.state}${job.reservation.note ? ` — ${job.reservation.note}` : ''}` : '—',
          'Last error': job.lastError ?? '—',
        }).flatMap(([k, v]) => [h('dt', {}, k), h('dd', {}, String(v))]),
      ),
      h('h3', {}, 'Job audit'),
      h('div', { class: 'table-wrap' }, h('table', {},
        h('thead', {}, h('tr', {}, ['When', 'Event', 'Actor', 'Detail'].map((x) => h('th', {}, x)))),
        h('tbody', {}, ...audit.map((e) => h('tr', {},
          h('td', { class: 'mono' }, fmtTime(e.at)),
          h('td', { class: 'event-kind' }, e.kind),
          h('td', {}, e.actor),
          h('td', { class: 'audit-detail' }, JSON.stringify(e.detail)),
        ))),
      )),
    );
    $('#job-dialog').showModal();
  } catch (err) {
    toast(err.message, 'error');
  }
}

// ---- operator view --------------------------------------------------------

function renderOperator() {
  const o = state.operator;
  if (!o) return;
  const q = o.quota;
  const pct = (n) => `${(n / Math.max(1, q.sourceLimit)) * 100}%`;

  $('#operator-quota').replaceChildren(
    h('div', { class: 'card-head' }, h('h2', {}, 'Cross-tenant allowance'), h('span', { class: 'badge' }, q.day)),
    h('dl', { class: 'kv' },
      h('dt', {}, 'Source limit / used'), h('dd', {}, `${q.sourceLimit} / ${q.sourceUsed}`),
      h('dt', {}, 'Ledger charged'), h('dd', {}, q.charged),
      h('dt', {}, 'Held (unverified)'), h('dd', {}, q.held),
      h('dt', { class: 'strong' }, 'Local remaining'), h('dd', { class: 'strong' }, q.localRemaining),
    ),
    h('div', { class: 'gauge' },
      h('div', { class: 'gauge-track' },
        h('i', { class: 'charged', style: `width:${pct(q.charged)}` }),
        h('i', { class: 'held', style: `width:${pct(q.held)}` }),
        h('i', { class: 'free', style: `width:${pct(q.localRemaining)}` }))),
    q.discrepancy
      ? h('p', { class: 'hint', style: 'color:var(--bad)' }, `Discrepancy of ${q.discrepancy.amount}: ${q.discrepancy.explanation}`)
      : h('p', { class: 'hint', style: 'color:var(--good)' }, 'Ledger charged matches source usage.'),
  );

  $('#operator-policy').replaceChildren(
    h('div', { class: 'card-head' }, h('h2', {}, 'Scheduling policy'), h('span', { class: 'badge' }, o.policy)),
    h('p', { style: 'margin:0 0 10px; font-weight:600' }, o.policyInfo.label),
    h('p', { class: 'hint', style: 'margin:0 0 12px' }, o.policyInfo.summary),
    h('div', { class: 'inline' },
      ...['expiring-first', 'fifo'].map((key) => h('button', {
        class: o.policy === key ? 'primary' : '',
        onclick: async () => {
          try {
            await api('/api/operator/policy', { method: 'POST', body: { policy: key } });
            toast(`Policy set to ${key}`);
            await refresh();
          } catch (err) { toast(err.message, 'error'); }
        },
      }, key === 'expiring-first' ? 'Expiring results first' : 'Strict queue order')),
    ),
  );

  // Attention rows arrive with the overview payload, so this view has no
  // per-tenant fan-out and stays consistent across a refresh.
  const rows = o.attention ?? [];
  $('#attention-empty').classList.toggle('hidden', rows.length > 0);
  $('#attention-table').classList.toggle('hidden', rows.length === 0);
  $('#attention-table tbody').replaceChildren(...rows.map((j) => h('tr', {},
    h('td', {}, h('div', { class: 'title' }, j.name), h('div', { class: 'sub mono' }, j.id.slice(-10))),
    h('td', {}, j.tenant),
    h('td', {}, pill(j.state)),
    h('td', { class: 'sub' }, j.lastError ?? (j.reservation ? `${j.reservation.state}: ${j.reservation.note ?? ''}` : '—')),
    h('td', {}, h('div', { class: 'actions' },
      j.state === 'uncertain'
        ? [h('button', { class: 'small primary', onclick: () => reconcileJob(j, true) }, 'Probe source'),
           h('button', { class: 'small', onclick: () => resolveManually(j) }, 'Decide manually')]
        : [h('button', { class: 'small primary', onclick: () => recoverJob(j) }, 'Recover'),
           h('button', { class: 'small', onclick: () => resolveManually(j) }, 'Decide manually')],
    )),
  )));

  $('#tenants-table tbody').replaceChildren(...o.tenants.map((t) => h('tr', {},
    h('td', {}, h('strong', {}, t.tenant)),
    h('td', {}, t.counts.queued ?? 0),
    h('td', {}, t.counts.ready ?? 0),
    h('td', {}, t.counts.uncertain ?? 0),
    h('td', {}, t.counts.ingested ?? 0),
    h('td', {}, t.counts.expired ?? 0),
    h('td', {}, t.jobs),
  )));

  renderQueueList($('#operator-create-queue'), o.queue.create, 'queued', true);
  $('#operator-create-empty').classList.toggle('hidden', o.queue.create.length > 0);
  renderQueueList($('#operator-download-queue'), o.queue.download, 'ready', false);
  $('#operator-download-empty').classList.toggle('hidden', o.queue.download.length > 0);

  renderLedgerReport();
}

function renderLedgerReport() {
  const r = state.ledgerReport;
  const root = $('#ledger-report');
  if (!r) {
    root.replaceChildren(h('p', { class: 'hint' }, 'No ledger check run yet. It costs no quota and never creates jobs.'));
    return;
  }
  const lines = [];
  lines.push(h('div', { class: `line ${r.consistency.ok ? 'good' : 'bad'}` },
    r.consistency.ok
      ? `Consistent: source charged ${r.quota.sourceUsed}, ledger recorded ${r.quota.charged}.`
      : `Inconsistent by ${r.consistency.amount}: ${r.consistency.explanation}.`));
  if (r.unreconciled > 0) {
    lines.push(h('div', { class: 'line warn' },
      `${r.unreconciled} reservation${r.unreconciled === 1 ? '' : 's'} still uncertain. Quota stays held until each is reconciled; probes cost no allowance.`));
  }
  if (r.sourceJobsWithoutLocalCharge.length > 0) {
    lines.push(h('div', { class: 'line bad' },
      `${r.sourceJobsWithoutLocalCharge.length} job(s) exist at the source that no local job claims — likely accepted-but-lost creates. Adopt or release them explicitly.`));
    for (const orphan of r.sourceJobsWithoutLocalCharge) {
      lines.push(h('div', { class: 'line' },
        h('span', { class: 'mono' }, orphan.remoteId), ` · ${orphan.tenant} · ${orphan.name} · created ${fmtTime(orphan.createdAt)} · ${orphan.status}`));
    }
  }
  if (r.uncertainReservations.length > 0) {
    for (const u of r.uncertainReservations) {
      lines.push(h('div', { class: 'line' },
        h('span', { class: 'mono' }, u.jobId.slice(-10)), ` · ${u.tenant} · ${u.jobName ?? '—'} · `,
        u.probeFound ? `probe sees ${u.probeFound.remoteId} (${u.probeFound.status})` : 'probe found no match yet',
        u.retryAt ? ` · retry ${fmtTime(u.retryAt)}` : ''));
    }
  }
  lines.push(h('p', { class: 'hint' }, `Checked ${fmtTime(r.checkedAt)}.`));
  root.replaceChildren(...lines);
}

async function auditLedger() {
  const btn = $('#audit-ledger-btn');
  btn.disabled = true;
  try {
    state.ledgerReport = await api('/api/operator/ledger');
    renderLedgerReport();
    toast('Ledger check complete');
  } catch (err) {
    toast(err.message, 'error');
  } finally {
    btn.disabled = false;
  }
}

// ---- source view ----------------------------------------------------------

const FAULTS = [
  ['lostCreateResponseRate', 'Lost create response', 'Accepted and charged, but the console never hears back.'],
  ['droppedBeforeChargeRate', 'Dropped before charging', 'Network failure before the source charges anything.'],
  ['lostDownloadRate', 'Lost download', 'Download fails mid-transfer; the result stays alive until expiry.'],
  ['limitAdjustment', 'Daily limit shift', 'Add or remove allowance for today (e.g. the source changed its plan).'],
  ['createDelayMs', 'Create latency (ms)', 'Delay before the create reply, to force client timeouts.'],
  ['probeLagMs', 'Probe index lag (ms)', 'Recently created jobs are invisible to reconciliation probes.'],
];

function renderSource() {
  const s = state.source;
  if (!s) return;
  const q = s.quota;
  $('#source-quota').replaceChildren(
    h('div', { class: 'card-head' }, h('h2', {}, 'Source truth'), h('span', { class: 'badge' }, q.day)),
    h('dl', { class: 'kv' },
      h('dt', {}, 'Limit today'), h('dd', {}, q.limit),
      h('dt', {}, 'Used today'), h('dd', {}, q.used),
      h('dt', { class: 'strong' }, 'Remaining'), h('dd', { class: 'strong' }, q.remaining),
      h('dt', {}, 'Resets'), h('dd', {}, new Date(q.resetsAt).toLocaleString()),
    ),
    h('p', { class: 'hint' }, 'This is the simulator’s own counter. The console must not assume it matches its ledger.'),
  );

  const events = s.events.slice(-6).reverse();
  const eventList = h('ul', { class: 'explain-list' },
    ...events.map((e) => {
      const rest = Object.fromEntries(Object.entries(e).filter(([k]) => !['type', 'at'].includes(k)));
      return h('li', { style: 'padding-left:0' },
        h('div', { class: 'top' },
          h('strong', { class: 'mono' }, e.type),
          h('span', { class: 'mono' }, fmtTime(e.at))),
        h('div', { class: 'why' }, JSON.stringify(rest)));
    }));
  $('#source-faults').replaceChildren(
    h('div', { class: 'card-head' }, h('h2', {}, 'Recent source events')),
    eventList,
  );

  const form = $('#fault-form');
  if (!form.dataset.built) {
    form.dataset.built = '1';
    form.replaceChildren(...FAULTS.map(([key, label, hint]) => {
      const isRange = key.endsWith('Rate');
      const input = isRange
        ? h('input', { type: 'range', min: 0, max: 1, step: 0.05, id: `f-${key}`, name: key })
        : h('input', {
          type: 'number',
          min: key === 'limitAdjustment' ? -999 : 0,
          step: key === 'limitAdjustment' ? 1 : 100,
          id: `f-${key}`, name: key,
        });
      return h('label', { class: 'field' },
        h('span', {}, label),
        input,
        h('small', { class: 'hint', id: `v-${key}` }, hint));
    }),
      h('div', { style: 'align-self:end' },
        h('button', { type: 'submit', class: 'primary' }, 'Apply fault settings')),
    );
    form.addEventListener('submit', applyFaults);
    for (const [key] of FAULTS) {
      const input = $(`#f-${key}`);
      input.addEventListener('input', () => updateFaultLabel(key, input.value));
    }
  }
  for (const [key] of FAULTS) {
    const input = $(`#f-${key}`);
    if (document.activeElement !== input) input.value = s.faults[key];
    updateFaultLabel(key, s.faults[key]);
  }

  $('#source-jobs tbody').replaceChildren(...s.jobs.slice().reverse().map((j) => h('tr', {},
    h('td', { class: 'mono' }, j.remoteId),
    h('td', {}, j.tenant),
    h('td', {}, j.name),
    h('td', {}, pill(j.status)),
    h('td', {}, fmtTime(j.createdAt)),
    h('td', {}, fmtTime(j.expiresAt)),
  )));
}

function updateFaultLabel(key, value) {
  const meta = FAULTS.find(([k]) => k === key);
  if (!meta) return;
  const suffix = key.endsWith('Rate')
    ? ` — ${Math.round(Number(value) * 100)}% of requests`
    : key.endsWith('Ms') ? ' ms' : ' jobs';
  $(`#v-${key}`).textContent = meta[2] + (key === 'limitAdjustment' ? ` (currently ${value >= 0 ? '+' : ''}${value})` : suffix);
}

async function applyFaults(ev) {
  ev.preventDefault();
  const body = {};
  for (const [key] of FAULTS) body[key] = Number($(`#f-${key}`).value);
  try {
    await api('/api/source/faults', { method: 'POST', body });
    toast('Fault settings applied');
    await refresh();
  } catch (err) {
    toast(err.message, 'error');
  }
}

// ---- audit view -----------------------------------------------------------

function renderAudit() {
  const a = state.audit;
  if (!a) return;
  const chain = a.chain;
  const badge = $('#chain-status');
  badge.textContent = chain.ok ? `chain verified · ${chain.checked} events` : `CHAIN BROKEN at #${chain.brokenAt}`;
  badge.style.background = chain.ok ? 'var(--good-bg)' : 'var(--bad-bg)';
  badge.style.color = chain.ok ? 'var(--good)' : 'var(--bad)';

  $('#audit-table tbody').replaceChildren(...a.events.map((e) => h('tr', {},
    h('td', { class: 'mono' }, fmtTime(e.at)),
    h('td', { class: 'event-kind' }, e.kind),
    h('td', {}, e.actor),
    h('td', {}, e.tenant ?? '—'),
    h('td', { class: 'mono' }, e.jobId ? e.jobId.slice(-10) : '—'),
    h('td', { class: 'audit-detail' }, JSON.stringify(e.detail)),
  )));
}

// ---- wiring ---------------------------------------------------------------

function switchView(view) {
  state.view = view;
  $$('.tab').forEach((t) => t.classList.toggle('active', t.dataset.view === view));
  $$('.view').forEach((v) => v.classList.toggle('active', v.id === `view-${view}`));
  refresh();
}

async function boot() {
  const select = $('#tenant-select');
  for (const t of ['acme', 'globex', 'initech']) {
    select.append(h('option', { value: t, selected: t === state.tenant }, t));
  }
  select.value = state.tenant;
  select.addEventListener('change', () => {
    state.tenant = select.value;
    localStorage.setItem('ec.tenant', state.tenant);
    refresh();
  });
  $('#submit-form').addEventListener('submit', submitJob);
  $$('.tab').forEach((t) => t.addEventListener('click', () => switchView(t.dataset.view)));
  $('#audit-ledger-btn').addEventListener('click', auditLedger);
  $('#job-dialog-close').addEventListener('click', () => $('#job-dialog').close());
  $('#resolve-dialog').addEventListener('close', () => { $('#resolve-confirm').onclick = null; });

  await refresh();
  setInterval(refresh, 1500);
}

boot().catch((err) => toast(err.message, 'error'));
