package web

// dashboardJS is the whole application.
//
// Not named ui_js.go, and that is not a style preference: `js` is a GOOS, so
// any file called *_js.go carries an implicit build constraint and is
// excluded from every non-WebAssembly build. The symptom is
// `undefined: dashboardJS` against a file that is plainly sitting there.
// lemmego/observe lost an afternoon to exactly this.
//
// Plain ES5-ish DOM work on purpose: no framework, no build step, no npm, so
// the dashboard is a string the server prints. The structure mirrors
// observe's — a panel table, one render function per panel, one fetch
// helper — because the two are read by the same person doing the same job.
const dashboardJS = `
'use strict';

/* ---- panels ------------------------------------------------------------
 *
 * One table drives the sidebar, the title and which render function runs,
 * so adding a panel is one entry rather than four edits in four places.
 *
 * The state panels carry the state they filter on. Everything a queue can
 * hold is here and in the order work actually moves through it — pending,
 * scheduled, available, running, then the three endings — rather than
 * alphabetically, because the sidebar doubles as the explanation of what
 * the states are.
 */
var PANELS = [
  {id:'overview', title:'Overview',  group:'',          kind:'overview'},

  {id:'failed',    title:'Failed',    group:'Jobs', state:'failed',    tone:'bad'},
  {id:'retryable', title:'Retryable', group:'Jobs', state:'retryable', tone:'warn'},
  {id:'running',   title:'Running',   group:'Jobs', state:'running',   tone:'run'},
  {id:'available', title:'Available', group:'Jobs', state:'available', tone:''},
  {id:'scheduled', title:'Scheduled', group:'Jobs', state:'scheduled', tone:''},
  {id:'pending',   title:'Pending',   group:'Jobs', state:'pending',   tone:''},
  {id:'completed', title:'Completed', group:'Jobs', state:'completed', tone:'ok'},
  {id:'cancelled', title:'Cancelled', group:'Jobs', state:'cancelled', tone:''},
  {id:'all',       title:'All jobs',  group:'Jobs', state:''},

  {id:'queues',  title:'Queues',    group:'Fleet', kind:'queues'},
  {id:'workers', title:'Workers',   group:'Fleet', kind:'workers'},
  {id:'kinds',   title:'Job types', group:'Fleet', kind:'kinds'}
];

/* How a state reads. Used for the sidebar dot, the row's left edge and the
 * pill, so a failed job looks failed in all three without three mappings. */
var TONES = {
  failed:'bad', retryable:'warn', running:'run', completed:'ok',
  available:'', scheduled:'', pending:'', cancelled:''
};

var state = {
  panel: 'overview',
  job: null,          /* a job id when the detail view is open */
  counts: {},         /* per-state totals, aggregated across queues */
  queues: [],
  kinds: [],
  live: true,
  seen: {},           /* job ids already rendered, so new ones can flash */
  painted: false      /* a panel has rendered, so an unseen row is genuinely new */
};

/* ---- helpers ----------------------------------------------------------- */

function el(id){ return document.getElementById(id); }

function esc(s){ return String(s === null || s === undefined ? '' : s)
  .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

function ago(iso){
  var then = Date.parse(iso);
  if (isNaN(then)) return '';
  /* Go marshals a zero time.Time as 0001-01-01T00:00:00Z, which parses
   * perfectly well and renders as "739921d ago" — a job scheduled two
   * thousand years before the common era. An unset timestamp is absent,
   * not ancient. No job timestamp is legitimately before 1970, so the
   * epoch is a safe floor and future times stay positive. */
  if (then < 0) return '';
  var s = (Date.now() - then)/1000;
  /* Negative is ordinary here and nowhere else in this file: a scheduled job
   * is due in the future, so the same formatter has to read forwards. */
  if (s < 0) return 'in ' + span(-s);
  if (s < 10) return 'just now';
  return span(s) + ' ago';
}
function span(s){
  if (s < 60) return Math.floor(s)+'s';
  if (s < 3600) return Math.floor(s/60)+'m';
  if (s < 86400) return Math.floor(s/3600)+'h';
  return Math.floor(s/86400)+'d';
}
function millis(v){
  if (!v) return '—';
  if (v < 1000) return v.toFixed(0)+'ms';
  return (v/1000).toFixed(2)+'s';
}
function num(n){
  if (n === null || n === undefined) return '—';
  return Number(n).toLocaleString();
}
function rate(v){
  if (!v) return '0';
  return v < 10 ? v.toFixed(1) : Math.round(v).toLocaleString();
}

/* A job's payload and output arrive base64-encoded, because Go marshals
 * []byte that way. Decoded for display and pretty-printed when it parses as
 * JSON, which it almost always is — a wall of minified JSON is the thing
 * somebody opened this view to avoid reading. */
function decode(b64){
  if (!b64) return '';
  var raw;
  try { raw = atob(b64); } catch (err) { return '(not decodable)'; }
  try { return JSON.stringify(JSON.parse(raw), null, 2); } catch (err) { return raw; }
}

function panel(){
  for (var i = 0; i < PANELS.length; i++) {
    if (PANELS[i].id === state.panel) return PANELS[i];
  }
  return PANELS[0];
}

function toast(msg, bad){
  var t = el('toast');
  t.textContent = msg;
  t.className = 'toast show' + (bad ? ' bad' : '');
  clearTimeout(toast.timer);
  toast.timer = setTimeout(function(){ t.className = 'toast'; }, 2600);
}

function get(path){
  return fetch(path, {headers:{'Accept':'application/json'}}).then(function(r){
    if (!r.ok) return r.json().then(function(b){
      throw new Error(b && b.error ? b.error : 'HTTP '+r.status);
    }, function(){ throw new Error('HTTP '+r.status); });
    return r.json();
  });
}
function post(path, body){
  return fetch(path, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: body ? JSON.stringify(body) : null
  }).then(function(r){
    if (!r.ok) return r.json().then(function(b){
      throw new Error(b && b.error ? b.error : 'HTTP '+r.status);
    }, function(){ throw new Error('HTTP '+r.status); });
    return r.status === 204 ? {} : r.json();
  });
}

/* ---- sidebar ----------------------------------------------------------- */

function paintNav(){
  var html = '', group = null;
  for (var i = 0; i < PANELS.length; i++) {
    var p = PANELS[i];
    if (p.group !== group) {
      group = p.group;
      if (group) html += '<div class="navgroup">' + esc(group) + '</div>';
    }
    var tone = p.tone === undefined ? '' : p.tone;
    var count = '';
    if (p.state) {
      var n = state.counts[p.state];
      /* Zero is shown rather than hidden. "Failed" with nothing beside it
       * reads as "not measured"; "Failed 0" is the answer somebody wanted. */
      count = '<span class="n">' + (n === undefined ? '' : num(n)) + '</span>';
    } else if (p.id === 'queues') {
      count = '<span class="n">' + state.queues.length + '</span>';
    } else if (p.id === 'kinds') {
      count = '<span class="n">' + state.kinds.length + '</span>';
    }
    html += '<div class="nav' + (p.id === state.panel ? ' on' : '') + '" data-panel="' +
      esc(p.id) + '">' +
      (p.state !== undefined && p.state ? '<span class="dot ' + tone + '"></span>' : '') +
      esc(p.title) + count + '</div>';
  }
  el('nav').innerHTML = html;
}

/* ---- panels ------------------------------------------------------------ */

function refresh(){
  var p = panel();
  el('title').textContent = p.title;
  /* The filter bar belongs to the job panels. Showing it over a workers
   * table would offer filters that do nothing, which is worse than no
   * filters. */
  /* The bar belongs to a list. Over a detail view its filters do nothing,
   * and over a workers table they would offer filters that cannot apply —
   * which is worse than offering none. */
  var filtering = p.state !== undefined && state.job === null;
  el('bar').style.display = filtering ? '' : 'none';
  measureHead();

  if (state.job !== null) return renderDetail();

  el('detail').innerHTML = '';
  if (p.kind === 'overview') return renderOverview();
  if (p.kind === 'queues')   return renderQueues();
  if (p.kind === 'workers')  return renderWorkers();
  if (p.kind === 'kinds')    return renderKinds();
  return renderJobs();
}

function renderOverview(){
  el('list').innerHTML = '<div class="empty">Loading&hellip;</div>';
  Promise.all([get('api/stats'), get('api/queues'), get('api/workers')])
    .then(function(r){
      var s = r[0], queues = r[1] || [], workers = r[2] || [];
      el('subtitle').textContent = s.status ? s.status : '';

      var paused = s.paused_masters > 0;
      var figures =
        fig('Jobs / minute', num(s.jobs_per_minute), '', '') +
        fig('Recent jobs', num(s.recent_jobs), 'last hour', '') +
        fig('Failed', num(s.failed_jobs), 'needs attention', s.failed_jobs > 0 ? 'bad' : 'ok') +
        fig('Processes', num(s.processes), workers.length === 0 ? 'none reporting' : '',
            s.processes > 0 ? 'ok' : 'bad') +
        fig('Paused', num(s.paused_masters), paused ? 'queues held' : '', paused ? 'warn' : '');

      var html = '<div class="cards">' + figures + '</div>';

      /* The one notice worth interrupting for. A queue with work and no
       * worker is the failure that looks like nothing at all: jobs pile up,
       * nothing errors, and every log line is about something else. */
      var waiting = 0;
      for (var i = 0; i < queues.length; i++) {
        var qs = queues[i].stats || {};
        waiting += (qs.available || 0) + (qs.running || 0);
      }
      if (waiting > 0 && s.processes === 0) {
        html = '<div class="note bad">' + num(waiting) + ' job' + (waiting === 1 ? '' : 's') +
          ' waiting and no worker is reporting. Start one with ' +
          '<code>lemmego run tasker:work</code>.</div>' + html;
      } else if (s.processes === 0) {
        html = '<div class="note warn">No worker is reporting. Nothing will run until one ' +
          'starts: <code>lemmego run tasker:work</code>.</div>' + html;
      }

      html += '<div class="card"><h2>Queues</h2>' + queueTable(queues, true) + '</div>';
      html += '<div class="card"><h2>Workers</h2>' + workerTable(workers) + '</div>';
      el('list').innerHTML = html;
    })
    .catch(fail);
}

function fig(k, v, u, tone){
  return '<div class="fig' + (tone ? ' ' + tone : '') + '">' +
    '<div class="k">' + esc(k) + '</div><div class="v">' + esc(v) + '</div>' +
    '<div class="u">' + esc(u || '') + '</div></div>';
}

function renderJobs(){
  var p = panel();
  var params = ['limit=100'];
  if (p.state) params.push('states=' + encodeURIComponent(p.state));
  var q = el('q').value.trim();
  if (q) params.push('search=' + encodeURIComponent(q));
  var queue = el('queue').value;
  if (queue) params.push('queues=' + encodeURIComponent(queue));
  var kind = el('kind').value;
  if (kind) params.push('kinds=' + encodeURIComponent(kind));

  el('list').innerHTML = '<div class="empty">Loading&hellip;</div>';
  get('api/jobs?' + params.join('&')).then(function(r){
    var jobs = r.data || [];
    el('subtitle').textContent = jobs.length ?
      num(jobs.length) + ' of ' + num(r.total) : '';
    el('counts').textContent = r.total ? num(r.total) + ' matching' : '';

    if (!jobs.length) {
      el('list').innerHTML = '<div class="empty">Nothing here' +
        (p.state ? ' in ' + esc(p.state) : '') + '.</div>';
      return;
    }

    var rows = '';
    for (var i = 0; i < jobs.length; i++) rows += jobRow(jobs[i]);
    state.painted = true;
    el('list').innerHTML =
      '<table><thead><tr>' +
      '<th style="width:70px">Job</th><th>Kind</th><th style="width:120px">Queue</th>' +
      '<th style="width:94px">State</th><th style="width:64px">Try</th>' +
      '<th style="width:96px">When</th><th style="width:150px"></th>' +
      '</tr></thead><tbody>' + rows + '</tbody></table>';
  }).catch(fail);
}

function jobRow(j){
  var tone = TONES[j.state] || '';
  /* Nothing flashes on the first paint. Every row is unseen then, so the
   * whole table lights up at once — which says "all of this is new" about a
   * list that is mostly hours old, and is the opposite of what the highlight
   * is for. */
  var fresh = (state.painted && !state.seen[j.id]) ? ' fresh' : '';
  state.seen[j.id] = true;

  /* Which timestamp is the interesting one depends on the state, and showing
   * the wrong one is actively misleading: "created 3d ago" on a scheduled job
   * says nothing about when it will run. */
  var when = j.created_at, label = 'created';
  if (j.state === 'scheduled' || j.state === 'pending') { when = j.scheduled_at; label = 'due'; }
  else if (j.state === 'running' && j.started_at) { when = j.started_at; label = 'started'; }
  else if (j.completed_at) { when = j.completed_at; label = 'finished'; }

  var last = (j.errors && j.errors.length) ? j.errors[j.errors.length-1] : null;
  var retryable = j.state === 'failed' || j.state === 'retryable' || j.state === 'cancelled';
  var cancellable = j.state === 'available' || j.state === 'scheduled' ||
    j.state === 'pending' || j.state === 'retryable';

  return '<tr class="row ' + tone + fresh + '" data-job="' + esc(j.id) + '">' +
    '<td class="num">#' + esc(j.id) + '</td>' +
    '<td><span class="sum">' + esc(j.kind) + '</span>' +
      (last ? '<div class="err">' + esc(last.error) + '</div>' : '') +
      (j.tags && j.tags.length ? '<div>' + tagList(j.tags) + '</div>' : '') + '</td>' +
    '<td class="dim">' + esc(j.queue) + '</td>' +
    '<td><span class="pill ' + tone + '">' + esc(j.state) + '</span></td>' +
    '<td class="attempt">' + esc(j.attempt) + '/' + esc(j.max_attempts) + '</td>' +
    '<td class="when" title="' + esc(label) + ' ' + esc(when) + '">' + esc(ago(when)) + '</td>' +
    '<td class="act">' +
      (retryable ? '<button data-retry="' + esc(j.id) + '">Retry</button> ' : '') +
      (cancellable ? '<button class="danger" data-cancel="' + esc(j.id) + '">Cancel</button>' : '') +
    '</td></tr>';
}

function tagList(tags){
  var html = '';
  for (var i = 0; i < tags.length; i++) html += '<span class="tag">' + esc(tags[i]) + '</span>';
  return html;
}

function renderDetail(){
  el('list').innerHTML = '';
  el('detail').innerHTML = '<div class="empty">Loading&hellip;</div>';
  get('api/jobs/' + encodeURIComponent(state.job)).then(function(j){
    var tone = TONES[j.state] || '';
    el('title').textContent = j.kind;
    el('subtitle').textContent = '#' + j.id;

    var html = '<button class="back" id="back">&larr; Back</button>';
    html += '<div class="card"><h2>Job</h2><div class="facts">' +
      fact('State', '<span class="pill ' + tone + '">' + esc(j.state) + '</span>') +
      fact('Queue', esc(j.queue)) +
      fact('Attempt', esc(j.attempt) + ' of ' + esc(j.max_attempts)) +
      fact('Priority', esc(j.priority)) +
      fact('Created', esc(ago(j.created_at))) +
      fact('Scheduled', esc(ago(j.scheduled_at))) +
      (j.started_at ? fact('Started', esc(ago(j.started_at))) : '') +
      (j.completed_at ? fact('Finished', esc(ago(j.completed_at))) : '') +
      (j.node_id ? fact('Node', esc(j.node_id)) : '') +
      (j.batch_id ? fact('Batch', esc(j.batch_id)) : '') +
      (j.uuid ? fact('UUID', '<span class="mono">' + esc(j.uuid) + '</span>') : '') +
      (j.tags && j.tags.length ? fact('Tags', tagList(j.tags)) : '') +
      '</div></div>';

    var payload = decode(j.payload);
    if (payload) html += '<div class="card"><h2>Payload</h2><pre>' + esc(payload) + '</pre></div>';

    var output = decode(j.output);
    if (output) html += '<div class="card"><h2>Output</h2><pre>' + esc(output) + '</pre></div>';

    if (j.errors && j.errors.length) {
      /* Newest first, and every attempt kept. The question after a failure
       * is almost always whether it failed the same way each time. */
      html += '<div class="card"><h2>Attempts</h2>';
      for (var i = j.errors.length - 1; i >= 0; i--) {
        var e = j.errors[i];
        html += '<div style="margin-bottom:12px">' +
          '<div class="sub">Attempt ' + esc(e.attempt) + ' &middot; ' + esc(ago(e.timestamp)) + '</div>' +
          '<div class="err">' + esc(e.error) + '</div>' +
          (e.stack ? '<pre class="stack">' + esc(e.stack) + '</pre>' : '') +
          '</div>';
      }
      html += '</div>';
    }
    el('detail').innerHTML = html;
  }).catch(fail);
}

function fact(k, v){
  /* An empty value means the thing never happened — an unset timestamp, no
   * node, no batch — so the row goes rather than showing a label over a
   * blank. */
  if (v === '' || v === null || v === undefined) return '';
  return '<div class="fact"><div class="k">' + esc(k) + '</div><div class="v">' + v + '</div></div>';
}

function renderQueues(){
  el('list').innerHTML = '<div class="empty">Loading&hellip;</div>';
  queues().then(function(qs){
    el('subtitle').textContent = qs.length + ' queue' + (qs.length === 1 ? '' : 's');
    el('list').innerHTML = queueTable(qs, false);
  });
}

function queueTable(queues, compact){
  if (!queues || !queues.length) {
    /* Not "there are no queues". This lists the queues the process serving
     * this page is configured for, so a process constructed without a
     * supervisor legitimately has none — and saying "no queues" there would
     * be a dashboard contradicting the job counts in its own sidebar. */
    return '<div class="empty">This process is not configured with any queues, so ' +
      'there is nothing here to pause or resume. The job counts in the sidebar come ' +
      'from the store and are complete.</div>';
  }
  var rows = '';
  for (var i = 0; i < queues.length; i++) {
    var q = queues[i], s = q.stats || {};
    var backlog = (s.available || 0) + (s.scheduled || 0);
    rows += '<tr>' +
      '<td><b>' + esc(q.name) + '</b>' + (q.paused ?
        ' <span class="pill warn">paused</span>' : '') + '</td>' +
      '<td class="r">' + num(s.available) + '</td>' +
      '<td class="r">' + num(s.running) + '</td>' +
      '<td class="r' + (s.failed > 0 ? '" style="color:var(--bad)' : '') + '">' +
        num(s.failed) + '</td>' +
      '<td class="r">' + num(s.retryable) + '</td>' +
      '<td class="r">' + num(s.scheduled) + '</td>' +
      (compact ? '' :
        '<td class="r">' + millis(s.avg_runtime_ms) + '</td>' +
        '<td class="r">' + millis(s.wait_time_ms) + '</td>' +
        '<td class="r">' + rate(s.throughput_per_min) + '</td>') +
      '<td class="act">' + (q.paused ?
        '<button data-resume="' + esc(q.name) + '">Resume</button>' :
        '<button class="danger" data-pause="' + esc(q.name) + '">Pause</button>') +
      '</td></tr>';
  }
  return '<table><thead><tr><th>Queue</th>' +
    '<th class="r">Avail</th><th class="r">Running</th><th class="r">Failed</th>' +
    '<th class="r">Retry</th><th class="r">Sched</th>' +
    (compact ? '' : '<th class="r">Avg run</th><th class="r">Wait</th><th class="r">Per min</th>') +
    '<th></th></tr></thead><tbody>' + rows + '</tbody></table>';
}

function renderWorkers(){
  el('list').innerHTML = '<div class="empty">Loading&hellip;</div>';
  get('api/workers').then(function(workers){
    workers = workers || [];
    el('subtitle').textContent = workers.length + ' process' +
      (workers.length === 1 ? '' : 'es');
    el('list').innerHTML = workerTable(workers);
  }).catch(fail);
}

function workerTable(workers){
  if (!workers || !workers.length) {
    return '<div class="empty">No worker is reporting. Nothing will run until one starts: ' +
      '<code>lemmego run tasker:work</code>.</div>';
  }
  var rows = '';
  for (var i = 0; i < workers.length; i++) {
    var n = workers[i];
    /* A heartbeat is the only thing that says a worker is alive, so a stale
     * one is shown as stale rather than as the status it last claimed. The
     * row would otherwise say "active" about a process that died an hour
     * ago, which is the single most misleading thing this table could do. */
    var since = (Date.now() - Date.parse(n.last_heartbeat))/1000;
    var stale = !(since < 90);
    rows += '<tr class="' + (stale ? 'warn' : '') + '">' +
      '<td class="mono">' + esc(n.id) + '</td>' +
      '<td>' + esc(n.host) + (n.port ? ':' + esc(n.port) : '') + '</td>' +
      '<td class="dim">' + esc((n.queues || []).join(', ')) + '</td>' +
      '<td class="r">' + num(n.workers) + '</td>' +
      '<td><span class="pill ' + (stale ? 'warn' : 'ok') + '">' +
        esc(stale ? 'stale' : (n.status || 'active')) + '</span></td>' +
      '<td class="when">' + esc(ago(n.last_heartbeat)) + '</td>' +
      '<td class="when">' + esc(ago(n.started_at)) + '</td>' +
      '<td class="faint">' + esc(n.version || '') + '</td>' +
      '</tr>';
  }
  return '<table><thead><tr><th>Node</th><th>Host</th><th>Queues</th>' +
    '<th class="r">Workers</th><th>Status</th><th>Heartbeat</th><th>Up since</th>' +
    '<th>Version</th></tr></thead><tbody>' + rows + '</tbody></table>';
}

function renderKinds(){
  el('list').innerHTML = '<div class="empty">Loading&hellip;</div>';
  get('api/metrics/jobs').then(function(kinds){
    state.kinds = kinds || [];
    el('subtitle').textContent = state.kinds.length + ' kind' +
      (state.kinds.length === 1 ? '' : 's');
    paintNav();
    if (!state.kinds.length) {
      el('list').innerHTML = '<div class="empty">No job has run yet.</div>';
      return;
    }
    var rows = '';
    for (var i = 0; i < state.kinds.length; i++) {
      var k = state.kinds[i];
      /* A failure rate, because a count without a denominator cannot be
       * compared between a kind that ran ten times and one that ran a
       * million. */
      var pct = k.total_count ? (k.failed_count / k.total_count * 100) : 0;
      rows += '<tr>' +
        '<td><b>' + esc(k.kind) + '</b></td>' +
        '<td class="r">' + num(k.total_count) + '</td>' +
        '<td class="r' + (k.failed_count > 0 ? '" style="color:var(--bad)' : '') + '">' +
          num(k.failed_count) + '</td>' +
        '<td class="r">' + (k.total_count ? pct.toFixed(1) + '%' : '—') + '</td>' +
        '<td class="r">' + millis(k.avg_runtime_ms) + '</td>' +
        '<td class="r">' + rate(k.throughput) + '</td>' +
        '</tr>';
    }
    el('list').innerHTML = '<table><thead><tr><th>Kind</th>' +
      '<th class="r">Total</th><th class="r">Failed</th><th class="r">Rate</th>' +
      '<th class="r">Avg run</th><th class="r">Per min</th>' +
      '</tr></thead><tbody>' + rows + '</tbody></table>';
  }).catch(fail);
}

function fail(err){
  var msg = err && err.message ? err.message : String(err);
  el('list').innerHTML = '<div class="note bad">' + esc(msg) + '</div>';
  el('detail').innerHTML = '';
}

/* ---- counts, for the sidebar ------------------------------------------
 *
 * One bounded listing per state, read for its total rather than its rows.
 *
 * The obvious cheaper source is api/queues, and it is the wrong one for two
 * reasons. It is driven by supervisor.Queues(), which is the queues this
 * process is *configured* for rather than the queues the store holds work
 * in — so a job sitting in a queue nobody configured here is invisible, and
 * a process constructed without a supervisor sees nothing at all. And
 * QueueStats carries five states: completed, cancelled and pending are
 * simply not in it, so three of the eight panels could never have a number.
 *
 * Eight counts is more requests than one, so they are deliberately not on
 * the live tick: COUNT_EVERY below spaces them out, because a backlog that
 * is one tick stale is no worse than a backlog, and eight counts every five
 * seconds against a jobs table is a load nobody asked for.
 */
var COUNTED = ['failed','retryable','running','available','scheduled','pending',
               'completed','cancelled'];

function counts(){
  var pending = COUNTED.map(function(st){
    /* limit=1 because only the total is wanted; the rows are discarded. */
    return get('api/jobs?limit=1&states=' + encodeURIComponent(st))
      .then(function(r){ return {state: st, total: r.total || 0}; })
      .catch(function(){ return null; });
  });
  return Promise.all(pending).then(function(results){
    var totals = {};
    for (var i = 0; i < results.length; i++) {
      /* A state whose count could not be read is left absent rather than
       * set to zero: a confident 0 beside "Failed" on a system with
       * failures is the most misleading thing this sidebar could say. */
      if (results[i]) totals[results[i].state] = results[i].total;
    }
    state.counts = totals;
    paintNav();
  });
}

/* Queues, for the panel and the filter. Kept separate from counts because
 * it answers a different question and fails differently — see above. */
function queues(){
  return get('api/queues').then(function(qs){
    state.queues = qs || [];
    paintNav();
    paintFilters();
    return state.queues;
  }).catch(function(){ return []; });
}

/* The queue and kind filters are populated from what exists rather than
 * typed, so a filter can never name something that is not there. */
function paintFilters(){
  var sel = el('queue'), chosen = sel.value, html = '<option value="">every queue</option>';
  for (var i = 0; i < state.queues.length; i++) {
    var q = state.queues[i].name;
    html += '<option value="' + esc(q) + '"' + (q === chosen ? ' selected' : '') + '>' +
      esc(q) + '</option>';
  }
  sel.innerHTML = html;
}
function paintKindFilter(){
  get('api/metrics/jobs').then(function(kinds){
    state.kinds = kinds || [];
    var sel = el('kind'), chosen = sel.value, html = '<option value="">every kind</option>';
    for (var i = 0; i < state.kinds.length; i++) {
      var k = state.kinds[i].kind;
      html += '<option value="' + esc(k) + '"' + (k === chosen ? ' selected' : '') + '>' +
        esc(k) + '</option>';
    }
    sel.innerHTML = html;
    paintNav();
  }).catch(function(){});
}

/* ---- live -------------------------------------------------------------- */
/*
 * The stream carries the global stats on a server-side tick. The dashboard
 * uses it as a signal rather than as data: a frame means something changed,
 * so the panel in front of you is re-read. That keeps one code path for
 * rendering whether or not the stream is connected, which is what makes the
 * Live button safe to turn off.
 */
var stream = null;

/* Four five-second frames, so the sidebar is at most twenty seconds stale. */
var COUNT_EVERY = 4;

function live(on){
  if (live.ticks === undefined) live.ticks = 0;
  state.live = on;
  var b = el('live');
  b.className = on ? 'on' : '';
  b.textContent = on ? 'Live' : 'Paused';
  if (!on) {
    if (stream) { stream.close(); stream = null; }
    el('status').textContent = '';
    return;
  }
  if (stream) return;
  try {
    stream = new EventSource('api/events');
  } catch (err) {
    /* No EventSource is survivable: the panels all fetch on their own. */
    el('status').textContent = '';
    return;
  }
  stream.onmessage = function(ev){
    var s = null;
    try { s = JSON.parse(ev.data); } catch (err) { return; }
    el('status').textContent = s.status ? s.status : '';
    if (state.job !== null) return;
    refresh();
    /* Eight counts, so not on every frame. */
    if (++live.ticks % COUNT_EVERY === 0) counts();
  };
  stream.onerror = function(){
    /* The browser reconnects by itself; saying "reconnecting" on every
     * redeploy would be noise. The dot going quiet is the signal. */
    el('status').textContent = '';
  };
}

/* ---- theme ------------------------------------------------------------- */

function theme(){
  var root = document.documentElement;
  var current = root.getAttribute('data-theme');
  if (!current) {
    /* No explicit choice yet, so the first press has to move away from
     * whatever the operating system is currently showing. */
    current = window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ?
      'light' : 'dark';
  }
  var next = current === 'dark' ? 'light' : 'dark';
  root.setAttribute('data-theme', next);
  try { localStorage.setItem('tasker.theme', next); } catch (err) {}
}

/* ---- actions ----------------------------------------------------------- */

function act(promise, done){
  return promise.then(function(r){
    toast(done(r));
    /* The row that was acted on has moved state, so the count in the
     * sidebar and the list are both stale. */
    refresh();
    counts();
  }).catch(function(e){ toast(e.message, true); });
}

/* ---- wiring ------------------------------------------------------------ */

el('nav').addEventListener('click', function(ev){
  var n = ev.target.closest('.nav');
  if (!n) return;
  state.panel = n.getAttribute('data-panel');
  state.job = null;
  state.painted = false;
  el('subtitle').textContent = '';
  el('counts').textContent = '';
  paintNav();
  refresh();
  if (window.location.hash !== '#' + state.panel) {
    window.history.replaceState(null, '', '#' + state.panel);
  }
});

/* One delegated listener for the list, so rows re-rendered on every tick do
 * not each need their own handlers — and so a click during a refresh cannot
 * land on a node that has already been replaced. */
el('list').addEventListener('click', function(ev){
  var t = ev.target;

  var retry = t.getAttribute && t.getAttribute('data-retry');
  if (retry) {
    ev.stopPropagation();
    return act(post('api/jobs/' + encodeURIComponent(retry) + '/retry'),
      function(){ return 'Job #' + retry + ' queued again'; });
  }
  var cancel = t.getAttribute && t.getAttribute('data-cancel');
  if (cancel) {
    ev.stopPropagation();
    return act(post('api/jobs/' + encodeURIComponent(cancel) + '/cancel'),
      function(){ return 'Job #' + cancel + ' cancelled'; });
  }
  var pause = t.getAttribute && t.getAttribute('data-pause');
  if (pause) {
    ev.stopPropagation();
    return act(post('api/queues/' + encodeURIComponent(pause) + '/pause'),
      function(){ return esc(pause) + ' paused'; });
  }
  var resume = t.getAttribute && t.getAttribute('data-resume');
  if (resume) {
    ev.stopPropagation();
    return act(post('api/queues/' + encodeURIComponent(resume) + '/resume'),
      function(){ return esc(resume) + ' resumed'; });
  }

  var row = t.closest && t.closest('tr.row');
  if (row) {
    state.job = row.getAttribute('data-job');
    refresh();
  }
});

el('detail').addEventListener('click', function(ev){
  if (ev.target.id === 'back') { state.job = null; refresh(); }
});

el('live').addEventListener('click', function(){ live(!state.live); });
el('theme').addEventListener('click', theme);
el('reload').addEventListener('click', function(){ refresh(); counts(); });

el('prune').addEventListener('click', function(){
  if (!window.confirm('Delete finished jobs past their retention? This cannot be undone.')) return;
  act(post('api/prune'), function(r){
    return 'Pruned ' + num(r && r.pruned ? r.pruned : 0) + ' job' +
      ((r && r.pruned) === 1 ? '' : 's');
  });
});

/* Typing filters the list, debounced — a request per keystroke against a
 * jobs table is a lot of database for no benefit. */
var typing;
function onFilter(){
  clearTimeout(typing);
  typing = setTimeout(function(){ if (state.job === null) refresh(); }, 220);
}
el('q').addEventListener('input', onFilter);
el('queue').addEventListener('change', onFilter);
el('kind').addEventListener('change', onFilter);

/* The filter bar sticks below the header, so it needs the header's measured
 * height rather than a guess: the controls wrap at narrow widths and a
 * hardcoded offset would leave a gap on one layout and an overlap on the
 * other. */
function measureHead(){
  var head = el('head').getBoundingClientRect().height;
  var bar = el('bar').getBoundingClientRect().height;
  var root = document.documentElement.style;
  root.setProperty('--headh', head + 'px');
  /* The column headers pin below both, so they need the pair's height and
   * not either one. */
  root.setProperty('--stickyh', (head + bar) + 'px');
}

/* A panel in the fragment, so a link to the failed jobs is a link. */
var hash = window.location.hash.replace('#', '');
for (var h = 0; h < PANELS.length; h++) {
  if (PANELS[h].id === hash) { state.panel = hash; break; }
}

paintNav();
el('title').textContent = panel().title;
measureHead();
window.addEventListener('resize', measureHead);
refresh();
counts();
queues();
paintKindFilter();
live(true);
`
