package web

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tasker Dashboard</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,system-ui,sans-serif; background:#0f172a; color:#e2e8f0; padding:24px; }
h1 { font-size:24px; margin-bottom:24px; color:#f8fafc; display:flex; align-items:center; gap:8px; }
h1 span { background:#1e293b; color:#94a3b8; font-size:12px; padding:2px 8px; border-radius:4px; }
h2 { font-size:18px; margin-bottom:16px; color:#f1f5f9; display:flex; align-items:center; gap:12px; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:16px; margin-bottom:24px; }
.card { background:#1e293b; border-radius:12px; padding:20px; border:1px solid #334155; }
.card h3 { font-size:12px; text-transform:uppercase; color:#94a3b8; margin-bottom:8px; letter-spacing:1px; }
.card .value { font-size:32px; font-weight:700; color:#f8fafc; }
.card .value.green { color:#22c55e; }
.card .value.red { color:#ef4444; }
.card .value.yellow { color:#eab308; }
.card .value.blue { color:#3b82f6; }
.row { display:flex; gap:16px; flex-wrap:wrap; }
.col { flex:1; min-width:300px; }
.section { margin-bottom:32px; }
table { width:100%; border-collapse:collapse; }
th { text-align:left; font-size:12px; text-transform:uppercase; color:#94a3b8; padding:8px 12px; border-bottom:1px solid #334155; }
td { padding:8px 12px; border-bottom:1px solid #1e293b; font-size:14px; }
tr.clickable { cursor:pointer; }
tr.clickable:hover { background:#1e293b; }
.badge { display:inline-block; padding:2px 8px; border-radius:4px; font-size:11px; font-weight:600; }
.badge.available { background:#1e3a5f; color:#60a5fa; }
.badge.running { background:#1a3a2a; color:#4ade80; }
.badge.completed { background:#1a2a1a; color:#22c55e; }
.badge.failed { background:#3a1a1a; color:#ef4444; }
.badge.retryable { background:#3a2a1a; color:#eab308; }
.badge.cancelled { background:#3a1a2a; color:#e879f9; }
.badge.pending { background:#1e293b; color:#94a3b8; }
.badge.scheduled { background:#1e2a3a; color:#38bdf8; }
.btn { display:inline-block; padding:3px 10px; border-radius:4px; border:none; font-size:11px; font-weight:600; cursor:pointer; text-decoration:none; }
.btn-sm { font-size:10px; padding:2px 6px; }
.btn-retry { background:#1e3a5f; color:#60a5fa; }
.btn-retry:hover { background:#1e4a7f; }
.btn-cancel { background:#3a1a1a; color:#ef4444; }
.btn-cancel:hover { background:#4a1a1a; }
.btn-pause { background:#3a2a1a; color:#eab308; }
.btn-pause:hover { background:#4a3a1a; }
.btn-resume { background:#1a3a2a; color:#4ade80; }
.btn-resume:hover { background:#1a4a2a; }
.btn-view { background:#1e293b; color:#94a3b8; }
.btn-view:hover { background:#2a3a4b; }
.btn-filter { background:transparent; color:#94a3b8; border:1px solid #334155; border-radius:4px; padding:4px 10px; font-size:11px; cursor:pointer; }
.btn-filter:hover { background:#1e293b; color:#f8fafc; }
.btn-filter.active { background:#1e3a5f; color:#60a5fa; border-color:#1e3a5f; }
.filter-bar { display:flex; gap:6px; margin-bottom:12px; flex-wrap:wrap; }
.error { color:#ef4444; text-align:center; padding:40px; }
.loading { text-align:center; padding:40px; color:#64748b; }
.toast { position:fixed; bottom:24px; right:24px; background:#1e293b; border:1px solid #334155; border-radius:8px; padding:12px 20px; font-size:14px; opacity:0; transition:opacity .3s; z-index:999; }
.toast.show { opacity:1; }

/* Modal */
.modal-overlay { display:none; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.6); z-index:1000; justify-content:center; align-items:center; }
.modal-overlay.show { display:flex; }
.modal { background:#1e293b; border:1px solid #334155; border-radius:12px; max-width:700px; width:90%; max-height:80vh; overflow-y:auto; padding:24px; }
.modal h3 { font-size:18px; margin-bottom:16px; color:#f1f5f9; }
.modal-close { float:right; background:none; border:none; color:#94a3b8; font-size:24px; cursor:pointer; }
.modal-close:hover { color:#f8fafc; }
.modal .field { margin-bottom:12px; }
.modal .label { font-size:11px; text-transform:uppercase; color:#94a3b8; margin-bottom:4px; }
.modal .val { font-size:14px; color:#e2e8f0; word-break:break-all; }
.modal pre { background:#0f172a; border-radius:6px; padding:12px; font-size:12px; overflow-x:auto; color:#e2e8f0; }
</style>
</head>
<body>
<h1>&#9881; Tasker <span>background jobs</span></h1>

<div class="grid" id="stats"></div>

<div class="row section">
<div class="col">
<h2>Queues</h2>
<table id="queues"><tr><td colspan="6" class="loading">Loading...</td></tr></table>
</div>
<div class="col">
<h2>Job Metrics</h2>
<table id="jobMetrics"><tr><td colspan="4" class="loading">Loading...</td></tr></table>
</div>
</div>

<div class="section">
<h2>Workers <span id="workerCount" style="font-size:14px;color:#94a3b8;"></span></h2>
<table id="workers"><tr><td colspan="5" class="loading">Loading...</td></tr></table>
</div>

<div class="section">
<h2>Jobs
	<button class="btn btn-retry btn-sm" onclick="retryAllFailed()">Retry All Failed</button>
	<button class="btn btn-cancel btn-sm" onclick="cancelAllPending()">Cancel All Pending</button>
	<button class="btn btn-sm btn-view" onclick="pruneOld()">Prune Old</button>
	<button class="btn btn-sm btn-view" onclick="load()">Refresh</button>
</h2>
<div class="filter-bar" id="jobFilters">
	<button class="btn-filter active" data-state="" onclick="filterJobs('')">All</button>
	<button class="btn-filter" data-state="failed" onclick="filterJobs('failed')">Failed</button>
	<button class="btn-filter" data-state="retryable" onclick="filterJobs('retryable')">Retryable</button>
	<button class="btn-filter" data-state="running" onclick="filterJobs('running')">Running</button>
	<button class="btn-filter" data-state="available" onclick="filterJobs('available')">Available</button>
	<button class="btn-filter" data-state="completed" onclick="filterJobs('completed')">Completed</button>
	<button class="btn-filter" data-state="cancelled" onclick="filterJobs('cancelled')">Cancelled</button>
</div>
<table id="jobs"><tr><td colspan="7" class="loading">Loading...</td></tr></table>
</div>

<div id="toast" class="toast"></div>

<div class="modal-overlay" id="modal">
<div class="modal">
<button class="modal-close" onclick="closeModal()">&times;</button>
<h3 id="modalTitle">Job Detail</h3>
<div id="modalBody"></div>
</div>
</div>

<script>
function toast(msg) { var t=document.getElementById('toast'); t.textContent=msg; t.classList.add('show'); setTimeout(function(){t.classList.remove('show');},2500); }

async function post(url, body) {
	var r = await fetch(url, {method:'POST', headers:{'Content-Type':'application/json'}, body:body?JSON.stringify(body):undefined});
	if (!r.ok) { var e=await r.json(); throw new Error(e.error||r.statusText); }
	return r.json();
}

async function load() {
	try {
		var [stats, queues, workers, jobMetrics] = await Promise.all([
			fetch('api/stats').then(function(r){return r.json();}),
			fetch('api/queues').then(function(r){return r.json();}),
			fetch('api/workers').then(function(r){return r.json();}),
			fetch('api/metrics/jobs').then(function(r){return r.json();})
		]);
		renderStats(stats);
		renderQueues(queues);
		renderWorkers(workers);
		renderJobMetrics(jobMetrics);
		loadJobs();
	} catch(e) {
		document.getElementById('stats').innerHTML = '<div class="card error">Failed to load: ' + e.message + '</div>';
	}
}

function renderStats(s) {
	document.getElementById('stats').innerHTML =
		'<div class="card"><h3>Status</h3><div class="value '+(s.status||'running')+'">'+(s.status||'running')+'</div></div>' +
		'<div class="card"><h3>Jobs/min</h3><div class="value blue">'+(s.jobs_per_minute||0)+'</div></div>' +
		'<div class="card"><h3>Processes</h3><div class="value green">'+(s.processes||0)+'</div></div>' +
		'<div class="card"><h3>Failed</h3><div class="value red">'+(s.failed_jobs||0)+'</div></div>';
}

function renderQueues(q) {
	if (!q||!q.length) { document.getElementById('queues').innerHTML='<tr><td colspan="6" style="text-align:center;color:#64748b;padding:20px;">No queues</td></tr>'; return; }
	var html = '<tr><th>Queue</th><th>Avail</th><th>Run</th><th>Done</th><th>Fail</th><th>Actions</th></tr>';
	for (var i=0;i<q.length;i++) {
		var qd=q[i], s=qd.stats||{}, pauseBtn=qd.paused
			? '<button class="btn btn-resume btn-sm" onclick="resumeQueue(\''+qd.name+'\')">Resume</button>'
			: '<button class="btn btn-pause btn-sm" onclick="pauseQueue(\''+qd.name+'\')">Pause</button>';
		html += '<tr><td><strong>'+qd.name+'</strong>'+(qd.paused?' <span class="badge cancelled">paused</span>':'')+'</td>'+
			'<td>'+(s.available||0)+'</td><td>'+(s.running||0)+'</td><td>'+(s.completed||0)+'</td><td>'+(s.failed||0)+'</td>'+
			'<td>'+pauseBtn+'</td></tr>';
	}
	document.getElementById('queues').innerHTML = html;
}

function renderWorkers(w) {
	if (!w||!w.length) { document.getElementById('workers').innerHTML='<tr><td colspan="5" style="text-align:center;color:#64748b;padding:20px;">No workers connected</td></tr>'; return; }
	document.getElementById('workerCount').textContent = '('+w.length+' connected)';
	var html = '<tr><th>Node</th><th>Host</th><th>Queues</th><th>Workers</th><th>Status</th><th>Uptime</th></tr>';
	for (var i=0;i<w.length;i++) {
		var n=w[i];
		html += '<tr><td><strong>'+n.id+'</strong></td><td>'+n.host+(n.port?':'+n.port:'')+'</td>'+
			'<td>'+(n.queues?n.queues.join(', '):'-')+'</td><td>'+n.workers+'</td>'+
			'<td><span class="badge '+(n.status==='active'?'running':n.status)+'">'+n.status+'</span></td>'+
			'<td>'+timeAgo(n.started_at)+'</td></tr>';
	}
	document.getElementById('workers').innerHTML = html;
}

function renderJobMetrics(m) {
	if (!m||!m.length) { document.getElementById('jobMetrics').innerHTML='<tr><td colspan="4" style="text-align:center;color:#64748b;padding:20px;">No data yet</td></tr>'; return; }
	var html = '<tr><th>Job Type</th><th>Throughput</th><th>Avg Runtime</th><th>Failed/Total</th></tr>';
	for (var i=0;i<m.length;i++) {
		var md=m[i];
		html += '<tr><td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+(md.kind||'').replace('*','').replace('commands.','')+'</td>'+
			'<td>'+(md.throughput?md.throughput.toFixed(1):'0')+'/min</td>'+
			'<td>'+(md.avg_runtime_ms?md.avg_runtime_ms.toFixed(0):'0')+'ms</td>'+
			'<td>'+(md.failed||0)+'/'+(md.total||0)+'</td></tr>';
	}
	document.getElementById('jobMetrics').innerHTML = html;
}

function renderJobs(j) {
	if (!j||!j.data||!j.data.length) { document.getElementById('jobs').innerHTML='<tr><td colspan="7" style="text-align:center;color:#64748b;padding:20px;">No jobs yet</td></tr>'; return; }
	var html = '<tr><th>ID</th><th>Kind</th><th>Queue</th><th>State</th><th>Attempts</th><th>Age</th><th>Actions</th></tr>';
	for (var i=0;i<j.data.length;i++) {
		var jd=j.data[i];
		var actions = '';
		if (jd.state==='failed'||jd.state==='retryable') { actions += '<button class="btn btn-retry btn-sm" onclick="event.stopPropagation();retryJob('+jd.id+')">Retry</button> '; }
		if (jd.state==='available'||jd.state==='pending'||jd.state==='scheduled'||jd.state==='retryable') { actions += '<button class="btn btn-cancel btn-sm" onclick="event.stopPropagation();cancelJob('+jd.id+')">Cancel</button> '; }
		actions += '<button class="btn btn-view btn-sm" onclick="event.stopPropagation();viewJob('+jd.id+')">Detail</button>';
		html += '<tr class="clickable" onclick="viewJob('+jd.id+')"><td>#'+jd.id+'</td>'+
			'<td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+(jd.kind||'').replace('*','').replace('commands.','')+'</td>'+
			'<td>'+jd.queue+'</td>'+
			'<td><span class="badge '+jd.state+'">'+jd.state+'</span></td>'+
			'<td>'+jd.attempt+'/'+jd.max_attempts+'</td>'+
			'<td>'+timeAgo(jd.created_at)+'</td>'+
			'<td>'+actions+'</td></tr>';
	}
	document.getElementById('jobs').innerHTML = html;
}

function timeAgo(t) {
	var s=Math.floor((Date.now()-new Date(t).getTime())/1000);
	if (s<60) return s+'s ago';
	if (s<3600) return Math.floor(s/60)+'m ago';
	return Math.floor(s/3600)+'h ago';
}

function retryJob(id) { post('api/jobs/'+id+'/retry').then(function(){toast('Job #'+id+' retried');load();}).catch(function(e){toast('Error: '+e.message);}); }
function cancelJob(id) { post('api/jobs/'+id+'/cancel').then(function(){toast('Job #'+id+' cancelled');load();}).catch(function(e){toast('Error: '+e.message);}); }
function pauseQueue(name) { post('api/queues/'+name+'/pause').then(function(){toast('Queue '+name+' paused');load();}).catch(function(e){toast('Error: '+e.message);}); }
function resumeQueue(name) { post('api/queues/'+name+'/resume').then(function(){toast('Queue '+name+' resumed');load();}).catch(function(e){toast('Error: '+e.message);}); }

function pruneOld() { post('api/prune').then(function(r){toast('Pruned '+r.pruned+' old jobs');load();}).catch(function(e){toast('Error: '+e.message);}); }

function retryAllFailed() {
	fetch('api/jobs?limit=100&states=failed,retryable').then(function(r){return r.json();}).then(function(j){
		if(!j.data||!j.data.length){toast('No failed jobs');return;}
		var ids=j.data.map(function(jd){return jd.id;});
		post('api/jobs/batch/retry',{ids:ids}).then(function(){toast('Retried '+ids.length+' jobs');load();}).catch(function(e){toast('Error: '+e.message);});
	});
}
function cancelAllPending() {
	fetch('api/jobs?limit=100&states=available,pending,scheduled').then(function(r){return r.json();}).then(function(j){
		if(!j.data||!j.data.length){toast('No pending jobs');return;}
		var ids=j.data.map(function(jd){return jd.id;});
		post('api/jobs/batch/cancel',{ids:ids}).then(function(){toast('Cancelled '+ids.length+' jobs');load();}).catch(function(e){toast('Error: '+e.message);});
	});
}

function viewJob(id) {
	document.getElementById('modal').classList.add('show');
	document.getElementById('modalTitle').textContent = 'Job #'+id;
	document.getElementById('modalBody').innerHTML = '<div class="loading">Loading...</div>';
	fetch('api/jobs/'+id).then(function(r){return r.json();}).then(function(j){
		var html = '';
		html += '<div class="field"><div class="label">Kind</div><div class="val">'+(j.kind||'').replace('*','')+'</div></div>';
		html += '<div class="field"><div class="label">ID / UUID</div><div class="val">#'+j.id+' / '+j.uuid+'</div></div>';
		html += '<div class="field"><div class="label">Queue</div><div class="val">'+j.queue+'</div></div>';
		html += '<div class="field"><div class="label">State</div><div class="val"><span class="badge '+j.state+'">'+j.state+'</span></div></div>';
		html += '<div class="field"><div class="label">Attempts</div><div class="val">'+j.attempt+'/'+j.max_attempts+'</div></div>';
		html += '<div class="field"><div class="label">Created</div><div class="val">'+new Date(j.created_at).toLocaleString()+'</div></div>';
		if (j.started_at) { html += '<div class="field"><div class="label">Started</div><div class="val">'+new Date(j.started_at).toLocaleString()+'</div></div>'; }
		if (j.completed_at) { html += '<div class="field"><div class="label">Completed</div><div class="val">'+new Date(j.completed_at).toLocaleString()+'</div></div>'; }
		if (j.scheduled_at) { html += '<div class="field"><div class="label">Scheduled At</div><div class="val">'+new Date(j.scheduled_at).toLocaleString()+'</div></div>'; }
		if (j.node_id) { html += '<div class="field"><div class="label">Node</div><div class="val">'+j.node_id+'</div></div>'; }
		if (j.batch_id) { html += '<div class="field"><div class="label">Batch ID</div><div class="val">'+j.batch_id+'</div></div>'; }
		var payloadStr = '';
		try { payloadStr = JSON.stringify(JSON.parse(atob(j.payload)), null, 2); }
		catch(e) { try { payloadStr = JSON.stringify(JSON.parse(j.payload), null, 2); } catch(e2) { payloadStr = j.payload || '{}'; } }
		html += '<div class="field"><div class="label">Payload</div><pre>'+escapeHtml(payloadStr)+'</pre></div>';
		if (j.errors&&j.errors.length) {
			html += '<div class="field"><div class="label">Errors</div>';
			for (var e=0;e<j.errors.length;e++) {
				html += '<pre style="margin-top:8px;color:#ef4444;">#'+(e+1)+' '+escapeHtml(j.errors[e].error)+'</pre>';
			}
			html += '</div>';
		}
		if (j.output) {
			var outStr = '';
			try { outStr = atob(j.output); } catch(e) { outStr = j.output; }
			html += '<div class="field"><div class="label">Output</div><pre>'+escapeHtml(outStr)+'</pre></div>';
		}
		if (j.tags&&j.tags.length) { html += '<div class="field"><div class="label">Tags</div><div class="val">'+j.tags.join(', ')+'</div></div>'; }
		document.getElementById('modalBody').innerHTML = html;
	}).catch(function(e){
		document.getElementById('modalBody').innerHTML = '<div class="error">'+e.message+'</div>';
	});
}
function closeModal() { document.getElementById('modal').classList.remove('show'); }
document.getElementById('modal').addEventListener('click', function(e){if(e.target===this)closeModal();});

function escapeHtml(s) {
	if (!s) return '';
	return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

var currentFilter = '';

function filterJobs(state) {
	currentFilter = state;
	document.querySelectorAll('#jobFilters .btn-filter').forEach(function(b){
		b.classList.toggle('active', b.getAttribute('data-state') === state);
	});
	loadJobs();
}

async function loadJobs() {
	try {
		var url = 'api/jobs?limit=15';
		if (currentFilter) url += '&states=' + currentFilter;
		var jobsResp = await (await fetch(url)).json();
		renderJobs(jobsResp);
	} catch(e) {
		document.getElementById('jobs').innerHTML = '<tr><td colspan="7" class="error">Failed to load: ' + e.message + '</td></tr>';
	}
}

// SSE for real-time updates
if (typeof EventSource !== 'undefined') {
	var es = new EventSource('api/events');
	es.onmessage = function(e) {
		try { var data = JSON.parse(e.data); renderStats(data); } catch(ex) {}
	};
	es.onerror = function() { /* will auto-reconnect */ };
}

load();
setInterval(load, 10000);
</script>
</body>
</html>`
