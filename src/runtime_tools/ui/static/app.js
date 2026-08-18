const state = { data: null, runIndex: 0, zoom: 1, entity: "", kind: "" };

const fmtDuration = seconds => seconds == null ? "unknown" : seconds < 1 ? `${(seconds * 1000).toFixed(1)} ms` : `${seconds.toFixed(3)} s`;
const fmtBytes = bytes => bytes == null ? "unknown" : bytes < 1048576 ? `${(bytes / 1024).toFixed(1)} KiB` : `${(bytes / 1048576).toFixed(1)} MiB`;
const fmtNumber = value => value == null ? "unknown" : new Intl.NumberFormat().format(value);
const fmtRate = value => value == null ? "unknown" : `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value)}/s`;
const fmtEquivalence = value => value == null ? "unknown" : value ? "equivalent" : "different";
const nsToSeconds = value => value == null ? null : value / 1e9;
const escapeHtml = value => String(value).replace(/[&<>'"]/g, char => ({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"})[char]);

function currentRun() { return state.data.runs[state.runIndex]; }

function renderSwitcher() {
  const node = document.querySelector("#run-switcher");
  node.innerHTML = state.data.runs.map((run, index) => `<button class="${index === state.runIndex ? "active" : ""}" data-run="${index}">${escapeHtml(run.summary.name)}</button>`).join("");
  node.querySelectorAll("button").forEach(button => button.addEventListener("click", () => {
    state.runIndex = Number(button.dataset.run); renderAll();
  }));
}

function renderSummary() {
  const run = currentRun();
  const critical = run.analysis.critical_path;
  const values = [
    ["Outcome", run.summary.exit_code == null ? "No exit evidence" : `Exit ${run.summary.exit_code}`],
    ["Wall time", fmtDuration(run.summary.wall_time_seconds)],
    ["Peak memory", fmtBytes(run.summary.peak_memory_bytes)],
    ["Critical path", critical ? fmtDuration(critical.duration_seconds) : "unavailable"],
    ["Path active", critical ? fmtDuration(critical.active_seconds) : "unavailable"],
    ["Path waiting", critical ? fmtDuration(critical.waiting_seconds) : "unavailable"],
    ["Attachments", fmtNumber(run.summary.record_counts.attachments)],
  ];
  const warning = run.summary.annotation_error ? `<div class="metric warning"><span>Evidence warning</span><strong>Annotations ignored: ${escapeHtml(run.summary.annotation_error)}</strong></div>` : "";
  document.querySelector("#summary").innerHTML = warning + values.map(([label, value]) => `<div class="metric"><span>${label}</span><strong>${escapeHtml(value)}</strong></div>`).join("");
}

function renderAnalysis() {
  const analysis = currentRun().analysis;
  const lifecycle = analysis.lifecycle.length
    ? analysis.lifecycle.map(phase => `<li><span>${escapeHtml(phase.name)}</span><strong>${fmtDuration(phase.duration_seconds)}</strong><small>${escapeHtml(phase.source)}</small></li>`).join("")
    : '<li class="empty">No lifecycle evidence</li>';
  const throughput = analysis.throughput;
  let throughputBody = '<p class="empty">No progress evidence</p>';
  if (throughput) {
    const percent = throughput.total > 0 ? Math.min(100, Math.max(0, throughput.completed / throughput.total * 100)) : 0;
    const drain = throughput.estimated_drain_seconds == null ? "unknown" : fmtDuration(throughput.estimated_drain_seconds);
    const postCompute = throughput.compute_finished_at_ns == null
      ? "<p>Compute boundary: <strong>not observed</strong></p>"
      : `<p>Remaining after compute: <strong>${fmtNumber(throughput.remaining_at_compute_completion)}</strong></p><p>Post-compute wall: <strong>${fmtDuration(throughput.post_compute_seconds)}</strong></p><p>Post-compute rate: <strong>${fmtRate(throughput.post_compute_rate_per_second)}</strong></p>`;
    throughputBody = `<div class="progress"><span style="width:${percent}%"></span></div><p><strong>${fmtNumber(throughput.completed)}</strong> / ${fmtNumber(throughput.total)} completed</p><p>Observed rate: <strong>${fmtRate(throughput.rate_per_second)}</strong></p><p>Estimated drain: <strong>${drain}</strong></p>${postCompute}`;
  }
  const bottlenecks = analysis.bottlenecks.length
    ? analysis.bottlenecks.map(item => `<li><span class="confidence">${Math.round(item.confidence * 100)}%</span><strong>${escapeHtml(item.classification.replaceAll("_", " "))}</strong><p>${escapeHtml(item.evidence)}</p></li>`).join("")
    : '<li class="empty">No constraint classified from available evidence</li>';
  document.querySelector("#analysis").innerHTML = `<article><h3>Lifecycle</h3><ol class="phase-list">${lifecycle}</ol></article><article><h3>Throughput &amp; drain</h3><div class="throughput">${throughputBody}</div></article><article><h3>Bottlenecks</h3><ul class="bottleneck-list">${bottlenecks}</ul></article>`;
}

function renderComparison() {
  const section = document.querySelector("#comparison");
  const diff = state.data.comparison;
  if (!diff) { section.classList.add("hidden"); return; }
  section.classList.remove("hidden");
  const operations = diff.operation_count_changes.slice(0, 5).map(item => `<p><span class="positive">${item.baseline} → ${item.candidate}</span> ${escapeHtml(item.entity_name)} :: ${escapeHtml(item.operation_name)}</p>`).join("") || "<p>No count changes</p>";
  const durations = diff.operation_duration_changes.slice(0, 5).map(item => `<p><span class="positive">${fmtDuration(item.baseline_seconds)} → ${fmtDuration(item.candidate_seconds)}</span> ${escapeHtml(item.entity_name)} :: ${escapeHtml(item.operation_name)}</p>`).join("") || "<p>No duration changes</p>";
  const edges = diff.edge_count_changes.slice(0, 5).map(item => `<p>${escapeHtml(item.change_kind.toUpperCase())} ${escapeHtml(item.source_name)} → ${escapeHtml(item.target_name)}</p>`).join("") || "<p>No dependency changes</p>";
  const environment = diff.environment_changes.slice(0, 5).map(item => `${escapeHtml(item.variable)} (${escapeHtml(item.change_kind)})`).join(", ") || "no selected drift";
  const warnings = [["Baseline", diff.baseline_annotation_error], ["Candidate", diff.candidate_annotation_error]].filter(([, error]) => error).map(([side, error]) => `<p class="evidence-warning">${side} annotations ignored: ${escapeHtml(error)}</p>`).join("");
  const timing = `${warnings}<p>Outcome: ${escapeHtml(diff.outcome)}</p><p>Stdout: ${fmtEquivalence(diff.output_equivalent)} · stderr: ${fmtEquivalence(diff.stderr_equivalent)}</p><p>Runtime: ${fmtDuration(diff.wall_time.baseline)} → <span class="positive">${fmtDuration(diff.wall_time.candidate)}</span></p><p>Critical path: ${fmtDuration(diff.critical_path.baseline)} → <span class="positive">${fmtDuration(diff.critical_path.candidate)}</span></p><p>Environment: ${environment}</p>`;
  document.querySelector("#comparison-grid").innerHTML = `<div class="change-list"><h3>Outcome & timing</h3>${timing}</div><div class="change-list"><h3>Operation counts</h3>${operations}</div><div class="change-list"><h3>Duration shifts</h3>${durations}</div><div class="change-list"><h3>Dependencies</h3>${edges}</div>`;
}

function renderFilters() {
  const run = currentRun();
  const entities = [...run.entities].sort((a, b) => a.name.localeCompare(b.name));
  const kinds = [...new Set(run.events.map(event => event.kind))].sort();
  document.querySelector("#entity-filter").innerHTML = `<option value="">All</option>${entities.map(item => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)}</option>`).join("")}`;
  document.querySelector("#kind-filter").innerHTML = `<option value="">All</option>${kinds.map(kind => `<option value="${escapeHtml(kind)}">${escapeHtml(kind)}</option>`).join("")}`;
  state.entity = ""; state.kind = "";
}

function renderTimeline() {
  const run = currentRun();
  const totalNs = Math.max(1, Math.round((run.summary.wall_time_seconds || 0) * 1e9));
  const entities = new Map(run.entities.map(entity => [entity.id, entity]));
  const visible = run.events.filter(event => (!state.entity || event.entity_id === state.entity) && (!state.kind || event.kind === state.kind));
  const criticalIds = new Set(run.analysis.critical_path ? run.analysis.critical_path.event_ids : []);
  const grouped = new Map();
  visible.forEach(event => {
    const key = event.entity_id || "unowned";
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(event);
  });
  const width = state.zoom * 100;
  const lanes = [...grouped].map(([entityId, events]) => {
    const entity = entities.get(entityId) || { name: "Unowned", kind: "unknown" };
    const bars = events.map(event => {
      const left = Math.max(0, (event.start_offset_ns || 0) / totalNs * 100);
      const barWidth = Math.max(.15, (event.duration_ns || 0) / totalNs * 100);
      const criticalClass = criticalIds.has(event.id) ? " critical" : "";
      return `<button class="event${criticalClass}" data-id="${escapeHtml(event.id)}" data-kind="${escapeHtml(event.kind)}" style="left:${left}%;width:${barWidth}%" title="${escapeHtml(event.name)}">${escapeHtml(event.name)}</button>`;
    }).join("");
    return `<div class="lane"><div class="lane-label">${escapeHtml(entity.name)}<small>${escapeHtml(entity.kind)} · ${events.length} events</small></div><div class="track">${bars}</div></div>`;
  }).join("");
  document.querySelector("#timeline").innerHTML = `<div class="timeline-inner" style="width:${width}%">${lanes || '<div class="lane-label">No matching events</div>'}</div>`;
  const midpoint = run.summary.wall_time_seconds == null ? null : run.summary.wall_time_seconds / 2;
  document.querySelector("#axis").innerHTML = `<span>0</span><span>${fmtDuration(midpoint)}</span><span>${fmtDuration(run.summary.wall_time_seconds)}</span>`;
  document.querySelectorAll(".event").forEach(button => button.addEventListener("click", () => showDetail(visible.find(event => event.id === button.dataset.id), entities)));
}

function showDetail(event, entities) {
  const entity = entities.get(event.entity_id);
  document.querySelector("#detail").innerHTML = `<p class="eyebrow">SELECTED EVIDENCE</p><h2>${escapeHtml(event.name)}</h2><dl><dt>Entity</dt><dd>${escapeHtml(entity ? `${entity.name} / ${entity.kind}` : "unowned")}</dd><dt>Kind</dt><dd>${escapeHtml(event.kind)}</dd><dt>Start offset</dt><dd>${fmtDuration(nsToSeconds(event.start_offset_ns))}</dd><dt>Duration</dt><dd>${fmtDuration(nsToSeconds(event.duration_ns))}</dd><dt>Normalized attributes</dt><dd class="attributes">${escapeHtml(JSON.stringify(event.attributes, null, 2))}</dd></dl>`;
}

function renderAll() { renderSwitcher(); renderSummary(); renderAnalysis(); renderComparison(); renderFilters(); renderTimeline(); }

document.querySelector("#entity-filter").addEventListener("change", event => { state.entity = event.target.value; renderTimeline(); });
document.querySelector("#kind-filter").addEventListener("change", event => { state.kind = event.target.value; renderTimeline(); });
document.querySelector("#zoom").addEventListener("input", event => { state.zoom = Number(event.target.value); renderTimeline(); });

fetch("/api/data").then(response => {
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}).then(data => { state.data = data; renderAll(); }).catch(error => {
  document.querySelector("main").innerHTML = `<p>Could not load execution evidence: ${escapeHtml(error.message)}</p>`;
});
