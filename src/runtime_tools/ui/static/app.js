const state = {
  data: null,
  runIndex: 0,
  zoom: 1,
  entity: "",
  kind: "",
  selectedEventId: null,
  activeFindingIndex: null,
  highlightedEventIds: [],
};
const EVENT_LANE_TOP_PX = 9;
const EVENT_LANE_STRIDE_PX = 36;
const EVENT_TRACK_HEIGHT_PX = 50;

const finiteNumber = value => typeof value === "number" && Number.isFinite(value);
const fmtDuration = seconds => !finiteNumber(seconds) ? "unknown" : seconds < .001 ? `${(seconds * 1e6).toFixed(1)} µs` : seconds < 1 ? `${(seconds * 1000).toFixed(1)} ms` : `${seconds.toFixed(3)} s`;
const fmtBytes = bytes => !finiteNumber(bytes) ? "unknown" : bytes < 1048576 ? `${(bytes / 1024).toFixed(1)} KiB` : `${(bytes / 1048576).toFixed(1)} MiB`;
const fmtNumber = value => !finiteNumber(value) ? "unknown" : new Intl.NumberFormat().format(value);
const fmtRate = value => !finiteNumber(value) ? "unknown" : `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value)}/s`;
const fmtEquivalence = value => value == null ? "unknown" : value ? "equivalent" : "different";
const fmtExitStatus = value => value == null ? "No exit evidence" : value < 0 ? `Signal ${-value}` : `Exit ${value}`;
const nsToSeconds = value => value == null ? null : value / 1e9;
const escapeDisplayControls = value => Array.from(String(value), char => {
  if (char === "\n" || char === "\t" || !/[\p{Cc}\p{Cf}\p{Cs}]/u.test(char)) return char;
  const codePoint = char.codePointAt(0);
  const width = codePoint <= 0xff ? 2 : codePoint <= 0xffff ? 4 : 8;
  const prefix = width === 2 ? "\\x" : width === 4 ? "\\u" : "\\U";
  return `${prefix}${codePoint.toString(16).padStart(width, "0")}`;
}).join("");
const escapeHtml = value => escapeDisplayControls(value).replace(/[&<>'"]/g, char => ({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"})[char]);
const displayValue = value => {
  if (typeof value === "string") return value;
  if (value === undefined) return "unavailable";
  try {
    const encoded = JSON.stringify(value);
    return encoded === undefined ? String(value) : encoded;
  } catch (_) {
    return String(value);
  }
};

function currentRun() { return state.data.runs[state.runIndex]; }

function resetFilters() {
  state.entity = "";
  state.kind = "";
  document.querySelector("#entity-filter").value = "";
  document.querySelector("#kind-filter").value = "";
}

function syncTimelineSelection() {
  document.querySelectorAll(".event, .untimed-event").forEach(button => {
    const selected = button.dataset.id === state.selectedEventId;
    button.classList.toggle("selected", selected);
    button.setAttribute("aria-pressed", String(selected));
  });
}

function resetDetail() {
  state.selectedEventId = null;
  document.querySelector("#detail").innerHTML = '<p class="eyebrow">SELECTED EVIDENCE</p><h2>Choose an interval</h2><p>Click a bar to inspect its normalized attributes and timing.</p>';
}

function clearFindingNavigation() {
  state.activeFindingIndex = null;
  state.highlightedEventIds = [];
}

function switchRunManually(runIndex) {
  state.runIndex = runIndex;
  clearFindingNavigation();
  resetFilters();
  resetDetail();
  renderAll();
  const activeButton = [...document.querySelector("#run-switcher").querySelectorAll("button")].find(button => Number(button.dataset.run) === runIndex);
  if (activeButton) activeButton.focus({ preventScroll: true });
}

function renderSwitcher() {
  const node = document.querySelector("#run-switcher");
  node.innerHTML = state.data.runs.map((run, index) => {
    const active = index === state.runIndex;
    const name = displayValue(run.summary.name);
    return `<button class="${active ? "active" : ""}" data-run="${index}" aria-pressed="${active}" aria-label="Show execution ${escapeHtml(name)}">${escapeHtml(name)}</button>`;
  }).join("");
  node.querySelectorAll("button").forEach(button => button.addEventListener("click", () => {
    switchRunManually(Number(button.dataset.run));
  }));
}

function prooflineSelection(proofline, finding) {
  if (!finding.selection_id || !proofline.selections || typeof proofline.selections !== "object") return null;
  const selection = proofline.selections[finding.selection_id];
  return selection && typeof selection === "object" ? selection : null;
}

function renderEvidenceReferences(finding, retainedReport) {
  const references = Array.isArray(finding.evidence) ? finding.evidence : [];
  if (!references.length) return '<span class="finding-evidence empty">No diff reference available</span>';
  const factLabel = !retainedReport ? "Evaluated fact" : finding.report_assurance === "artifact_bound_policy_replayed" ? "Artifact-bound fact" : finding.report_assurance === "policy_replayed_against_current_evidence" ? "Replayed fact" : "Reported fact";
  return references.map(reference => {
    const diffPath = reference && typeof reference === "object" ? reference.diff_path : undefined;
    const fact = reference && typeof reference === "object" ? reference.fact : undefined;
    return `<span class="finding-evidence"><span>Diff path</span><code>${escapeHtml(displayValue(diffPath))}</code><span>${factLabel}</span><code>${escapeHtml(displayValue(fact))}</code></span>`;
  }).join("");
}

function reportAssurance(finding) {
  if (finding.report_assurance === "artifact_bound_policy_replayed") return "Policy and result replayed against the exact bound runpack artifacts";
  if (finding.report_assurance === "policy_replayed_against_current_evidence") return "Policy and result replayed against current runpacks";
  return "Report-authored policy/result · runtime values consistent";
}

function renderFindingAction(proofline, finding) {
  const selection = prooflineSelection(proofline, finding);
  if (finding.focus !== "candidate_events" || !selection) return "Inspect candidate summary";
  const count = displayValue(selection.matched_event_count);
  const relationship = displayValue(selection.relationship);
  const truncation = selection.truncated === true ? " · selection truncated" : "";
  return `Inspect ${count} candidate event(s) · ${relationship}${truncation}`;
}

function renderProofline() {
  const section = document.querySelector("#proofline");
  const proofline = state.data.proofline;
  if (!proofline || typeof proofline !== "object") {
    section.classList.add("hidden");
    document.querySelector("#proofline-summary").innerHTML = "";
    document.querySelector("#proofline-findings").innerHTML = "";
    return;
  }
  section.classList.remove("hidden");
  const retainedReport = proofline.source === "report";
  document.querySelector("#proofline-heading").textContent = retainedReport ? "Retained runtime contract findings" : "Runtime contract findings";
  const verification = proofline.verification && typeof proofline.verification === "object" ? proofline.verification : {};
  const passed = verification.passed === true;
  const findings = Array.isArray(proofline.findings) ? proofline.findings : [];
  const artifactBoundReport = retainedReport && findings.length > 0 && findings.every(finding => finding.report_assurance === "artifact_bound_policy_replayed");
  const replayedReport = retainedReport && findings.length > 0 && findings.every(finding => finding.report_assurance === "policy_replayed_against_current_evidence");
  document.querySelector("#proofline-eyebrow").textContent = artifactBoundReport ? "PROOFLINE / ARTIFACT-BOUND REPORT" : replayedReport ? "PROOFLINE / REPLAY-VERIFIED REPORT" : retainedReport ? "PROOFLINE / CONSISTENCY-CHECKED REPORT" : "PROOFLINE / VERIFIED";
  const verdictClass = retainedReport ? "report" : passed ? "pass" : "fail";
  const verdictText = artifactBoundReport ? "ARTIFACT-BOUND REPORT" : replayedReport ? "REPLAYED REPORT" : retainedReport ? "CHECKED REPORT" : passed ? "PASS" : "ATTENTION";
  const resultLabel = retainedReport ? `${findings.length} report results` : `${findings.length} findings`;
  document.querySelector("#proofline-summary").innerHTML = `<span class="proofline-verdict ${verdictClass}">${verdictText}</span><span>${resultLabel} · ${escapeHtml(displayValue(verification.claim_count))} claims</span><span title="${escapeHtml(displayValue(proofline.source))}">${escapeHtml(displayValue(proofline.source))}</span>`;
  const findingsNode = document.querySelector("#proofline-findings");
  findingsNode.innerHTML = findings.length ? findings.map((finding, index) => {
    const status = displayValue(finding.status);
    const statusClass = status === "pass" ? "pass" : status === "fail" ? "fail" : status === "unverifiable" ? "unverifiable" : "unknown";
    const contract = displayValue(finding.contract);
    const name = displayValue(finding.name);
    const active = index === state.activeFindingIndex;
    const artifactBound = finding.report_assurance === "artifact_bound_policy_replayed";
    const replayed = finding.report_assurance === "policy_replayed_against_current_evidence";
    const shownStatus = retainedReport ? `${artifactBound ? "artifact-bound" : replayed ? "replayed" : "reported"} ${status}` : status;
    const expectedLabel = artifactBound ? "Artifact-bound expected" : replayed ? "Replayed expected" : retainedReport ? "Reported expected" : "Expected";
    const observedLabel = artifactBound ? "Artifact-bound observed" : replayed ? "Replayed observed" : retainedReport ? "Reported observed" : "Observed";
    const assurance = retainedReport ? `<span class="finding-assurance">${escapeHtml(reportAssurance(finding))}</span>` : "";
    return `<button type="button" class="proofline-finding status-${statusClass}${active ? " active" : ""}" data-finding="${index}" aria-pressed="${active}" aria-label="Inspect ${escapeHtml(shownStatus)} finding ${escapeHtml(contract)}: ${escapeHtml(name)}"><span class="finding-heading"><span class="finding-status">${escapeHtml(shownStatus)}</span><strong>${escapeHtml(contract)} / ${escapeHtml(name)}</strong><small>${escapeHtml(displayValue(finding.type))} · claim ${escapeHtml(displayValue(finding.result_index))}</small></span>${assurance}<span class="finding-expectation"><span>${expectedLabel}</span><code>${escapeHtml(displayValue(finding.expected))}</code><span>${observedLabel}</span><code>${escapeHtml(displayValue(finding.observed))}</code></span>${renderEvidenceReferences(finding, retainedReport)}<span class="finding-action">${escapeHtml(renderFindingAction(proofline, finding))} →</span></button>`;
  }).join("") : '<p class="empty proofline-empty">No actionable contract findings</p>';
  findingsNode.querySelectorAll("button").forEach(button => button.addEventListener("click", () => {
    activateFinding(Number(button.dataset.finding));
  }));
}

function focusSummary() {
  const summary = document.querySelector("#summary");
  summary.focus({ preventScroll: true });
  summary.scrollIntoView({ block: "start" });
}

function activateFinding(findingIndex) {
  const proofline = state.data.proofline;
  const findings = proofline && Array.isArray(proofline.findings) ? proofline.findings : [];
  const finding = findings[findingIndex];
  if (!finding) return;
  const verification = proofline.verification && typeof proofline.verification === "object" ? proofline.verification : {};
  const candidateIndex = state.data.runs.findIndex(run => run.summary.id === verification.candidate_id);
  state.activeFindingIndex = findingIndex;
  state.highlightedEventIds = [];
  resetFilters();
  resetDetail();
  if (candidateIndex >= 0) state.runIndex = candidateIndex;

  const selection = prooflineSelection(proofline, finding);
  let firstEvent = null;
  if (candidateIndex >= 0 && finding.focus === "candidate_events" && selection && Array.isArray(selection.candidate_event_ids)) {
    const run = currentRun();
    const eventsById = new Map(run.events.map(event => [event.id, event]));
    const existingIds = selection.candidate_event_ids.filter(eventId => eventsById.has(eventId));
    state.highlightedEventIds = [...new Set(existingIds)];
    firstEvent = state.highlightedEventIds.length ? eventsById.get(state.highlightedEventIds[0]) : null;
  }

  renderAll();
  if (!firstEvent) {
    state.highlightedEventIds = [];
    focusSummary();
    return;
  }
  const entities = new Map(currentRun().entities.map(entity => [entity.id, entity]));
  showDetail(firstEvent, entities);
  const target = [...document.querySelectorAll(".event, .untimed-event")].find(button => button.dataset.id === firstEvent.id);
  if (!target) {
    state.highlightedEventIds = [];
    resetDetail();
    renderTimeline();
    focusSummary();
    return;
  }
  target.focus({ preventScroll: true });
  target.scrollIntoView({ block: "nearest", inline: "center" });
}

function renderSummary() {
  const run = currentRun();
  const critical = run.analysis.critical_path;
  const rawCpuTime = run.summary.cpu_user_seconds == null || run.summary.cpu_system_seconds == null
    ? null
    : run.summary.cpu_user_seconds + run.summary.cpu_system_seconds;
  const cpuTime = finiteNumber(rawCpuTime) ? rawCpuTime : null;
  const values = [
    ["Outcome", fmtExitStatus(run.summary.exit_code)],
    ["Wall time", fmtDuration(run.summary.wall_time_seconds)],
    ["CPU time", fmtDuration(cpuTime)],
    ["Peak memory", fmtBytes(run.summary.peak_memory_bytes)],
    ["Critical path", critical ? fmtDuration(critical.duration_seconds) : "unavailable"],
    ["Path certainty", critical ? critical.certainty : "unavailable"],
    ["Path active", critical ? fmtDuration(critical.active_seconds) : "unavailable"],
    ["Path waiting", critical ? fmtDuration(critical.waiting_seconds) : "unavailable"],
    ["Untimed events", fmtNumber(run.events.filter(event => event.start_offset_ns == null).length)],
    ["Attachments", fmtNumber(run.summary.record_counts.attachments)],
  ];
  const warningMessages = [];
  if (critical && critical.cycle_detected) warningMessages.push("Causal cycle detected; critical path is inferred");
  if (run.summary.annotation_error) warningMessages.push(`Annotations ignored: ${escapeHtml(run.summary.annotation_error)}`);
  if (run.summary.missing_causal_references == null) warningMessages.push("Causal completeness metadata invalid");
  else if (run.summary.missing_causal_references > 0) warningMessages.push(`${run.summary.missing_causal_references} unresolved causal references`);
  if (run.summary.dropped_attribute_count == null) warningMessages.push("OTLP dropped-attribute metadata invalid");
  else if (run.summary.dropped_attribute_count > 0) warningMessages.push(`${run.summary.dropped_attribute_count} exporter-dropped OTLP attributes`);
  for (const stream of ["stdout", "stderr"]) {
    const relayError = run.summary[`${stream}_relay_error`];
    if (relayError) warningMessages.push(`${stream} relay failed: ${escapeHtml(relayError)}`);
  }
  const incompleteStreams = ["stdout", "stderr"].filter(stream => run.summary[`${stream}_complete`] === false);
  if (incompleteStreams.length) warningMessages.push(`Incomplete output identity: ${incompleteStreams.join(", ")}`);
  const warning = warningMessages.map(message => `<div class="metric warning"><span>Evidence warning</span><strong>${message}</strong></div>`).join("");
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
  const errors = diff.operation_error_count_changes.slice(0, 5).map(item => `<p><span class="positive">${item.baseline} → ${item.candidate}</span> ${escapeHtml(item.entity_name)} :: ${escapeHtml(item.operation_name)}</p>`).join("") || "<p>No failed-operation changes</p>";
  const entities = diff.entity_count_changes.slice(0, 5).map(item => `<p>${escapeHtml(item.change_kind.toUpperCase())} <span class="positive">${item.baseline} → ${item.candidate}</span> ${escapeHtml(item.entity_name)} [${escapeHtml(item.entity_kind)}]</p>`).join("") || "<p>No entity changes</p>";
  const concurrency = diff.operation_concurrency_changes.slice(0, 5).map(item => `<p><span class="positive">${item.baseline} → ${item.candidate}</span> ${escapeHtml(item.entity_name)} :: ${escapeHtml(item.operation_name)}</p>`).join("") || "<p>No concurrency changes</p>";
  const durations = diff.operation_duration_changes.filter(item => Math.abs(item.candidate_seconds - item.baseline_seconds) >= .001).slice(0, 5).map(item => `<p><span class="positive">${fmtDuration(item.baseline_seconds)} → ${fmtDuration(item.candidate_seconds)}</span> ${escapeHtml(item.entity_name)} :: ${escapeHtml(item.operation_name)}</p>`).join("") || "<p>No duration changes of at least 1 ms</p>";
  const edges = diff.edge_count_changes.slice(0, 5).map(item => `<p>${escapeHtml(item.change_kind.toUpperCase())} ${escapeHtml(item.source_name)} → ${escapeHtml(item.target_name)}</p>`).join("") || "<p>No dependency changes</p>";
  const environment = diff.environment_changes.slice(0, 5).map(item => `${escapeHtml(item.variable)} (${escapeHtml(item.change_kind)})`).join(", ") || "no selected drift";
  const annotationWarnings = [["Baseline", diff.baseline_annotation_error], ["Candidate", diff.candidate_annotation_error]].filter(([, error]) => error).map(([side, error]) => `<p class="evidence-warning">${side} annotations ignored: ${escapeHtml(error)}</p>`).join("");
  const relayWarnings = [
    ["Baseline", "stdout", diff.baseline_stdout_relay_error],
    ["Baseline", "stderr", diff.baseline_stderr_relay_error],
    ["Candidate", "stdout", diff.candidate_stdout_relay_error],
    ["Candidate", "stderr", diff.candidate_stderr_relay_error],
  ].filter(([, , error]) => error).map(([side, stream, error]) => `<p class="evidence-warning">${side} ${stream} relay failed: ${escapeHtml(error)}</p>`).join("");
  const outputWarnings = [["Baseline", diff.baseline_incomplete_streams], ["Candidate", diff.candidate_incomplete_streams]].filter(([, streams]) => streams.length).map(([side, streams]) => `<p class="evidence-warning">${side} output identity incomplete: ${escapeHtml(streams.join(", "))}</p>`).join("");
  const causalWarnings = [["Baseline", diff.baseline_missing_causal_references], ["Candidate", diff.candidate_missing_causal_references]].filter(([, count]) => count == null || count > 0).map(([side, count]) => `<p class="evidence-warning">${side} ${count == null ? "causal completeness metadata invalid" : `${count} unresolved causal references`}</p>`).join("");
  const semanticWarnings = [["Baseline", diff.baseline_dropped_attribute_count], ["Candidate", diff.candidate_dropped_attribute_count]].filter(([, count]) => count == null || count > 0).map(([side, count]) => `<p class="evidence-warning">${side} ${count == null ? "dropped-attribute metadata invalid" : `${count} exporter-dropped OTLP attributes`}</p>`).join("");
  const warnings = annotationWarnings + relayWarnings + outputWarnings + causalWarnings + semanticWarnings;
  const timing = `${warnings}<p>Outcome: ${escapeHtml(diff.outcome)}</p><p>Exit status: ${fmtExitStatus(diff.baseline.exit_code)} → ${fmtExitStatus(diff.candidate.exit_code)}</p><p>Stdout: ${fmtEquivalence(diff.output_equivalent)} · stderr: ${fmtEquivalence(diff.stderr_equivalent)} · operation errors: ${fmtEquivalence(diff.operation_errors_equivalent)}</p><p>Runtime: ${fmtDuration(diff.wall_time.baseline)} → <span class="positive">${fmtDuration(diff.wall_time.candidate)}</span></p><p>CPU time: ${fmtDuration(diff.cpu_time.baseline)} → <span class="positive">${fmtDuration(diff.cpu_time.candidate)}</span></p><p>Critical path: ${fmtDuration(diff.critical_path.baseline)} → <span class="positive">${fmtDuration(diff.critical_path.candidate)}</span></p><p>Environment: ${environment}</p>`;
  document.querySelector("#comparison-grid").innerHTML = `<div class="change-list"><h3>Outcome & timing</h3>${timing}</div><div class="change-list"><h3>Entities</h3>${entities}</div><div class="change-list"><h3>Operation counts</h3>${operations}</div><div class="change-list"><h3>Failed operations</h3>${errors}</div><div class="change-list"><h3>Max concurrency</h3>${concurrency}</div><div class="change-list"><h3>Duration shifts</h3>${durations}</div><div class="change-list"><h3>Dependencies</h3>${edges}</div>`;
}

function renderFilters() {
  const run = currentRun();
  const entities = [...run.entities].sort((a, b) => a.name.localeCompare(b.name));
  const kinds = [...new Set(run.events.map(event => event.kind))].sort();
  document.querySelector("#entity-filter").innerHTML = `<option value="">All</option>${entities.map(item => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)}</option>`).join("")}`;
  document.querySelector("#kind-filter").innerHTML = `<option value="">All</option>${kinds.map(kind => `<option value="${escapeHtml(kind)}">${escapeHtml(kind)}</option>`).join("")}`;
  state.entity = ""; state.kind = "";
}

function stackEvents(events) {
  const ordered = [...events].sort((left, right) => {
    const startDelta = left.start_offset_ns - right.start_offset_ns;
    if (startDelta) return startDelta;
    const leftFinish = left.finish_offset_ns ?? left.start_offset_ns;
    const rightFinish = right.finish_offset_ns ?? right.start_offset_ns;
    const finishDelta = leftFinish - rightFinish;
    if (finishDelta) return finishDelta;
    return String(left.id).localeCompare(String(right.id));
  });
  const active = [];
  const earlier = (left, right) => left.finish < right.finish || (left.finish === right.finish && left.lane < right.lane);
  const push = item => {
    let index = active.push(item) - 1;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (!earlier(item, active[parent])) break;
      active[index] = active[parent];
      index = parent;
    }
    active[index] = item;
  };
  const pop = () => {
    const earliest = active[0];
    const last = active.pop();
    if (active.length) {
      let index = 0;
      while (true) {
        const left = index * 2 + 1;
        if (left >= active.length) break;
        const right = left + 1;
        const child = right < active.length && earlier(active[right], active[left]) ? right : left;
        if (!earlier(active[child], last)) break;
        active[index] = active[child];
        index = child;
      }
      active[index] = last;
    }
    return earliest;
  };
  let laneCount = 0;
  const stacked = ordered.map(event => {
    const start = event.start_offset_ns;
    const finish = Math.max(start, event.finish_offset_ns ?? start);
    const lane = active.length && active[0].finish <= start ? pop().lane : laneCount++;
    push({ finish, lane });
    return { event, lane };
  });
  return { stacked, laneCount };
}

function renderTimeline() {
  const run = currentRun();
  const timelineDurationNs = run.timeline_duration_ns;
  const scaleNs = timelineDurationNs > 0 ? timelineDurationNs : 1;
  const entities = new Map(run.entities.map(entity => [entity.id, entity]));
  const visible = run.events.filter(event => (!state.entity || event.entity_id === state.entity) && (!state.kind || event.kind === state.kind));
  if (state.selectedEventId && !visible.some(event => event.id === state.selectedEventId)) resetDetail();
  const timed = visible.filter(event => event.start_offset_ns != null);
  const untimed = visible.filter(event => event.start_offset_ns == null);
  const criticalIds = new Set(run.analysis.critical_path ? run.analysis.critical_path.event_ids : []);
  const highlightedIds = new Set(state.highlightedEventIds);
  const evidenceContext = highlightedIds.size > 0;
  const evidenceClasses = event => `${highlightedIds.has(event.id) ? " evidence-highlight" : evidenceContext ? " evidence-dimmed" : ""}${state.selectedEventId === event.id ? " selected" : ""}`;
  const grouped = new Map();
  timed.forEach(event => {
    const key = event.entity_id || "unowned";
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(event);
  });
  const width = state.zoom * 100;
  const lanes = [...grouped].map(([entityId, events]) => {
    const entity = entities.get(entityId) || { name: "Unowned", kind: "unknown" };
    const { stacked, laneCount } = stackEvents(events);
    const trackHeight = EVENT_TRACK_HEIGHT_PX + (laneCount - 1) * EVENT_LANE_STRIDE_PX;
    const bars = stacked.map(({ event, lane }) => {
      const left = Math.max(0, (event.start_offset_ns || 0) / scaleNs * 100);
      const barWidth = Math.max(.15, (event.duration_ns || 0) / scaleNs * 100);
      const criticalClass = criticalIds.has(event.id) ? " critical" : "";
      const selected = state.selectedEventId === event.id;
      return `<button class="event${criticalClass}${evidenceClasses(event)}" data-id="${escapeHtml(event.id)}" data-kind="${escapeHtml(event.kind)}" style="left:${left}%;width:${barWidth}%;top:${EVENT_LANE_TOP_PX + lane * EVENT_LANE_STRIDE_PX}px" title="${escapeHtml(event.name)}" aria-label="Inspect event ${escapeHtml(event.name)}" aria-pressed="${selected}">${escapeHtml(event.name)}</button>`;
    }).join("");
    return `<div class="lane"><div class="lane-label">${escapeHtml(entity.name)}<small>${escapeHtml(entity.kind)} · ${events.length} events</small></div><div class="track" style="height:${trackHeight}px">${bars}</div></div>`;
  }).join("");
  const untimedLane = untimed.length ? `<div class="lane untimed-lane"><div class="lane-label">Untimed evidence<small>${untimed.length} events · no fabricated position</small></div><div class="untimed-track">${untimed.map(event => `<button class="untimed-event${evidenceClasses(event)}" data-id="${escapeHtml(event.id)}" data-kind="${escapeHtml(event.kind)}" aria-label="Inspect untimed event ${escapeHtml(event.name)}" aria-pressed="${state.selectedEventId === event.id}">${escapeHtml(event.name)}</button>`).join("")}</div></div>` : "";
  document.querySelector("#timeline").innerHTML = `<div class="timeline-inner" style="width:${width}%">${lanes || (!untimedLane ? '<div class="lane-label">No matching events</div>' : '')}${untimedLane}</div>`;
  const timelineSeconds = nsToSeconds(timelineDurationNs);
  document.querySelector("#axis").innerHTML = `<span>0</span><span>${fmtDuration(timelineSeconds / 2)}</span><span>${fmtDuration(timelineSeconds)}</span>`;
  document.querySelectorAll(".event, .untimed-event").forEach(button => button.addEventListener("click", () => {
    const event = visible.find(item => item.id === button.dataset.id);
    if (event) showDetail(event, entities);
  }));
}

function showDetail(event, entities) {
  state.selectedEventId = event.id;
  const run = currentRun();
  const entity = entities.get(event.entity_id);
  const eventNames = new Map(run.events.map(item => [item.id, item.name]));
  const links = run.edges.filter(edge => edge.source_event_id === event.id || edge.target_event_id === event.id).map(edge => {
    const incoming = edge.target_event_id === event.id;
    const peerId = incoming ? edge.source_event_id : edge.target_event_id;
    const peer = eventNames.get(peerId) || peerId;
    return `<li><span>${incoming ? "←" : "→"}</span><strong>${escapeHtml(peer)}</strong><small>${escapeHtml(edge.kind)} · ${Math.round(edge.confidence * 100)}%</small></li>`;
  }).join("");
  const causalLinks = links ? `<ul class="causal-links">${links}</ul>` : "none observed";
  const eventFinish = event.start_offset_ns == null ? null : event.start_offset_ns + (event.duration_ns || 0);
  const measurements = run.measurements.filter(item => item.entity_id === event.entity_id && item.timestamp_offset_ns != null && event.start_offset_ns != null && item.timestamp_offset_ns >= event.start_offset_ns && item.timestamp_offset_ns <= eventFinish).slice(0, 20).map(item => `<li><strong>${escapeHtml(item.name)}</strong><span>${escapeHtml(fmtNumber(item.value))} ${escapeHtml(item.unit)}</span></li>`).join("");
  const resourceMeasurements = measurements ? `<ul class="resource-measurements">${measurements}</ul>` : "none in interval";
  const uncertainty = event.uncertainty_ns == null ? "unknown" : `${fmtNumber(event.uncertainty_ns)} ns`;
  document.querySelector("#detail").innerHTML = `<p class="eyebrow">SELECTED EVIDENCE</p><h2>${escapeHtml(event.name)}</h2><dl><dt>Entity</dt><dd>${escapeHtml(entity ? `${entity.name} / ${entity.kind}` : "unowned")}</dd><dt>Kind</dt><dd>${escapeHtml(event.kind)}</dd><dt>Clock domain</dt><dd>${escapeHtml(event.clock_domain || "unknown")}</dd><dt>Clock uncertainty</dt><dd>${escapeHtml(uncertainty)}</dd><dt>Start offset</dt><dd>${fmtDuration(nsToSeconds(event.start_offset_ns))}</dd><dt>Finish offset</dt><dd>${fmtDuration(nsToSeconds(event.finish_offset_ns))}</dd><dt>Duration</dt><dd>${fmtDuration(nsToSeconds(event.duration_ns))}</dd><dt>Causal links</dt><dd>${causalLinks}</dd><dt>Resource measurements</dt><dd>${resourceMeasurements}</dd><dt>Normalized attributes</dt><dd class="attributes">${escapeHtml(JSON.stringify(event.attributes, null, 2))}</dd></dl>`;
  syncTimelineSelection();
}

function renderAll() { renderSwitcher(); renderProofline(); renderSummary(); renderAnalysis(); renderComparison(); renderFilters(); renderTimeline(); }

document.querySelector("#entity-filter").addEventListener("change", event => { state.entity = event.target.value; renderTimeline(); });
document.querySelector("#kind-filter").addEventListener("change", event => { state.kind = event.target.value; renderTimeline(); });
document.querySelector("#zoom").addEventListener("input", event => { state.zoom = Number(event.target.value); renderTimeline(); });

fetch("/api/data").then(response => {
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}).then(data => { state.data = data; renderAll(); }).catch(error => {
  document.querySelector("main").innerHTML = `<p>Could not load execution evidence: ${escapeHtml(error.message)}</p>`;
});
