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
const fmtRunResult = value => value == null ? "Unknown" : value === 0 ? "Completed" : value < 0 ? "Stopped unexpectedly" : "Failed";
const fmtActivityDuration = nanoseconds => nanoseconds == null ? "Timing unavailable" : nanoseconds === 0 ? "Instant" : fmtDuration(nsToSeconds(nanoseconds));
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
const humanizeLabel = value => {
  const text = displayValue(value).replaceAll("_", " ").replaceAll("-", " ").replace(/\s+/g, " ").trim();
  return text ? text[0].toUpperCase() + text.slice(1) : "Unnamed safeguard";
};

function currentRun() { return state.data.runs[state.runIndex]; }

function candidateRunIndex(data = state.data) {
  const verification = data.proofline && typeof data.proofline === "object" && data.proofline.verification && typeof data.proofline.verification === "object" ? data.proofline.verification : {};
  const verifiedIndex = data.runs.findIndex(run => run.summary.id === verification.candidate_id);
  if (verifiedIndex >= 0) return verifiedIndex;
  return data.comparison && data.runs.length > 1 ? data.runs.length - 1 : 0;
}

function findingCounts() {
  const proofline = state.data.proofline;
  const findings = proofline && Array.isArray(proofline.findings) ? proofline.findings : [];
  return findings.reduce((counts, finding) => {
    const status = finding.status === "pass" || finding.status === "fail" || finding.status === "unverifiable" ? finding.status : "unknown";
    counts[status] += 1;
    return counts;
  }, { pass: 0, fail: 0, unverifiable: 0, unknown: 0 });
}

function fmtPercentChange(baseline, candidate) {
  if (!finiteNumber(baseline) || !finiteNumber(candidate)) return "change unavailable";
  if (baseline === candidate) return "no change";
  if (baseline === 0) return candidate > 0 ? "new in candidate" : "removed in candidate";
  const percent = (candidate - baseline) / Math.abs(baseline) * 100;
  return `${percent > 0 ? "+" : ""}${percent.toFixed(1)}%`;
}

function comparisonMetric(label, values, formatter) {
  const baseline = values ? values.baseline : null;
  const candidate = values ? values.candidate : null;
  return `<div class="comparison-metric"><span>${escapeHtml(label)}</span><div class="metric-pair"><span><small>Earlier version</small><strong>${escapeHtml(formatter(baseline))}</strong></span><span class="metric-arrow" aria-hidden="true">→</span><span><small>New version</small><strong>${escapeHtml(formatter(candidate))}</strong></span></div><p>${escapeHtml(fmtPercentChange(baseline, candidate))}</p></div>`;
}

function renderOverview() {
  const section = document.querySelector("#overview");
  const diff = state.data.comparison;
  if (!diff) { section.classList.add("hidden"); return; }
  section.classList.remove("hidden");
  const counts = findingCounts();
  const problemCount = counts.fail + counts.unverifiable;
  const hasProofline = state.data.proofline && typeof state.data.proofline === "object";
  const title = counts.fail > 0 ? "Do not ship yet" : counts.unverifiable > 0 ? "Review missing evidence" : hasProofline ? "Safe to ship" : diff.outcome === "equivalent" ? "No meaningful change found" : "Review the changes";
  const tone = counts.fail > 0 ? "danger" : counts.unverifiable > 0 || diff.outcome !== "equivalent" ? "attention" : "success";
  const message = counts.fail > 0 ? `${counts.fail} safeguard${counts.fail === 1 ? "" : "s"} failed. Review the problems below before releasing this version.` : counts.unverifiable > 0 ? `${counts.unverifiable} safeguard${counts.unverifiable === 1 ? "" : "s"} could not be checked with the available data.` : hasProofline ? `All ${counts.pass} safeguards passed.` : "No safeguards were provided, so the page can show changes but cannot decide whether they are safe.";
  const outputStatus = `<div class="comparison-metric"><span>Application result</span><div class="metric-pair"><span><small>Earlier version</small><strong>${escapeHtml(fmtRunResult(diff.baseline.exit_code))}</strong></span><span class="metric-arrow" aria-hidden="true">→</span><span><small>New version</small><strong>${escapeHtml(fmtRunResult(diff.candidate.exit_code))}</strong></span></div><p>${diff.output_equivalent === true ? "Same user-visible output" : diff.output_equivalent === false ? "Output changed" : "Output could not be compared"}</p></div>`;
  document.querySelector("#comparison-overview").innerHTML = `<div class="verdict-card ${tone}"><p class="eyebrow">RELEASE GUIDANCE</p><h2>${escapeHtml(title)}</h2><p>${escapeHtml(message)}</p><div class="verdict-counts"><span><strong>${counts.fail}</strong> failed</span><span><strong>${counts.unverifiable}</strong> not checked</span><span><strong>${counts.pass}</strong> passed</span></div></div><div class="overview-metrics">${outputStatus}${comparisonMetric("Overall runtime", diff.wall_time, fmtDuration)}</div>`;
}

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
  document.querySelector("#detail").innerHTML = '<p class="eyebrow">ACTIVITY DETAILS</p><h2>Choose an activity</h2><p>Select a failed safeguard or an activity in the timeline to see what happened.</p>';
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
  const candidateIndex = candidateRunIndex();
  node.innerHTML = state.data.runs.map((run, index) => {
    const active = index === state.runIndex;
    const name = displayValue(run.summary.name);
    const role = state.data.comparison ? index === 0 ? "Earlier version" : index === candidateIndex ? "New version" : "Other run" : "Run";
    return `<button class="${active ? "active" : ""}" data-run="${index}" aria-pressed="${active}" aria-label="Show ${escapeHtml(role.toLowerCase())} execution ${escapeHtml(name)}"><span>${escapeHtml(role)}</span>${escapeHtml(name)}</button>`;
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

function findingExplanation(finding) {
  const explanations = {
    candidate_exit_success: "The new version completed successfully.",
    exit_code_equivalent: "Both versions completed in the same way.",
    output_equivalent: "Both versions produced the same application result.",
    forbid_new_dependency: "The new version contacted a service that the earlier version did not use.",
    max_operation_count: "An activity ran more often than this safeguard allows.",
    max_operation_error_count: "An activity that previously succeeded started failing in the new version.",
    max_runtime_regression: "The new version took longer than this safeguard allows.",
  };
  if (explanations[finding.type]) return explanations[finding.type];
  return finding.status === "pass" ? "The new version stayed within this safeguard." : finding.status === "unverifiable" ? "There is not enough evidence to check this safeguard." : "The new version did not meet this safeguard.";
}

function renderFindingComparison(finding) {
  const evidence = Array.isArray(finding.evidence) ? finding.evidence[0] : null;
  const fact = evidence && evidence.fact && typeof evidence.fact === "object" ? evidence.fact : null;
  if (!fact || !finiteNumber(fact.baseline) || !finiteNumber(fact.candidate)) {
    return `<span class="finding-comparison"><span><small>Safeguard</small><strong>${escapeHtml(displayValue(finding.expected))}</strong></span><span><small>New version</small><strong>${escapeHtml(displayValue(finding.observed))}</strong></span></span>`;
  }
  const format = value => {
    if (finding.type === "forbid_new_dependency") return value === 0 ? "Not contacted" : `${fmtNumber(value)} ${value === 1 ? "contact" : "contacts"}`;
    if (finding.type === "max_operation_error_count") return `${fmtNumber(value)} ${value === 1 ? "error" : "errors"}`;
    return value === 1 ? "Once" : `${fmtNumber(value)} times`;
  };
  const limit = finiteNumber(fact.limit) ? `<span><small>Allowed</small><strong>${escapeHtml(format(fact.limit))}</strong></span>` : "";
  return `<span class="finding-comparison"><span><small>Earlier version</small><strong>${escapeHtml(format(fact.baseline))}</strong></span><span><small>New version</small><strong>${escapeHtml(format(fact.candidate))}</strong></span>${limit}</span>`;
}

function renderFindingAction(proofline, finding) {
  const selection = prooflineSelection(proofline, finding);
  if (finding.focus !== "candidate_events" || !selection) return "View the new version overview";
  const count = finiteNumber(selection.matched_event_count) ? selection.matched_event_count : 0;
  return count === 1 ? "View the related activity" : `View ${fmtNumber(count)} related activities`;
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
  const verification = proofline.verification && typeof proofline.verification === "object" ? proofline.verification : {};
  const passed = verification.passed === true;
  const findings = Array.isArray(proofline.findings) ? proofline.findings : [];
  const counts = findingCounts();
  document.querySelector("#proofline-heading").textContent = counts.fail > 0 ? "Why this version is blocked" : counts.unverifiable > 0 ? "What still needs evidence" : "All safeguards passed";
  const artifactBoundReport = retainedReport && findings.length > 0 && findings.every(finding => finding.report_assurance === "artifact_bound_policy_replayed");
  const replayedReport = retainedReport && findings.length > 0 && findings.every(finding => finding.report_assurance === "policy_replayed_against_current_evidence");
  document.querySelector("#proofline-eyebrow").textContent = "RELEASE SAFEGUARDS";
  const verdictClass = counts.fail > 0 ? "fail" : counts.unverifiable > 0 ? "report" : "pass";
  const verdictText = counts.fail > 0 ? `${counts.fail} failed` : counts.unverifiable > 0 ? `${counts.unverifiable} not checked` : passed ? "All passed" : "Review needed";
  const assuranceExplanation = artifactBoundReport ? "The saved review was rechecked against these exact execution files." : replayedReport ? "The saved rules were run again against the current execution files." : retainedReport ? "The saved results are consistent with the current execution files." : "The safeguards were checked directly against these executions.";
  document.querySelector("#proofline-summary").innerHTML = `<span class="proofline-verdict ${verdictClass}">${verdictText}</span><span>${counts.pass} passed</span><details class="report-details"><summary>How this was checked</summary><p>${escapeHtml(assuranceExplanation)}</p><p>Source: ${escapeHtml(displayValue(proofline.source))} · ${escapeHtml(displayValue(verification.claim_count))} checks</p></details>`;
  const findingsNode = document.querySelector("#proofline-findings");
  const renderFinding = (finding, index) => {
    const status = displayValue(finding.status);
    const statusClass = status === "pass" ? "pass" : status === "fail" ? "fail" : status === "unverifiable" ? "unverifiable" : "unknown";
    const contract = displayValue(finding.contract);
    const name = humanizeLabel(finding.name);
    const active = index === state.activeFindingIndex;
    const shownStatus = status === "fail" ? "Failed" : status === "pass" ? "Passed" : status === "unverifiable" ? "Not checked" : "Review";
    const assurance = retainedReport ? `<span class="finding-assurance">${escapeHtml(reportAssurance(finding))}</span>` : "";
    return `<button type="button" class="proofline-finding status-${statusClass}${active ? " active" : ""}" data-finding="${index}" aria-pressed="${active}" aria-label="Inspect ${escapeHtml(shownStatus)} safeguard ${escapeHtml(name)}"><span class="finding-heading"><span class="finding-status">${escapeHtml(shownStatus)}</span><strong>${escapeHtml(name)}</strong><small>${escapeHtml(contract)} · ${escapeHtml(displayValue(finding.type))} · check ${escapeHtml(displayValue(finding.result_index))}</small></span><span class="finding-explanation">${escapeHtml(findingExplanation(finding))}</span>${renderFindingComparison(finding)}${assurance}${renderEvidenceReferences(finding, retainedReport)}<span class="finding-action">${escapeHtml(renderFindingAction(proofline, finding))} →</span></button>`;
  };
  const renderPass = (finding, index) => {
    const name = humanizeLabel(finding.name);
    const active = index === state.activeFindingIndex;
    return `<button type="button" class="proofline-finding compact-pass${active ? " active" : ""}" data-finding="${index}" aria-pressed="${active}" aria-label="View passed safeguard ${escapeHtml(name)}"><span class="pass-check" aria-hidden="true">✓</span><span><strong>${escapeHtml(name)}</strong><small>${escapeHtml(findingExplanation(finding))}</small></span></button>`;
  };
  const entries = findings.map((finding, index) => ({ finding, index }));
  const problems = entries.filter(({ finding }) => finding.status !== "pass");
  const passes = entries.filter(({ finding }) => finding.status === "pass");
  const problemGroup = problems.length ? `<div class="finding-group problem-findings"><div class="finding-group-heading"><h3>Needs attention</h3><span>${problems.length} check${problems.length === 1 ? "" : "s"}</span></div><div class="finding-grid">${problems.map(({ finding, index }) => renderFinding(finding, index)).join("")}</div></div>` : "";
  const passGroup = passes.length ? `<div class="finding-group passed-findings"><div class="finding-group-heading"><h3>What stayed safe</h3><span>${passes.length} safeguard${passes.length === 1 ? "" : "s"}</span></div><div class="passed-summary">${passes.map(({ finding, index }) => renderPass(finding, index)).join("")}</div></div>` : "";
  findingsNode.innerHTML = findings.length ? problemGroup + passGroup : '<p class="empty proofline-empty">No contract results were available</p>';
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
  const candidateIndex = candidateRunIndex();
  const role = state.data.comparison ? state.runIndex === 0 ? "Earlier version" : state.runIndex === candidateIndex ? "New version" : "Selected run" : "Run overview";
  document.querySelector("#selected-run-heading").textContent = role;
  document.querySelector("#selected-run-description").textContent = "A concise summary of the version currently selected above.";
  const critical = run.analysis.critical_path;
  const rawCpuTime = run.summary.cpu_user_seconds == null || run.summary.cpu_system_seconds == null
    ? null
    : run.summary.cpu_user_seconds + run.summary.cpu_system_seconds;
  const cpuTime = finiteNumber(rawCpuTime) ? rawCpuTime : null;
  const values = [
    ["Status", fmtRunResult(run.summary.exit_code)],
    ["Run time", fmtDuration(run.summary.wall_time_seconds)],
    ["Recorded activities", fmtNumber(run.events.length)],
    ["Peak memory", fmtBytes(run.summary.peak_memory_bytes)],
  ];
  const technicalValues = [
    ["CPU time", fmtDuration(cpuTime)],
    ["Critical path", critical ? fmtDuration(critical.duration_seconds) : "unavailable"],
    ["Path certainty", critical ? critical.certainty : "unavailable"],
    ["Active on critical path", critical ? fmtDuration(critical.active_seconds) : "unavailable"],
    ["Waiting on critical path", critical ? fmtDuration(critical.waiting_seconds) : "unavailable"],
    ["Activities without timing", fmtNumber(run.events.filter(event => event.start_offset_ns == null).length)],
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
  const technical = `<details class="summary-technical"><summary>More performance details</summary><dl>${technicalValues.map(([label, value]) => `<div><dt>${label}</dt><dd>${escapeHtml(value)}</dd></div>`).join("")}</dl></details>`;
  document.querySelector("#summary").innerHTML = warning + values.map(([label, value]) => `<div class="metric"><span>${label}</span><strong>${escapeHtml(value)}</strong></div>`).join("") + technical;
}

function renderAnalysis() {
  const analysis = currentRun().analysis;
  const groupedLifecycle = analysis.lifecycle.reduce((groups, phase) => {
    const previous = groups[groups.length - 1];
    if (previous && previous.name === phase.name && previous.source === phase.source) {
      previous.count += 1;
      previous.duration_seconds += phase.duration_seconds;
    } else {
      groups.push({ ...phase, count: 1 });
    }
    return groups;
  }, []);
  const lifecycle = groupedLifecycle.length
    ? groupedLifecycle.map(phase => `<li><span>${escapeHtml(humanizeLabel(phase.name))}${phase.count > 1 ? ` <small>×${phase.count}</small>` : ""}</span><strong>${fmtDuration(phase.duration_seconds)}</strong><small>${phase.count > 1 ? `${phase.count} repeated steps` : "Recorded step"}</small></li>`).join("")
    : '<li class="empty">No application steps were recorded</li>';
  const throughput = analysis.throughput;
  let throughputBody = '<p class="empty">No progress updates were recorded</p>';
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
    : '<li class="empty">No likely slowdown was identified</li>';
  document.querySelector("#analysis").innerHTML = `<article><h3>Steps</h3><ol class="phase-list">${lifecycle}</ol></article><article><h3>Work progress</h3><div class="throughput">${throughputBody}</div></article><article><h3>Performance clues</h3><ul class="bottleneck-list">${bottlenecks}</ul></article>`;
}

function deltaPair(baseline, candidate, formatter = fmtNumber) {
  return `<span class="delta-pair"><span><small>Earlier version</small><strong>${escapeHtml(formatter(baseline))}</strong></span><span class="metric-arrow" aria-hidden="true">→</span><span><small>New version</small><strong>${escapeHtml(formatter(candidate))}</strong></span></span>`;
}

function renderComparison() {
  const section = document.querySelector("#comparison");
  const diff = state.data.comparison;
  if (!diff) { section.classList.add("hidden"); return; }
  section.classList.remove("hidden");
  const operations = diff.operation_count_changes.slice(0, 5).map(item => `<div class="change-row">${deltaPair(item.baseline, item.candidate)}<p><strong>${escapeHtml(item.operation_name)}</strong><span>${escapeHtml(item.entity_name)}</span></p></div>`).join("") || '<p class="empty-state">No operation count changes</p>';
  const errors = diff.operation_error_count_changes.slice(0, 5).map(item => `<div class="change-row problem">${deltaPair(item.baseline, item.candidate)}<p><strong>${escapeHtml(item.operation_name)}</strong><span>${escapeHtml(item.entity_name)}</span></p></div>`).join("") || '<p class="empty-state">No failed-operation changes</p>';
  const entities = diff.entity_count_changes.slice(0, 5).map(item => `<div class="change-row"><span class="change-kind">${escapeHtml(item.change_kind)}</span>${deltaPair(item.baseline, item.candidate)}<p><strong>${escapeHtml(item.entity_name)}</strong><span>${escapeHtml(item.entity_kind)}</span></p></div>`).join("") || '<p class="empty-state">No entity changes</p>';
  const concurrency = diff.operation_concurrency_changes.slice(0, 5).map(item => `<div class="change-row">${deltaPair(item.baseline, item.candidate)}<p><strong>${escapeHtml(item.operation_name)}</strong><span>${escapeHtml(item.entity_name)}</span></p></div>`).join("") || '<p class="empty-state">No concurrency changes</p>';
  const durations = diff.operation_duration_changes.filter(item => Math.abs(item.candidate_seconds - item.baseline_seconds) >= .001).slice(0, 5).map(item => `<div class="change-row">${deltaPair(item.baseline_seconds, item.candidate_seconds, fmtDuration)}<p><strong>${escapeHtml(item.operation_name)}</strong><span>${escapeHtml(item.entity_name)}</span></p></div>`).join("") || '<p class="empty-state">No duration changes of at least 1 ms</p>';
  const edges = diff.edge_count_changes.slice(0, 5).map(item => `<div class="change-row dependency"><span class="change-kind">${escapeHtml(item.change_kind)}</span><p><strong>${escapeHtml(item.source_name)} → ${escapeHtml(item.target_name)}</strong><span>${escapeHtml(item.relation || "dependency")}</span></p></div>`).join("") || '<p class="empty-state">No dependency changes</p>';
  const environment = diff.environment_changes.slice(0, 5).map(item => `<div class="change-row dependency"><span class="change-kind">${escapeHtml(item.change_kind)}</span><p><strong>${escapeHtml(item.variable)}</strong><span>Environment setting</span></p></div>`).join("");
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
  const failedTypes = new Set((state.data.proofline && Array.isArray(state.data.proofline.findings) ? state.data.proofline.findings : []).filter(finding => finding.status === "fail").map(finding => finding.type));
  const sections = [];
  if (diff.operation_count_changes.length && !failedTypes.has("max_operation_count")) sections.push(["Activity counts", operations]);
  if (diff.operation_error_count_changes.length && !failedTypes.has("max_operation_error_count")) sections.push(["Activity errors", errors]);
  if (diff.edge_count_changes.length && !failedTypes.has("forbid_new_dependency")) sections.push(["Service connections", edges]);
  if (diff.operation_duration_changes.some(item => Math.abs(item.candidate_seconds - item.baseline_seconds) >= .001)) sections.push(["Time spent in activities", durations]);
  if (diff.operation_concurrency_changes.length) sections.push(["Parallel work", concurrency]);
  if (diff.entity_count_changes.length) sections.push(["Components", entities]);
  if (diff.environment_changes.length) sections.push(["Environment settings", environment]);
  const warningBlock = warnings ? `<div class="change-list comparison-warnings"><h3>Evidence notes</h3>${warnings}</div>` : "";
  document.querySelector("#comparison-grid").innerHTML = warningBlock + (sections.length ? sections.map(([heading, body]) => `<div class="change-list"><h3>${heading}</h3>${body}</div>`).join("") : '<p class="comparison-empty">No additional changes beyond the safeguards above.</p>');
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
    const relation = edge.kind === "parent" ? incoming ? `Started as part of ${peer}` : `${peer} started as part of this activity` : edge.kind === "calls" ? incoming ? `Called by ${peer}` : `Called ${peer}` : incoming ? `Triggered by ${peer}` : `Led to ${peer}`;
    const certainty = edge.confidence >= .99 ? "Directly recorded" : `Likely relationship · ${Math.round(edge.confidence * 100)}% confidence`;
    return `<li><strong>${escapeHtml(relation)}</strong><small>${escapeHtml(certainty)}</small></li>`;
  }).join("");
  const causalLinks = links ? `<ul class="causal-links">${links}</ul>` : "none observed";
  const eventFinish = event.start_offset_ns == null ? null : event.start_offset_ns + (event.duration_ns || 0);
  const measurements = run.measurements.filter(item => item.entity_id === event.entity_id && item.timestamp_offset_ns != null && event.start_offset_ns != null && item.timestamp_offset_ns >= event.start_offset_ns && item.timestamp_offset_ns <= eventFinish).slice(0, 20).map(item => `<li><strong>${escapeHtml(item.name)}</strong><span>${escapeHtml(fmtNumber(item.value))} ${escapeHtml(item.unit)}</span></li>`).join("");
  const resourceMeasurements = measurements ? `<ul class="resource-measurements">${measurements}</ul>` : "none in interval";
  const uncertainty = event.uncertainty_ns == null ? "unknown" : `${fmtNumber(event.uncertainty_ns)} ns`;
  const peerService = event.attributes && typeof event.attributes === "object" ? event.attributes["peer.service"] : null;
  const activitySummary = event.attributes && event.attributes.error === true ? "This activity ended with an error." : typeof peerService === "string" ? `Contacted ${peerService}.` : `Recorded ${humanizeLabel(event.kind).toLowerCase()} activity.`;
  const relationshipSection = links ? `<section class="detail-section"><h3>How it fits</h3><ul class="causal-links">${links}</ul></section>` : "";
  document.querySelector("#detail").innerHTML = `<p class="eyebrow">ACTIVITY DETAILS</p><h2>${escapeHtml(event.name)}</h2><p class="activity-summary">${escapeHtml(activitySummary)}</p><dl class="detail-highlights"><dt>Component</dt><dd>${escapeHtml(entity ? entity.name : "Not assigned")}</dd><dt>Duration</dt><dd>${fmtActivityDuration(event.duration_ns)}</dd></dl>${relationshipSection}<details class="technical-details"><summary>Technical details</summary><dl><dt>Activity type</dt><dd>${escapeHtml(event.kind)}</dd><dt>Component type</dt><dd>${escapeHtml(entity ? entity.kind : "unknown")}</dd><dt>Clock source</dt><dd>${escapeHtml(event.clock_domain || "unknown")}</dd><dt>Clock uncertainty</dt><dd>${escapeHtml(uncertainty)}</dd><dt>Start after run began</dt><dd>${fmtDuration(nsToSeconds(event.start_offset_ns))}</dd><dt>Finish after run began</dt><dd>${fmtDuration(nsToSeconds(event.finish_offset_ns))}</dd><dt>Resource measurements</dt><dd>${resourceMeasurements}</dd><dt>Raw attributes</dt><dd class="attributes">${escapeHtml(JSON.stringify(event.attributes, null, 2))}</dd></dl></details>`;
  syncTimelineSelection();
}

function renderAll() {
  document.querySelector("#loading-state").classList.add("hidden");
  renderOverview(); renderSwitcher(); renderProofline(); renderComparison();
  renderSummary(); renderAnalysis(); renderFilters(); renderTimeline();
}

document.querySelector("#entity-filter").addEventListener("change", event => { state.entity = event.target.value; renderTimeline(); });
document.querySelector("#kind-filter").addEventListener("change", event => { state.kind = event.target.value; renderTimeline(); });
document.querySelector("#zoom").addEventListener("input", event => { state.zoom = Number(event.target.value); renderTimeline(); });

fetch("/api/data").then(response => {
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}).then(data => { state.data = data; state.runIndex = candidateRunIndex(data); renderAll(); }).catch(error => {
  document.querySelector("main").innerHTML = `<p>Could not load execution evidence: ${escapeHtml(error.message)}</p>`;
});
