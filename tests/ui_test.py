from __future__ import annotations

import json
import shutil
import socket
import sqlite3
import subprocess
import sys
import textwrap
import threading
import urllib.request
from importlib.resources import files
from pathlib import Path

import pytest

import runtime_tools.ui.server as server_module
from runtime_tools import record_process
from runtime_tools.inspect import inspect_runpack, render_summary
from runtime_tools.model import Entity, Event, Execution
from runtime_tools.storage import RunpackReader, RunpackWriter
from runtime_tools.ui import TimelineError, build_timeline_payload, create_server, serve_runpacks
from runtime_tools.ui.server import _log_line


def test_timeline_payload_exposes_normalized_evidence_and_comparison(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.runpack"
    candidate = tmp_path / "candidate.runpack"
    record_process((sys.executable, "-c", "print('same')"), baseline, name="baseline")
    record_process((sys.executable, "-c", "print('same')"), candidate, name="candidate")

    payload = build_timeline_payload(baseline, candidate)

    runs = payload["runs"]
    assert isinstance(runs, list)
    assert len(runs) == 2
    baseline_value = runs[0]
    assert isinstance(baseline_value, dict)
    summary = baseline_value["summary"]
    assert isinstance(summary, dict)
    assert summary["name"] == "baseline"
    timeline_duration_ns = baseline_value["timeline_duration_ns"]
    assert isinstance(timeline_duration_ns, int)
    assert timeline_duration_ns > 0
    events = baseline_value["events"]
    assert isinstance(events, list)
    first_event = events[0]
    assert isinstance(first_event, dict)
    assert first_event["kind"] == "process.run"
    assert first_event["clock_domain"] == "host.wall"
    assert first_event["uncertainty_ns"] is None
    measurements = baseline_value["measurements"]
    assert isinstance(measurements, list)
    assert {item["name"] for item in measurements if isinstance(item, dict)} == {
        "process.cpu.system",
        "process.cpu.user",
        "process.memory.peak",
        "process.stderr.bytes",
        "process.stdout.bytes",
        "process.wall_time",
    }
    analysis = baseline_value["analysis"]
    assert isinstance(analysis, dict)
    critical_path = analysis["critical_path"]
    assert isinstance(critical_path, dict)
    assert critical_path["event_ids"] == [first_event["id"]]
    comparison = payload["comparison"]
    assert isinstance(comparison, dict)
    assert comparison["outcome"] == "equivalent"


def test_default_summary_and_timeline_omit_attachment_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runpack = tmp_path / "private-attachment.runpack"
    secret = b"attachment-secret-that-must-not-be-rendered"
    monkeypatch.setenv("CONTRAIL_PRIVATE_ATTACHMENT_TEST", secret.decode())
    record_process(
        (
            sys.executable,
            "-c",
            "import os, sys; "
            "sys.stdout.buffer.write(os.environ['CONTRAIL_PRIVATE_ATTACHMENT_TEST'].encode())",
        ),
        runpack,
        name="private",
        capture_output_limit=1_024,
    )

    summary = render_summary(inspect_runpack(runpack), "json")
    payload = json.dumps(build_timeline_payload(runpack), sort_keys=True)

    assert secret.decode() not in summary
    assert secret.decode() not in payload
    assert json.loads(summary)["record_counts"]["attachments"] > 0
    assert json.loads(payload)["runs"][0]["summary"]["record_counts"]["attachments"] > 0


def test_timeline_derives_an_extent_for_open_executions(tmp_path: Path) -> None:
    runpack = tmp_path / "open.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("open", "open", 100, None, (), str(tmp_path), None, None, {})
        )
        writer.add_entity(Entity("worker", "worker", "worker", None, {}))
        writer.add_event(
            Event("work", "operation", "work", "worker", 110, 150, "test", None, None, {})
        )

    payload = build_timeline_payload(runpack)

    runs = payload["runs"]
    assert isinstance(runs, list)
    run = runs[0]
    assert isinstance(run, dict)
    assert run["timeline_duration_ns"] == 50
    summary = run["summary"]
    assert isinstance(summary, dict)
    assert summary["wall_time_seconds"] is None


def test_timeline_preserves_finish_only_event_evidence(tmp_path: Path) -> None:
    runpack = tmp_path / "finish-only.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(
            Execution("finish-only", "finish-only", 100, 200, (), str(tmp_path), 0, None, {})
        )
        writer.add_event(
            Event("completed", "workload.job", "completed", None, None, 175, "test", None, None, {})
        )

    payload = build_timeline_payload(runpack)

    runs = payload["runs"]
    assert isinstance(runs, list)
    run = runs[0]
    assert isinstance(run, dict)
    events = run["events"]
    assert isinstance(events, list)
    event = events[0]
    assert isinstance(event, dict)
    assert event["start_offset_ns"] is None
    assert event["duration_ns"] is None
    assert event["finish_offset_ns"] == 75


def test_timeline_preserves_a_zero_duration_extent(tmp_path: Path) -> None:
    runpack = tmp_path / "instant.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("instant", "instant", 100, 100, (), ".", 0, None, {}))
        writer.add_event(Event("instant", "event", "instant", None, 100, 100, "test", None, 0, {}))

    payload = build_timeline_payload(runpack)

    runs = payload["runs"]
    assert isinstance(runs, list)
    run = runs[0]
    assert isinstance(run, dict)
    assert run["timeline_duration_ns"] == 0


def test_local_ui_serves_packaged_assets_and_read_only_data(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")
    server = create_server(runpack, None, host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    port = server.server_address[1]
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2) as response:
            html = response.read().decode()
            assert (
                response.headers["Content-Security-Policy"]
                == "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                "frame-ancestors 'none'"
            )
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/data", timeout=2) as response:
            payload = json.load(response)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert "Can this change ship?" in html
    assert payload["runs"][0]["summary"]["name"] == "served"


def test_local_ui_escapes_terminal_controls_in_request_logs() -> None:
    line = _log_line('"%s" %s', ("GET /\x1b[31m", 400))

    assert "\x1b" not in line
    assert r"GET /\x1b[31m" in line


@pytest.mark.parametrize(
    ("bound_host", "host_header"),
    (
        ("127.0.0.1", "127.0.0.1"),
        ("127.0.0.1", "127.0.0.1:8765"),
        ("127.0.0.1", "LOCALHOST"),
        ("127.0.0.1", "localhost.:443"),
        ("::1", "[::1]"),
        ("::1", "[::1]:8765"),
    ),
)
def test_local_ui_accepts_well_formed_loopback_host_headers(
    bound_host: str, host_header: str
) -> None:
    assert server_module._request_host_allowed((host_header,), bound_host) is True


@pytest.mark.parametrize(
    "host_headers",
    (
        (),
        ("attacker.example",),
        ("localhost:invalid",),
        (f"localhost:{'1' * 4_301}",),
        ("localhost:0",),
        ("localhost:65536",),
        ("::1",),
        ("[::1",),
        ("localhost", "attacker.example"),
    ),
)
def test_local_ui_rejects_unsafe_loopback_host_headers(host_headers: tuple[str, ...]) -> None:
    assert server_module._request_host_allowed(host_headers, "127.0.0.1") is False


def test_local_ui_preserves_explicit_non_loopback_host_behavior() -> None:
    assert server_module._request_host_allowed((), "0.0.0.0") is True
    assert server_module._request_host_allowed(("example.internal:8765",), "0.0.0.0") is True


@pytest.mark.parametrize(
    ("host", "expected_family"),
    (
        ("::1", socket.AF_INET6),
        ("2001:db8::1", socket.AF_INET6),
        ("127.0.0.1", socket.AF_INET),
        ("localhost", socket.AF_INET),
        ("[::1]", socket.AF_INET),
    ),
)
def test_local_ui_selects_socket_family_from_literal_host(
    host: str, expected_family: socket.AddressFamily
) -> None:
    assert server_module._server_type(host).address_family == expected_family


def test_local_ui_closes_server_when_browser_launch_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")
    server = create_server(runpack, None, host="127.0.0.1", port=0)

    def fail_browser_launch(url: str) -> None:
        raise RuntimeError("simulated browser failure")

    monkeypatch.setattr(server_module, "create_server", lambda *args, **kwargs: server)
    monkeypatch.setattr("runtime_tools.ui.server.webbrowser.open", fail_browser_launch)

    with pytest.raises(RuntimeError, match="simulated browser failure"):
        serve_runpacks(runpack, None)

    assert server.fileno() == -1


@pytest.mark.parametrize(
    ("actual_host", "expected_url"),
    (
        ("::1", "http://[::1]:8765"),
        ("127.0.0.1", "http://127.0.0.1:8765"),
        ("localhost", "http://localhost:8765"),
    ),
)
def test_local_ui_formats_the_bound_host_as_a_browser_url(
    actual_host: str,
    expected_url: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    opened_urls: list[str] = []

    class FakeServer:
        server_address = (actual_host, 8765)

        def serve_forever(self) -> None:
            pass

        def server_close(self) -> None:
            pass

    monkeypatch.setattr(server_module, "create_server", lambda *args, **kwargs: FakeServer())
    monkeypatch.setattr("runtime_tools.ui.server.webbrowser.open", opened_urls.append)

    serve_runpacks(Path("unused.runpack"), None, host=actual_host, port=8765)

    assert opened_urls == [expected_url]
    assert capsys.readouterr().err == f"runtime UI: {expected_url}\n"


def test_packaged_ui_renders_batchscope_analysis() -> None:
    static = files("runtime_tools.ui").joinpath("static")
    html = static.joinpath("index.html").read_text()
    javascript = static.joinpath("app.js").read_text()
    stylesheet = static.joinpath("styles.css").read_text()

    assert 'id="proofline"' in html
    assert 'id="proofline-eyebrow"' in html
    assert 'id="proofline-findings"' in html
    assert 'id="comparison-overview"' in html
    assert html.index('id="overview"') < html.index('id="proofline"')
    assert html.index('id="proofline"') < html.index('id="comparison"')
    assert "renderProofline()" in javascript
    assert "renderOverview()" in javascript
    assert "state.runIndex = candidateRunIndex(data)" in javascript
    assert ".verdict-card.danger" in stylesheet
    assert ".problem-findings" in stylesheet
    assert ".passed-findings" in stylesheet
    assert ".event.evidence-highlight" in stylesheet
    assert ".event.selected" in stylesheet
    assert 'id="analysis"' in html
    assert "renderAnalysis()" in javascript
    assert "Waiting on critical path" in javascript
    assert "Path certainty" in javascript
    assert "Causal cycle detected; critical path is inferred" in javascript
    assert "run.summary.cpu_user_seconds + run.summary.cpu_system_seconds" in javascript
    assert "const cpuTime = finiteNumber(rawCpuTime) ? rawCpuTime : null" in javascript
    assert "`Signal ${-value}`" in javascript
    assert "Time spent in activities" in javascript
    assert "Math.abs(item.candidate_seconds - item.baseline_seconds) >= .001" in javascript
    assert "No duration changes of at least 1 ms" in javascript
    assert "fmtActivityDuration(event.duration_ns)" in javascript
    assert "run.summary.record_counts.attachments" in javascript
    assert "Evidence warning" in javascript
    assert "diff.candidate_annotation_error" in javascript
    assert "diff.environment_changes" in javascript
    assert "diff.entity_count_changes" in javascript
    assert "diff.operation_concurrency_changes" in javascript
    assert "diff.operation_error_count_changes" in javascript
    assert "diff.baseline_incomplete_streams" in javascript
    assert "diff.candidate_missing_causal_references" in javascript
    assert "diff.candidate_dropped_attribute_count" in javascript
    assert "run.summary.dropped_attribute_count" in javascript
    assert "run.summary[`${stream}_relay_error`]" in javascript
    assert "diff.baseline_stdout_relay_error" in javascript
    assert "diff.candidate_stderr_relay_error" in javascript
    assert "Remaining after compute" in javascript
    assert "No likely slowdown was identified" in javascript
    assert "Untimed evidence" in javascript
    assert "event.start_offset_ns != null" in javascript
    assert "run.timeline_duration_ns" in javascript
    assert "const scaleNs = timelineDurationNs > 0 ? timelineDurationNs : 1" in javascript
    assert "How it fits" in javascript
    assert "Clock source" in javascript
    assert "Resource measurements" in javascript
    assert "escapeDisplayControls(value).replace" in javascript
    assert r"[\p{Cc}\p{Cf}\p{Cs}]" in javascript


def test_packaged_ui_stacks_selectable_overlaps_and_clears_stale_evidence() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required to exercise the packaged browser script")
    javascript = files("runtime_tools.ui").joinpath("static", "app.js")
    harness = textwrap.dedent(
        r"""
        const fs = require("fs");
        const vm = require("vm");

        class Element {
          constructor(selector, dataset = {}) {
            this.selector = selector;
            this.dataset = dataset;
            this.children = [];
            this.listeners = {};
            this.attributes = {};
            this.value = "";
            this.scrolled = false;
            this.classes = new Set();
            this.classList = {
              add: (...names) => names.forEach(name => this.classes.add(name)),
              remove: (...names) => names.forEach(name => this.classes.delete(name)),
              toggle: (name, force) => force ? this.classes.add(name) : this.classes.delete(name),
              contains: name => this.classes.has(name),
            };
            this._innerHTML = "";
          }

          set innerHTML(value) {
            this._innerHTML = value;
            this.children = [];
            if (!["#run-switcher", "#timeline", "#proofline-findings"].includes(
              this.selector,
            )) return;
            for (const match of value.matchAll(/<button\b([^>]*)>/g)) {
              const rawAttributes = match[1];
              const read = name => {
                const found = rawAttributes.match(new RegExp(name + '="([^"]*)"'));
                return found ? found[1] : null;
              };
              const key = this.selector === "#run-switcher"
                ? "run"
                : this.selector === "#timeline" ? "id" : "finding";
              const datum = read(`data-${key}`);
              if (datum === null) continue;
              const child = new Element("button", { [key]: datum });
              const classes = read("class");
              if (classes) classes.split(/\s+/).forEach(name => child.classes.add(name));
              for (const name of ["aria-label", "aria-pressed"]) {
                const attribute = read(name);
                if (attribute !== null) child.attributes[name] = attribute;
              }
              this.children.push(child);
            }
          }

          get innerHTML() { return this._innerHTML; }
          addEventListener(name, listener) { this.listeners[name] = listener; }
          querySelectorAll() { return this.children; }
          setAttribute(name, value) { this.attributes[name] = String(value); }
          focus() { document.activeElement = this; }
          scrollIntoView() { this.scrolled = true; }
          trigger(name, value = null) {
            if (!this.listeners[name]) {
              throw new Error(`missing ${name} listener on ${this.selector}`);
            }
            this.listeners[name]({ target: value === null ? this : { value } });
          }
        }

        const selectors = [
          "#loading-state", "#overview", "#comparison-overview",
          "#run-switcher", "#proofline", "#proofline-eyebrow", "#proofline-heading",
          "#proofline-summary", "#proofline-findings",
          "#summary", "#analysis", "#comparison",
          "#selected-run-heading", "#selected-run-description",
          "#comparison-grid", "#entity-filter", "#kind-filter", "#zoom",
          "#timeline", "#axis", "#detail", "main",
        ];
        const nodes = Object.fromEntries(
          selectors.map(selector => [selector, new Element(selector)]),
        );
        globalThis.document = {
          activeElement: null,
          querySelector(selector) {
            if (!nodes[selector]) throw new Error(`unexpected selector: ${selector}`);
            return nodes[selector];
          },
          querySelectorAll(selector) {
            if (selector !== ".event, .untimed-event") {
              throw new Error(`unexpected selector list: ${selector}`);
            }
            return nodes["#timeline"].children;
          },
        };
        globalThis.fetch = () => new Promise(() => {});

        const run = (name, eventId) => ({
          summary: {
            id: `${name}-execution`, name, exit_code: 0, wall_time_seconds: 1,
            cpu_user_seconds: null,
            cpu_system_seconds: null, peak_memory_bytes: null,
            record_counts: { attachments: 0 }, missing_causal_references: 0,
            dropped_attribute_count: 0, stdout_complete: true, stderr_complete: true,
            stdout_relay_error: name === "baseline" ? "sink <closed>" : null,
            stderr_relay_error: name === "candidate" ? "sink <closed>" : null,
          },
          timeline_duration_ns: 1000000,
          entities: [{ id: "worker", kind: "worker", name: `${name} worker` }],
          events: [{
            id: eventId, entity_id: "worker", kind: "operation", name: `${name} event`,
            clock_domain: "test", uncertainty_ns: 0,
            start_offset_ns: name === "baseline" ? 0 : null,
            finish_offset_ns: name === "baseline" ? 10 : 500000,
            duration_ns: name === "baseline" ? 10 : null, attributes: {},
          }, ...(name === "baseline" ? [{
            id: "overlap-id", entity_id: "worker", kind: "operation",
            name: "overlapping event", clock_domain: "test", uncertainty_ns: 0,
            start_offset_ns: 5, finish_offset_ns: 15, duration_ns: 10, attributes: {},
          }, {
            id: "reuse-id", entity_id: "worker", kind: "operation",
            name: "non-overlapping event", clock_domain: "test", uncertainty_ns: 0,
            start_offset_ns: 10, finish_offset_ns: 20, duration_ns: 10, attributes: {},
          }] : name === "candidate" ? [{
            id: "candidate-timed", entity_id: "worker", kind: "operation",
            name: "candidate timed evidence", clock_domain: "test", uncertainty_ns: 0,
            start_offset_ns: 100, finish_offset_ns: 200, duration_ns: 100, attributes: {},
          }, {
            id: "candidate-unrelated", entity_id: "worker", kind: "operation",
            name: "candidate unrelated", clock_domain: "test", uncertainty_ns: 0,
            start_offset_ns: 300, finish_offset_ns: 400, duration_ns: 100, attributes: {},
          }] : [])],
          measurements: [], edges: [],
          analysis: { critical_path: null, lifecycle: [], throughput: null, bottlenecks: [] },
        });
        globalThis.nodes = nodes;
        globalThis.run = run;
        const source = fs.readFileSync(process.argv[1], "utf8");
        vm.runInThisContext(source + `
          state.data = {
            runs: [
              run("baseline", "baseline-id"),
              run("decoy", "decoy-id"),
              run("candidate", "candidate-id"),
            ],
            comparison: {
              baseline: { exit_code: 0 }, candidate: { exit_code: 0 },
              outcome: "equivalent", output_equivalent: true, stderr_equivalent: true,
              operation_errors_equivalent: true,
              wall_time: { baseline: 1, candidate: 1 },
              cpu_time: { baseline: null, candidate: null },
              critical_path: { baseline: null, candidate: null },
              baseline_annotation_error: null, candidate_annotation_error: null,
              baseline_stdout_relay_error: "sink <closed>",
              baseline_stderr_relay_error: null,
              candidate_stdout_relay_error: null,
              candidate_stderr_relay_error: "sink <closed>",
              baseline_incomplete_streams: [], candidate_incomplete_streams: [],
              baseline_missing_causal_references: 0,
              candidate_missing_causal_references: 0,
              baseline_dropped_attribute_count: 0,
              candidate_dropped_attribute_count: 0,
              operation_count_changes: [], operation_error_count_changes: [],
              entity_count_changes: [], operation_concurrency_changes: [],
              operation_duration_changes: [], edge_count_changes: [],
              environment_changes: [],
            },
            proofline: {
              source: "contracts <unsafe>.yaml",
              verification: {
                candidate_id: "candidate-execution", claim_count: 3, passed: false,
              },
              findings: [{
                result_index: 7, contract: "contract <unsafe>", name: 'count "claim"',
                type: "max_operation_count", status: "fail", expected: "at most <2>",
                observed: "candidate & baseline differ", focus: "candidate_events",
                selection_id: "operation-selection", evidence: [{
                  diff_path: "/operation_count_changes/0<&",
                  fact: { candidate: "<3>", baseline: 1 },
                }],
              }, {
                result_index: 8, contract: "runtime", name: "wall time",
                type: "max_runtime_regression", status: "unverifiable",
                expected: "known timing", observed: "missing", focus: "candidate_summary",
                evidence: [{ diff_path: "/wall_time", fact: 17 }],
              }, {
                result_index: 9, contract: "dependency", name: "missing selection",
                type: "forbid_new_dependency", status: "fail", expected: "no edge",
                observed: "edge added", focus: "candidate_events", selection_id: "absent",
                evidence: [{ diff_path: "/edge_count_changes/0", fact: null }],
              }],
              selections: {
                "operation-selection": {
                  relationship: "operation <match>",
                  candidate_event_ids: ["candidate-id", "candidate-timed"],
                  matched_event_count: 2,
                  truncated: false,
                },
              },
            },
          };
          renderAll();
          const prooflineHtml = nodes["#proofline-findings"].innerHTML;
          if (nodes["#proofline-findings"].children.length !== 3) {
            throw new Error("every Proofline finding was not actionable");
          }
          if (!nodes["#proofline-summary"].innerHTML.includes("2 failed") ||
              !nodes["#proofline-summary"].innerHTML.includes("contracts &lt;unsafe&gt;.yaml")) {
            throw new Error("the safeguard summary was incomplete or unsafe");
          }
          if (!prooflineHtml.includes("Count &quot;claim&quot;") ||
              !prooflineHtml.includes("at most &lt;2&gt;")) {
            throw new Error("unsafe finding fields were not escaped");
          }
          if (!prooflineHtml.includes("/operation_count_changes/0&lt;&amp;")) {
            throw new Error("the exact escaped diff path was not rendered");
          }
          if (!prooflineHtml.includes(
            "{&quot;candidate&quot;:&quot;&lt;3&gt;&quot;,&quot;baseline&quot;:1}",
          )) {
            throw new Error("the exact evaluated fact was not rendered");
          }
          if (!prooflineHtml.includes("<code>17</code>")) {
            throw new Error("scalar evaluated facts did not use the readable fallback");
          }
          if (nodes["#proofline-findings"].children.some(
            button => button.attributes["aria-pressed"] !== "false" ||
              !button.attributes["aria-label"],
          )) {
            throw new Error("finding actions were not accessibly labelled and unpressed");
          }
          const timelineHtml = nodes["#timeline"].innerHTML;
          const eventStyle = eventId => {
            const match = timelineHtml.match(
              new RegExp('data-id="' + eventId + '"[^>]*style="([^"]+)"'),
            );
            if (!match) throw new Error("missing rendered event: " + eventId);
            return match[1];
          };
          if (!timelineHtml.includes('<div class="track" style="height:86px">')) {
            throw new Error("overlapping intervals did not expand the entity track");
          }
          if (!eventStyle("baseline-id").includes("top:9px")) {
            throw new Error("first interval was not placed in the first vertical lane");
          }
          if (!eventStyle("overlap-id").includes("top:45px")) {
            throw new Error("overlapping interval was not placed in a separate lane");
          }
          if (!eventStyle("reuse-id").includes("top:9px")) {
            throw new Error("non-overlapping interval did not reuse an available lane");
          }
          nodes["#timeline"].children.find(
            child => child.dataset.id === "overlap-id",
          ).trigger("click");
          if (!nodes["#detail"].innerHTML.includes("overlapping event")) {
            throw new Error("event in the stacked lane was not selectable");
          }
          if (!nodes["#summary"].innerHTML.includes("stdout relay failed: sink &lt;closed&gt;")) {
            throw new Error("single-run relay warning was not safely rendered");
          }
          const comparisonHtml = nodes["#comparison-grid"].innerHTML;
          if (!comparisonHtml.includes(
            "Baseline stdout relay failed: sink &lt;closed&gt;",
          )) {
            throw new Error("baseline comparison relay warning was not safely rendered");
          }
          if (!comparisonHtml.includes(
            "Candidate stderr relay failed: sink &lt;closed&gt;",
          )) {
            throw new Error("candidate comparison relay warning was not safely rendered");
          }
          document.querySelectorAll(".event, .untimed-event").find(
            button => button.dataset.id === "baseline-id",
          ).trigger("click");
          if (!nodes["#detail"].innerHTML.includes("baseline event")) {
            throw new Error("baseline selection was not rendered");
          }

          nodes["#run-switcher"].children[2].trigger("click");
          if (!nodes["#summary"].innerHTML.includes("stderr relay failed: sink &lt;closed&gt;")) {
            throw new Error("candidate relay warning was not safely rendered");
          }
          if (nodes["#detail"].innerHTML.includes("baseline event")) {
            throw new Error("run switch retained stale evidence");
          }
          if (!nodes["#detail"].innerHTML.includes("Choose an activity")) {
            throw new Error("run switch did not reset detail");
          }

          document.querySelectorAll(".event, .untimed-event").find(
            button => button.dataset.id === "candidate-id",
          ).trigger("click");
          if (!nodes["#detail"].innerHTML.includes("candidate event")) {
            throw new Error("candidate selection was not rendered");
          }
          if (!nodes["#timeline"].innerHTML.includes(
            'class="untimed-event" data-id="candidate-id"',
          )) {
            throw new Error("finish-only event was assigned a fabricated start position");
          }
          if (!nodes["#detail"].innerHTML.includes(
            "<dt>Finish after run began</dt><dd>500.0 µs</dd>",
          )) {
            throw new Error("finish-only evidence did not render its known finish");
          }
          if (!nodes["#detail"].innerHTML.includes(
            "<dt>Start after run began</dt><dd>unknown</dd>",
          )) {
            throw new Error("finish-only evidence fabricated a known start");
          }
          nodes["#kind-filter"].trigger("change", "hidden-kind");
          if (nodes["#detail"].innerHTML.includes("candidate event")) {
            throw new Error("filter retained hidden evidence");
          }
          if (!nodes["#detail"].innerHTML.includes("Choose an activity")) {
            throw new Error("filter did not reset detail");
          }

          nodes["#run-switcher"].children[0].trigger("click");
          document.querySelectorAll(".event, .untimed-event").find(
            button => button.dataset.id === "baseline-id",
          ).trigger("click");
          state.entity = "stale-entity";
          state.kind = "stale-kind";
          nodes["#entity-filter"].value = "stale-entity";
          nodes["#kind-filter"].value = "stale-kind";
          nodes["#proofline-findings"].children[0].trigger("click");

          if (state.runIndex !== 2 || currentRun().summary.id !== "candidate-execution") {
            throw new Error("finding navigation did not resolve the candidate by execution ID");
          }
          if (state.entity || state.kind || nodes["#entity-filter"].value ||
              nodes["#kind-filter"].value) {
            throw new Error("finding navigation retained stale timeline filters");
          }
          const eventButton = eventId => nodes["#timeline"].children.find(
            button => button.dataset.id === eventId,
          );
          const untimedEvidence = eventButton("candidate-id");
          const timedEvidence = eventButton("candidate-timed");
          const unrelatedEvidence = eventButton("candidate-unrelated");
          if (!untimedEvidence.classList.contains("evidence-highlight") ||
              !timedEvidence.classList.contains("evidence-highlight")) {
            throw new Error("timed and untimed backend-selected evidence was not highlighted");
          }
          if (!unrelatedEvidence.classList.contains("evidence-dimmed") ||
              unrelatedEvidence.classList.contains("evidence-highlight")) {
            throw new Error("unrelated evidence was not dimmed without being highlighted");
          }
          if (!untimedEvidence.classList.contains("selected") ||
              timedEvidence.classList.contains("selected")) {
            throw new Error("the first backend-ordered event was not the exact selection");
          }
          if (document.activeElement !== untimedEvidence || !untimedEvidence.scrolled) {
            throw new Error("the first backend-ordered event was not focused and scrolled");
          }
          if (!nodes["#detail"].innerHTML.includes("candidate event") ||
              nodes["#detail"].innerHTML.includes("baseline event")) {
            throw new Error("finding navigation retained stale detail or showed the wrong event");
          }
          if (nodes["#proofline-findings"].children[0].attributes["aria-pressed"] !== "true") {
            throw new Error("the active finding did not expose its pressed state");
          }
          if (!nodes["#proofline-findings"].innerHTML.includes("View 2 related activities")) {
            throw new Error("the related-activity action was not rendered");
          }

          nodes["#proofline-findings"].children[1].trigger("click");
          if (document.activeElement !== nodes["#summary"] || !nodes["#summary"].scrolled) {
            throw new Error("candidate summary findings did not focus and scroll the summary");
          }
          if (state.highlightedEventIds.length || state.selectedEventId !== null ||
              !nodes["#detail"].innerHTML.includes("Choose an activity")) {
            throw new Error("candidate summary findings retained event evidence state");
          }
          if (nodes["#timeline"].children.some(button =>
            button.classList.contains("evidence-highlight") ||
            button.classList.contains("evidence-dimmed") ||
            button.classList.contains("selected"))) {
            throw new Error("candidate summary findings invented an event match");
          }

          document.activeElement = null;
          nodes["#proofline-findings"].children[2].trigger("click");
          if (document.activeElement !== nodes["#summary"] || state.highlightedEventIds.length) {
            throw new Error(
              "a missing backend selection did not fall back to the candidate summary",
            );
          }

          nodes["#proofline-findings"].children[0].trigger("click");
          nodes["#run-switcher"].children[0].trigger("click");
          if (state.activeFindingIndex !== null || state.highlightedEventIds.length ||
              state.selectedEventId !== null ||
              !nodes["#detail"].innerHTML.includes("Choose an activity")) {
            throw new Error("manual run switching retained Proofline navigation state");
          }
          if (nodes["#timeline"].children.some(button =>
            button.classList.contains("evidence-highlight") ||
            button.classList.contains("evidence-dimmed") ||
            button.classList.contains("selected"))) {
            throw new Error("manual run switching retained Proofline timeline styling");
          }
          if (nodes["#proofline-findings"].children.some(
            button => button.attributes["aria-pressed"] !== "false",
          )) {
            throw new Error("manual run switching retained an active finding");
          }
          if (nodes["#run-switcher"].children[0].attributes["aria-pressed"] !== "true") {
            throw new Error("the manually selected run did not expose its pressed state");
          }
          if (document.activeElement !== nodes["#run-switcher"].children[0]) {
            throw new Error("manual run switching did not preserve keyboard focus");
          }

          state.data.proofline.source = "report";
          state.data.proofline.findings.forEach(finding => {
            finding.report_assurance = "artifact_bound_policy_replayed";
          });
          renderProofline();
          if (nodes["#proofline-eyebrow"].textContent !==
                "RELEASE SAFEGUARDS" ||
              !nodes["#proofline-summary"].innerHTML.includes(
                "saved review was rechecked against these exact execution files",
              ) || nodes["#proofline-summary"].innerHTML.toLowerCase().includes("authentic")) {
            throw new Error("artifact-bound checking was not explained plainly and precisely");
          }

          state.data.proofline.findings.forEach(finding => {
            finding.report_assurance = "policy_replayed_against_current_evidence";
          });
          renderProofline();
          if (!nodes["#proofline-summary"].innerHTML.includes(
                "saved rules were run again against the current execution files",
              )) {
            throw new Error("replayed checking was not explained plainly");
          }

          state.data.proofline.findings.forEach(finding => {
            finding.report_assurance = "report_authored_policy_runtime_consistent";
          });
          renderProofline();
          if (nodes["#proofline-heading"].textContent !==
                "Why this version is blocked" ||
              !nodes["#proofline-summary"].innerHTML.includes(
                "saved results are consistent with the current execution files",
              )) {
            throw new Error("consistency checking was not explained plainly");
          }
        `, { filename: process.argv[1] });
        """
    )

    result = subprocess.run(
        (node, "-e", harness, str(javascript)),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_timeline_rejects_oversized_artifact_before_analysis(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process(
        (
            sys.executable,
            "-c",
            "from runtime_tools import runtime; runtime.event('extra')",
        ),
        runpack,
        name="served",
    )

    with pytest.raises(TimelineError, match="timeline has 2 events; local UI limit is 1"):
        build_timeline_payload(runpack, event_limit=1)


def test_timeline_limit_and_rows_share_one_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runpack = tmp_path / "run.runpack"
    with RunpackWriter(runpack) as writer:
        writer.add_execution(Execution("run", "run", 0, 10, (), str(tmp_path), 0, None, {}))
        writer.add_event(Event("first", "operation", "first", None, 0, 1, "test", None, 0, {}))
    real_close = RunpackReader.close
    mutated = False

    def mutate_after_first_snapshot(reader: RunpackReader) -> None:
        nonlocal mutated
        real_close(reader)
        if reader.path == runpack.resolve() and not mutated:
            mutated = True
            with RunpackWriter.open_existing(runpack) as writer:
                writer.add_event(
                    Event("late", "operation", "late", None, 2, 3, "test", None, 1, {})
                )

    monkeypatch.setattr(RunpackReader, "close", mutate_after_first_snapshot)

    payload = build_timeline_payload(runpack, event_limit=1)

    runs = payload["runs"]
    assert isinstance(runs, list)
    run = runs[0]
    assert isinstance(run, dict)
    events = run["events"]
    assert isinstance(events, list)
    assert [event["id"] for event in events if isinstance(event, dict)] == ["first"]
    with sqlite3.connect(runpack) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 2


def test_timeline_rejects_boolean_limits_before_loading_artifacts(tmp_path: Path) -> None:
    with pytest.raises(TimelineError, match="timeline limits must be integers"):
        build_timeline_payload(tmp_path / "missing.runpack", event_limit=True)


def test_timeline_rejects_oversized_edge_sets_before_analysis(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process(
        (
            sys.executable,
            "-c",
            "from runtime_tools import runtime; runtime.event('one'); runtime.event('two')",
        ),
        runpack,
        name="served",
    )

    with pytest.raises(TimelineError, match="timeline has 2 edges; local UI limit is 1"):
        build_timeline_payload(runpack, edge_limit=1)


def test_timeline_rejects_oversized_measurement_sets_before_analysis(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")

    with pytest.raises(TimelineError, match="timeline has 6 measurements; local UI limit is 1"):
        build_timeline_payload(runpack, measurement_limit=1)


def test_timeline_rejects_large_aggregate_normalized_json_before_loading_rows(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")

    with pytest.raises(
        TimelineError,
        match=r"timeline has [\d,]+ normalized JSON bytes; local UI limit is 1",
    ):
        build_timeline_payload(runpack, json_byte_limit=1)


def test_timeline_rejects_large_aggregate_normalized_text_before_loading_rows(
    tmp_path: Path,
) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")

    with pytest.raises(
        TimelineError,
        match=r"timeline has [\d,]+ normalized text bytes; local UI limit is 1",
    ):
        build_timeline_payload(runpack, text_byte_limit=1)


def test_timeline_rejects_large_serialized_payload(tmp_path: Path) -> None:
    runpack = tmp_path / "run.runpack"
    record_process((sys.executable, "-c", "pass"), runpack, name="served")

    with pytest.raises(
        TimelineError,
        match=r"timeline payload is [\d,]+ bytes; local UI limit is 1",
    ):
        build_timeline_payload(runpack, payload_byte_limit=1)


@pytest.mark.parametrize(
    ("host", "port", "message"),
    (
        ("", 0, "host must be a non-empty string"),
        ("127.0.0.1", True, "port must be an integer"),
        ("127.0.0.1", 70_000, "port must be between 0 and 65535"),
    ),
)
def test_local_ui_validates_bind_arguments_before_loading_artifacts(
    tmp_path: Path,
    host: str,
    port: int,
    message: str,
) -> None:
    with pytest.raises(TimelineError, match=message):
        create_server(tmp_path / "missing.runpack", None, host=host, port=port)
