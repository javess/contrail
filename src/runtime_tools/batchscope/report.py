"""Render BatchScope analysis facts."""

from __future__ import annotations

import json

from runtime_tools.batchscope.analysis import (
    BatchAnalysis,
    ProfileNormalizationMetrics,
    ProfilePublicationMetrics,
    ProfileSnapshotMetrics,
    PythonHotspot,
    PythonProfileCoverage,
    PythonSampleHotspot,
)
from runtime_tools.deep_profile import MAX_DEEP_PROFILE_FILES
from runtime_tools.terminal import terminal_text

MAX_TEXT_SECTION_ITEMS = 100
MAX_TEXT_ATTRIBUTED_HOTSPOTS = 10
MAX_TEXT_PROCESS_CONTRIBUTIONS = 5


def render_analysis(analysis: BatchAnalysis, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(analysis.as_json_value(), allow_nan=False, indent=2, sort_keys=True)
    lines = [
        "BATCHSCOPE",
        f"run:   {terminal_text(analysis.name)} ({terminal_text(analysis.execution_id[:8])})",
        f"total: {_duration(analysis.total_seconds)}",
    ]
    lines.extend(("", "Bottleneck" if len(analysis.bottlenecks) == 1 else "Bottlenecks"))
    if not analysis.bottlenecks:
        lines.append("  none classified from available evidence")
    for bottleneck in analysis.bottlenecks[:MAX_TEXT_SECTION_ITEMS]:
        lines.append(f"  {terminal_text(bottleneck.classification)} ({bottleneck.confidence:.0%})")
        lines.append(f"    {terminal_text(bottleneck.evidence)}")
    _append_omitted(lines, len(analysis.bottlenecks))
    if analysis.process_observer is not None:
        observer = analysis.process_observer
        lines.extend(("", "Process tree capture", "  controller-side; no workload injection"))
        if observer.status in {"unavailable", "invalid"}:
            lines.append(f"  {observer.status}: {terminal_text(observer.error or 'no samples')}")
        else:
            process_label = "process" if observer.process_count == 1 else "processes"
            descendant_label = (
                "descendant" if observer.descendant_process_count == 1 else "descendants"
            )
            lines.append(
                f"  {observer.status}: {observer.process_count:,} {process_label} "
                f"({observer.descendant_process_count:,} {descendant_label}), "
                f"{observer.sample_count:,} resource samples across "
                f"{observer.poll_count:,} polls"
            )
            lines.append(f"  interval {_duration(observer.interval_seconds)}")
            if observer.truncated:
                lines.append(
                    f"  truncated: {observer.dropped_process_count:,} processes were omitted"
                )
        if analysis.process_hotspots:
            lines.extend(("", "Process resource peaks"))
            for hotspot in analysis.process_hotspots[:MAX_TEXT_SECTION_ITEMS]:
                parent = (
                    f", parent {terminal_text(hotspot.parent_name)}"
                    if hotspot.parent_name is not None
                    else ""
                )
                lines.append(
                    f"  {terminal_text(hotspot.name)} (pid {hotspot.pid}{parent})"
                    f"  CPU {_duration(hotspot.cpu_seconds)},"
                    f" peak RSS {_bytes(hotspot.peak_rss_bytes)},"
                    f" {hotspot.sample_count:,} samples"
                )
            _append_omitted(lines, len(analysis.process_hotspots))
    if analysis.semantic_capture is not None:
        semantic = analysis.semantic_capture
        lines.extend(
            (
                "",
                "Automatic subprocess capture",
                "  zero-code Python boundary observer; arguments, environment, and cwd omitted",
            )
        )
        if semantic.status in {"unavailable", "invalid"}:
            lines.append(f"  {semantic.status}: no trustworthy subprocess boundaries")
        else:
            process_label = "process" if semantic.process_count == 1 else "processes"
            call_label = "boundary" if semantic.subprocess_count == 1 else "boundaries"
            lines.append(
                f"  {semantic.status}: {semantic.subprocess_count:,} {call_label} across "
                f"{semantic.process_count:,} Python {process_label}"
            )
            if semantic.dropped_subprocess_count:
                lines.append(
                    f"  truncated: {semantic.dropped_subprocess_count:,} subprocess boundaries "
                    "were omitted"
                )
            if semantic.callback_error_count:
                lines.append(
                    f"  observer errors: {semantic.callback_error_count:,}; workload outcome "
                    "was left unchanged"
                )
        caller = semantic.caller_attribution_status
        if caller == "complete":
            lines.append(
                f"  caller attribution complete: "
                f"{semantic.attributed_subprocess_count:,} / "
                f"{semantic.subprocess_count:,} boundaries across "
                f"{semantic.caller_count:,} callsites"
            )
        elif caller == "unavailable":
            lines.append("  caller attribution unavailable")
        else:
            lines.append(
                f"  caller attribution {caller}: "
                f"{semantic.attributed_subprocess_count:,} / "
                f"{semantic.subprocess_count:,} boundaries"
            )
        if semantic.caller_callback_error_count:
            lines.append(
                f"  caller observer errors: {semantic.caller_callback_error_count:,}; "
                "subprocess evidence was retained"
            )
        if analysis.subprocess_calls:
            lines.extend(("", "Subprocess boundaries"))
            ordered_calls = sorted(
                analysis.subprocess_calls,
                key=lambda call: (
                    call.outcome == "exited" and call.exit_code == 0,
                    -(call.duration_seconds or 0),
                    call.started_at_ns,
                ),
            )
            for call in ordered_calls[:MAX_TEXT_SECTION_ITEMS]:
                if call.outcome == "exited":
                    outcome = f"exit {call.exit_code}"
                elif call.outcome == "launch_error":
                    outcome = f"launch error {terminal_text(call.error_type or 'unknown')}"
                else:
                    outcome = "completion unknown"
                child = ""
                if call.child_pid is not None:
                    observed = (
                        ", process-tree matched" if call.child_observed_in_process_tree else ""
                    )
                    child = f", child pid {call.child_pid}{observed}"
                shell = (
                    ", shell"
                    if call.shell is True
                    else ", shell use unknown"
                    if call.shell is None
                    else ""
                )
                lines.append(
                    f"  {terminal_text(call.name)}  {_duration(call.duration_seconds)}, "
                    f"{outcome}, parent pid {call.parent_pid} [{call.role}]"
                    f"{child}{shell}"
                )
                if call.caller is not None:
                    lines.append(
                        f"    called by {terminal_text(call.caller.name)} "
                        f"[{call.caller.observation}, {call.caller.confidence:.0%}] "
                        f"at {terminal_text(call.caller.filename)}:"
                        f"{call.caller.firstlineno}"
                    )
            _append_omitted(lines, len(analysis.subprocess_calls))
    if analysis.http_capture is not None:
        http = analysis.http_capture
        lines.extend(
            (
                "",
                "Automatic HTTP client capture",
                "  zero-code Python HTTP boundary observer; server address, path, query, "
                "headers, bodies, and credentials omitted",
                "  duration ends when response headers arrive",
            )
        )
        if http.adapters:
            lines.append(f"  active adapters: {', '.join(http.adapters)}")
        if http.status in {"unavailable", "invalid"}:
            lines.append(f"  {http.status}: no trustworthy HTTP request boundaries")
        else:
            process_label = "process" if http.process_count == 1 else "processes"
            request_label = "request" if http.request_count == 1 else "requests"
            lines.append(
                f"  {http.status}: {http.request_count:,} {request_label} across "
                f"{http.process_count:,} Python {process_label}"
            )
            if http.dropped_request_count:
                lines.append(
                    f"  truncated: {http.dropped_request_count:,} HTTP requests were omitted"
                )
            if http.callback_error_count:
                lines.append(
                    f"  observer errors: {http.callback_error_count:,}; workload outcome "
                    "was left unchanged"
                )
        caller = http.caller_attribution_status
        if caller == "complete":
            lines.append(
                f"  caller attribution complete: {http.attributed_request_count:,} / "
                f"{http.request_count:,} requests across {http.caller_count:,} callsites"
            )
        elif caller == "unavailable":
            lines.append("  caller attribution unavailable")
        else:
            lines.append(
                f"  caller attribution {caller}: {http.attributed_request_count:,} / "
                f"{http.request_count:,} requests"
            )
        if http.caller_callback_error_count:
            lines.append(
                f"  caller observer errors: {http.caller_callback_error_count:,}; "
                "HTTP evidence was retained"
            )
        if analysis.http_requests:
            lines.extend(("", "Outbound HTTP requests"))
            ordered_requests = sorted(
                analysis.http_requests,
                key=lambda request: (
                    request.outcome == "response"
                    and request.status_code is not None
                    and request.status_code < 500,
                    -(request.duration_seconds or 0),
                    request.started_at_ns,
                ),
            )
            for request in ordered_requests[:MAX_TEXT_SECTION_ITEMS]:
                if request.outcome == "response":
                    outcome = (
                        f"status {request.status_code}"
                        if request.status_code is not None
                        else "response status unknown"
                    )
                elif request.outcome == "request_error":
                    outcome = f"request error {terminal_text(request.error_type or 'unknown')}"
                elif request.outcome == "closed":
                    outcome = "connection closed before response"
                else:
                    outcome = "completion unknown"
                port = f":{request.server_port}" if request.server_port is not None else ""
                lines.append(
                    f"  {terminal_text(request.method)} "
                    f"{request.scheme}://<redacted>{port}  "
                    f"{_duration(request.duration_seconds)}, {outcome}, "
                    f"pid {request.parent_pid} [{request.role}], adapter {request.adapter}"
                )
                if request.caller is not None:
                    lines.append(
                        f"    called by {terminal_text(request.caller.name)} "
                        f"[{request.caller.observation}, {request.caller.confidence:.0%}] "
                        f"at {terminal_text(request.caller.filename)}:"
                        f"{request.caller.firstlineno}"
                    )
            _append_omitted(lines, len(analysis.http_requests))
    if analysis.network_capture is not None:
        network = analysis.network_capture
        lines.extend(
            (
                "",
                "Automatic network connection capture",
                "  zero-code Python connection observer; server address, Unix path, and "
                "credentials omitted",
                "  duration ends when the connection or asyncio transport is ready",
            )
        )
        if network.adapters:
            lines.append(f"  active adapters: {', '.join(network.adapters)}")
        if network.status in {"unavailable", "invalid"}:
            lines.append(f"  {network.status}: no trustworthy network connection boundaries")
        else:
            process_label = "process" if network.process_count == 1 else "processes"
            connection_label = "connection" if network.connection_count == 1 else "connections"
            lines.append(
                f"  {network.status}: {network.connection_count:,} {connection_label} across "
                f"{network.process_count:,} Python {process_label}"
            )
            if network.dropped_connection_count:
                lines.append(
                    f"  truncated: {network.dropped_connection_count:,} connections were omitted"
                )
            if network.callback_error_count:
                lines.append(
                    f"  observer errors: {network.callback_error_count:,}; workload outcome "
                    "was left unchanged"
                )
        caller = network.caller_attribution_status
        if caller == "complete":
            lines.append(
                f"  caller attribution complete: {network.attributed_connection_count:,} / "
                f"{network.connection_count:,} connections across "
                f"{network.caller_count:,} callsites"
            )
        elif caller == "unavailable":
            lines.append("  caller attribution unavailable")
        else:
            lines.append(
                f"  caller attribution {caller}: {network.attributed_connection_count:,} / "
                f"{network.connection_count:,} connections"
            )
        if network.caller_callback_error_count:
            lines.append(
                f"  caller observer errors: {network.caller_callback_error_count:,}; "
                "connection evidence was retained"
            )
        if analysis.network_connection_hotspots:
            lines.extend(("", "Network connection hotspots"))
            for hotspot in analysis.network_connection_hotspots[:MAX_TEXT_SECTION_ITEMS]:
                if hotspot.caller is None:
                    caller = "unattributed"
                else:
                    caller = (
                        f"{terminal_text(hotspot.caller.name)} "
                        f"[{hotspot.caller.observation}, {hotspot.caller.confidence:.0%}]"
                    )
                connection_label = "attempt" if hotspot.connection_count == 1 else "attempts"
                lines.append(f"  {caller}, adapter {terminal_text(hotspot.adapter)}")
                lines.append(
                    f"    {hotspot.connection_count:,} {connection_label}: "
                    f"{hotspot.connected_connection_count:,} connected, "
                    f"{hotspot.failed_connection_count:,} failed, "
                    f"{hotspot.unfinished_connection_count:,} unfinished; "
                    f"setup {_duration(hotspot.total_duration_seconds)} total, "
                    f"max {_duration(hotspot.max_duration_seconds)}"
                )
            _append_omitted(lines, network.connection_hotspot_count)
        if analysis.network_connections:
            lines.extend(("", "Outbound network connections"))
            ordered_connections = sorted(
                analysis.network_connections,
                key=lambda connection: (
                    connection.outcome == "connected",
                    -(connection.duration_seconds or 0),
                    connection.started_at_ns,
                ),
            )
            for connection in ordered_connections[:MAX_TEXT_SECTION_ITEMS]:
                if connection.outcome == "connected":
                    outcome = "connected"
                elif connection.outcome == "connect_error":
                    outcome = f"connect error {terminal_text(connection.error_type or 'unknown')}"
                else:
                    outcome = "completion unknown"
                port = f":{connection.server_port}" if connection.server_port is not None else ""
                tls = ", TLS requested" if connection.tls_requested is True else ""
                lines.append(
                    f"  {connection.transport}://<redacted>{port}  "
                    f"{_duration(connection.duration_seconds)}, {outcome}{tls}, "
                    f"pid {connection.parent_pid} [{connection.role}], "
                    f"adapter {connection.adapter}"
                )
                if connection.caller is not None:
                    lines.append(
                        f"    called by {terminal_text(connection.caller.name)} "
                        f"[{connection.caller.observation}, "
                        f"{connection.caller.confidence:.0%}] "
                        f"at {terminal_text(connection.caller.filename)}:"
                        f"{connection.caller.firstlineno}"
                    )
            _append_omitted(lines, network.connection_count)
    if analysis.network_setup_capture is not None:
        setup = analysis.network_setup_capture
        lines.extend(
            (
                "",
                "Automatic network setup capture",
                "  zero-code DNS and TLS observer; hostnames, addresses, SNI, certificates, "
                "credentials, and payloads omitted",
                "  durations cover name resolution or TLS handshake completion",
            )
        )
        if setup.adapters:
            lines.append(f"  active adapters: {', '.join(setup.adapters)}")
        if setup.status in {"unavailable", "invalid"}:
            lines.append(f"  {setup.status}: no trustworthy network setup phases")
        else:
            process_label = "process" if setup.process_count == 1 else "processes"
            phase_label = "phase" if setup.phase_count == 1 else "phases"
            lines.append(
                f"  {setup.status}: {setup.phase_count:,} {phase_label} across "
                f"{setup.process_count:,} Python {process_label}"
            )
            if setup.dropped_phase_count:
                lines.append(
                    f"  truncated: {setup.dropped_phase_count:,} setup phases were omitted"
                )
            if setup.callback_error_count:
                lines.append(
                    f"  observer errors: {setup.callback_error_count:,}; workload outcome "
                    "was left unchanged"
                )
        caller_status = setup.caller_attribution_status
        if caller_status == "complete":
            lines.append(
                f"  caller attribution complete: {setup.attributed_phase_count:,} / "
                f"{setup.phase_count:,} phases across {setup.caller_count:,} callsites"
            )
        elif caller_status == "unavailable":
            lines.append("  caller attribution unavailable")
        else:
            lines.append(
                f"  caller attribution {caller_status}: {setup.attributed_phase_count:,} / "
                f"{setup.phase_count:,} phases"
            )
        if setup.caller_callback_error_count:
            lines.append(
                f"  caller observer errors: {setup.caller_callback_error_count:,}; "
                "setup evidence was retained"
            )
        if analysis.network_setup_hotspots:
            lines.extend(("", "Network setup hotspots"))
            for hotspot in analysis.network_setup_hotspots[:MAX_TEXT_SECTION_ITEMS]:
                if hotspot.caller is None:
                    caller = "unattributed"
                else:
                    caller = (
                        f"{terminal_text(hotspot.caller.name)} "
                        f"[{hotspot.caller.observation}, {hotspot.caller.confidence:.0%}]"
                    )
                phase_label = "call" if hotspot.phase_count == 1 else "calls"
                lines.append(
                    f"  {hotspot.phase.upper()} {caller}, adapter {terminal_text(hotspot.adapter)}"
                )
                lines.append(
                    f"    {hotspot.phase_count:,} {phase_label}: "
                    f"{hotspot.completed_phase_count:,} completed, "
                    f"{hotspot.failed_phase_count:,} failed, "
                    f"{hotspot.unfinished_phase_count:,} unfinished; "
                    f"{_duration(hotspot.total_duration_seconds)} total, "
                    f"max {_duration(hotspot.max_duration_seconds)}"
                )
            _append_omitted(lines, setup.hotspot_count)
        if analysis.network_setup_phases:
            lines.extend(("", "Network setup phases"))
            ordered_phases = sorted(
                analysis.network_setup_phases,
                key=lambda phase: (
                    phase.outcome == "completed",
                    -(phase.duration_seconds or 0),
                    phase.started_at_ns,
                ),
            )
            for phase in ordered_phases[:MAX_TEXT_SECTION_ITEMS]:
                outcome = (
                    "completed"
                    if phase.outcome == "completed"
                    else f"error {terminal_text(phase.error_type or 'unknown')}"
                    if phase.outcome == "setup_error"
                    else "completion unknown"
                )
                lines.append(
                    f"  {phase.phase.upper()}  {_duration(phase.duration_seconds)}, {outcome}, "
                    f"pid {phase.parent_pid} [{phase.role}], adapter {phase.adapter}"
                )
                if phase.caller is not None:
                    lines.append(
                        f"    called by {terminal_text(phase.caller.name)} "
                        f"[{phase.caller.observation}, {phase.caller.confidence:.0%}] "
                        f"at {terminal_text(phase.caller.filename)}:"
                        f"{phase.caller.firstlineno}"
                    )
            _append_omitted(lines, setup.phase_count)
    if analysis.logical_operation_capture is not None:
        logical = analysis.logical_operation_capture
        lines.extend(
            (
                "",
                "Automatic logical operation capture",
                "  zero-code Deep observer for supported database, cache, queue, broker, "
                "executor, scheduler, and inbound server boundaries; statements, parameters, "
                "keys, destinations, routes, URLs, addresses, headers, items, payloads, "
                "callables, awaitables, task names, contexts, arguments, return values, and "
                "exception messages omitted",
                "  expensive and intentionally unavailable below Deep capture",
            )
        )
        if logical.adapters:
            lines.append(f"  active adapters: {', '.join(logical.adapters)}")
        if logical.status in {"unavailable", "invalid"}:
            lines.append(f"  {logical.status}: no trustworthy logical operation evidence")
        else:
            process_label = "process" if logical.process_count == 1 else "processes"
            operation_label = "operation" if logical.operation_count == 1 else "operations"
            lines.append(
                f"  {logical.status}: {logical.operation_count:,} {operation_label} across "
                f"{logical.process_count:,} Python {process_label}"
            )
            if logical.dropped_operation_count:
                lines.append(
                    f"  truncated: {logical.dropped_operation_count:,} operations were omitted"
                )
            if logical.callback_error_count:
                lines.append(
                    f"  observer errors: {logical.callback_error_count:,}; workload outcome "
                    "was left unchanged"
                )
        caller_status = logical.caller_attribution_status
        if caller_status == "complete":
            lines.append(
                f"  caller attribution complete: {logical.attributed_operation_count:,} / "
                f"{logical.operation_count:,} operations across "
                f"{logical.caller_count:,} callsites"
            )
        elif caller_status == "unavailable":
            lines.append("  caller attribution unavailable")
        else:
            lines.append(
                f"  caller attribution {caller_status}: "
                f"{logical.attributed_operation_count:,} / "
                f"{logical.operation_count:,} operations"
            )
        if logical.caller_callback_error_count:
            lines.append(
                f"  caller observer errors: {logical.caller_callback_error_count:,}; "
                "operation evidence was retained"
            )
        if analysis.logical_operation_hotspots:
            lines.extend(("", "Logical operation hotspots"))
            for hotspot in analysis.logical_operation_hotspots[:MAX_TEXT_SECTION_ITEMS]:
                if hotspot.caller is None:
                    caller = "unattributed"
                else:
                    caller = (
                        f"{terminal_text(hotspot.caller.name)} "
                        f"[{hotspot.caller.observation}, {hotspot.caller.confidence:.0%}]"
                    )
                operation_label = "call" if hotspot.operation_count == 1 else "calls"
                lines.append(
                    f"  {hotspot.category.upper()} {hotspot.operation} {caller}, "
                    f"adapter {terminal_text(hotspot.adapter)}"
                )
                lines.append(
                    f"    {hotspot.operation_count:,} {operation_label}: "
                    f"{hotspot.completed_operation_count:,} completed, "
                    f"{hotspot.failed_operation_count:,} failed, "
                    f"{hotspot.unfinished_operation_count:,} unfinished; "
                    f"{_duration(hotspot.total_duration_seconds)} total, "
                    f"max {_duration(hotspot.max_duration_seconds)}"
                )
            _append_omitted(lines, logical.hotspot_count)
        if analysis.logical_operations:
            lines.extend(("", "Logical operations"))
            ordered_operations = sorted(
                analysis.logical_operations,
                key=lambda operation: (
                    operation.outcome == "completed",
                    -(operation.duration_seconds or 0),
                    operation.started_at_ns,
                ),
            )
            for operation in ordered_operations[:MAX_TEXT_SECTION_ITEMS]:
                status = (
                    f", status {operation.status_code}" if operation.status_code is not None else ""
                )
                outcome = (
                    "completed"
                    if operation.outcome == "completed"
                    else f"error {terminal_text(operation.error_type or 'unknown')}"
                    if operation.outcome == "operation_error"
                    else "completion unknown"
                )
                lines.append(
                    f"  {operation.category.upper()} {operation.operation}  "
                    f"{_duration(operation.duration_seconds)}{status}, {outcome}, "
                    f"pid {operation.parent_pid} [{operation.role}], "
                    f"adapter {operation.adapter}"
                )
                if operation.caller is not None:
                    lines.append(
                        f"    called by {terminal_text(operation.caller.name)} "
                        f"[{operation.caller.observation}, "
                        f"{operation.caller.confidence:.0%}] "
                        f"at {terminal_text(operation.caller.filename)}:"
                        f"{operation.caller.firstlineno}"
                    )
            _append_omitted(lines, logical.operation_count)
    if analysis.sample_profile is not None:
        profile = analysis.sample_profile
        lines.extend(
            (
                "",
                "Sampling capture",
                "  statistical estimates; timings include sampler overhead",
            )
        )
        if profile.status == "unavailable":
            if (
                profile.process_coverage.status == "complete"
                and profile.process_coverage.observed_python_process_count == 0
            ):
                lines.append("  no Python process was observed; no samples were expected")
            else:
                lines.append(
                    "  no Python samples were observed (the interpreter may disable site startup)"
                )
        elif profile.status == "invalid":
            lines.append("  generated sampling evidence was invalid and was ignored")
        else:
            process_label = "process" if profile.process_count == 1 else "processes"
            lines.append(
                f"  {profile.status}: {profile.process_count:,} {process_label}, "
                f"{profile.sample_count:,} sampling ticks, "
                f"{profile.thread_sample_count:,} thread samples"
            )
            lines.append(
                f"  {profile.function_count:,} functions, "
                f"{profile.edge_count:,} sampled stack relationships, "
                f"interval {_duration(profile.interval_seconds)}"
            )
            if profile.truncated:
                lines.append(
                    "  truncated: at least "
                    f"{profile.dropped_frame_sample_count:,} frame samples were omitted"
                )
        _append_profile_transport(
            lines,
            transport=profile.transport,
            first_checkpoint_delay_seconds=profile.first_checkpoint_delay_seconds,
            checkpoint_interval_seconds=profile.checkpoint_interval_seconds,
            checkpoint_process_count=profile.checkpoint_process_count,
            registration_only_process_count=profile.registration_only_process_count,
            dropped_profile_process_count=profile.dropped_profile_process_count,
            dropped_profile_process_count_truncated=(
                profile.dropped_profile_process_count_truncated
            ),
            collector_error_count=profile.collector_error_count,
            snapshot_metrics=profile.snapshot_metrics,
            publication_metrics=profile.publication_metrics,
            normalization_metrics=profile.normalization_metrics,
        )
        _append_profile_process_coverage(
            lines,
            profile.process_coverage,
            observer_name="sampler",
            process_observer_status=(
                analysis.process_observer.status if analysis.process_observer is not None else None
            ),
        )
        if analysis.python_sample_hotspots:
            lines.extend(("", "Sampled Python hotspots"))
            for index, hotspot in enumerate(
                analysis.python_sample_hotspots[:MAX_TEXT_SECTION_ITEMS]
            ):
                lines.append(
                    f"  {terminal_text(hotspot.name)} [{hotspot.scope}]"
                    f"  leaf {hotspot.leaf_sample_count:,} samples"
                    f" (~{_duration(hotspot.estimated_leaf_seconds)}),"
                    f" on stack {hotspot.sample_count:,}"
                    f" (~{_duration(hotspot.estimated_total_seconds)})"
                )
                if profile.process_count > 1 and index < MAX_TEXT_ATTRIBUTED_HOTSPOTS:
                    _append_sample_process_contributions(lines, hotspot)
            _append_omitted(lines, len(analysis.python_sample_hotspots))
    if analysis.deep_profile is not None:
        profile = analysis.deep_profile
        lines.extend(
            (
                "",
                "Deep capture",
                "  intrusive; timings include profiler overhead",
                "  observes every Python and native C call plus Python exception propagation",
            )
        )
        if profile.status == "unavailable":
            if (
                profile.process_coverage.status == "complete"
                and profile.process_coverage.observed_python_process_count == 0
            ):
                lines.append("  no Python process was observed; no profile was expected")
            else:
                lines.append(
                    "  no Python profile was observed (the interpreter may disable site startup)"
                )
        elif profile.status == "invalid":
            lines.append("  generated profile evidence was invalid and was ignored")
        else:
            process_label = "process" if profile.process_count == 1 else "processes"
            lines.append(
                f"  {profile.status}: {profile.process_count:,} {process_label}, "
                f"{profile.function_count:,} functions, {profile.edge_count:,} call relationships"
            )
            if profile.dropped_call_count:
                lines.append(
                    f"  truncated: at least {profile.dropped_call_count:,} calls were omitted"
                )
            if profile.open_call_count:
                lines.append(
                    f"  incomplete: {profile.open_call_count:,} calls were still open at the "
                    "latest snapshot"
                )
            if (
                profile.truncated
                and profile.dropped_call_count == 0
                and profile.open_call_count == 0
            ):
                lines.append("  truncated: profile limits or callback errors omitted evidence")
        integrity = profile.observer_integrity
        if integrity is not None:
            if integrity.status == "invalid":
                lines.append("  observer-integrity metadata was invalid and was ignored")
            elif integrity.status == "unavailable":
                lines.append("  observer integrity unavailable for this artifact")
            elif integrity.status == "complete":
                lines.append("  observer integrity complete: no tracing-hook setter calls detected")
            else:
                if integrity.missing_process_count:
                    lines.append(
                        "  observer integrity unavailable for "
                        f"{integrity.missing_process_count:,} processes"
                    )
                if integrity.profile_hook_setter_call_count:
                    lines.append(
                        "  profile hook setter called: "
                        f"{integrity.profile_hook_setter_call_count:,} calls across "
                        f"{integrity.profile_hook_setter_process_count:,} processes; "
                        "exact call and caller evidence may be incomplete"
                    )
                if integrity.trace_hook_setter_call_count:
                    lines.append(
                        "  trace hook setter called: "
                        f"{integrity.trace_hook_setter_call_count:,} calls across "
                        f"{integrity.trace_hook_setter_process_count:,} processes; "
                        "Python exception evidence may be incomplete"
                    )
        native = profile.native_call_capture
        python_exceptions = profile.python_exception_capture
        if python_exceptions is not None:
            if python_exceptions.status == "invalid":
                lines.append("  Python-exception capture metadata was invalid and was ignored")
            else:
                event_label = "event" if python_exceptions.event_count == 1 else "events"
                lines.append(
                    "  Python exceptions: "
                    f"{python_exceptions.event_count:,} propagation {event_label} across "
                    f"{python_exceptions.function_count:,} functions"
                )
                if python_exceptions.dropped_event_count:
                    lines.append(
                        "  Python exceptions omitted by limits: "
                        f"{python_exceptions.dropped_event_count:,} events"
                    )
                control_flow_filter = python_exceptions.control_flow_filter
                if control_flow_filter is not None:
                    lines.append(
                        "  exception diagnosis: "
                        f"{control_flow_filter.non_control_flow_event_count:,} "
                        "non-control-flow events across "
                        f"{control_flow_filter.non_control_flow_function_count:,} functions; "
                        f"{control_flow_filter.filtered_event_count:,} built-in "
                        "iterator-control events filtered"
                    )
                    if control_flow_filter.dropped_non_control_flow_event_count:
                        lines.append(
                            "  diagnostic exception events omitted by limits: "
                            f"{control_flow_filter.dropped_non_control_flow_event_count:,} events"
                        )
                    lines.append(
                        "  type identity inspected only for control-flow filtering; types, "
                        "values, messages, tracebacks, arguments, and locals not retained"
                    )
                else:
                    lines.append(
                        "  control-flow filtering unavailable; exception-churn diagnosis disabled"
                    )
                    lines.append(
                        "  exception types, values, messages, tracebacks, arguments, "
                        "and locals omitted"
                    )
                lines.append(
                    "  line and opcode tracing disabled; propagation may count one error "
                    "in multiple frames"
                )
        if native is not None:
            if native.status == "invalid":
                lines.append("  native-call capture metadata was invalid and was ignored")
            else:
                native_exception_label = (
                    "exception" if native.exception_count == 1 else "exceptions"
                )
                lines.append(
                    "  native calls: "
                    f"{native.call_count:,} calls across {native.function_count:,} functions, "
                    f"{native.exception_count:,} {native_exception_label}"
                )
                lines.append("  native arguments, return values, and exception messages omitted")
        _append_profile_transport(
            lines,
            transport=profile.transport,
            first_checkpoint_delay_seconds=profile.first_checkpoint_delay_seconds,
            checkpoint_interval_seconds=profile.checkpoint_interval_seconds,
            checkpoint_process_count=profile.checkpoint_process_count,
            registration_only_process_count=profile.registration_only_process_count,
            dropped_profile_process_count=profile.dropped_profile_process_count,
            dropped_profile_process_count_truncated=(
                profile.dropped_profile_process_count_truncated
            ),
            collector_error_count=profile.collector_error_count,
            snapshot_metrics=profile.snapshot_metrics,
            publication_metrics=profile.publication_metrics,
            normalization_metrics=profile.normalization_metrics,
        )
        _append_profile_process_coverage(
            lines,
            profile.process_coverage,
            observer_name="profiler",
            process_observer_status=(
                analysis.process_observer.status if analysis.process_observer is not None else None
            ),
        )
        if analysis.python_hotspots:
            lines.extend(("", "Python hotspots (including native calls)"))
            for index, hotspot in enumerate(analysis.python_hotspots[:MAX_TEXT_SECTION_ITEMS]):
                if hotspot.exception_count and hotspot.non_control_flow_exception_count is not None:
                    exception_suffix = (
                        f", {hotspot.non_control_flow_exception_count:,} diagnostic / "
                        f"{hotspot.exception_count:,} raw exception events"
                    )
                elif hotspot.exception_count:
                    hotspot_exception_label = (
                        "exception" if hotspot.exception_count == 1 else "exceptions"
                    )
                    exception_suffix = f", {hotspot.exception_count:,} {hotspot_exception_label}"
                else:
                    exception_suffix = ""
                lines.append(
                    f"  {terminal_text(hotspot.name)} "
                    f"[{hotspot.scope}, {hotspot.implementation}]"
                    f"  self {_duration(hotspot.self_seconds)},"
                    f" total {_duration(hotspot.total_seconds)},"
                    f" {hotspot.call_count:,} calls, max {_duration(hotspot.max_seconds)}"
                    f"{exception_suffix}"
                )
                if profile.process_count > 1 and index < MAX_TEXT_ATTRIBUTED_HOTSPOTS:
                    _append_call_process_contributions(lines, hotspot)
            _append_omitted(lines, len(analysis.python_hotspots))
    lines.extend(("", "Observation"))
    _append_observation(lines, analysis)
    lines.extend(("", "Lifecycle"))
    for phase in analysis.lifecycle[:MAX_TEXT_SECTION_ITEMS]:
        lines.append(
            f"  {terminal_text(phase.name):<24} "
            f"{_duration(phase.duration_seconds):>10}  {phase.source}"
        )
    _append_omitted(lines, len(analysis.lifecycle))
    lines.extend(("", "Critical path"))
    if analysis.critical_path is None:
        lines.append("  unavailable")
    else:
        path = analysis.critical_path
        lines.append(f"  {path.certainty}: {_duration(path.duration_seconds)}")
        if path.cycle_detected:
            lines.append("  warning: causal cycle detected; critical path is inferred")
        lines.append(f"  active execution: {_duration(path.active_seconds)}")
        lines.append(f"  causal waiting: {_duration(path.waiting_seconds)}")
        lines.append(f"  parallel slack: {_duration(path.parallel_slack_seconds)}")
        lines.append(
            "  "
            + " → ".join(terminal_text(name) for name in path.event_names[:MAX_TEXT_SECTION_ITEMS])
        )
        _append_omitted(lines, len(path.event_names))
    lines.extend(("", "Throughput"))
    if analysis.throughput is None:
        lines.append("  no progress evidence")
    else:
        throughput = analysis.throughput
        lines.append(f"  completed: {throughput.completed:g} / {throughput.total:g}")
        lines.append(f"  rate: {_rate(throughput.rate_per_second)}")
        lines.append(f"  remaining: {throughput.remaining:g}")
        lines.append(f"  estimated drain: {_duration(throughput.estimated_drain_seconds)}")
        if throughput.compute_finished_at_ns is not None:
            lines.append(
                "  remaining at compute completion: "
                f"{_number(throughput.remaining_at_compute_completion)}"
            )
            lines.append(f"  post-compute wall time: {_duration(throughput.post_compute_seconds)}")
            lines.append(f"  post-compute rate: {_rate(throughput.post_compute_rate_per_second)}")
    return "\n".join(lines)


def _append_observation(lines: list[str], analysis: BatchAnalysis) -> None:
    throughput = analysis.throughput
    if throughput is None:
        lines.append("  no explicit progress evidence")
        return
    if throughput.compute_finished_at_ns is None:
        lines.append("  progress evidence did not identify a compute boundary")
        return
    lines.append(
        "  compute completed with "
        f"{_number(throughput.remaining_at_compute_completion)} / "
        f"{_number(throughput.total)} work items remaining"
    )
    if throughput.post_compute_seconds is not None:
        lines.append(
            f"  {_duration(throughput.post_compute_seconds)} of post-compute wall time followed"
        )


def _append_omitted(lines: list[str], total: int) -> None:
    omitted = total - MAX_TEXT_SECTION_ITEMS
    if omitted > 0:
        lines.append(f"  … {omitted:,} additional items omitted from text output")


def _append_profile_process_coverage(
    lines: list[str],
    coverage: PythonProfileCoverage,
    *,
    observer_name: str,
    process_observer_status: str | None,
) -> None:
    if coverage.status == "unavailable":
        lines.append(
            "  process coverage unavailable: process-tree evidence or reporter IDs missing"
        )
        return
    if coverage.status == "invalid":
        lines.append("  process coverage invalid: reporter IDs were inconsistent")
        return
    observed_count = coverage.observed_python_process_count
    assert observed_count is not None
    if coverage.status == "complete" and observed_count == 0:
        lines.append("  process coverage complete: no observed Python processes")
        return
    lines.append(
        f"  process coverage {coverage.status}: {coverage.matched_process_count:,} / "
        f"{observed_count:,} observed Python processes reported"
    )
    for gap in coverage.unprofiled_processes[:MAX_TEXT_PROCESS_CONTRIBUTIONS]:
        parent = f", parent {terminal_text(gap.parent_name)}" if gap.parent_name is not None else ""
        lines.append(
            f"    {terminal_text(gap.process_name)} (pid {gap.pid}{parent}) "
            f"did not load the {observer_name}"
        )
    rendered_gap_count = min(
        len(coverage.unprofiled_processes),
        MAX_TEXT_PROCESS_CONTRIBUTIONS,
    )
    omitted = coverage.unprofiled_process_count - rendered_gap_count
    if omitted > 0:
        lines.append(f"    … {omitted:,} additional unprofiled processes omitted")
    if coverage.unobserved_profile_process_ids:
        pids = ", ".join(
            str(pid)
            for pid in coverage.unobserved_profile_process_ids[:MAX_TEXT_PROCESS_CONTRIBUTIONS]
        )
        omitted_pids = len(coverage.unobserved_profile_process_ids) - MAX_TEXT_PROCESS_CONTRIBUTIONS
        suffix = f", plus {omitted_pids:,} more" if omitted_pids > 0 else ""
        lines.append(
            f"    reporting processes not observed in the process tree: pids {pids}{suffix}"
        )
    if (
        coverage.status == "partial"
        and coverage.unprofiled_process_count == 0
        and not coverage.unobserved_profile_process_ids
        and process_observer_status is not None
    ):
        lines.append(
            "    process coverage is partial because process-tree observation was "
            f"{terminal_text(process_observer_status)}"
        )


def _append_profile_transport(
    lines: list[str],
    *,
    transport: str,
    first_checkpoint_delay_seconds: float,
    checkpoint_interval_seconds: float,
    checkpoint_process_count: int,
    registration_only_process_count: int,
    dropped_profile_process_count: int,
    dropped_profile_process_count_truncated: bool,
    collector_error_count: int,
    snapshot_metrics: ProfileSnapshotMetrics,
    publication_metrics: ProfilePublicationMetrics,
    normalization_metrics: ProfileNormalizationMetrics,
) -> None:
    if transport != "unknown":
        if transport == "controller-unix-socket":
            label = "controller-side Unix socket"
        elif transport == "mixed":
            label = "controller socket with retained workload-file fallback"
        else:
            label = "workload-side atomic file fallback"
        cadence = f"every {_duration(checkpoint_interval_seconds)}"
        if first_checkpoint_delay_seconds > 0:
            cadence = (
                f"first evidence after {_duration(first_checkpoint_delay_seconds)}, then {cadence}"
            )
        lines.append(f"  snapshots: {label}, {cadence}")
    if snapshot_metrics.status == "available":
        lines.append(
            f"  snapshot transport: {snapshot_metrics.message_count:,} messages, "
            f"{_bytes(float(snapshot_metrics.payload_bytes))} total, largest "
            f"{_bytes(float(snapshot_metrics.max_payload_bytes))}"
        )
        if snapshot_metrics.checkpoint_message_count:
            lines.append(
                f"  evidence checkpoints: {snapshot_metrics.checkpoint_message_count:,}, "
                f"{_bytes(float(snapshot_metrics.checkpoint_payload_bytes))} transferred; "
                f"serialization {_duration(snapshot_metrics.checkpoint_serialization_seconds)} "
                f"total, max {_duration(snapshot_metrics.max_checkpoint_serialization_seconds)}"
            )
    elif snapshot_metrics.status == "invalid":
        lines.append("  snapshot transport metrics were invalid and were ignored")
    if publication_metrics.status == "available" and publication_metrics.fallback_process_count:
        process_label = (
            "process" if publication_metrics.fallback_process_count == 1 else "processes"
        )
        line = (
            "  retained snapshot fallback: "
            f"{publication_metrics.fallback_process_count:,} {process_label}"
        )
        if publication_metrics.socket_attempted_process_count:
            attempt_label = (
                "process"
                if publication_metrics.socket_attempted_process_count == 1
                else "processes"
            )
            line += (
                "; controller socket attempted by "
                f"{publication_metrics.socket_attempted_process_count:,} {attempt_label}, "
                f"failed after {_duration(publication_metrics.socket_failure_seconds)} total, "
                f"max {_duration(publication_metrics.max_socket_failure_seconds)}"
            )
        unavailable_count = (
            publication_metrics.fallback_process_count
            - publication_metrics.socket_attempted_process_count
        )
        if unavailable_count:
            line += f"; controller socket unavailable for {unavailable_count:,} " + (
                "process" if unavailable_count == 1 else "processes"
            )
        lines.append(line)
    elif publication_metrics.status == "invalid":
        lines.append("  snapshot publication metrics were invalid and were ignored")
    if normalization_metrics.status == "available":
        lines.append(
            f"  controller normalization: {_duration(normalization_metrics.duration_seconds)}; "
            "ranking database peak "
            f"{_bytes(float(normalization_metrics.ranking_database_peak_bytes))} / "
            f"{_bytes(float(normalization_metrics.ranking_database_limit_bytes))} limit"
        )
    elif normalization_metrics.status == "invalid":
        lines.append("  profile normalization metrics were invalid and were ignored")
    if registration_only_process_count:
        process_label = "process" if registration_only_process_count == 1 else "processes"
        lines.append(
            f"  registration-only: {registration_only_process_count:,} {process_label} loaded "
            "capture but ended before the first periodic checkpoint; no Python hotspot "
            "evidence was retained"
        )
    evidence_checkpoint_process_count = checkpoint_process_count - registration_only_process_count
    if evidence_checkpoint_process_count > 0:
        process_label = "process" if evidence_checkpoint_process_count == 1 else "processes"
        lines.append(
            f"  checkpoint-only: {evidence_checkpoint_process_count:,} {process_label} ended "
            "without a final report; using the latest checkpoint"
        )
    if dropped_profile_process_count:
        qualifier = "at least " if dropped_profile_process_count_truncated else ""
        process_label = "report" if dropped_profile_process_count == 1 else "reports"
        lines.append(
            f"  process limit: {qualifier}{dropped_profile_process_count:,} profile process "
            f"{process_label} omitted after the {MAX_DEEP_PROFILE_FILES:,}-process bound"
        )
    if collector_error_count:
        lines.append(
            f"  snapshot collector errors: {collector_error_count:,}; final-file fallback remains"
        )


def _process_label(
    *,
    pid: int,
    role: str,
    process_name: str | None,
    parent_name: str | None,
    observed_in_process_tree: bool,
) -> str:
    name = terminal_text(process_name or "unmatched process")
    details = [f"pid {pid}", role]
    if parent_name is not None:
        details.append(f"parent {terminal_text(parent_name)}")
    if not observed_in_process_tree:
        details.append("not observed in process tree")
    return f"{name} ({', '.join(details)})"


def _append_call_process_contributions(lines: list[str], hotspot: PythonHotspot) -> None:
    if hotspot.process_attribution_status != "complete":
        lines.append(f"    process attribution: {hotspot.process_attribution_status}")
        return
    lines.append("    by process:")
    for process in hotspot.processes[:MAX_TEXT_PROCESS_CONTRIBUTIONS]:
        label = _process_label(
            pid=process.pid,
            role=process.role,
            process_name=process.process_name,
            parent_name=process.parent_name,
            observed_in_process_tree=process.observed_in_process_tree,
        )
        lines.append(
            f"      {label}: self {_duration(process.self_seconds)}, "
            f"total {_duration(process.total_seconds)}, {process.call_count:,} calls"
        )
    omitted = len(hotspot.processes) - MAX_TEXT_PROCESS_CONTRIBUTIONS
    if omitted > 0:
        lines.append(f"      … {omitted:,} additional process contributors omitted")


def _append_sample_process_contributions(lines: list[str], hotspot: PythonSampleHotspot) -> None:
    if hotspot.process_attribution_status != "complete":
        lines.append(f"    process attribution: {hotspot.process_attribution_status}")
        return
    lines.append("    by process:")
    for process in hotspot.processes[:MAX_TEXT_PROCESS_CONTRIBUTIONS]:
        label = _process_label(
            pid=process.pid,
            role=process.role,
            process_name=process.process_name,
            parent_name=process.parent_name,
            observed_in_process_tree=process.observed_in_process_tree,
        )
        lines.append(
            f"      {label}: leaf {process.leaf_sample_count:,} samples "
            f"(~{_duration(process.estimated_leaf_seconds)}), "
            f"on stack {process.sample_count:,} "
            f"(~{_duration(process.estimated_total_seconds)})"
        )
    omitted = len(hotspot.processes) - MAX_TEXT_PROCESS_CONTRIBUTIONS
    if omitted > 0:
        lines.append(f"      … {omitted:,} additional process contributors omitted")


def _duration(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.001:
        return f"{value * 1_000_000:.1f}µs"
    if value < 1:
        return f"{value * 1000:.1f}ms"
    return f"{value:.3f}s"


def _rate(value: float | None) -> str:
    return "unknown" if value is None else f"{value:,.2f}/s"


def _number(value: float | None) -> str:
    return "unknown" if value is None else f"{value:g}"


def _bytes(value: float) -> str:
    if value < 1_024:
        return f"{value:.0f} B"
    if value < 1_024 * 1_024:
        return f"{value / 1_024:.1f} KiB"
    return f"{value / (1_024 * 1_024):.1f} MiB"
