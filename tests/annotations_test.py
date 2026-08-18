from __future__ import annotations

import errno
import fcntl
import json
import os
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import runtime_tools.annotations as annotations_module
from runtime_tools import record_process, runtime
from runtime_tools.annotations import AnnotationError, load_annotations
from runtime_tools.inspect import inspect_runpack, render_summary
from runtime_tools.storage import RunpackReader


def test_record_process_normalizes_nested_domain_annotations(tmp_path: Path) -> None:
    workload = tmp_path / "workload.py"
    workload.write_text(
        """
from runtime_tools import runtime

with runtime.run("pipeline", total_work=10):
    with runtime.stage("transform"):
        first = runtime.event("db.write", kind="client.request", table="results")
        second = runtime.event("db.flush", kind="client.request")
        runtime.link(first, second, relation="flushes")
    runtime.progress(completed=10, total=10)
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "annotated.runpack"

    record_process((sys.executable, str(workload)), output, name="annotated")

    with RunpackReader(output) as reader:
        events = {event.name: event for event in reader.events()}
        edges = reader.causal_edges()
    assert set(events) == {
        Path(sys.executable).name,
        "pipeline",
        "transform",
        "db.write",
        "db.flush",
        "progress",
    }
    assert events["pipeline"].kind == "run"
    assert events["transform"].kind == "stage"
    assert events["db.write"].attributes["table"] == "results"
    assert events["progress"].attributes == {"completed": 10, "total": 10}
    relationships = {
        (events_by_id.source_event_id, events_by_id.target_event_id, events_by_id.kind)
        for events_by_id in edges
    }
    assert (events["pipeline"].id, events["transform"].id, "parent") in relationships
    assert (events[Path(sys.executable).name].id, events["pipeline"].id, "parent") in relationships
    assert (events["transform"].id, events["db.write"].id, "parent") in relationships
    assert (events["db.write"].id, events["db.flush"].id, "flushes") in relationships


def test_inherited_fd_annotations_survive_workload_chdir(tmp_path: Path) -> None:
    workload = tmp_path / "chdir-workload.py"
    workload.write_text(
        "import os\nfrom runtime_tools import runtime\n"
        "os.chdir('/')\n"
        "runtime.event('after-chdir')\n",
        encoding="utf-8",
    )
    output = tmp_path / "chdir.runpack"
    descriptor = os.open(os.devnull, os.O_RDONLY)
    try:
        record_process(
            (sys.executable, str(workload)),
            output,
            name="after-chdir",
            _annotation_fd=descriptor,
        )
        os.fstat(descriptor)
    finally:
        os.close(descriptor)

    with RunpackReader(output) as reader:
        event_names = {event.name for event in reader.events()}
    assert "after-chdir" in event_names
    assert not tuple(tmp_path.glob(".chdir.runpack.annotations-*"))


def test_closed_inherited_annotation_fd_uses_the_private_fallback(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "closed-fd-workload.py"
    workload.write_text(
        "import os\nfrom runtime_tools import runtime\n"
        "os.close(int(os.environ['_CONTRAIL_ANNOTATIONS_FD']))\n"
        "try:\n"
        "    runtime.event('should-not-exist')\n"
        "except OSError:\n"
        "    pass\n",
        encoding="utf-8",
    )
    output = tmp_path / "closed-fd.runpack"
    descriptor = os.open(os.devnull, os.O_RDONLY)
    try:
        exit_code = record_process(
            (sys.executable, str(workload)),
            output,
            name="closed-fd",
            _annotation_fd=descriptor,
        )
    finally:
        os.close(descriptor)

    with RunpackReader(output) as reader:
        event_names = {event.name for event in reader.events()}
    assert exit_code == 0
    assert "should-not-exist" in event_names


def test_subprocess_without_inherited_fds_uses_the_private_fallback(tmp_path: Path) -> None:
    workload = tmp_path / "child-workload.py"
    child = "from runtime_tools import runtime; runtime.event('child-event')"
    workload.write_text(
        f"import subprocess, sys\nsubprocess.run((sys.executable, '-c', {child!r}), check=True)\n",
        encoding="utf-8",
    )
    output = tmp_path / "child.runpack"
    descriptor = os.open(os.devnull, os.O_RDONLY)
    try:
        record_process(
            (sys.executable, str(workload)),
            output,
            name="child",
            _annotation_fd=descriptor,
        )
    finally:
        os.close(descriptor)

    with RunpackReader(output) as reader:
        event_names = {event.name for event in reader.events()}
    assert "child-event" in event_names


def test_reused_annotation_fd_does_not_receive_annotation_writes(tmp_path: Path) -> None:
    victim = tmp_path / "victim.txt"
    victim.write_bytes(b"preserved")
    workload = tmp_path / "reused-fd-workload.py"
    workload.write_text(
        "import os, sys\n"
        "from runtime_tools import runtime\n"
        "transport = int(os.environ['_CONTRAIL_ANNOTATIONS_FD'])\n"
        "os.close(transport)\n"
        "victim = os.open(sys.argv[1], os.O_APPEND | os.O_WRONLY)\n"
        "os.dup2(victim, transport)\n"
        "runtime.event('reused-fd')\n",
        encoding="utf-8",
    )
    output = tmp_path / "reused-fd.runpack"
    descriptor = os.open(os.devnull, os.O_RDONLY)
    try:
        record_process(
            (sys.executable, str(workload), str(victim)),
            output,
            name="reused-fd",
            _annotation_fd=descriptor,
        )
    finally:
        os.close(descriptor)

    with RunpackReader(output) as reader:
        event_names = {event.name for event in reader.events()}
    assert "reused-fd" in event_names
    assert victim.read_bytes() == b"preserved"


def test_replaced_fallback_hard_link_does_not_receive_annotation_writes(
    tmp_path: Path,
) -> None:
    victim = tmp_path / "victim.txt"
    victim.write_bytes(b"preserved")
    workload = tmp_path / "hard-link-workload.py"
    workload.write_text(
        "import os, sys\n"
        "from pathlib import Path\n"
        "from runtime_tools import runtime\n"
        "os.close(int(os.environ['_CONTRAIL_ANNOTATIONS_FD']))\n"
        "fallback = Path(os.environ['_CONTRAIL_ANNOTATIONS_FALLBACK'])\n"
        "fallback.unlink()\n"
        "os.link(sys.argv[1], fallback)\n"
        "try:\n"
        "    runtime.event('hard-link-event')\n"
        "except OSError:\n"
        "    pass\n",
        encoding="utf-8",
    )
    output = tmp_path / "hard-link.runpack"
    descriptor = os.open(os.devnull, os.O_RDONLY)
    try:
        exit_code = record_process(
            (sys.executable, str(workload), str(victim)),
            output,
            name="hard-link",
            _annotation_fd=descriptor,
        )
    finally:
        os.close(descriptor)

    summary = inspect_runpack(output)
    with RunpackReader(output) as reader:
        event_names = {event.name for event in reader.events()}
    assert exit_code == 0
    assert summary.annotation_error == "invalid annotation JSON on line 1"
    assert "hard-link-event" not in event_names
    assert victim.read_bytes() == b"preserved"
    assert not tuple(tmp_path.glob(".hard-link.runpack.annotations-*"))


@pytest.mark.parametrize(
    ("descriptor", "target", "message"),
    (
        (" 3", "/dev/fd/3", "canonical decimal integer"),
        ("03", "/dev/fd/3", "canonical descriptor above 2"),
        ("2", "/dev/fd/2", "canonical descriptor above 2"),
        ("3", "/dev/fd/4", "does not match"),
    ),
)
def test_annotation_writer_rejects_invalid_inherited_fd_environments(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    descriptor: str,
    target: str,
    message: str,
) -> None:
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", target)
    monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_FD", descriptor)
    monkeypatch.setenv(
        "_CONTRAIL_ANNOTATIONS_FALLBACK",
        str(tmp_path / f".capture.runpack.annotations-{'a' * 32}"),
    )
    monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_IDENTITY", "1:1")

    with pytest.raises(ValueError, match=message):
        runtime.event("invalid-fd")


@pytest.mark.parametrize(
    "fallback",
    (
        None,
        "relative.annotations",
        "/private/tmp/annotations",
        "/dev/fd/3",
    ),
)
def test_annotation_writer_rejects_invalid_inherited_fd_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
    fallback: str | None,
) -> None:
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", "/dev/fd/3")
    monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_FD", "3")
    monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_IDENTITY", "1:1")
    monkeypatch.delenv("_CONTRAIL_ANNOTATIONS_FALLBACK", raising=False)
    if fallback is not None:
        monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_FALLBACK", fallback)

    with pytest.raises(ValueError, match="private fallback|canonical capture path"):
        runtime.event("invalid-fallback")


@pytest.mark.parametrize(
    "identity",
    (None, "", "1", "1:2:3", "01:2", "1:02", "-1:2", "a:2"),
)
def test_annotation_writer_rejects_invalid_inherited_fd_identities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    identity: str | None,
) -> None:
    fallback = tmp_path / f".capture.runpack.annotations-{'a' * 32}"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", "/dev/fd/3")
    monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_FD", "3")
    monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_FALLBACK", str(fallback))
    monkeypatch.delenv("_CONTRAIL_ANNOTATIONS_IDENTITY", raising=False)
    if identity is not None:
        monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_IDENTITY", identity)

    with pytest.raises(ValueError, match="private file identity|canonical device:inode"):
        runtime.event("invalid-identity")


def test_annotation_writer_does_not_mask_non_bad_fd_duplication_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fallback = tmp_path / f".capture.runpack.annotations-{'a' * 32}"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", "/dev/fd/3")
    monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_FD", "3")
    monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_FALLBACK", str(fallback))
    monkeypatch.setenv("_CONTRAIL_ANNOTATIONS_IDENTITY", "1:1")

    def fail(descriptor: int) -> int:
        raise OSError(errno.EMFILE, "too many open files")

    monkeypatch.setattr(os, "dup", fail)

    with pytest.raises(OSError) as error:
        runtime.event("duplication-failed")

    assert error.value.errno == errno.EMFILE
    assert not fallback.exists()


def test_record_process_preserves_an_incomplete_stage_after_abrupt_exit(tmp_path: Path) -> None:
    workload = tmp_path / "crash.py"
    workload.write_text(
        """
import os
from runtime_tools import runtime

with runtime.stage("before-crash"):
    os._exit(3)
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "crash.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="crash")

    with RunpackReader(output) as reader:
        event = next(event for event in reader.events() if event.name == "before-crash")
    assert exit_code == 3
    assert event.started_at_ns is not None
    assert event.finished_at_ns is None
    assert not tuple(tmp_path.glob("*.annotations-*"))


@pytest.mark.parametrize("release_during_capture", (True, False))
def test_record_process_bounds_a_descendant_annotation_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    release_during_capture: bool,
) -> None:
    descendant = tmp_path / "descendant.py"
    descendant.write_text(
        """
import os
import time
from pathlib import Path

from runtime_tools import runtime

real_write = os.write
partial_write = True


def pause_mid_record(descriptor, data):
    global partial_write
    if partial_write:
        partial_write = False
        written = real_write(descriptor, data[: len(data) // 2])
        Path(os.environ["CONTRAIL_PARTIAL_ANNOTATION"]).touch()
        release = Path(os.environ["CONTRAIL_RELEASE_ANNOTATION"])
        while not release.exists():
            time.sleep(0.001)
        return written
    return real_write(descriptor, data)


os.write = pause_mid_record
runtime.event("descendant-event")
""".strip(),
        encoding="utf-8",
    )
    partial = tmp_path / "partial-annotation"
    release = tmp_path / "release-annotation"
    monkeypatch.setenv("CONTRAIL_PARTIAL_ANNOTATION", str(partial))
    monkeypatch.setenv("CONTRAIL_RELEASE_ANNOTATION", str(release))
    workload = tmp_path / "descendant-workload.py"
    workload.write_text(
        f"""
import subprocess
import sys
import time
from pathlib import Path

from runtime_tools import runtime

runtime.event("root-event")
subprocess.Popen(
    (sys.executable, {str(descendant)!r}),
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
partial = Path({str(partial)!r})
while not partial.exists():
    time.sleep(0.001)
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "descendant.runpack"
    flock = fcntl.flock

    def release_on_shared_lock(descriptor: int, operation: int) -> None:
        if release_during_capture and operation == fcntl.LOCK_SH | fcntl.LOCK_NB:
            release.touch()
        flock(descriptor, operation)

    monkeypatch.setattr(fcntl, "flock", release_on_shared_lock)
    if not release_during_capture:
        monkeypatch.setattr(annotations_module, "ANNOTATION_LOCK_TIMEOUT_SECONDS", 0.0)

    try:
        exit_code = record_process((sys.executable, str(workload)), output, name="descendant")
    finally:
        release.touch()

    summary = inspect_runpack(output)
    with RunpackReader(output) as reader:
        event_names = {event.name for event in reader.events()}
    assert exit_code == 0
    if release_during_capture:
        assert summary.annotation_error is None
        assert event_names == {Path(sys.executable).name, "root-event", "descendant-event"}
    else:
        assert summary.annotation_error == "timed out waiting for captured annotations"
        assert event_names == {Path(sys.executable).name}


def test_record_process_preserves_core_capture_when_annotations_are_malformed(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "malformed.py"
    workload.write_text(
        """
import os
from pathlib import Path

Path(os.environ["CONTRAIL_ANNOTATIONS_FILE"]).write_text("{")
print("result")
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "malformed.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="malformed")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.exit_code == 0
    assert summary.annotation_error == "invalid annotation JSON on line 1"
    assert "annotations: ignored (invalid annotation JSON on line 1)" in render_summary(
        summary, "text"
    )


def test_record_process_preserves_core_capture_for_non_finite_annotation_json(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "non-finite.py"
    workload.write_text(
        """
import os
from pathlib import Path

Path(os.environ["CONTRAIL_ANNOTATIONS_FILE"]).write_text(
    '{"record":"event_instant","id":"bad","kind":"event","name":"bad",'
    '"timestamp_ns":1,"attributes":{"value":NaN}}'
)
print("result")
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "non-finite.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="non-finite")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.annotation_error == "invalid annotation JSON on line 1: non-finite constant NaN"
    assert summary.record_counts["events"] == 1


def test_annotation_loader_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    annotations = tmp_path / "duplicates.jsonl"
    annotations.write_text(
        '{"record":"event_instant","id":"one","id":"two",'
        '"kind":"event","name":"duplicate","timestamp_ns":1}\n',
        encoding="utf-8",
    )

    with pytest.raises(AnnotationError, match="duplicate JSON key: id"):
        load_annotations(annotations, entity_id="process")


def test_record_process_preserves_core_capture_for_non_utf8_annotations(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "non-utf8.py"
    workload.write_text(
        """
import os
from pathlib import Path

Path(os.environ["CONTRAIL_ANNOTATIONS_FILE"]).write_bytes(b"\\xff")
print("result")
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "non-utf8.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="non-utf8")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.annotation_error == "captured annotations must be UTF-8"
    assert summary.record_counts["events"] == 1


def test_annotation_loader_rejects_escaped_invalid_unicode(tmp_path: Path) -> None:
    annotations = tmp_path / "surrogate.jsonl"
    annotations.write_text(
        json.dumps(
            {
                "record": "event_instant",
                "id": "event",
                "kind": "event",
                "name": "bad-\ud800",
                "timestamp_ns": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(AnnotationError, match="string that is not valid UTF-8"):
        load_annotations(annotations, entity_id="process")


def test_record_process_preserves_core_capture_for_oversized_annotation_fields(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "oversized-field.py"
    workload.write_text(
        """
from runtime_tools import runtime

runtime.event("oversized", value="x" * (4 * 1024 * 1024))
print("result")
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "oversized-field.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="oversized-field")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.annotation_error is not None
    assert summary.annotation_error.startswith("runpack JSON exceeds the 4194304-byte field limit")
    assert summary.record_counts["events"] == 1


def test_record_process_preserves_core_capture_for_reversed_annotation_intervals(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "reversed.py"
    workload.write_text(
        """
import json
import os
from pathlib import Path

records = [
    {"record": "event_start", "id": "bad", "kind": "stage", "name": "bad", "timestamp_ns": 10},
    {"record": "event_end", "id": "bad", "timestamp_ns": 9},
]
Path(os.environ["CONTRAIL_ANNOTATIONS_FILE"]).write_text(
    "".join(json.dumps(record) + "\\n" for record in records)
)
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "reversed.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="reversed")

    summary = inspect_runpack(output)
    assert exit_code == 0
    assert summary.annotation_error == "annotation event bad ends before it starts"
    assert summary.record_counts["events"] == 1


def test_record_process_deduplicates_annotation_links(
    tmp_path: Path,
) -> None:
    workload = tmp_path / "duplicate-link.py"
    workload.write_text(
        """
from runtime_tools import runtime

source = runtime.event("source")
target = runtime.event("target")
runtime.link(source, target)
runtime.link(source, target)
""".strip(),
        encoding="utf-8",
    )
    output = tmp_path / "duplicate-link.runpack"

    exit_code = record_process((sys.executable, str(workload)), output, name="duplicate-link")

    summary = inspect_runpack(output)
    with RunpackReader(output) as reader:
        events = {event.name: event for event in reader.events()}
        edges = reader.causal_edges()
    assert exit_code == 0
    assert summary.annotation_error is None
    assert summary.record_counts["events"] == 3
    assert (
        sum(
            edge.source_event_id == events["source"].id
            and edge.target_event_id == events["target"].id
            and edge.kind == "causes"
            for edge in edges
        )
        == 1
    )


def test_annotation_writer_completes_short_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))
    write = os.write

    def short_write(descriptor: int, data: bytes | bytearray | memoryview) -> int:
        return write(descriptor, data[:7])

    monkeypatch.setattr(os, "write", short_write)

    runtime.event("short-write", detail="complete")

    record = json.loads(annotations.read_text(encoding="utf-8"))
    assert record["name"] == "short-write"
    assert record["attributes"] == {"detail": "complete"}


def test_annotation_writer_retries_interrupted_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))
    write = os.write
    calls = 0

    def interrupted_once(descriptor: int, data: bytes | bytearray | memoryview) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InterruptedError
        return write(descriptor, data)

    monkeypatch.setattr(os, "write", interrupted_once)

    runtime.event("interrupted-write", detail="complete")

    record = json.loads(annotations.read_text(encoding="utf-8"))
    assert calls == 2
    assert record["name"] == "interrupted-write"
    assert record["attributes"] == {"detail": "complete"}


def test_annotation_writer_retries_interrupted_locks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))
    flock = fcntl.flock
    calls: list[int] = []
    interrupted: set[int] = set()

    def interrupted_once_per_operation(descriptor: int, operation: int) -> None:
        calls.append(operation)
        if operation not in interrupted:
            interrupted.add(operation)
            raise InterruptedError
        flock(descriptor, operation)

    monkeypatch.setattr(fcntl, "flock", interrupted_once_per_operation)

    runtime.event("interrupted-lock", detail="complete")

    record = json.loads(annotations.read_text(encoding="utf-8"))
    assert calls == [fcntl.LOCK_EX] * 2 + [fcntl.LOCK_UN] * 2
    assert record["name"] == "interrupted-lock"
    assert record["attributes"] == {"detail": "complete"}


def test_annotation_writer_rejects_invalid_unicode_without_poisoning_the_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))
    runtime.event("valid", detail="preserved")
    valid_content = annotations.read_bytes()

    with pytest.raises(ValueError, match="annotation strings must be valid UTF-8"):
        runtime.event("invalid", detail="bad-\ud800")

    assert annotations.read_bytes() == valid_content
    events, edges = load_annotations(annotations, entity_id="process")
    assert [event.name for event in events] == ["valid"]
    assert edges == ()


@pytest.mark.parametrize(
    ("write_invalid", "message"),
    (
        (lambda: runtime.event(""), "annotation name must be a non-empty string"),
        (lambda: runtime.event("valid", kind="bad\0kind"), "annotation kind must be"),
        (
            lambda: runtime.link(runtime.EventRef(""), runtime.EventRef("target")),
            "annotation source id must be",
        ),
        (
            lambda: runtime.link(
                runtime.EventRef("source"), runtime.EventRef("target"), relation=""
            ),
            "annotation relation must be",
        ),
    ),
)
def test_annotation_writer_rejects_invalid_semantic_text_without_poisoning_the_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    write_invalid: Callable[[], None],
    message: str,
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))
    runtime.event("valid")
    valid_content = annotations.read_bytes()

    with pytest.raises(ValueError, match=message):
        write_invalid()

    assert annotations.read_bytes() == valid_content


def test_annotation_writer_rejects_self_links_without_poisoning_the_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))
    event = runtime.event("valid")
    valid_content = annotations.read_bytes()

    with pytest.raises(ValueError, match="cannot link to themselves"):
        runtime.link(event, event)

    assert annotations.read_bytes() == valid_content


@pytest.mark.parametrize(
    ("completed", "total"),
    (
        (True, 1),
        (0, False),
        (-1, 1),
        (2, 1),
        (float("nan"), 1),
        (0, float("inf")),
        (10**400, 10**400),
        (0, 2**53 + 1),
    ),
)
def test_progress_rejects_invalid_values_without_poisoning_the_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    completed: int | float,
    total: int | float,
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))
    runtime.event("valid")
    valid_content = annotations.read_bytes()

    with pytest.raises(
        ValueError,
        match=(
            "annotation progress completed and total must be finite numbers with "
            "0 <= completed <= total"
        ),
    ):
        runtime.progress(completed=completed, total=total)

    assert annotations.read_bytes() == valid_content


@pytest.mark.parametrize(("completed", "total"), ((0, 0), (0.5, 1.0), (2**60, 2**60)))
def test_progress_writes_valid_boundary_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    completed: int | float,
    total: int | float,
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))

    runtime.progress(completed=completed, total=total)

    record = json.loads(annotations.read_text(encoding="utf-8"))
    assert record["kind"] == "progress"
    assert record["attributes"] == {"completed": completed, "total": total}


def test_annotation_writer_keeps_concurrent_short_writes_as_complete_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    monkeypatch.setenv("CONTRAIL_ANNOTATIONS_FILE", str(annotations))
    write = os.write

    def short_write(descriptor: int, data: bytes | bytearray | memoryview) -> int:
        return write(descriptor, data[:17])

    monkeypatch.setattr(os, "write", short_write)

    with ThreadPoolExecutor(max_workers=8) as executor:
        tuple(
            executor.map(
                lambda index: runtime.event(f"event-{index}", payload="x" * 1_000),
                range(100),
            )
        )

    records = [json.loads(line) for line in annotations.read_text().splitlines()]
    assert len(records) == 100
    assert {record["name"] for record in records} == {f"event-{index}" for index in range(100)}


def test_scope_restores_parent_context_when_end_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records: list[dict[str, object]] = []
    failed = False

    def fail_first_end(record: dict[str, object]) -> None:
        nonlocal failed
        records.append(record)
        if record["record"] == "event_end" and not failed:
            failed = True
            raise OSError("annotation sink failed")

    monkeypatch.setattr(runtime, "_write", fail_first_end)

    with runtime.run("outer") as outer:
        with pytest.raises(OSError, match="annotation sink failed"):
            with runtime.stage("inner"):
                pass
        after_failure = runtime.event("after-failure")

    event_record = next(record for record in records if record.get("id") == after_failure.id)
    assert event_record["parent_id"] == outer.id
    assert runtime._current_event_id.get() is None


def test_annotation_scopes_reject_reuse_without_emitting_duplicate_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records: list[dict[str, object]] = []
    monkeypatch.setattr(runtime, "_write", records.append)
    scope = runtime.stage("one-shot")

    with scope:
        pass
    with pytest.raises(RuntimeError, match="annotation scopes cannot be reused"):
        with scope:
            pass

    assert [record["record"] for record in records] == ["event_start", "event_end"]
    assert runtime._current_event_id.get() is None


@pytest.mark.parametrize(
    ("records", "message"),
    (
        (
            (
                {"record": "event_instant", "id": "same"},
                {"record": "event_instant", "id": "same"},
            ),
            "duplicate annotation event id on line 2",
        ),
        (
            (
                {"record": "event_start", "id": "scope"},
                {"record": "event_end", "id": "scope"},
                {"record": "event_end", "id": "scope"},
            ),
            "duplicate annotation event end on line 3",
        ),
        (
            ({"record": "event_end", "id": "missing"},),
            "orphan annotation event end on line 1",
        ),
    ),
)
def test_annotation_loader_rejects_conflicting_lifecycle_records(
    tmp_path: Path,
    records: tuple[dict[str, object], ...],
    message: str,
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    annotations.write_text(
        "".join(f"{json.dumps(record)}\n" for record in records), encoding="utf-8"
    )

    with pytest.raises(AnnotationError, match=message):
        load_annotations(annotations, entity_id="process")


@pytest.mark.parametrize(
    ("records", "message"),
    (
        (
            (
                {
                    "record": "event_instant",
                    "id": "child",
                    "kind": "event",
                    "name": "child",
                    "timestamp_ns": 1,
                    "parent_id": "missing",
                },
            ),
            "parent event is unresolved: missing",
        ),
        (
            (
                {
                    "record": "event_instant",
                    "id": "event",
                    "kind": "event",
                    "name": "event",
                    "timestamp_ns": 1,
                },
                {
                    "record": "link",
                    "source_id": "event",
                    "target_id": "missing",
                    "relation": "causes",
                },
            ),
            "link event -> missing is unresolved",
        ),
        (
            (
                {
                    "record": "event_instant",
                    "id": "event",
                    "kind": "event",
                    "name": "event",
                    "timestamp_ns": 1,
                },
                {
                    "record": "link",
                    "source_id": "event",
                    "target_id": "event",
                    "relation": "causes",
                },
            ),
            "event cannot link to itself: event",
        ),
        (
            (
                {
                    "record": "event_instant",
                    "id": "first",
                    "kind": "event",
                    "name": "first",
                    "timestamp_ns": 1,
                    "parent_id": "second",
                },
                {
                    "record": "event_instant",
                    "id": "second",
                    "kind": "event",
                    "name": "second",
                    "timestamp_ns": 2,
                    "parent_id": "first",
                },
            ),
            "parent relationships contain a cycle",
        ),
        (
            (
                {
                    "record": "event_instant",
                    "id": "first",
                    "kind": "event",
                    "name": "first",
                    "timestamp_ns": 1,
                },
                {
                    "record": "event_instant",
                    "id": "second",
                    "kind": "event",
                    "name": "second",
                    "timestamp_ns": 1,
                },
                {
                    "record": "event_instant",
                    "id": "child",
                    "kind": "event",
                    "name": "child",
                    "timestamp_ns": 2,
                    "parent_id": "first",
                },
                {
                    "record": "link",
                    "source_id": "second",
                    "target_id": "child",
                    "relation": "parent",
                },
            ),
            "event cannot have multiple parents",
        ),
    ),
)
def test_annotation_loader_rejects_incomplete_or_cyclic_causality(
    tmp_path: Path,
    records: tuple[dict[str, object], ...],
    message: str,
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    annotations.write_text(
        "".join(f"{json.dumps(record)}\n" for record in records), encoding="utf-8"
    )

    with pytest.raises(AnnotationError, match=message):
        load_annotations(annotations, entity_id="process")


@pytest.mark.parametrize(
    ("records", "message"),
    (
        (
            (
                {
                    "record": "event_instant",
                    "id": "event",
                    "kind": "event",
                    "name": "event",
                    "timestamp_ns": 1,
                    "atributes": {},
                },
            ),
            "contains unsupported fields: atributes",
        ),
        (
            (
                {
                    "record": "event_start",
                    "id": "event",
                    "kind": "event",
                    "name": "event",
                    "timestamp_ns": 1,
                },
                {
                    "record": "event_end",
                    "id": "event",
                    "timestamp_ns": 2,
                    "error": "yes",
                },
            ),
            "event_end error must be a boolean",
        ),
    ),
)
def test_annotation_loader_rejects_ignored_record_fields(
    tmp_path: Path,
    records: tuple[dict[str, object], ...],
    message: str,
) -> None:
    annotations = tmp_path / "annotations.jsonl"
    annotations.write_text(
        "".join(f"{json.dumps(record)}\n" for record in records), encoding="utf-8"
    )

    with pytest.raises(AnnotationError, match=message):
        load_annotations(annotations, entity_id="process")


def test_annotation_loader_rejects_oversized_streams_before_decoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    annotation_path = tmp_path / "oversized.jsonl"
    annotation_path.write_bytes(b"{" + b" " * 32 + b"}")
    monkeypatch.setattr(annotations_module, "MAX_ANNOTATION_STREAM_BYTES", 32)

    with pytest.raises(AnnotationError, match="annotations exceed the 32-byte input limit"):
        load_annotations(annotation_path, entity_id="process")


def test_annotation_loader_rejects_too_many_records_before_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    annotation_path = tmp_path / "too-many.jsonl"
    annotation_path.write_bytes(b"{}\n{}\n{}\n")
    monkeypatch.setattr(annotations_module, "MAX_ANNOTATION_RECORDS", 2)

    with pytest.raises(AnnotationError, match="annotations exceed the 2-record input limit"):
        load_annotations(annotation_path, entity_id="process")
