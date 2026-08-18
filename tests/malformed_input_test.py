from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy

from runtime_tools.kubernetes import KubernetesImportError, import_kubernetes_snapshot
from runtime_tools.model import Execution
from runtime_tools.otel import OtelImportError, import_otlp_json
from runtime_tools.prometheus import PrometheusImportError, import_prometheus_response
from runtime_tools.proofline.contracts import ContractError, load_contracts
from runtime_tools.storage import RunpackError, RunpackReader, RunpackWriter

_PROPERTY_SETTINGS = settings(
    max_examples=30,
    derandomize=True,
    deadline=None,
    database=None,
    suppress_health_check=(HealthCheck.function_scoped_fixture,),
)
_JSON_SCALARS: SearchStrategy[object] = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**70), max_value=2**70),
    st.floats(allow_nan=True, allow_infinity=True, width=64),
    st.text(max_size=24),
)
_JSON_VALUES = st.recursive(
    _JSON_SCALARS,
    lambda children: (
        st.lists(children, max_size=5) | st.dictionaries(st.text(max_size=12), children, max_size=5)
    ),
    max_leaves=20,
)
_EVIDENCE_BYTES = st.one_of(
    st.binary(max_size=512),
    _JSON_VALUES.map(
        lambda value: json.dumps(
            value,
            allow_nan=True,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
    ),
)
_FIXTURES = Path(__file__).parent / "fixtures" / "malformed"


def _write_base(path: Path) -> None:
    if path.exists():
        return
    with RunpackWriter(path) as writer:
        writer.add_execution(
            Execution("base", "base", 0, 10**15, (), str(path.parent), 0, None, {})
        )


def _source_is_unchanged(path: Path, expected: bytes) -> None:
    assert hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(expected).digest()


@_PROPERTY_SETTINGS
@given(payload=_EVIDENCE_BYTES)
def test_otlp_import_is_atomic_for_bounded_generated_evidence(
    tmp_path: Path, payload: bytes
) -> None:
    source = tmp_path / "generated-otel.json"
    output = tmp_path / "generated-otel.runpack"
    source.write_bytes(payload)
    output.unlink(missing_ok=True)

    try:
        import_otlp_json(source, output, name="generated")
    except OtelImportError:
        assert not output.exists()
    else:
        with RunpackReader(output) as reader:
            reader.execution()
        output.unlink()
    _source_is_unchanged(source, payload)


@_PROPERTY_SETTINGS
@given(payload=_EVIDENCE_BYTES)
def test_kubernetes_import_is_atomic_for_bounded_generated_evidence(
    tmp_path: Path, payload: bytes
) -> None:
    runpack = tmp_path / "kubernetes-base.runpack"
    source = tmp_path / "generated-kubernetes.json"
    output = tmp_path / "generated-kubernetes.runpack"
    _write_base(runpack)
    source.write_bytes(payload)
    output.unlink(missing_ok=True)

    try:
        import_kubernetes_snapshot(runpack, source, output)
    except KubernetesImportError:
        assert not output.exists()
    else:
        with RunpackReader(output) as reader:
            reader.execution()
        output.unlink()
    _source_is_unchanged(source, payload)


@_PROPERTY_SETTINGS
@given(payload=_EVIDENCE_BYTES)
def test_prometheus_import_is_atomic_for_bounded_generated_evidence(
    tmp_path: Path, payload: bytes
) -> None:
    runpack = tmp_path / "prometheus-base.runpack"
    source = tmp_path / "generated-prometheus.json"
    output = tmp_path / "generated-prometheus.runpack"
    _write_base(runpack)
    source.write_bytes(payload)
    output.unlink(missing_ok=True)

    try:
        import_prometheus_response(runpack, source, output)
    except PrometheusImportError:
        assert not output.exists()
    else:
        with RunpackReader(output) as reader:
            reader.execution()
        output.unlink()
    _source_is_unchanged(source, payload)


@_PROPERTY_SETTINGS
@given(payload=_EVIDENCE_BYTES)
def test_contract_loader_normalizes_bounded_generated_evidence(
    tmp_path: Path, payload: bytes
) -> None:
    source = tmp_path / "generated-contract.yaml"
    source.write_bytes(payload)

    try:
        load_contracts(source)
    except ContractError:
        pass
    _source_is_unchanged(source, payload)


@_PROPERTY_SETTINGS
@given(payload=st.binary(max_size=4096))
def test_runpack_reader_normalizes_bounded_generated_files(tmp_path: Path, payload: bytes) -> None:
    source = tmp_path / "generated.runpack"
    source.write_bytes(payload)

    try:
        with RunpackReader(source) as reader:
            reader.execution()
    except RunpackError:
        pass
    _source_is_unchanged(source, payload)


@pytest.mark.parametrize(
    ("relative_path", "load", "error"),
    (
        (
            "otel/duplicate-key.json",
            lambda source, output, _base: import_otlp_json(source, output, name="corpus"),
            OtelImportError,
        ),
        (
            "kubernetes/truncated.json",
            lambda source, output, base: import_kubernetes_snapshot(base, source, output),
            KubernetesImportError,
        ),
        (
            "prometheus/nonfinite.json",
            lambda source, output, base: import_prometheus_response(base, source, output),
            PrometheusImportError,
        ),
    ),
)
def test_malformed_adapter_corpus_fails_without_partial_output(
    tmp_path: Path,
    relative_path: str,
    load: Callable[[Path, Path, Path], object],
    error: type[Exception],
) -> None:
    source = _FIXTURES / relative_path
    output = tmp_path / "corpus-output.runpack"
    base = tmp_path / "corpus-base.runpack"
    _write_base(base)
    before = source.read_bytes()

    with pytest.raises(error):
        load(source, output, base)

    assert not output.exists()
    assert source.read_bytes() == before


def test_malformed_contract_and_runpack_corpus_raise_public_errors() -> None:
    with pytest.raises(ContractError):
        load_contracts(_FIXTURES / "contracts" / "alias.yaml")
    with pytest.raises(RunpackError):
        with RunpackReader(_FIXTURES / "runpack" / "not-sqlite.runpack"):
            pass
