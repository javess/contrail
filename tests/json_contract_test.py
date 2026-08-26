from __future__ import annotations

import json

import pytest

from runtime_tools.batchscope.analysis import BatchAnalysis
from runtime_tools.batchscope.analysis._boundary_models import (
    HttpCaptureSummary,
    HttpRequest,
)
from runtime_tools.batchscope.analysis._profile_models import LifecyclePhase
from runtime_tools.json_support import OUTPUT_FORMAT_VERSION, output_document


def test_output_document_adds_the_format_2_discriminator() -> None:
    assert output_document("runtime.inspect", {"id": "run-1"}) == {
        "document_type": "runtime.inspect",
        "format_version": "2",
        "id": "run-1",
    }
    assert OUTPUT_FORMAT_VERSION == "2"


@pytest.mark.parametrize("reserved", ["document_type", "format_version"])
def test_output_document_rejects_reserved_body_fields(reserved: str) -> None:
    with pytest.raises(ValueError, match="reserved protocol fields"):
        output_document("runtime.inspect", {reserved: "collision"})


def test_pydantic_serializes_typed_models_without_shadow_serializers() -> None:
    summary = HttpCaptureSummary(
        status="complete",
        process_count=1,
        request_count=2,
        dropped_request_count=0,
        callback_error_count=0,
        invalid_event_count=0,
        caller_count=1,
        attributed_request_count=2,
        adapters=("stdlib.http.client",),
    )

    value = summary.as_json_value()

    assert value == {
        "status": "complete",
        "process_count": 1,
        "request_count": 2,
        "dropped_request_count": 0,
        "callback_error_count": 0,
        "invalid_event_count": 0,
        "caller_attribution_status": "unavailable",
        "caller_count": 1,
        "attributed_request_count": 2,
        "unattributed_request_count": 0,
        "invalid_caller_count": 0,
        "caller_callback_error_count": 0,
        "adapters": ["stdlib.http.client"],
    }
    assert "observer" not in value
    assert "server_address_captured" not in value
    json.dumps(value, allow_nan=False)


def test_batch_analysis_uses_the_same_recursive_pydantic_path() -> None:
    analysis = BatchAnalysis(
        execution_id="run-1",
        name="example",
        total_seconds=1.5,
        lifecycle=(LifecyclePhase("work", 1.5, "derived"),),
        critical_path=None,
        throughput=None,
        bottlenecks=(),
        http_capture=HttpCaptureSummary("complete", 1, 1, 0, 0, 0),
        http_requests=(
            HttpRequest(
                "request-1",
                "GET",
                "https",
                443,
                1,
                "root",
                "response",
                200,
                None,
                1,
                0.1,
            ),
        ),
    )

    value = analysis.as_json_value()

    assert value["document_type"] == "batchscope.inspect"
    assert value["format_version"] == "2"
    assert value["lifecycle"] == [{"name": "work", "duration_seconds": 1.5, "source": "derived"}]
    requests = value["http_requests"]
    assert isinstance(requests, list)
    request = requests[0]
    assert isinstance(request, dict)
    assert request["adapter"] == "stdlib.http.client"
    assert "server_address" not in request
    assert value["semantic_capture"] is None
    json.dumps(value, allow_nan=False)


def test_json_schema_is_generated_from_the_model_annotations() -> None:
    schema = HttpRequest.json_schema()

    assert schema["type"] == "object"
    required = schema["required"]
    assert isinstance(required, list)
    assert "event_id" in required
    properties = schema["properties"]
    assert isinstance(properties, dict)
    scheme = properties["scheme"]
    assert isinstance(scheme, dict)
    assert scheme["enum"] == ["http", "https"]
