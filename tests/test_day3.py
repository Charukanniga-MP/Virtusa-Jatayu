"""Comprehensive Test Suite for Day 3: Event-Driven Orchestration & Processing Pipeline.

Covers all 25 mandated Day 3 validation criteria:
1. Event type enum
2. Correlation ID
3. Causation ID
4. Retry count
5. Handler registration
6. Handler unregistration
7. Multiple handlers
8. Event routing
9. Workflow creation
10. Workflow state update
11. Workflow completion
12. Workflow failure
13. Handler exception
14. Retry mechanism
15. Maximum retry limit
16. Event idempotency
17. Duplicate event handling
18. Event audit trail
19. Correlation event history
20. Workflow API
21. Event history API
22. Event metrics
23. Complete successful pipeline
24. Failed pipeline
25. API event processing
"""

import uuid
from datetime import datetime, timezone
import pytest
from fastapi.testclient import TestClient

from main import app, audit_trail, orchestrator, pipeline, workflow_tracker
from orchestrator.event_audit import AuditStatus, EventAuditTrail
from orchestrator.event_orchestrator import Event, EventOrchestrator
from orchestrator.event_types import EventType
from orchestrator.handlers import (
    BaseEventHandler,
    ValidationHandler,
)
from orchestrator.processing_pipeline import ProcessingPipeline
from orchestrator.workflow_state import (
    StageStatus,
    WorkflowStatus,
    WorkflowTracker,
)
from reconciliation.models import Transaction


# ==============================================================================
# 1. Event Type Enum Test
# ==============================================================================
def test_event_type_enum():
    """1. Verify that EventType enum defines all required banking event types."""
    assert EventType.TRANSACTION_RECEIVED.value == "TRANSACTION_RECEIVED"
    assert EventType.TRANSACTION_VALIDATED.value == "TRANSACTION_VALIDATED"
    assert EventType.TRANSACTION_RECONCILIATION_REQUESTED.value == "TRANSACTION_RECONCILIATION_REQUESTED"
    assert EventType.TRANSACTION_RECONCILED.value == "TRANSACTION_RECONCILED"
    assert EventType.FRAUD_ANALYSIS_REQUESTED.value == "FRAUD_ANALYSIS_REQUESTED"
    assert EventType.FRAUD_ANALYSIS_COMPLETED.value == "FRAUD_ANALYSIS_COMPLETED"
    assert EventType.RISK_ANALYSIS_REQUESTED.value == "RISK_ANALYSIS_REQUESTED"
    assert EventType.RISK_ANALYSIS_COMPLETED.value == "RISK_ANALYSIS_COMPLETED"
    assert EventType.TRANSACTION_APPROVED.value == "TRANSACTION_APPROVED"
    assert EventType.PROCESSING_FAILED.value == "PROCESSING_FAILED"
    # String equivalence
    assert str(EventType.TRANSACTION_RECEIVED) == "TRANSACTION_RECEIVED"


# ==============================================================================
# 2. Correlation ID Test
# ==============================================================================
def test_event_correlation_id():
    """2. Verify that correlation IDs are preserved across event representations."""
    test_corr_id = "CORR-TEST-998877"
    event = Event(
        event_type=EventType.TRANSACTION_RECEIVED.value,
        source="test_source",
        correlation_id=test_corr_id,
        payload={"tx_id": "TXN-01"},
    )
    assert event.correlation_id == test_corr_id
    assert event.event_id is not None


# ==============================================================================
# 3. Causation ID Test
# ==============================================================================
def test_event_causation_id():
    """3. Verify that causation IDs establish traceable parent-child linkages."""
    parent_event = Event(
        event_type=EventType.TRANSACTION_RECEIVED.value,
        source="gateway",
        payload={"amount": 100},
    )
    child_event = Event(
        event_type=EventType.TRANSACTION_VALIDATED.value,
        source="validator",
        correlation_id=parent_event.correlation_id,
        causation_id=parent_event.event_id,
        payload={"amount": 100},
    )
    assert child_event.causation_id == parent_event.event_id
    assert child_event.correlation_id == parent_event.correlation_id


# ==============================================================================
# 4. Retry Count Test
# ==============================================================================
def test_event_retry_count():
    """4. Verify that retry counts default to 0 and increment accurately."""
    event = Event(
        event_type=EventType.TRANSACTION_RECEIVED.value,
        source="test",
        payload={},
    )
    assert event.retry_count == 0
    event.retry_count += 1
    assert event.retry_count == 1


# ==============================================================================
# 5. Handler Registration Test
# ==============================================================================
def test_handler_registration():
    """5. Verify that registering handlers updates registry and counts."""
    orch = EventOrchestrator()
    assert orch.get_metrics()["handlers_registered"] == 0

    def sample_handler(e: Event):
        return "HANDLED"

    orch.register_handler(EventType.TRANSACTION_RECEIVED.value, sample_handler)
    assert orch.get_metrics()["handlers_registered"] == 1
    assert EventType.TRANSACTION_RECEIVED.value in orch.get_registered_event_types()


# ==============================================================================
# 6. Handler Unregistration Test
# ==============================================================================
def test_handler_unregistration():
    """6. Verify that unregistering handlers removes them from active dispatch."""
    orch = EventOrchestrator()

    def dummy_handler(e: Event):
        return "DUMMY"

    orch.register_handler("TEST_EVENT", dummy_handler)
    assert orch.get_metrics()["handlers_registered"] == 1

    removed = orch.unregister_handler("TEST_EVENT", dummy_handler)
    assert removed is True
    assert orch.get_metrics()["handlers_registered"] == 0


# ==============================================================================
# 7. Multiple Handlers for Same Event Type Test
# ==============================================================================
def test_multiple_handlers_same_event():
    """7. Verify that multiple registered handlers for a single event type all execute."""
    orch = EventOrchestrator()
    calls = []

    orch.register_handler("MULTI_EVENT", lambda e: calls.append("handler_1"))
    orch.register_handler("MULTI_EVENT", lambda e: calls.append("handler_2"))

    test_evt = Event(event_type="MULTI_EVENT", source="test", payload={})
    orch.publish(test_evt)

    assert "handler_1" in calls
    assert "handler_2" in calls
    assert len(calls) == 2


# ==============================================================================
# 8. Event Routing to Correct Handlers Test
# ==============================================================================
def test_event_routing():
    """8. Verify that events route strictly to matched handlers without cross-pollution."""
    orch = EventOrchestrator()
    received_a = []
    received_b = []

    orch.register_handler("EVENT_A", lambda e: received_a.append(e.event_id))
    orch.register_handler("EVENT_B", lambda e: received_b.append(e.event_id))

    evt = Event(event_type="EVENT_A", source="test", payload={})
    orch.publish(evt)

    assert len(received_a) == 1
    assert len(received_b) == 0


# ==============================================================================
# 9. Workflow Creation Test
# ==============================================================================
def test_workflow_creation():
    """9. Verify that WorkflowTracker properly initializes workflow states."""
    tracker = WorkflowTracker()
    state = tracker.create_workflow("CORR-WF-001")

    assert state.correlation_id == "CORR-WF-001"
    assert state.status == WorkflowStatus.PROCESSING.value
    assert "validation" in state.stages
    assert state.stages["validation"] == StageStatus.PENDING.value


# ==============================================================================
# 10. Workflow State Update Test
# ==============================================================================
def test_workflow_state_update():
    """10. Verify updating individual stages in WorkflowTracker."""
    tracker = WorkflowTracker()
    tracker.create_workflow("CORR-WF-002")

    updated = tracker.update_stage("CORR-WF-002", "validation", StageStatus.COMPLETED.value)
    assert updated.stages["validation"] == StageStatus.COMPLETED.value
    assert updated.status == WorkflowStatus.PROCESSING.value


# ==============================================================================
# 11. Workflow Completion Test
# ==============================================================================
def test_workflow_completion():
    """11. Verify marking a workflow as COMPLETED."""
    tracker = WorkflowTracker()
    tracker.create_workflow("CORR-WF-003")

    completed = tracker.mark_completed("CORR-WF-003", final_disposition="APPROVED")
    assert completed.status == WorkflowStatus.COMPLETED.value
    assert completed.stages["final_decision"] == StageStatus.COMPLETED.value
    assert completed.metadata.get("final_disposition") == "APPROVED"


# ==============================================================================
# 12. Workflow Failure Test
# ==============================================================================
def test_workflow_failure():
    """12. Verify marking a workflow as FAILED with error recording."""
    tracker = WorkflowTracker()
    tracker.create_workflow("CORR-WF-004")

    failed = tracker.mark_failed("CORR-WF-004", error="Signature mismatch", failed_stage="validation")
    assert failed.status == WorkflowStatus.FAILED.value
    assert failed.error == "Signature mismatch"
    assert failed.stages["validation"] == StageStatus.FAILED.value


# ==============================================================================
# 13. Handler Exception Handling Test
# ==============================================================================
def test_handler_exception_handled():
    """13. Verify that exceptions in handlers are caught gracefully without crashing."""
    orch = EventOrchestrator(max_retries=1)

    def failing_handler(e: Event):
        raise ValueError("Simulated network timeout")

    orch.register_handler("FAIL_EVENT", failing_handler)
    evt = Event(event_type="FAIL_EVENT", source="test", payload={})

    # Must return without throwing exception
    results = orch.publish(evt)
    assert len(results) == 1
    assert "error" in results[0]
    assert orch.get_metrics()["events_failed"] == 1


# ==============================================================================
# 14. Retry Mechanism Test
# ==============================================================================
def test_retry_mechanism():
    """14. Verify that failing handlers are retried up to max_retries."""
    orch = EventOrchestrator(max_retries=2)
    attempts = [0]

    def retryable_handler(e: Event):
        attempts[0] += 1
        if attempts[0] < 2:
            raise RuntimeError("Transient glitch")
        return "SUCCESS_ON_RETRY"

    orch.register_handler("RETRY_EVENT", retryable_handler)
    evt = Event(event_type="RETRY_EVENT", source="test", payload={})

    results = orch.publish(evt)
    assert attempts[0] == 2
    assert results == ["SUCCESS_ON_RETRY"]
    assert orch.get_metrics()["events_retried"] == 1


# ==============================================================================
# 15. Maximum Retry Limit Test
# ==============================================================================
def test_maximum_retry_limit():
    """15. Verify that retries stop strictly once max_retries ceiling is reached."""
    orch = EventOrchestrator(max_retries=3)
    call_count = [0]

    def persistent_failure_handler(e: Event):
        call_count[0] += 1
        raise ConnectionResetError("Connection permanently dropped")

    orch.register_handler("PERM_FAIL_EVENT", persistent_failure_handler)
    evt = Event(event_type="PERM_FAIL_EVENT", source="test", payload={})

    orch.publish(evt)
    # Initial call (0) + 3 retries = 4 invocations total
    assert call_count[0] == 4
    assert orch.get_metrics()["events_failed"] == 1
    assert orch.get_metrics()["events_retried"] == 3


# ==============================================================================
# 16. Event Idempotency Test
# ==============================================================================
def test_event_idempotency():
    """16. Verify that an identical event ID is never executed more than once."""
    orch = EventOrchestrator()
    execution_counter = [0]

    def idempotent_handler(e: Event):
        execution_counter[0] += 1
        return "PROCESSED"

    orch.register_handler("IDEMPOTENT_EVENT", idempotent_handler)
    fixed_id = str(uuid.uuid4())
    event_1 = Event(event_id=fixed_id, event_type="IDEMPOTENT_EVENT", source="test", payload={})
    event_2 = Event(event_id=fixed_id, event_type="IDEMPOTENT_EVENT", source="test", payload={})

    res1 = orch.publish(event_1)
    res2 = orch.publish(event_2)

    assert execution_counter[0] == 1  # Executed only once!
    assert res1 == ["PROCESSED"]
    assert res2[0]["status"] == "DUPLICATE"


# ==============================================================================
# 17. Duplicate Event Handling Test
# ==============================================================================
def test_duplicate_event_metrics():
    """17. Verify that duplicate events update the duplicate metric counter."""
    orch = EventOrchestrator()
    orch.register_handler("DUP_TEST", lambda e: "OK")

    dup_id = "EVT-DUP-001"
    e1 = Event(event_id=dup_id, event_type="DUP_TEST", source="test", payload={})
    e2 = Event(event_id=dup_id, event_type="DUP_TEST", source="test", payload={})

    orch.publish(e1)
    orch.publish(e2)

    metrics = orch.get_metrics()
    assert metrics["events_duplicate"] == 1


# ==============================================================================
# 18. Event Audit Trail Recording Test
# ==============================================================================
def test_event_audit_trail_recording():
    """18. Verify that EventAuditTrail records status transitions correctly."""
    audit = EventAuditTrail()
    rec = audit.record(
        event_id="EVT-AUD-1",
        event_type="TRANSACTION_RECEIVED",
        correlation_id="CORR-AUD-1",
        source="test",
        processing_status=AuditStatus.COMPLETED.value,
        causation_id="EVT-ROOT",
    )
    assert rec.event_id == "EVT-AUD-1"
    assert rec.processing_status == "COMPLETED"
    assert rec.causation_id == "EVT-ROOT"
    assert len(audit.get_all_records()) == 1


# ==============================================================================
# 19. Correlation Event History Test
# ==============================================================================
def test_correlation_event_history():
    """19. Verify that get_event_history returns complete trace for correlation ID."""
    audit = EventAuditTrail()
    corr = "CORR-HIST-100"

    audit.record(
        event_id="E1",
        event_type="STEP_1",
        correlation_id=corr,
        source="s1",
        processing_status="COMPLETED",
    )
    audit.record(
        event_id="E2",
        event_type="STEP_2",
        correlation_id=corr,
        source="s2",
        processing_status="COMPLETED",
        causation_id="E1",
    )

    history = audit.get_event_history(corr)
    assert len(history) == 2
    assert history[0]["event_id"] == "E1"
    assert history[1]["causation_id"] == "E1"


# ==============================================================================
# 20. Workflow API Endpoint Test
# ==============================================================================
def test_workflow_api_endpoint():
    """20. Verify GET /workflows/{correlation_id} returns state or 404."""
    with TestClient(app) as client:
        # 1. Existing workflow
        test_corr = f"CORR-API-{uuid.uuid4().hex[:6]}"
        workflow_tracker.create_workflow(test_corr)
        workflow_tracker.update_stage(test_corr, "validation", StageStatus.COMPLETED.value)

        resp = client.get(f"/workflows/{test_corr}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["correlation_id"] == test_corr
        assert data["stages"]["validation"] == StageStatus.COMPLETED.value

        # 2. Non-existent workflow returns 404
        resp_404 = client.get("/workflows/NON_EXISTENT_CORRELATION_ID")
        assert resp_404.status_code == 404


# ==============================================================================
# 21. Event History API Endpoint Test
# ==============================================================================
def test_event_history_api_endpoint():
    """21. Verify GET /events/{correlation_id} returns event audit trail."""
    with TestClient(app) as client:
        test_corr = f"CORR-AUD-API-{uuid.uuid4().hex[:6]}"
        audit_trail.record(
            event_id="EVT-API-01",
            event_type="TRANSACTION_RECEIVED",
            correlation_id=test_corr,
            source="API_TEST",
            processing_status="COMPLETED",
        )

        resp = client.get(f"/events/{test_corr}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["correlation_id"] == test_corr
        assert data["event_count"] >= 1


# ==============================================================================
# 22. Event Metrics API Endpoint Test
# ==============================================================================
def test_event_metrics_endpoint():
    """22. Verify GET /events/metrics exposes extended Day 3 counters."""
    with TestClient(app) as client:
        resp = client.get("/events/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert "events_received" in data
        assert "events_processed" in data
        assert "events_failed" in data
        assert "events_retried" in data
        assert "events_duplicate" in data
        assert "handlers_registered" in data


# ==============================================================================
# 23. Complete Successful Pipeline Test
# ==============================================================================
def test_complete_successful_pipeline():
    """23. Verify end-to-end pipeline execution from RECEIVED to APPROVED."""
    local_orch = EventOrchestrator()
    local_tracker = WorkflowTracker()
    pipe = ProcessingPipeline(orchestrator=local_orch, workflow_tracker=local_tracker)

    tx = Transaction(
        transaction_id="TXN-PIPE-001",
        account_id="ACC-100001",
        transaction_type="PAYMENT",
        amount=500.00,
        currency="USD",
        transaction_timestamp=datetime.now(timezone.utc),
        source_system="CORE",
        destination_system="ACH",
    )

    state = pipe.execute_transaction_workflow(tx)
    assert state.status == WorkflowStatus.COMPLETED.value
    assert state.stages["validation"] == StageStatus.COMPLETED.value
    assert state.stages["reconciliation"] == StageStatus.COMPLETED.value
    assert state.stages["fraud_analysis"] == StageStatus.COMPLETED.value
    assert state.stages["risk_analysis"] == StageStatus.COMPLETED.value
    assert state.stages["final_decision"] == StageStatus.COMPLETED.value


# ==============================================================================
# 24. Failed Pipeline Test
# ==============================================================================
def test_failed_pipeline_flow():
    """24. Verify that pipeline marks workflow as FAILED when a handler encounters fatal error."""
    local_tracker = WorkflowTracker()
    local_orch = EventOrchestrator(max_retries=1, workflow_tracker=local_tracker)

    def failing_recon(e: Event):
        raise ValueError("General Ledger account blocked")

    local_orch.register_handler(
        EventType.TRANSACTION_RECONCILIATION_REQUESTED.value,
        failing_recon,
    )

    corr = "CORR-FAIL-PIPELINE"
    local_tracker.create_workflow(corr)

    fail_event = Event(
        event_type=EventType.TRANSACTION_RECONCILIATION_REQUESTED.value,
        source="test",
        correlation_id=corr,
        payload={"transaction_id": "TXN-FAIL-01"},
    )
    local_orch.publish(fail_event)

    wf = local_tracker.get_workflow(corr)
    assert wf is not None
    assert wf.status == WorkflowStatus.FAILED.value
    assert "General Ledger account blocked" in str(wf.error)


# ==============================================================================
# 25. API Event Processing Test
# ==============================================================================
def test_api_event_processing():
    """25. Verify POST /events/process accepts and dispatches arbitrary event payload."""
    with TestClient(app) as client:
        payload = {
            "event_type": "TRANSACTION_RECEIVED",
            "source": "EXTERNAL_GATEWAY",
            "payload": {"transaction_id": "TXN-MANUAL-001", "amount": 120.0},
        }
        resp = client.post("/events/process", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "processed"
        assert "event_id" in data
        assert "correlation_id" in data
