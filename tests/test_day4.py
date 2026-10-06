"""Unit and Integration Tests for Day 4 — Intelligent Ledger Reconciliation Engine.

Covers:
1. Reconciliation Result Model Validation
2. Deterministic Rule Matching (Scenarios A through G)
3. Decimal Money Arithmetic & Precision
4. Weighted Scoring & Evidence Generation
5. Ledger Repository CRUD & Lookups
6. Reconciliation Service Coordination
7. Event Integration, Correlation, and Causation Lineage
8. REST API Endpoints (POST /reconciliation/run, GET /reconciliation/{id}, GET /reconciliation/metrics)
9. End-to-End Workflow State and Audit Trail Tracking
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from database.database import init_db, save_transaction
from kafka.local_event_publisher import LocalEventPublisher
from main import app
from orchestrator.event_audit import EventAuditTrail
from orchestrator.event_orchestrator import Event, EventOrchestrator
from orchestrator.event_types import EventType
from orchestrator.workflow_state import StageStatus, WorkflowStatus, WorkflowTracker
from reconciliation.engine import ReconciliationEngine
from reconciliation.ledger_repository import LedgerRepository
from reconciliation.models import LedgerEntry, Transaction
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus
from reconciliation.reconciliation_service import ReconciliationService
from reconciliation.rules import (
    ReconciliationWeights,
    evaluate_account_id,
    evaluate_amount,
    evaluate_currency,
    evaluate_direction,
    evaluate_id_reference,
    evaluate_timestamp,
    to_decimal,
)


# ==============================================================================
# Fixtures
# ==============================================================================
@pytest.fixture
def sample_transaction():
    """Create a canonical base transaction for testing."""
    return Transaction(
        transaction_id="TXN-D4-001",
        account_id="ACC-100001",
        transaction_type="TRANSFER",
        amount=1500.00,
        currency="USD",
        transaction_timestamp=datetime(2026, 10, 6, 12, 0, 0, tzinfo=timezone.utc),
        source_system="CORE_BANKING",
        destination_system="ACH_NETWORK",
        reference_id="REF-D4-001",
    )


@pytest.fixture
def matching_ledger_entry():
    """Create a perfectly matching ledger entry for sample_transaction."""
    return LedgerEntry(
        ledger_entry_id="LEDGER-D4-001",
        transaction_id="TXN-D4-001",
        account_id="ACC-100001",
        amount=1500.00,
        currency="USD",
        debit_credit="DEBIT",
        reference_id="REF-D4-001",
        timestamp=datetime(2026, 10, 6, 12, 1, 0, tzinfo=timezone.utc),
        source_system="GENERAL_LEDGER_CORE",
    )


# ==============================================================================
# 1. Models (1 - 3)
# ==============================================================================
def test_valid_reconciliation_result():
    """1. Verify valid ReconciliationResult creation and attributes."""
    res = ReconciliationResult(
        transaction_id="TXN-101",
        ledger_entry_id="LEDGER-101",
        correlation_id="CORR-101",
        status=ReconciliationStatus.MATCHED.value,
        score=1.0,
        matched_fields=["transaction_id", "amount"],
        mismatched_fields=[],
        mismatch_reasons=[],
        evidence=[{"field": "amount", "match": True}],
    )
    assert res.reconciliation_id is not None
    assert res.status == "MATCHED"
    assert res.score == 1.0
    assert "amount" in res.matched_fields


def test_invalid_score_rejected():
    """2. Verify that scores outside [0.0, 1.0] are rejected by validation."""
    with pytest.raises(ValidationError):
        ReconciliationResult(
            transaction_id="TXN-102",
            status="MATCHED",
            score=1.5,  # Exceeds maximum 1.0
        )

    with pytest.raises(ValidationError):
        ReconciliationResult(
            transaction_id="TXN-102",
            status="MATCHED",
            score=-0.1,  # Sub-zero invalid
        )


def test_invalid_status_rejected():
    """3. Verify that non-controlled status strings are rejected."""
    with pytest.raises(ValidationError):
        ReconciliationResult(
            transaction_id="TXN-103",
            status="RANDOM_STATUS",  # Not in ReconciliationStatus
            score=0.8,
        )


# ==============================================================================
# 2. Matching Scenarios (4 - 12)
# ==============================================================================
def test_perfect_match(sample_transaction, matching_ledger_entry):
    """4. Scenario A: Perfect match yields MATCHED status and score 1.0."""
    engine = ReconciliationEngine()
    result = engine.reconcile(sample_transaction, matching_ledger_entry)

    assert result.status == ReconciliationStatus.MATCHED.value
    assert result.score == 1.0
    assert len(result.mismatched_fields) == 0
    assert len(result.mismatch_reasons) == 0
    assert "amount" in result.matched_fields
    assert "account_id" in result.matched_fields
    assert "currency" in result.matched_fields
    assert "timestamp" in result.matched_fields


def test_amount_mismatch(sample_transaction, matching_ledger_entry):
    """5. Scenario B: Amount mismatch detects delta and lists Amount mismatch reason."""
    mismatched_entry = matching_ledger_entry.model_copy(update={"amount": 1400.00})
    engine = ReconciliationEngine()
    result = engine.reconcile(sample_transaction, mismatched_entry)

    assert result.status in (ReconciliationStatus.PARTIAL_MATCH.value, ReconciliationStatus.MISMATCHED.value)
    assert "amount" in result.mismatched_fields
    assert any("Amount mismatch" in r for r in result.mismatch_reasons)
    assert result.score < 1.0


def test_currency_mismatch(sample_transaction, matching_ledger_entry):
    """6. Scenario C: Currency mismatch lists Currency mismatch reason."""
    mismatched_entry = matching_ledger_entry.model_copy(update={"currency": "EUR"})
    engine = ReconciliationEngine()
    result = engine.reconcile(sample_transaction, mismatched_entry)

    assert "currency" in result.mismatched_fields
    assert any("Currency mismatch" in r for r in result.mismatch_reasons)


def test_account_mismatch(sample_transaction, matching_ledger_entry):
    """7. Scenario D: Account mismatch lists Account mismatch reason."""
    mismatched_entry = matching_ledger_entry.model_copy(update={"account_id": "ACC-999999"})
    engine = ReconciliationEngine()
    result = engine.reconcile(sample_transaction, mismatched_entry)

    assert "account_id" in result.mismatched_fields
    assert any("Account mismatch" in r for r in result.mismatch_reasons)


def test_reference_mismatch(sample_transaction, matching_ledger_entry):
    """8. Verify reference mismatch behavior when tx_id also differs."""
    entry = matching_ledger_entry.model_copy(
        update={"transaction_id": "TXN-DIFF-99", "reference_id": "REF-DIFF-99"}
    )
    engine = ReconciliationEngine()
    result = engine.reconcile(sample_transaction, entry)

    assert "transaction_id" in result.mismatched_fields
    assert any("Reference ID mismatch" in r for r in result.mismatch_reasons)


def test_timestamp_within_tolerance(sample_transaction, matching_ledger_entry):
    """9. Scenario E: Timestamp within 300s tolerance passes timestamp rule."""
    within_tol_entry = matching_ledger_entry.model_copy(
        update={"timestamp": sample_transaction.transaction_timestamp + timedelta(seconds=120)}
    )
    engine = ReconciliationEngine(timestamp_tolerance_seconds=300)
    result = engine.reconcile(sample_transaction, within_tol_entry)

    assert "timestamp" in result.matched_fields


def test_timestamp_outside_tolerance(sample_transaction, matching_ledger_entry):
    """10. Scenario F: Timestamp outside 300s tolerance fails timestamp rule."""
    outside_tol_entry = matching_ledger_entry.model_copy(
        update={"timestamp": sample_transaction.transaction_timestamp + timedelta(seconds=600)}
    )
    engine = ReconciliationEngine(timestamp_tolerance_seconds=300)
    result = engine.reconcile(sample_transaction, outside_tol_entry)

    assert "timestamp" in result.mismatched_fields
    assert any("Timestamp mismatch" in r for r in result.mismatch_reasons)


def test_missing_ledger_entry(sample_transaction):
    """11. Scenario G: Missing ledger entry returns NOT_FOUND status with score 0.0."""
    engine = ReconciliationEngine()
    result = engine.reconcile(sample_transaction, ledger_entry=None)

    assert result.status == ReconciliationStatus.NOT_FOUND.value
    assert result.score == 0.0
    assert result.ledger_entry_id is None
    assert any("not found" in r.lower() for r in result.mismatch_reasons)


def test_multiple_mismatches(sample_transaction, matching_ledger_entry):
    """12. Multiple critical mismatches yield MISMATCHED status."""
    bad_entry = matching_ledger_entry.model_copy(
        update={
            "transaction_id": "TXN-BAD",
            "reference_id": "REF-BAD",
            "amount": 9999.00,
            "currency": "GBP",
            "account_id": "ACC-000000",
        }
    )
    engine = ReconciliationEngine()
    result = engine.reconcile(sample_transaction, bad_entry)

    assert result.status == ReconciliationStatus.MISMATCHED.value
    assert result.score < 0.50
    assert len(result.mismatched_fields) >= 3


# ==============================================================================
# 3. Money & Decimal Precision (13 - 14)
# ==============================================================================
def test_decimal_amount_comparison():
    """13. Verify exact Decimal financial precision preventing IEEE-754 float drift."""
    val1 = to_decimal(0.1 + 0.2)  # IEEE 754 gives 0.30000000000000004
    val2 = to_decimal("0.30")
    assert val1 == val2

    d1 = to_decimal(1500.005)
    d2 = to_decimal("1500.01")
    assert d1 == d2


def test_decimal_rounding_behavior():
    """14. Verify predictable banker's rounding behavior for fractional cents."""
    rounded_down = to_decimal("100.124")
    assert rounded_down == Decimal("100.12")

    rounded_up = to_decimal("100.126")
    assert rounded_up == Decimal("100.13")


# ==============================================================================
# 4. Scoring & Evidence (15 - 17)
# ==============================================================================
def test_correct_weighted_score(sample_transaction, matching_ledger_entry):
    """15. Verify that individual rule weights sum accurately to 1.0."""
    custom_weights = ReconciliationWeights(
        id_reference=0.30,
        account=0.20,
        amount=0.25,
        currency=0.10,
        timestamp=0.10,
        direction=0.05,
    )
    engine = ReconciliationEngine(weights=custom_weights)
    result = engine.reconcile(sample_transaction, matching_ledger_entry)
    assert result.score == 1.0

    # Falsify amount only (lose 0.25)
    amt_fail_entry = matching_ledger_entry.model_copy(update={"amount": 10.00})
    res_partial = engine.reconcile(sample_transaction, amt_fail_entry)
    assert res_partial.score == 0.75


def test_score_boundaries(sample_transaction, matching_ledger_entry):
    """16. Verify that score is strictly bounded between 0.0 and 1.0."""
    engine = ReconciliationEngine()
    res_perfect = engine.reconcile(sample_transaction, matching_ledger_entry)
    assert 0.0 <= res_perfect.score <= 1.0

    res_none = engine.reconcile(sample_transaction, None)
    assert res_none.score == 0.0


def test_explainable_evidence(sample_transaction, matching_ledger_entry):
    """17. Verify that deterministic evidence records are generated for every rule."""
    engine = ReconciliationEngine()
    result = engine.reconcile(sample_transaction, matching_ledger_entry)

    assert len(result.evidence) >= 6
    field_names = {e["field"] for e in result.evidence}
    assert {"transaction_id", "account_id", "amount", "currency", "timestamp", "direction"}.issubset(field_names)
    for ev in result.evidence:
        assert "message" in ev
        assert "match" in ev


# ==============================================================================
# 5. Ledger Repository Tests (18 - 21)
# ==============================================================================
def test_save_ledger_entry(tmp_path, matching_ledger_entry):
    """18. Verify saving a ledger entry to SQLite."""
    db_file = str(tmp_path / "test_repo.db")
    repo = LedgerRepository(db_path=db_file)

    repo.save_ledger_entry(matching_ledger_entry)
    fetched = repo.get_ledger_entry(matching_ledger_entry.ledger_entry_id)
    assert fetched is not None
    assert fetched.ledger_entry_id == matching_ledger_entry.ledger_entry_id


def test_retrieve_ledger_entry(tmp_path, matching_ledger_entry):
    """19. Verify retrieving a saved ledger entry matches all domain attributes."""
    db_file = str(tmp_path / "test_repo_retrieve.db")
    repo = LedgerRepository(db_path=db_file)

    repo.save_ledger_entry(matching_ledger_entry)
    entry = repo.get_ledger_entry("LEDGER-D4-001")
    assert entry.amount == 1500.00
    assert entry.currency == "USD"
    assert entry.account_id == "ACC-100001"
    assert entry.debit_credit == "DEBIT"


def test_find_by_transaction_id(tmp_path, matching_ledger_entry):
    """20. Verify candidate ledger lookup by transaction_id."""
    db_file = str(tmp_path / "test_repo_find_tx.db")
    repo = LedgerRepository(db_path=db_file)

    repo.save_ledger_entry(matching_ledger_entry)
    found = repo.find_by_transaction_id("TXN-D4-001")
    assert found is not None
    assert found.transaction_id == "TXN-D4-001"

    missing = repo.find_by_transaction_id("TXN-NON-EXISTENT")
    assert missing is None


def test_find_by_reference_id(tmp_path, matching_ledger_entry):
    """21. Verify candidate ledger lookup by reference_id."""
    db_file = str(tmp_path / "test_repo_find_ref.db")
    repo = LedgerRepository(db_path=db_file)

    repo.save_ledger_entry(matching_ledger_entry)
    found = repo.find_by_reference_id("REF-D4-001")
    assert found is not None
    assert found.reference_id == "REF-D4-001"


# ==============================================================================
# 6. Service Tests (22 - 25)
# ==============================================================================
def test_service_successful_reconciliation(tmp_path, sample_transaction, matching_ledger_entry):
    """22. Verify service reconciliation with candidate yields MATCHED result."""
    db_file = str(tmp_path / "test_service.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(matching_ledger_entry)

    tracker = WorkflowTracker()
    service = ReconciliationService(repository=repo, workflow_tracker=tracker, db_path=db_file)

    result = service.reconcile_transaction(sample_transaction)
    assert result.status == ReconciliationStatus.MATCHED.value
    assert result.score == 1.0


def test_service_partial_reconciliation(tmp_path, sample_transaction, matching_ledger_entry):
    """23. Verify service reconciliation with minor discrepancy yields PARTIAL_MATCH."""
    db_file = str(tmp_path / "test_service_part.db")
    repo = LedgerRepository(db_path=db_file)
    entry = matching_ledger_entry.model_copy(update={"amount": 1450.00})
    repo.save_ledger_entry(entry)

    service = ReconciliationService(repository=repo, db_path=db_file)
    result = service.reconcile_transaction(sample_transaction)

    assert result.status == ReconciliationStatus.PARTIAL_MATCH.value
    assert "amount" in result.mismatched_fields


def test_service_mismatched_reconciliation(tmp_path, sample_transaction, matching_ledger_entry):
    """24. Verify service reconciliation with heavy discrepancies yields MISMATCHED."""
    db_file = str(tmp_path / "test_service_mis.db")
    repo = LedgerRepository(db_path=db_file)
    bad_entry = matching_ledger_entry.model_copy(
        update={"amount": 999.00, "currency": "INR", "account_id": "ACC-DIFF"}
    )
    repo.save_ledger_entry(bad_entry)

    service = ReconciliationService(repository=repo, db_path=db_file)
    result = service.reconcile_transaction(sample_transaction)

    assert result.status == ReconciliationStatus.MISMATCHED.value


def test_service_not_found_handling(tmp_path, sample_transaction):
    """25. Verify service handles absent ledger entry with NOT_FOUND status."""
    db_file = str(tmp_path / "test_service_nf.db")
    repo = LedgerRepository(db_path=db_file)

    service = ReconciliationService(repository=repo, db_path=db_file)
    result = service.reconcile_transaction(sample_transaction)

    assert result.status == ReconciliationStatus.NOT_FOUND.value
    assert result.score == 0.0


# ==============================================================================
# 7. Event Integration Tests (26 - 29)
# ==============================================================================
def test_reconciliation_request_event(tmp_path, sample_transaction, matching_ledger_entry):
    """26. Verify handling of TRANSACTION_RECONCILIATION_REQUESTED event."""
    db_file = str(tmp_path / "test_event_req.db")
    init_db(db_file)
    save_transaction(sample_transaction, db_path=db_file)

    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(matching_ledger_entry)

    service = ReconciliationService(repository=repo, db_path=db_file)
    event = Event(
        event_type=EventType.TRANSACTION_RECONCILIATION_REQUESTED.value,
        source="coordinator",
        correlation_id="CORR-EV-001",
        payload={"transaction_id": sample_transaction.transaction_id},
    )

    resp = service.on_reconciliation_requested(event)
    assert resp["status"] == "MATCHED"
    assert resp["score"] == 1.0


def test_reconciliation_completed_event(tmp_path, sample_transaction, matching_ledger_entry):
    """27. Verify that reconcile_transaction publishes a TRANSACTION_RECONCILED event."""
    db_file = str(tmp_path / "test_event_pub.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(matching_ledger_entry)

    orch = EventOrchestrator()
    received_events = []
    orch.register_handler(
        EventType.TRANSACTION_RECONCILED.value,
        lambda e: received_events.append(e),
    )

    publisher = LocalEventPublisher(orchestrator=orch)
    service = ReconciliationService(repository=repo, publisher=publisher, db_path=db_file)

    service.reconcile_transaction(sample_transaction, correlation_id="CORR-COMP-001")

    assert len(received_events) == 1
    ev = received_events[0]
    assert ev.event_type == EventType.TRANSACTION_RECONCILED.value
    assert ev.correlation_id == "CORR-COMP-001"
    assert ev.payload["status"] == "MATCHED"


def test_correlation_id_propagation(tmp_path, sample_transaction, matching_ledger_entry):
    """28. Verify correlation_id is consistently preserved in results and emitted events."""
    db_file = str(tmp_path / "test_corr.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(matching_ledger_entry)

    orch = EventOrchestrator()
    captured = []
    orch.register_handler(
        EventType.TRANSACTION_RECONCILED.value,
        lambda e: captured.append(e),
    )
    publisher = LocalEventPublisher(orchestrator=orch)
    service = ReconciliationService(repository=repo, publisher=publisher, db_path=db_file)

    res = service.reconcile_transaction(sample_transaction, correlation_id="CORR-TRACE-XYZ")
    assert res.correlation_id == "CORR-TRACE-XYZ"
    assert captured[0].correlation_id == "CORR-TRACE-XYZ"


def test_causation_id_propagation(tmp_path, sample_transaction, matching_ledger_entry):
    """29. Verify causation_id is mapped to the parent event ID."""
    db_file = str(tmp_path / "test_caus.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(matching_ledger_entry)

    orch = EventOrchestrator()
    captured = []
    orch.register_handler(
        EventType.TRANSACTION_RECONCILED.value,
        lambda e: captured.append(e),
    )
    publisher = LocalEventPublisher(orchestrator=orch)
    service = ReconciliationService(repository=repo, publisher=publisher, db_path=db_file)

    service.reconcile_transaction(
        sample_transaction,
        correlation_id="CORR-01",
        causation_id="PARENT-EVENT-UUID-99",
    )
    assert captured[0].causation_id == "PARENT-EVENT-UUID-99"


# ==============================================================================
# 8. API Endpoints (30 - 32)
# ==============================================================================
def test_api_post_reconciliation_run(sample_transaction, matching_ledger_entry):
    """30. Verify POST /reconciliation/run executes reconciliation and returns result."""
    with TestClient(app) as client:
        # Ingest transaction first via standard POST /transactions
        tx_payload = {
            "transaction_id": sample_transaction.transaction_id,
            "account_id": sample_transaction.account_id,
            "transaction_type": sample_transaction.transaction_type,
            "amount": sample_transaction.amount,
            "currency": sample_transaction.currency,
            "transaction_timestamp": sample_transaction.transaction_timestamp.isoformat(),
            "source_system": sample_transaction.source_system,
            "destination_system": sample_transaction.destination_system,
            "reference_id": sample_transaction.reference_id,
        }
        client.post("/transactions", json=tx_payload)

        # Save matching ledger entry in the repository
        from main import ledger_repository
        ledger_repository.save_ledger_entry(matching_ledger_entry)

        # Invoke POST /reconciliation/run
        resp = client.post("/reconciliation/run", json={"transaction_id": sample_transaction.transaction_id})
        assert resp.status_code == 200
        data = resp.json()
        assert data["transaction_id"] == sample_transaction.transaction_id
        assert data["status"] == "MATCHED"
        assert data["score"] == 1.0
        assert "evidence" in data


def test_api_get_reconciliation_by_id(sample_transaction, matching_ledger_entry):
    """31. Verify GET /reconciliation/{reconciliation_id} retrieves result."""
    with TestClient(app) as client:
        from main import ledger_repository, reconciliation_service

        # Save ledger and run reconciliation
        ledger_repository.save_ledger_entry(matching_ledger_entry)
        result = reconciliation_service.reconcile_transaction(sample_transaction)

        # Query GET /reconciliation/{id}
        resp = client.get(f"/reconciliation/{result.reconciliation_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["reconciliation_id"] == result.reconciliation_id
        assert data["status"] == "MATCHED"
        assert data["score"] == 1.0

        # Query missing ID
        missing_resp = client.get("/reconciliation/NON-EXISTENT-REC-ID")
        assert missing_resp.status_code == 404


def test_api_get_reconciliation_metrics():
    """32. Verify GET /reconciliation/metrics exposes aggregated stats."""
    with TestClient(app) as client:
        resp = client.get("/reconciliation/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert "total" in data
        assert "matched" in data
        assert "partial_match" in data
        assert "mismatched" in data
        assert "not_found" in data
        assert "average_score" in data


# ==============================================================================
# 9. Integration & Workflow Tracking (33 - 35)
# ==============================================================================
def test_integration_transaction_to_reconciliation(sample_transaction, matching_ledger_entry):
    """33. Verify Transaction -> Service -> Candidate Discovery -> Result."""
    from main import ledger_repository, reconciliation_service

    ledger_repository.save_ledger_entry(matching_ledger_entry)
    result = reconciliation_service.reconcile_transaction(sample_transaction)

    assert result.status == "MATCHED"
    assert result.ledger_entry_id == matching_ledger_entry.ledger_entry_id
    assert "amount" in result.matched_fields


def test_integration_workflow_state_updated(sample_transaction, matching_ledger_entry):
    """34. Verify that reconciliation updates workflow state stages."""
    tracker = WorkflowTracker()
    repo = LedgerRepository()
    repo.save_ledger_entry(matching_ledger_entry)

    corr = "CORR-WORKFLOW-TEST-99"
    tracker.create_workflow(correlation_id=corr)

    service = ReconciliationService(repository=repo, workflow_tracker=tracker)
    service.reconcile_transaction(sample_transaction, correlation_id=corr)

    wf = tracker.get_workflow(corr)
    assert wf is not None
    assert wf.stages["reconciliation"] == StageStatus.COMPLETED.value


def test_integration_audit_trail_recorded(sample_transaction, matching_ledger_entry):
    """35. Verify that event audit trail records reconciliation lifecycle."""
    orch = EventOrchestrator()
    audit = EventAuditTrail()
    orch.audit_trail = audit

    publisher = LocalEventPublisher(orchestrator=orch)
    repo = LedgerRepository()
    repo.save_ledger_entry(matching_ledger_entry)

    service = ReconciliationService(repository=repo, publisher=publisher)
    corr = "CORR-AUDIT-TEST-88"
    service.reconcile_transaction(sample_transaction, correlation_id=corr)

    history = audit.get_event_history(corr)
    assert len(history) >= 1
    assert any(h["event_type"] == EventType.TRANSACTION_RECONCILED.value for h in history)
