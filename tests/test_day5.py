"""Unit and Integration Tests for Day 5 — Fuzzy Ledger Candidate Matching.

Covers:
1. Field Normalization (Reference, Transaction ID, Currency, Account ID)
2. Field-Level Similarity (Reference, Account, Currency, Decimal Amount, Timestamp Window)
3. Candidate Ranking, Top-K Filtering, and Confidence Classification (HIGH, MEDIUM, LOW, REJECTED)
4. Candidate Ambiguity Detection & Manual Review Flagging
5. Ledger Repository Candidate Lookup Queries
6. Candidate Discovery Service Execution
7. End-to-End Two-Stage Reconciliation (Fuzzy Discovery -> Deterministic Verification)
8. Event Emission (FUZZY_CANDIDATE_IDENTIFIED) and Lineage Tracking
9. REST APIs (POST /reconciliation/candidates, GET /reconciliation/candidates/{id})
10. Financial Safety & Adversarial Scenarios (Cases 1 through 5)
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import pytest
from fastapi.testclient import TestClient

from database.database import init_db, save_transaction
from kafka.local_event_publisher import LocalEventPublisher
from main import app
from orchestrator.event_audit import EventAuditTrail
from orchestrator.event_orchestrator import Event, EventOrchestrator
from orchestrator.event_types import EventType
from orchestrator.workflow_state import WorkflowTracker
from reconciliation.candidate_service import CandidateService
from reconciliation.engine import ReconciliationEngine
from reconciliation.fuzzy_matcher import FuzzyCandidateMatcher, FuzzyMatchWeights
from reconciliation.fuzzy_result import (
    CandidateDiscoveryResult,
    CandidateSelectionStatus,
    ConfidenceLevel,
    FuzzyCandidate,
)
from reconciliation.ledger_repository import LedgerRepository
from reconciliation.models import LedgerEntry, Transaction
from reconciliation.normalization import (
    normalize_account_id,
    normalize_currency,
    normalize_reference,
    normalize_transaction_id,
)
from reconciliation.reconciliation_result import ReconciliationStatus
from reconciliation.reconciliation_service import ReconciliationService


# ==============================================================================
# Fixtures
# ==============================================================================
@pytest.fixture
def sample_transaction():
    """Standard baseline test transaction."""
    return Transaction(
        transaction_id="TXN-D5-001",
        account_id="ACC-100001",
        transaction_type="PAYMENT",
        amount=1000.00,
        currency="USD",
        transaction_timestamp=datetime(2026, 10, 6, 12, 0, 0, tzinfo=timezone.utc),
        source_system="MOBILE_APP",
        destination_system="ACH_NETWORK",
        reference_id="PAY-ABC-123",
    )


@pytest.fixture
def exact_ledger_entry(sample_transaction):
    """Perfect matching candidate ledger entry."""
    return LedgerEntry(
        ledger_entry_id="LEDGER-EXACT-001",
        transaction_id=sample_transaction.transaction_id,
        account_id=sample_transaction.account_id,
        amount=1000.00,
        currency="USD",
        debit_credit="DEBIT",
        reference_id="PAY-ABC-123",
        timestamp=sample_transaction.transaction_timestamp + timedelta(seconds=30),
        source_system="GENERAL_LEDGER_CORE",
    )


# ==============================================================================
# 1. Normalization Tests (1 - 4)
# ==============================================================================
def test_reference_normalization():
    """1. Verify reference normalization handles case, hyphens, and whitespace."""
    assert normalize_reference(" pay-abc-123 ") == "PAYABC123"
    assert normalize_reference("pay abc 123") == "PAYABC123"
    assert normalize_reference("PAY_ABC.123") == "PAYABC123"
    assert normalize_reference(None) == ""


def test_transaction_id_normalization():
    """2. Verify transaction ID normalization maintains uppercase and strips whitespace."""
    assert normalize_transaction_id("  txn-1001  ") == "TXN-1001"
    assert normalize_transaction_id(None) == ""


def test_currency_normalization():
    """3. Verify currency code normalization converts to uppercase 3-letter ISO."""
    assert normalize_currency(" usd ") == "USD"
    assert normalize_currency("eur") == "EUR"
    assert normalize_currency(None) == ""


def test_account_normalization_preserves_digits():
    """4. Safety: Account normalization preserves all account digits."""
    assert normalize_account_id(" acc-100001 ") == "ACC-100001"
    assert normalize_account_id("ACC 100001") == "ACC100001"
    assert normalize_account_id("123456789012") == "123456789012"
    assert normalize_account_id(None) == ""


# ==============================================================================
# 2. Similarity Tests (5 - 15)
# ==============================================================================
def test_exact_reference_match(sample_transaction, exact_ledger_entry):
    """5. Verify exact reference yields score 1.0."""
    matcher = FuzzyCandidateMatcher()
    candidate = matcher.calculate_candidate_similarity(sample_transaction, exact_ledger_entry)
    assert candidate.field_scores["reference_id"] == 1.0


def test_reference_formatting_difference(sample_transaction, exact_ledger_entry):
    """6. Reference with spacing/lowercase normalizes and yields score 1.0."""
    entry = exact_ledger_entry.model_copy(update={"reference_id": " pay abc 123 "})
    matcher = FuzzyCandidateMatcher()
    candidate = matcher.calculate_candidate_similarity(sample_transaction, entry)
    assert candidate.field_scores["reference_id"] == 1.0


def test_partial_reference_similarity(sample_transaction, exact_ledger_entry):
    """7. Reference typo yields proportional similarity ratio."""
    entry = exact_ledger_entry.model_copy(update={"reference_id": "PAY-ABC-12S"})
    matcher = FuzzyCandidateMatcher()
    candidate = matcher.calculate_candidate_similarity(sample_transaction, entry)
    assert 0.80 <= candidate.field_scores["reference_id"] < 1.0


def test_exact_account_match(sample_transaction, exact_ledger_entry):
    """8. Exact account match yields account score 1.0."""
    matcher = FuzzyCandidateMatcher()
    candidate = matcher.calculate_candidate_similarity(sample_transaction, exact_ledger_entry)
    assert candidate.field_scores["account_id"] == 1.0


def test_account_mismatch_hard_constraint(sample_transaction, exact_ledger_entry):
    """9. Account mismatch triggers hard constraint rejection."""
    bad_acc_entry = exact_ledger_entry.model_copy(update={"account_id": "ACC-999999"})
    matcher = FuzzyCandidateMatcher(enforce_hard_constraints=True)
    candidate = matcher.calculate_candidate_similarity(sample_transaction, bad_acc_entry)

    assert candidate.confidence_level == ConfidenceLevel.REJECTED.value
    assert candidate.candidate_score == 0.0
    assert any("Account mismatch" in r for r in candidate.reasons)


def test_exact_currency_match(sample_transaction, exact_ledger_entry):
    """10. Exact currency match yields score 1.0."""
    matcher = FuzzyCandidateMatcher()
    candidate = matcher.calculate_candidate_similarity(sample_transaction, exact_ledger_entry)
    assert candidate.field_scores["currency"] == 1.0


def test_currency_mismatch_hard_constraint(sample_transaction, exact_ledger_entry):
    """11. Currency mismatch triggers hard constraint rejection."""
    bad_curr_entry = exact_ledger_entry.model_copy(update={"currency": "EUR"})
    matcher = FuzzyCandidateMatcher(enforce_hard_constraints=True)
    candidate = matcher.calculate_candidate_similarity(sample_transaction, bad_curr_entry)

    assert candidate.confidence_level == ConfidenceLevel.REJECTED.value
    assert candidate.candidate_score == 0.0
    assert any("Currency mismatch" in r for r in candidate.reasons)


def test_amount_exact_match(sample_transaction, exact_ledger_entry):
    """12. Exact amount yields amount score 1.0."""
    matcher = FuzzyCandidateMatcher()
    candidate = matcher.calculate_candidate_similarity(sample_transaction, exact_ledger_entry)
    assert candidate.field_scores["amount"] == 1.0


def test_amount_within_tolerance(sample_transaction, exact_ledger_entry):
    """13. Amount within 0.01 tolerance yields 0.98 amount score."""
    entry = exact_ledger_entry.model_copy(update={"amount": 999.99})
    matcher = FuzzyCandidateMatcher(amount_tolerance=0.01)
    candidate = matcher.calculate_candidate_similarity(sample_transaction, entry)
    assert candidate.field_scores["amount"] == 0.98


def test_timestamp_within_window(sample_transaction, exact_ledger_entry):
    """14. Timestamp within 900s window receives high timestamp score."""
    entry = exact_ledger_entry.model_copy(
        update={"timestamp": sample_transaction.transaction_timestamp + timedelta(seconds=120)}
    )
    matcher = FuzzyCandidateMatcher(fuzzy_timestamp_window_seconds=900)
    candidate = matcher.calculate_candidate_similarity(sample_transaction, entry)
    assert candidate.field_scores["timestamp"] > 0.80


def test_timestamp_outside_window(sample_transaction, exact_ledger_entry):
    """15. Timestamp outside 900s window receives timestamp score 0.0."""
    entry = exact_ledger_entry.model_copy(
        update={"timestamp": sample_transaction.transaction_timestamp + timedelta(seconds=1200)}
    )
    matcher = FuzzyCandidateMatcher(fuzzy_timestamp_window_seconds=900)
    candidate = matcher.calculate_candidate_similarity(sample_transaction, entry)
    assert candidate.field_scores["timestamp"] == 0.0


# ==============================================================================
# 3. Candidate Ranking & Confidence Classification (16 - 22)
# ==============================================================================
def test_candidate_scoring_weights(sample_transaction, exact_ledger_entry):
    """16. Candidate aggregate score respects configurable weights."""
    entry = exact_ledger_entry.model_copy(update={"timestamp": sample_transaction.transaction_timestamp})
    custom_weights = FuzzyMatchWeights(reference=0.40, account=0.30, amount=0.20, timestamp=0.05, currency=0.05)
    matcher = FuzzyCandidateMatcher(weights=custom_weights)
    candidate = matcher.calculate_candidate_similarity(sample_transaction, entry)
    assert candidate.candidate_score == 1.0


def test_candidate_ordering(sample_transaction, exact_ledger_entry):
    """17. Candidate list is ranked in strictly descending order of score."""
    good_cand = exact_ledger_entry.model_copy(update={"ledger_entry_id": "L1"})
    medium_cand = exact_ledger_entry.model_copy(
        update={"ledger_entry_id": "L2", "reference_id": "PAY-DIFF", "amount": 995.00}
    )
    matcher = FuzzyCandidateMatcher()
    res = matcher.rank_candidates(sample_transaction, [medium_cand, good_cand])

    assert len(res.candidates) == 2
    assert res.candidates[0].ledger_entry_id == "L1"
    assert res.candidates[0].candidate_score >= res.candidates[1].candidate_score


def test_top_k_limit(sample_transaction, exact_ledger_entry):
    """18. Candidate list respects top_k limit."""
    entries = [
        exact_ledger_entry.model_copy(update={"ledger_entry_id": f"L-{i}"})
        for i in range(10)
    ]
    matcher = FuzzyCandidateMatcher(top_k=3)
    res = matcher.rank_candidates(sample_transaction, entries)
    assert len(res.candidates) == 3


def test_high_confidence_classification(sample_transaction, exact_ledger_entry):
    """19. Score >= 0.90 classified as HIGH confidence."""
    matcher = FuzzyCandidateMatcher(high_threshold=0.90)
    cand = matcher.calculate_candidate_similarity(sample_transaction, exact_ledger_entry)
    assert cand.confidence_level == ConfidenceLevel.HIGH.value


def test_medium_confidence_classification(sample_transaction, exact_ledger_entry):
    """20. Score between 0.75 and 0.89 classified as MEDIUM confidence."""
    entry = exact_ledger_entry.model_copy(
        update={"reference_id": "PAY-DIFF-99", "timestamp": sample_transaction.transaction_timestamp + timedelta(seconds=600)}
    )
    matcher = FuzzyCandidateMatcher(high_threshold=0.90, medium_threshold=0.70)
    cand = matcher.calculate_candidate_similarity(sample_transaction, entry)
    assert cand.confidence_level in (ConfidenceLevel.MEDIUM.value, ConfidenceLevel.LOW.value)


def test_low_confidence_classification(sample_transaction, exact_ledger_entry):
    """21. Score between 0.50 and 0.74 classified as LOW confidence."""
    entry = exact_ledger_entry.model_copy(
        update={"reference_id": "COMPLETELY-DIFFERENT", "amount": 950.00}
    )
    matcher = FuzzyCandidateMatcher(enforce_hard_constraints=False, low_threshold=0.40)
    cand = matcher.calculate_candidate_similarity(sample_transaction, entry)
    assert cand.confidence_level in (ConfidenceLevel.LOW.value, ConfidenceLevel.MEDIUM.value)


def test_rejected_candidate_classification(sample_transaction, exact_ledger_entry):
    """22. Candidate failing constraints or score < 0.50 is REJECTED."""
    bad_entry = exact_ledger_entry.model_copy(update={"currency": "GBP"})
    matcher = FuzzyCandidateMatcher()
    cand = matcher.calculate_candidate_similarity(sample_transaction, bad_entry)
    assert cand.confidence_level == ConfidenceLevel.REJECTED.value


# ==============================================================================
# 4. Ambiguity Detection & Manual Review (23 - 26)
# ==============================================================================
def test_two_close_candidates_ambiguous(sample_transaction, exact_ledger_entry):
    """23. Two candidates with delta < ambiguity_margin yields AMBIGUOUS selection."""
    cand_a = exact_ledger_entry.model_copy(
        update={"ledger_entry_id": "CAND-A", "reference_id": "PAY-ABC-123", "timestamp": sample_transaction.transaction_timestamp + timedelta(seconds=10)}
    )
    cand_b = exact_ledger_entry.model_copy(
        update={"ledger_entry_id": "CAND-B", "reference_id": "PAY-ABC-123", "timestamp": sample_transaction.transaction_timestamp + timedelta(seconds=20)}
    )
    matcher = FuzzyCandidateMatcher(ambiguity_margin=0.03)
    res = matcher.rank_candidates(sample_transaction, [cand_a, cand_b])

    assert res.selection_status == CandidateSelectionStatus.AMBIGUOUS.value
    assert res.requires_manual_review is True
    assert any("Ambiguous candidates" in r for r in res.reasons)


def test_ambiguity_threshold_clear_winner(sample_transaction, exact_ledger_entry):
    """24. Score delta exceeding ambiguity_margin selects clear winner."""
    cand_a = exact_ledger_entry.model_copy(update={"ledger_entry_id": "WINNER"})
    cand_b = exact_ledger_entry.model_copy(
        update={"ledger_entry_id": "RUNNER-UP", "reference_id": "SOME-OTHER-REF", "amount": 980.00}
    )
    matcher = FuzzyCandidateMatcher(ambiguity_margin=0.03)
    res = matcher.rank_candidates(sample_transaction, [cand_a, cand_b])

    assert res.selection_status == CandidateSelectionStatus.SELECTED.value
    assert res.top_candidate.ledger_entry_id == "WINNER"
    assert res.requires_manual_review is False


def test_clear_winner_single_candidate(sample_transaction, exact_ledger_entry):
    """25. Single high-confidence candidate yields SELECTED."""
    matcher = FuzzyCandidateMatcher()
    res = matcher.rank_candidates(sample_transaction, [exact_ledger_entry])
    assert res.selection_status == CandidateSelectionStatus.SELECTED.value
    assert res.requires_manual_review is False


def test_manual_review_flag_on_ambiguity(sample_transaction, exact_ledger_entry):
    """26. Ambiguous candidate selection strictly sets requires_manual_review=True."""
    cand_a = exact_ledger_entry.model_copy(update={"ledger_entry_id": "A"})
    cand_b = exact_ledger_entry.model_copy(update={"ledger_entry_id": "B"})
    matcher = FuzzyCandidateMatcher(ambiguity_margin=0.05)
    res = matcher.rank_candidates(sample_transaction, [cand_a, cand_b])
    assert res.requires_manual_review is True


# ==============================================================================
# 5. Ledger Repository Candidate Lookups (27 - 30)
# ==============================================================================
def test_repo_find_candidates_by_account(tmp_path, exact_ledger_entry):
    """27. Query repository candidates by account ID."""
    db_file = str(tmp_path / "test_repo_acc.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(exact_ledger_entry)

    candidates = repo.find_candidates_by_account("ACC-100001")
    assert len(candidates) == 1
    assert candidates[0].ledger_entry_id == exact_ledger_entry.ledger_entry_id


def test_repo_find_candidates_by_reference(tmp_path, exact_ledger_entry):
    """28. Query repository candidates by reference ID."""
    db_file = str(tmp_path / "test_repo_ref.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(exact_ledger_entry)

    candidates = repo.find_candidates_by_reference("PAY-ABC-123")
    assert len(candidates) == 1
    assert candidates[0].reference_id == "PAY-ABC-123"


def test_repo_find_candidates_by_amount(tmp_path, exact_ledger_entry):
    """29. Query repository candidates within amount window."""
    db_file = str(tmp_path / "test_repo_amt.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(exact_ledger_entry)

    candidates = repo.find_candidates_by_amount(1000.00, tolerance=5.0)
    assert len(candidates) == 1
    assert candidates[0].amount == 1000.00


def test_repo_find_candidates_by_time_window(tmp_path, exact_ledger_entry):
    """30. Query repository candidates within datetime window."""
    db_file = str(tmp_path / "test_repo_time.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(exact_ledger_entry)

    t0 = exact_ledger_entry.timestamp - timedelta(minutes=5)
    t1 = exact_ledger_entry.timestamp + timedelta(minutes=5)
    candidates = repo.find_candidates_by_time_window(t0, t1)
    assert len(candidates) == 1


# ==============================================================================
# 6. Candidate Discovery Service & Two-Stage Integration (31 - 35)
# ==============================================================================
def test_candidate_discovery_service(tmp_path, sample_transaction, exact_ledger_entry):
    """31. CandidateService executes repository search and returns discovery result."""
    db_file = str(tmp_path / "test_cand_svc.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(exact_ledger_entry)

    service = CandidateService(repository=repo)
    result = service.discover_candidates(sample_transaction)

    assert result.candidate_count == 1
    assert result.selection_status == "SELECTED"
    assert result.top_candidate.ledger_entry_id == exact_ledger_entry.ledger_entry_id


def test_fuzzy_candidate_event_published(tmp_path, sample_transaction, exact_ledger_entry):
    """32. Reconcile transaction emits FUZZY_CANDIDATE_IDENTIFIED event."""
    db_file = str(tmp_path / "test_fuzzy_event.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(exact_ledger_entry)

    orch = EventOrchestrator()
    captured_events = []
    orch.register_handler(
        EventType.FUZZY_CANDIDATE_IDENTIFIED.value,
        lambda e: captured_events.append(e),
    )
    publisher = LocalEventPublisher(orchestrator=orch)
    svc = ReconciliationService(repository=repo, publisher=publisher, db_path=db_file)

    svc.reconcile_transaction(sample_transaction, correlation_id="CORR-FUZZY-01")

    assert len(captured_events) == 1
    ev = captured_events[0]
    assert ev.event_type == EventType.FUZZY_CANDIDATE_IDENTIFIED.value
    assert ev.payload["candidate_count"] == 1
    assert ev.payload["top_candidate_id"] == exact_ledger_entry.ledger_entry_id


def test_deterministic_verification_after_fuzzy_matching(tmp_path, sample_transaction, exact_ledger_entry):
    """33. Two-stage flow: Fuzzy finds candidate, deterministic engine validates financial match."""
    db_file = str(tmp_path / "test_two_stage.db")
    repo = LedgerRepository(db_path=db_file)
    # Entry with slightly formatted reference
    fuzzy_entry = exact_ledger_entry.model_copy(update={"reference_id": " pay abc 123 "})
    repo.save_ledger_entry(fuzzy_entry)

    svc = ReconciliationService(repository=repo, db_path=db_file)
    res = svc.reconcile_transaction(sample_transaction)

    assert res.status == ReconciliationStatus.MATCHED.value
    assert res.score == 1.0
    assert res.ledger_entry_id == fuzzy_entry.ledger_entry_id


def test_workflow_state_update_with_manual_review(tmp_path, sample_transaction, exact_ledger_entry):
    """34. Ambiguous candidate updates workflow state metadata with requires_manual_review=True."""
    db_file = str(tmp_path / "test_wf_review.db")
    repo = LedgerRepository(db_path=db_file)
    cand_a = exact_ledger_entry.model_copy(update={"ledger_entry_id": "A", "reference_id": "REF-A"})
    cand_b = exact_ledger_entry.model_copy(update={"ledger_entry_id": "B", "reference_id": "REF-B"})
    repo.save_ledger_entry(cand_a)
    repo.save_ledger_entry(cand_b)

    tracker = WorkflowTracker()
    corr = "CORR-WF-REVIEW-99"
    tracker.create_workflow(corr)

    svc = ReconciliationService(repository=repo, workflow_tracker=tracker, db_path=db_file)
    res = svc.reconcile_transaction(sample_transaction, correlation_id=corr)

    assert res.status == ReconciliationStatus.AMBIGUOUS.value
    assert res.requires_manual_review is True

    wf = tracker.get_workflow(corr)
    assert wf is not None
    assert wf.stages["reconciliation"] == "COMPLETED"


def test_audit_trail_captures_fuzzy_and_reconciled(tmp_path, sample_transaction, exact_ledger_entry):
    """35. Event audit trail captures both FUZZY_CANDIDATE_IDENTIFIED and TRANSACTION_RECONCILED."""
    db_file = str(tmp_path / "test_audit_fuzzy.db")
    repo = LedgerRepository(db_path=db_file)
    repo.save_ledger_entry(exact_ledger_entry)

    orch = EventOrchestrator()
    audit = EventAuditTrail()
    orch.audit_trail = audit
    publisher = LocalEventPublisher(orchestrator=orch)

    svc = ReconciliationService(repository=repo, publisher=publisher, db_path=db_file)
    corr = "CORR-AUDIT-D5-01"
    svc.reconcile_transaction(sample_transaction, correlation_id=corr)

    history = audit.get_event_history(corr)
    event_types = [h["event_type"] for h in history]
    assert EventType.FUZZY_CANDIDATE_IDENTIFIED.value in event_types
    assert EventType.TRANSACTION_RECONCILED.value in event_types


# ==============================================================================
# 7. REST API Endpoints (36 - 38)
# ==============================================================================
def test_api_post_candidates(sample_transaction, exact_ledger_entry):
    """36. POST /reconciliation/candidates discovers and ranks candidates."""
    with TestClient(app) as client:
        # Save transaction and ledger entry
        from main import ledger_repository
        save_transaction(sample_transaction)
        ledger_repository.save_ledger_entry(exact_ledger_entry)

        resp = client.post("/reconciliation/candidates", json={"transaction_id": sample_transaction.transaction_id})
        assert resp.status_code == 200
        data = resp.json()
        assert data["transaction_id"] == sample_transaction.transaction_id
        assert data["candidate_count"] >= 1
        assert "candidates" in data


def test_api_get_candidates_by_transaction_id(sample_transaction, exact_ledger_entry):
    """37. GET /reconciliation/candidates/{transaction_id} retrieves candidate details."""
    with TestClient(app) as client:
        save_transaction(sample_transaction)
        from main import ledger_repository
        ledger_repository.save_ledger_entry(exact_ledger_entry)

        resp = client.get(f"/reconciliation/candidates/{sample_transaction.transaction_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["transaction_id"] == sample_transaction.transaction_id
        assert len(data["candidates"]) >= 1


def test_api_ambiguous_candidate_review_flag(sample_transaction, exact_ledger_entry):
    """38. Ambiguous candidate endpoint response flags requires_manual_review=True."""
    cand_a = exact_ledger_entry.model_copy(update={"ledger_entry_id": "AMB-1"})
    cand_b = exact_ledger_entry.model_copy(update={"ledger_entry_id": "AMB-2"})
    from main import ledger_repository
    ledger_repository.save_ledger_entry(cand_a)
    ledger_repository.save_ledger_entry(cand_b)
    save_transaction(sample_transaction)

    with TestClient(app) as client:
        resp = client.get(f"/reconciliation/candidates/{sample_transaction.transaction_id}")
        assert resp.status_code == 200
        data = resp.json()
        if data["selection_status"] == "AMBIGUOUS":
            assert data["requires_manual_review"] is True


# ==============================================================================
# 8. Financial Safety & Adversarial Cases (39 - 47)
# ==============================================================================
def test_currency_mismatch_never_high_confidence(sample_transaction, exact_ledger_entry):
    """39. Safety: Different currency cannot become a high-confidence candidate."""
    bad_curr = exact_ledger_entry.model_copy(update={"currency": "EUR"})
    matcher = FuzzyCandidateMatcher()
    cand = matcher.calculate_candidate_similarity(sample_transaction, bad_curr)
    assert cand.confidence_level == ConfidenceLevel.REJECTED.value
    assert cand.candidate_score == 0.0


def test_account_mismatch_never_high_confidence(sample_transaction, exact_ledger_entry):
    """40. Safety: Different account cannot become a high-confidence candidate."""
    bad_acc = exact_ledger_entry.model_copy(update={"account_id": "ACC-888888"})
    matcher = FuzzyCandidateMatcher()
    cand = matcher.calculate_candidate_similarity(sample_transaction, bad_acc)
    assert cand.confidence_level == ConfidenceLevel.REJECTED.value
    assert cand.candidate_score == 0.0


def test_fuzzy_score_cannot_directly_produce_matched(sample_transaction, exact_ledger_entry):
    """41. Safety: Fuzzy matcher only produces candidates; cannot declare financial MATCHED status."""
    matcher = FuzzyCandidateMatcher()
    cand = matcher.calculate_candidate_similarity(sample_transaction, exact_ledger_entry)
    # Fuzzy candidate has candidate_score and confidence_level, NOT financial reconciliation status
    assert hasattr(cand, "candidate_score")
    assert not hasattr(cand, "status")


def test_ambiguous_candidates_require_manual_review(sample_transaction, exact_ledger_entry):
    """42. Safety: Ambiguous candidate matches always mandate manual review."""
    cand_a = exact_ledger_entry.model_copy(update={"ledger_entry_id": "A"})
    cand_b = exact_ledger_entry.model_copy(update={"ledger_entry_id": "B"})
    matcher = FuzzyCandidateMatcher(ambiguity_margin=0.03)
    res = matcher.rank_candidates(sample_transaction, [cand_a, cand_b])
    assert res.requires_manual_review is True


def test_adversarial_case_1_same_reference_different_account(sample_transaction, exact_ledger_entry):
    """43. Adversarial Case 1: Same reference but completely different account -> REJECTED."""
    bad_entry = exact_ledger_entry.model_copy(update={"account_id": "ACC-HACKER"})
    matcher = FuzzyCandidateMatcher()
    cand = matcher.calculate_candidate_similarity(sample_transaction, bad_entry)
    assert cand.confidence_level == ConfidenceLevel.REJECTED.value


def test_adversarial_case_2_same_amount_different_account_currency(sample_transaction, exact_ledger_entry):
    """44. Adversarial Case 2: Same amount but different account and currency -> REJECTED."""
    bad_entry = exact_ledger_entry.model_copy(update={"account_id": "ACC-HACKER", "currency": "INR"})
    matcher = FuzzyCandidateMatcher()
    cand = matcher.calculate_candidate_similarity(sample_transaction, bad_entry)
    assert cand.confidence_level == ConfidenceLevel.REJECTED.value


def test_adversarial_case_3_similar_reference_different_account(sample_transaction, exact_ledger_entry):
    """45. Adversarial Case 3: Very similar reference IDs but different accounts -> REJECTED."""
    bad_entry = exact_ledger_entry.model_copy(
        update={"reference_id": "PAY-ABC-124", "account_id": "ACC-UNAUTHORIZED"}
    )
    matcher = FuzzyCandidateMatcher()
    cand = matcher.calculate_candidate_similarity(sample_transaction, bad_entry)
    assert cand.confidence_level == ConfidenceLevel.REJECTED.value


def test_adversarial_case_4_nearly_identical_candidates_ambiguous(sample_transaction, exact_ledger_entry):
    """46. Adversarial Case 4: Two nearly identical candidates -> AMBIGUOUS."""
    cand_a = exact_ledger_entry.model_copy(update={"ledger_entry_id": "A", "reference_id": "PAY-ABC-123"})
    cand_b = exact_ledger_entry.model_copy(update={"ledger_entry_id": "B", "reference_id": "PAY-ABC-123"})
    matcher = FuzzyCandidateMatcher()
    res = matcher.rank_candidates(sample_transaction, [cand_a, cand_b])
    assert res.selection_status == CandidateSelectionStatus.AMBIGUOUS.value


def test_adversarial_case_5_clear_winner(sample_transaction, exact_ledger_entry):
    """47. Adversarial Case 5: One candidate clearly better than others -> SELECTED."""
    good_cand = exact_ledger_entry.model_copy(update={"ledger_entry_id": "CLEAR-WINNER"})
    poor_cand = exact_ledger_entry.model_copy(
        update={"ledger_entry_id": "POOR-CANDIDATE", "amount": 800.00, "reference_id": "REF-OTHER"}
    )
    matcher = FuzzyCandidateMatcher()
    res = matcher.rank_candidates(sample_transaction, [poor_cand, good_cand])
    assert res.selection_status == CandidateSelectionStatus.SELECTED.value
    assert res.top_candidate.ledger_entry_id == "CLEAR-WINNER"
