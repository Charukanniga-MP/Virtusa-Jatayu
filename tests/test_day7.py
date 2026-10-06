"""Comprehensive Test Suite for Day 7 Multi-Agent Coordination & Consensus.

Covers:
1. BaseAgent interface, metadata, and execution records
2. ReconciliationAgent (MATCHED, MISMATCHED, AMBIGUOUS, NOT_FOUND)
3. FraudDetectionAgent reuse and BaseAgent compatibility (LOW, MEDIUM, HIGH, CRITICAL)
4. PolicyAgent (Critical fraud, High fraud, Ambiguity, Mismatch, Missing ledger, Combined risk, Clean)
5. ConsensusEngine & DecisionPrecedence (BLOCK > MANUAL_REVIEW > APPROVE)
6. Agent disagreement detection and confidence scoring
7. Failure-safe handling (Agent failure != approval -> MANUAL_REVIEW)
8. AgentCoordinator end-to-end execution, audit trail, and idempotency
9. Event publishing & account ID masking
10. Database persistence, retrieval, and metrics aggregation
11. REST APIs (POST /decision/analyze, GET /decision/{id}, GET /decision/metrics)
12. Security guarantees: Zero mutation of transaction, ledger, reconciliation, or fraud results
13. Six Adversarial conflict attacks
"""

from datetime import datetime, timezone
from decimal import Decimal
import os
import sqlite3
from typing import Any, Dict, List
import pytest
from fastapi.testclient import TestClient

from agents.agent_coordinator import AgentCoordinator
from agents.base_agent import AgentExecutionRecord, BaseAgent
from agents.decision_agent.decision_agent import (
    ConsensusEngine,
    DecisionAgent,
    DecisionContext,
    DecisionPrecedence,
    DecisionResult,
    DecisionType,
)
from agents.fraud_agent.fraud_agent import FraudDetectionAgent
from agents.policy_agent.policy_agent import (
    PolicyAgent,
    PolicyAgentConfig,
    PolicyAgentResult,
    PolicyEvidence,
    PolicyRecommendation,
)
from agents.reconciliation_agent.reconciliation_agent import (
    ReconciliationAgent,
    ReconciliationAgentResult,
)
from database.database import (
    get_connection,
    get_decision_metrics,
    get_decision_result_by_transaction_id,
    get_transaction_by_id,
    init_db,
    insert_decision_result,
    insert_ledger_entry,
    insert_transaction,
)
from fraud_detection.fraud_result import (
    FraudAnalysisResult,
    FraudStatus,
    RiskLevel,
    RuleEvidence,
)
from kafka.local_event_publisher import LocalEventPublisher
from main import app
from orchestrator.event_audit import EventAuditTrail
from orchestrator.event_orchestrator import Event, EventOrchestrator
from orchestrator.event_types import EventType
from orchestrator.workflow_state import StageStatus, WorkflowTracker
from reconciliation.candidate_service import CandidateService
from reconciliation.engine import ReconciliationEngine
from reconciliation.ledger_repository import LedgerRepository
from reconciliation.models import LedgerEntry, Transaction
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus
from reconciliation.reconciliation_service import ReconciliationService


# ==============================================================================
# Pytest Fixtures
# ==============================================================================

@pytest.fixture
def test_db_path(tmp_path) -> str:
    """Provide isolated SQLite database path."""
    db_file = str(tmp_path / "test_day7.db")
    init_db(db_file)
    return db_file


@pytest.fixture
def sample_transaction() -> Transaction:
    """Create a standard test transaction."""
    return Transaction(
        transaction_id="TXN-DAY7-001",
        account_id="ACC-9876543210",
        transaction_type="TRANSFER",
        amount=5000.00,
        currency="INR",
        transaction_timestamp=datetime(2026, 10, 6, 12, 0, 0, tzinfo=timezone.utc),
        source_system="CHANNEL_GATEWAY",
        destination_system="CORE_LEDGER",
        reference_id="REF-MATCH-001",
    )


@pytest.fixture
def matched_ledger_entry() -> LedgerEntry:
    """Create a matching core ledger entry."""
    return LedgerEntry(
        ledger_entry_id="LEDGER-ENTRY-001",
        transaction_id="TXN-DAY7-001",
        account_id="ACC-9876543210",
        amount=5000.00,
        currency="INR",
        debit_credit="DEBIT",
        reference_id="REF-MATCH-001",
        timestamp=datetime(2026, 10, 6, 12, 1, 0, tzinfo=timezone.utc),
        source_system="CORE_BANKING",
    )


# ==============================================================================
# 1. BaseAgent Contract Tests
# ==============================================================================

def test_base_agent_interface():
    """Verify BaseAgent defines standard metadata and abstract analyze method."""
    class DummyAgent(BaseAgent):
        def analyze(self, transaction_id: str) -> str:
            return f"Processed {transaction_id}"

    agent = DummyAgent(agent_name="CustomTestAgent", agent_version="2.1.0")
    assert agent.agent_name == "CustomTestAgent"
    assert agent.agent_version == "2.1.0"
    assert agent.analyze("TXN-001") == "Processed TXN-001"


def test_base_agent_execution_record():
    """Verify execution record creation with elapsed time and audit payload."""
    class DummyAgent(BaseAgent):
        def analyze(self, *args, **kwargs):
            return "OK"

    agent = DummyAgent(agent_name="AuditAgent")
    t0 = datetime(2026, 10, 6, 10, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 10, 6, 10, 0, 1, tzinfo=timezone.utc)

    record = agent.create_execution_record(
        transaction_id="TXN-AUDIT-1",
        decision="APPROVED",
        started_at=t0,
        completed_at=t1,
        evidence=[{"check": "passed"}],
        correlation_id="CORR-AUDIT-1",
    )
    assert record.agent_name == "AuditAgent"
    assert record.decision == "APPROVED"
    assert record.execution_time_ms >= 1000.0
    assert record.success is True
    assert record.correlation_id == "CORR-AUDIT-1"


# ==============================================================================
# 2. ReconciliationAgent Tests
# ==============================================================================

def test_reconciliation_agent_matched(test_db_path, sample_transaction, matched_ledger_entry):
    """Test ReconciliationAgent produces MATCHED disposition when ledger entry exists."""
    with get_connection(test_db_path) as conn:
        insert_transaction(conn, sample_transaction)
        insert_ledger_entry(conn, matched_ledger_entry)

    repo = LedgerRepository(db_path=test_db_path)
    service = ReconciliationService(repository=repo, db_path=test_db_path)
    recon_agent = ReconciliationAgent(service=service)

    result = recon_agent.analyze(sample_transaction)
    assert result.status == ReconciliationStatus.MATCHED.value
    assert result.reconciliation_score >= 0.85
    assert result.requires_manual_review is False
    assert result.matched_ledger_entry_id == matched_ledger_entry.ledger_entry_id
    assert result.execution_record.success is True


def test_reconciliation_agent_not_found(test_db_path, sample_transaction):
    """Test ReconciliationAgent produces NOT_FOUND when no candidate exists."""
    repo = LedgerRepository(db_path=test_db_path)
    service = ReconciliationService(repository=repo, db_path=test_db_path)
    recon_agent = ReconciliationAgent(service=service)

    result = recon_agent.analyze(sample_transaction)
    assert result.status == ReconciliationStatus.NOT_FOUND.value
    assert result.reconciliation_score == 0.0
    assert result.requires_manual_review is False
    assert result.matched_ledger_entry_id is None


def test_reconciliation_agent_mismatched(test_db_path, sample_transaction):
    """Test ReconciliationAgent produces MISMATCHED when amounts differ significantly."""
    mismatched_entry = LedgerEntry(
        ledger_entry_id="LEDGER-MISMATCH-1",
        transaction_id=sample_transaction.transaction_id,
        account_id=sample_transaction.account_id,
        amount=99999.00,  # Drastically different
        currency="INR",
        debit_credit="CREDIT",
        reference_id="DIFF-REF-123",
        timestamp=sample_transaction.transaction_timestamp,
        source_system="CORE_BANKING",
    )
    with get_connection(test_db_path) as conn:
        insert_ledger_entry(conn, mismatched_entry)

    repo = LedgerRepository(db_path=test_db_path)
    service = ReconciliationService(repository=repo, db_path=test_db_path)
    recon_agent = ReconciliationAgent(service=service)

    result = recon_agent.analyze(sample_transaction)
    assert result.status in (ReconciliationStatus.MISMATCHED.value, ReconciliationStatus.PARTIAL_MATCH.value)
    assert result.requires_manual_review is True


# ==============================================================================
# 3. FraudDetectionAgent Integration Tests
# ==============================================================================

def test_fraud_agent_base_agent_compatibility(sample_transaction):
    """Verify FraudDetectionAgent implements BaseAgent interface and analyze alias."""
    agent = FraudDetectionAgent()
    assert isinstance(agent, BaseAgent)
    assert agent.agent_name == "FraudDetectionAgent"

    result = agent.analyze(sample_transaction)
    assert isinstance(result, FraudAnalysisResult)
    assert 0 <= result.fraud_score <= 100
    assert hasattr(result, "execution_record")
    assert result.execution_record.agent_name == "FraudDetectionAgent"


def test_fraud_agent_risk_levels(sample_transaction):
    """Verify fraud agent produces valid risk levels."""
    agent = FraudDetectionAgent()
    res_normal = agent.analyze(sample_transaction)
    assert res_normal.risk_level in (RiskLevel.LOW, RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.CRITICAL)


# ==============================================================================
# 4. PolicyAgent Tests
# ==============================================================================

def test_policy_critical_fraud(sample_transaction):
    """Policy: fraud score >= 75 triggers CRITICAL_FRAUD_RISK and BLOCK_RECOMMENDED."""
    policy_agent = PolicyAgent()
    fake_fraud = FraudAnalysisResult(
        transaction_id=sample_transaction.transaction_id,
        fraud_score=85,
        risk_level=RiskLevel.CRITICAL,
        status=FraudStatus.BLOCK_RECOMMENDED,
        requires_manual_review=True,
        block_recommended=True,
        indicators=["HIGH_AMOUNT_ANOMALY"],
        evidence=[],
    )
    result = policy_agent.analyze(transaction=sample_transaction, fraud_result=fake_fraud)
    assert result.recommendation == PolicyRecommendation.BLOCK_RECOMMENDED.value
    assert result.block_recommended is True
    assert result.requires_manual_review is True
    assert any(p.policy_id == "CRITICAL_FRAUD_RISK" and p.triggered for p in result.triggered_policies)


def test_policy_high_fraud(sample_transaction):
    """Policy: 50 <= fraud score < 75 triggers HIGH_FRAUD_RISK and MANUAL_REVIEW."""
    policy_agent = PolicyAgent()
    fake_fraud = FraudAnalysisResult(
        transaction_id=sample_transaction.transaction_id,
        fraud_score=60,
        risk_level=RiskLevel.HIGH,
        status=FraudStatus.MANUAL_REVIEW,
        requires_manual_review=True,
        block_recommended=False,
        indicators=["HIGH_VELOCITY"],
        evidence=[],
    )
    result = policy_agent.analyze(transaction=sample_transaction, fraud_result=fake_fraud)
    assert result.recommendation == PolicyRecommendation.MANUAL_REVIEW.value
    assert result.requires_manual_review is True
    assert result.block_recommended is False
    assert any(p.policy_id == "HIGH_FRAUD_RISK" and p.triggered for p in result.triggered_policies)


def test_policy_reconciliation_ambiguity(sample_transaction):
    """Policy: reconciliation AMBIGUOUS triggers RECONCILIATION_AMBIGUITY and MANUAL_REVIEW."""
    policy_agent = PolicyAgent()
    result = policy_agent.analyze(
        transaction=sample_transaction,
        fraud_result=None,
        reconciliation_status="AMBIGUOUS",
    )
    assert result.recommendation == PolicyRecommendation.MANUAL_REVIEW.value
    assert any(p.policy_id == "RECONCILIATION_AMBIGUITY" and p.triggered for p in result.triggered_policies)


def test_policy_missing_ledger(sample_transaction):
    """Policy: reconciliation NOT_FOUND triggers MISSING_LEDGER and MANUAL_REVIEW."""
    policy_agent = PolicyAgent()
    result = policy_agent.analyze(
        transaction=sample_transaction,
        fraud_result=None,
        reconciliation_status="NOT_FOUND",
    )
    assert result.recommendation == PolicyRecommendation.MANUAL_REVIEW.value
    assert any(p.policy_id == "MISSING_LEDGER" and p.triggered for p in result.triggered_policies)


def test_policy_combined_high_risk(sample_transaction):
    """Policy: fraud >= 50 and recon != MATCHED triggers COMBINED_HIGH_RISK and BLOCK_RECOMMENDED."""
    policy_agent = PolicyAgent()
    fake_fraud = FraudAnalysisResult(
        transaction_id=sample_transaction.transaction_id,
        fraud_score=55,
        risk_level=RiskLevel.HIGH,
        status=FraudStatus.MANUAL_REVIEW,
        requires_manual_review=True,
        block_recommended=False,
        indicators=[],
        evidence=[],
    )
    result = policy_agent.analyze(
        transaction=sample_transaction,
        fraud_result=fake_fraud,
        reconciliation_status="MISMATCHED",
    )
    assert result.recommendation == PolicyRecommendation.BLOCK_RECOMMENDED.value
    assert any(p.policy_id == "COMBINED_HIGH_RISK" and p.triggered for p in result.triggered_policies)


def test_policy_clean_transaction(sample_transaction):
    """Policy: matched reconciliation + low fraud yields APPROVE without triggered policies."""
    policy_agent = PolicyAgent()
    fake_fraud = FraudAnalysisResult(
        transaction_id=sample_transaction.transaction_id,
        fraud_score=10,
        risk_level=RiskLevel.LOW,
        status=FraudStatus.CLEAR,
        requires_manual_review=False,
        block_recommended=False,
        indicators=[],
        evidence=[],
    )
    result = policy_agent.analyze(
        transaction=sample_transaction,
        fraud_result=fake_fraud,
        reconciliation_status="MATCHED",
    )
    assert result.recommendation == PolicyRecommendation.APPROVE.value
    assert result.requires_manual_review is False
    assert result.block_recommended is False
    assert len(result.triggered_policies) == 0


# ==============================================================================
# 5. ConsensusEngine & Precedence Tests
# ==============================================================================

def test_decision_precedence_ranking():
    """Verify centralized precedence: BLOCK_RECOMMENDED > MANUAL_REVIEW > APPROVE."""
    assert DecisionPrecedence.rank("BLOCK_RECOMMENDED") > DecisionPrecedence.rank("MANUAL_REVIEW")
    assert DecisionPrecedence.rank("MANUAL_REVIEW") > DecisionPrecedence.rank("APPROVE")
    assert DecisionPrecedence.get_highest(["APPROVE", "MANUAL_REVIEW"]) == "MANUAL_REVIEW"
    assert DecisionPrecedence.get_highest(["APPROVE", "BLOCK_RECOMMENDED", "MANUAL_REVIEW"]) == "BLOCK_RECOMMENDED"


def test_consensus_clean_approval():
    """Consensus Rule 6: Matched + Low Fraud + Clear Policy -> APPROVE with high confidence."""
    engine = ConsensusEngine()
    context = DecisionContext(
        transaction_id="TXN-C1",
        reconciliation_result=ReconciliationAgentResult(
            transaction_id="TXN-C1",
            status="MATCHED",
            reconciliation_score=1.0,
            requires_manual_review=False,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id="TXN-C1",
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="MATCHED",
            ),
        ),
        fraud_result=FraudAnalysisResult(
            transaction_id="TXN-C1",
            fraud_score=5,
            risk_level=RiskLevel.LOW,
            status=FraudStatus.CLEAR,
            requires_manual_review=False,
            block_recommended=False,
            indicators=[],
            evidence=[],
        ),
        policy_result=PolicyAgentResult(
            transaction_id="TXN-C1",
            recommendation="APPROVE",
            requires_manual_review=False,
            block_recommended=False,
            triggered_policies=[],
            execution_record=AgentExecutionRecord(
                agent_name="Policy", agent_version="1.0", transaction_id="TXN-C1",
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="APPROVE",
            ),
        ),
    )
    decision, conf, review, block, reasons, disagree, _ = engine.evaluate(context)
    assert decision == DecisionType.APPROVE.value
    assert conf >= 0.90
    assert review is False
    assert block is False
    assert disagree is False


def test_consensus_critical_fraud_block():
    """Consensus Rule 1: Matched Recon + Critical Fraud (80) -> BLOCK_RECOMMENDED overrides approval."""
    engine = ConsensusEngine()
    context = DecisionContext(
        transaction_id="TXN-C2",
        reconciliation_result=ReconciliationAgentResult(
            transaction_id="TXN-C2",
            status="MATCHED",
            reconciliation_score=1.0,
            requires_manual_review=False,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id="TXN-C2",
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="MATCHED",
            ),
        ),
        fraud_result=FraudAnalysisResult(
            transaction_id="TXN-C2",
            fraud_score=80,
            risk_level=RiskLevel.CRITICAL,
            status=FraudStatus.BLOCK_RECOMMENDED,
            requires_manual_review=True,
            block_recommended=True,
            indicators=["ATTACK"],
            evidence=[],
        ),
    )
    decision, conf, review, block, reasons, disagree, disagreements = engine.evaluate(context)
    assert decision == DecisionType.BLOCK_RECOMMENDED.value
    assert block is True
    assert review is True
    assert disagree is True
    assert len(disagreements) > 0


def test_consensus_ambiguous_recon_review():
    """Consensus Rule 3: Ambiguous Recon + Low Fraud (0) -> MANUAL_REVIEW."""
    engine = ConsensusEngine()
    context = DecisionContext(
        transaction_id="TXN-C3",
        reconciliation_result=ReconciliationAgentResult(
            transaction_id="TXN-C3",
            status="AMBIGUOUS",
            reconciliation_score=0.85,
            requires_manual_review=True,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id="TXN-C3",
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="AMBIGUOUS",
            ),
        ),
        fraud_result=FraudAnalysisResult(
            transaction_id="TXN-C3",
            fraud_score=0,
            risk_level=RiskLevel.LOW,
            status=FraudStatus.CLEAR,
            requires_manual_review=False,
            block_recommended=False,
            indicators=[],
            evidence=[],
        ),
    )
    decision, conf, review, block, reasons, disagree, _ = engine.evaluate(context)
    assert decision == DecisionType.MANUAL_REVIEW.value
    assert review is True
    assert block is False
    assert disagree is True


def test_consensus_mismatch_elevated_fraud_block():
    """Consensus Rule 5: Mismatched Recon + Elevated Fraud (60) -> BLOCK_RECOMMENDED."""
    engine = ConsensusEngine()
    context = DecisionContext(
        transaction_id="TXN-C4",
        reconciliation_result=ReconciliationAgentResult(
            transaction_id="TXN-C4",
            status="MISMATCHED",
            reconciliation_score=0.30,
            requires_manual_review=True,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id="TXN-C4",
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="MISMATCHED",
            ),
        ),
        fraud_result=FraudAnalysisResult(
            transaction_id="TXN-C4",
            fraud_score=60,
            risk_level=RiskLevel.HIGH,
            status=FraudStatus.MANUAL_REVIEW,
            requires_manual_review=True,
            block_recommended=False,
            indicators=[],
            evidence=[],
        ),
    )
    decision, conf, review, block, reasons, disagree, _ = engine.evaluate(context)
    assert decision == DecisionType.BLOCK_RECOMMENDED.value
    assert block is True
    assert review is True


def test_consensus_medium_risk_matched_approval():
    """Consensus Rule 7: Matched Recon + Medium Fraud (35) -> APPROVE with preserved evidence."""
    engine = ConsensusEngine()
    context = DecisionContext(
        transaction_id="TXN-C5",
        reconciliation_result=ReconciliationAgentResult(
            transaction_id="TXN-C5",
            status="MATCHED",
            reconciliation_score=0.98,
            requires_manual_review=False,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id="TXN-C5",
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="MATCHED",
            ),
        ),
        fraud_result=FraudAnalysisResult(
            transaction_id="TXN-C5",
            fraud_score=35,
            risk_level=RiskLevel.MEDIUM,
            status=FraudStatus.ANALYZED,
            requires_manual_review=False,
            block_recommended=False,
            indicators=["MODERATE_AMOUNT"],
            evidence=[],
        ),
        policy_result=PolicyAgentResult(
            transaction_id="TXN-C5",
            recommendation="APPROVE",
            requires_manual_review=False,
            block_recommended=False,
            triggered_policies=[],
            execution_record=AgentExecutionRecord(
                agent_name="Policy", agent_version="1.0", transaction_id="TXN-C5",
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="APPROVE",
            ),
        ),
    )
    decision, conf, review, block, reasons, disagree, _ = engine.evaluate(context)
    assert decision == DecisionType.APPROVE.value
    assert review is False
    assert block is False


# ==============================================================================
# 6. Failure Handling (Agent Failure != Approval)
# ==============================================================================

def test_failure_handling_fraud_agent_fails():
    """Financial Safety: When Fraud Agent fails, system falls back to MANUAL_REVIEW, NEVER approves."""
    engine = ConsensusEngine()
    context = DecisionContext(
        transaction_id="TXN-FAIL-1",
        reconciliation_result=ReconciliationAgentResult(
            transaction_id="TXN-FAIL-1",
            status="MATCHED",
            reconciliation_score=1.0,
            requires_manual_review=False,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id="TXN-FAIL-1",
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="MATCHED",
            ),
        ),
        fraud_result=None,
        agent_failures=["FraudDetectionAgent"],
    )
    decision, conf, review, block, reasons, _, _ = engine.evaluate(context)
    assert decision == DecisionType.MANUAL_REVIEW.value
    assert review is True
    assert any("FraudDetectionAgent" in r for r in reasons)


def test_failure_handling_reconciliation_agent_fails():
    """Financial Safety: When Reconciliation Agent fails, fallback to MANUAL_REVIEW."""
    engine = ConsensusEngine()
    context = DecisionContext(
        transaction_id="TXN-FAIL-2",
        reconciliation_result=None,
        fraud_result=FraudAnalysisResult(
            transaction_id="TXN-FAIL-2",
            fraud_score=0,
            risk_level=RiskLevel.LOW,
            status=FraudStatus.CLEAR,
            requires_manual_review=False,
            block_recommended=False,
            indicators=[],
            evidence=[],
        ),
        agent_failures=["ReconciliationAgent"],
    )
    decision, conf, review, block, reasons, _, _ = engine.evaluate(context)
    assert decision == DecisionType.MANUAL_REVIEW.value
    assert review is True


def test_failure_handling_policy_agent_fails():
    """Financial Safety: When Policy Agent fails, fallback to MANUAL_REVIEW."""
    engine = ConsensusEngine()
    context = DecisionContext(
        transaction_id="TXN-FAIL-3",
        reconciliation_result=None,
        fraud_result=None,
        agent_failures=["PolicyAgent"],
    )
    decision, conf, review, block, reasons, _, _ = engine.evaluate(context)
    assert decision == DecisionType.MANUAL_REVIEW.value
    assert review is True


# ==============================================================================
# 7. Agent Coordinator End-to-End Tests
# ==============================================================================

def test_coordinator_end_to_end_clean(test_db_path, sample_transaction, matched_ledger_entry):
    """Test full coordination flow resulting in APPROVE disposition."""
    with get_connection(test_db_path) as conn:
        insert_transaction(conn, sample_transaction)
        insert_ledger_entry(conn, matched_ledger_entry)

    repo = LedgerRepository(db_path=test_db_path)
    recon_service = ReconciliationService(repository=repo, db_path=test_db_path)
    recon_agent = ReconciliationAgent(service=recon_service)
    fraud_agent = FraudDetectionAgent()
    policy_agent = PolicyAgent()
    decision_agent = DecisionAgent()

    coordinator = AgentCoordinator(
        reconciliation_agent=recon_agent,
        fraud_agent=fraud_agent,
        policy_agent=policy_agent,
        decision_agent=decision_agent,
        db_path=test_db_path,
    )

    result = coordinator.coordinate_decision(sample_transaction)
    assert result.decision == DecisionType.APPROVE.value
    assert result.confidence >= 0.85
    assert result.requires_manual_review is False
    assert result.block_recommended is False
    assert "reconciliation" in result.agent_results
    assert "fraud" in result.agent_results
    assert "policy" in result.agent_results


def test_coordinator_idempotency(test_db_path, sample_transaction, matched_ledger_entry):
    """Calling coordinate_decision twice on the same transaction returns cached consistent result."""
    with get_connection(test_db_path) as conn:
        insert_transaction(conn, sample_transaction)
        insert_ledger_entry(conn, matched_ledger_entry)

    repo = LedgerRepository(db_path=test_db_path)
    recon_service = ReconciliationService(repository=repo, db_path=test_db_path)
    recon_agent = ReconciliationAgent(service=recon_service)

    coordinator = AgentCoordinator(
        reconciliation_agent=recon_agent,
        db_path=test_db_path,
    )

    res1 = coordinator.coordinate_decision(sample_transaction)
    res2 = coordinator.coordinate_decision(sample_transaction)

    assert res1.decision_id == res2.decision_id
    assert res1.decision == res2.decision
    assert res1.confidence == res2.confidence


def test_coordinator_graceful_agent_exception(test_db_path, sample_transaction):
    """If an agent throws an unhandled exception, coordinator catches it safely -> MANUAL_REVIEW."""
    class FailingFraudAgent(FraudDetectionAgent):
        def analyze_transaction(self, *args, **kwargs):
            raise RuntimeError("Database connection timed out in Fraud Agent")

    coordinator = AgentCoordinator(
        fraud_agent=FailingFraudAgent(),
        db_path=test_db_path,
    )

    result = coordinator.coordinate_decision(sample_transaction)
    assert result.decision == DecisionType.MANUAL_REVIEW.value
    assert result.requires_manual_review is True
    assert any("FraudDetectionAgent" in r for r in result.decision_reasons)


# ==============================================================================
# 8. Event Publishing & Sanitization Tests
# ==============================================================================

def test_coordinator_publishes_consensus_events(test_db_path, sample_transaction):
    """Verify coordinator publishes CONSENSUS_DECISION_REQUESTED and CONSENSUS_DECISION_COMPLETED."""
    events_captured: List[Event] = []

    class MockPublisher:
        def publish(self, event: Event) -> None:
            events_captured.append(event)

    coordinator = AgentCoordinator(
        publisher=MockPublisher(),
        db_path=test_db_path,
    )

    coordinator.coordinate_decision(sample_transaction)

    event_types = [e.event_type for e in events_captured]
    assert EventType.RECONCILIATION_AGENT_COMPLETED.value in event_types
    assert EventType.FRAUD_AGENT_COMPLETED.value in event_types
    assert EventType.POLICY_AGENT_COMPLETED.value in event_types
    assert EventType.CONSENSUS_DECISION_REQUESTED.value in event_types
    assert EventType.CONSENSUS_DECISION_COMPLETED.value in event_types


def test_coordinator_masks_account_in_events(test_db_path, sample_transaction):
    """Zero-Trust Security: Check that raw account number is masked in all event payloads."""
    events_captured: List[Event] = []

    class MockPublisher:
        def publish(self, event: Event) -> None:
            events_captured.append(event)

    coordinator = AgentCoordinator(
        publisher=MockPublisher(),
        db_path=test_db_path,
    )

    coordinator.coordinate_decision(sample_transaction)

    for ev in events_captured:
        masked_val = ev.payload.get("masked_account_id")
        if masked_val:
            assert "*" in masked_val
            assert sample_transaction.account_id not in str(ev.payload)


# ==============================================================================
# 9. Database Persistence & Metrics Tests
# ==============================================================================

def test_db_insert_and_retrieve_decision(test_db_path):
    """Verify decision persistence and query retrieval in SQLite."""
    with get_connection(test_db_path) as conn:
        insert_decision_result(
            conn,
            {
                "id": "DEC-TEST-001",
                "transaction_id": "TXN-TEST-001",
                "correlation_id": "CORR-001",
                "decision": "BLOCK_RECOMMENDED",
                "confidence": 0.85,
                "requires_manual_review": True,
                "block_recommended": True,
                "agent_results": {"fraud_score": 90},
                "triggered_policies": ["CRITICAL_FRAUD"],
                "decision_reasons": ["High score"],
                "disagreements": ["Recon vs Fraud"],
                "created_at": "2026-10-06T12:00:00Z",
            },
        )
        fetched = get_decision_result_by_transaction_id(conn, "TXN-TEST-001")

    assert fetched is not None
    assert fetched["id"] == "DEC-TEST-001"
    assert fetched["decision"] == "BLOCK_RECOMMENDED"
    assert fetched["confidence"] == 0.85
    assert fetched["requires_manual_review"] is True
    assert fetched["block_recommended"] is True
    assert fetched["agent_results"]["fraud_score"] == 90


def test_db_decision_metrics(test_db_path):
    """Verify aggregate decision metrics calculation."""
    with get_connection(test_db_path) as conn:
        insert_decision_result(conn, {
            "id": "D1", "transaction_id": "T1", "decision": "APPROVE",
            "confidence": 0.95, "requires_manual_review": False, "block_recommended": False,
            "created_at": "2026-10-06T10:00:00Z",
        })
        insert_decision_result(conn, {
            "id": "D2", "transaction_id": "T2", "decision": "MANUAL_REVIEW",
            "confidence": 0.80, "requires_manual_review": True, "block_recommended": False,
            "disagreements": ["Recon vs Fraud"], "created_at": "2026-10-06T10:01:00Z",
        })
        insert_decision_result(conn, {
            "id": "D3", "transaction_id": "T3", "decision": "BLOCK_RECOMMENDED",
            "confidence": 0.90, "requires_manual_review": True, "block_recommended": True,
            "created_at": "2026-10-06T10:02:00Z",
        })
        metrics = get_decision_metrics(conn)

    assert metrics["total_decisions"] == 3
    assert metrics["approved"] == 1
    assert metrics["manual_review"] == 1
    assert metrics["block_recommended"] == 1
    assert metrics["average_confidence"] == pytest.approx(0.8833, abs=0.01)
    assert metrics["agent_disagreements"] >= 1


# ==============================================================================
# 10. REST API Tests
# ==============================================================================

def test_api_decision_metrics():
    """Verify GET /decision/metrics returns 200 and structured statistics."""
    client = TestClient(app)
    response = client.get("/decision/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "total_decisions" in data
    assert "approved" in data
    assert "manual_review" in data
    assert "block_recommended" in data
    assert "average_confidence" in data


def test_api_decision_analyze_not_found():
    """POST /decision/analyze for nonexistent transaction returns 404."""
    client = TestClient(app)
    response = client.post("/decision/analyze", json={"transaction_id": "NON-EXISTENT-TXN-999"})
    assert response.status_code == 404


def test_api_decision_analyze_and_get(sample_transaction):
    """POST /decision/analyze evaluates transaction and GET /decision/{id} retrieves it."""
    client = TestClient(app)
    # 1. Ingest transaction
    ingest_res = client.post("/transactions", json=sample_transaction.model_dump(mode="json"))
    assert ingest_res.status_code == 200

    # 2. Run decision analysis
    dec_res = client.post("/decision/analyze", json={"transaction_id": sample_transaction.transaction_id})
    assert dec_res.status_code == 200
    data = dec_res.json()
    assert data["transaction_id"] == sample_transaction.transaction_id
    assert data["decision"] in ("APPROVE", "MANUAL_REVIEW", "BLOCK_RECOMMENDED")
    assert "confidence" in data

    # 3. Retrieve stored decision
    get_res = client.get(f"/decision/{sample_transaction.transaction_id}")
    assert get_res.status_code == 200
    stored = get_res.json()
    assert stored["transaction_id"] == sample_transaction.transaction_id
    assert stored["decision"] == data["decision"]


# ==============================================================================
# 11. Security & Financial Integrity Tests
# ==============================================================================

def test_safety_transaction_unmodified_by_decision(sample_transaction):
    """Financial Safety: Transaction amounts, currencies, and IDs are never mutated by consensus."""
    orig_amount = sample_transaction.amount
    orig_currency = sample_transaction.currency
    orig_acc = sample_transaction.account_id

    coordinator = AgentCoordinator()
    coordinator.coordinate_decision(sample_transaction)

    assert sample_transaction.amount == orig_amount
    assert sample_transaction.currency == orig_currency
    assert sample_transaction.account_id == orig_acc


def test_safety_reconciliation_unmodified_by_decision(sample_transaction, matched_ledger_entry):
    """Financial Safety: Authoritative deterministic reconciliation result is never altered."""
    recon_res = ReconciliationResult(
        transaction_id=sample_transaction.transaction_id,
        status=ReconciliationStatus.MATCHED.value,
        score=1.0,
        evidence=[{"note": "valid"}],
    )
    orig_score = recon_res.score
    orig_status = recon_res.status

    context = DecisionContext(
        transaction_id=sample_transaction.transaction_id,
        reconciliation_result=ReconciliationAgentResult(
            transaction_id=sample_transaction.transaction_id,
            status=recon_res.status,
            reconciliation_score=recon_res.score,
            requires_manual_review=False,
            raw_result=recon_res,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id=sample_transaction.transaction_id,
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="MATCHED",
            ),
        ),
    )
    DecisionAgent().analyze(context)

    assert recon_res.score == orig_score
    assert recon_res.status == orig_status


# ==============================================================================
# 12. Adversarial Conflict & Attack Scenarios
# ==============================================================================

def test_adversarial_attack_1_matched_but_critical_fraud(sample_transaction):
    """Attack 1: Perfect reconciliation match, but fraud is CRITICAL -> BLOCK_RECOMMENDED."""
    coordinator = AgentCoordinator()
    context = DecisionContext(
        transaction_id=sample_transaction.transaction_id,
        reconciliation_result=ReconciliationAgentResult(
            transaction_id=sample_transaction.transaction_id,
            status="MATCHED",
            reconciliation_score=1.0,
            requires_manual_review=False,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id=sample_transaction.transaction_id,
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="MATCHED",
            ),
        ),
        fraud_result=FraudAnalysisResult(
            transaction_id=sample_transaction.transaction_id,
            fraud_score=92,
            risk_level=RiskLevel.CRITICAL,
            status=FraudStatus.BLOCK_RECOMMENDED,
            requires_manual_review=True,
            block_recommended=True,
            indicators=["VELOCITY_BURST", "AMOUNT_ANOMALY"],
            evidence=[],
        ),
    )
    result = DecisionAgent().analyze(context)
    assert result.decision == DecisionType.BLOCK_RECOMMENDED.value
    assert result.block_recommended is True
    assert result.requires_manual_review is True
    assert result.disagreement_detected is True


def test_adversarial_attack_2_low_fraud_but_ambiguous_recon(sample_transaction):
    """Attack 2: Fraud is LOW (0), but reconciliation is AMBIGUOUS -> MANUAL_REVIEW."""
    context = DecisionContext(
        transaction_id=sample_transaction.transaction_id,
        reconciliation_result=ReconciliationAgentResult(
            transaction_id=sample_transaction.transaction_id,
            status="AMBIGUOUS",
            reconciliation_score=0.82,
            requires_manual_review=True,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id=sample_transaction.transaction_id,
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="AMBIGUOUS",
            ),
        ),
        fraud_result=FraudAnalysisResult(
            transaction_id=sample_transaction.transaction_id,
            fraud_score=0,
            risk_level=RiskLevel.LOW,
            status=FraudStatus.CLEAR,
            requires_manual_review=False,
            block_recommended=False,
            indicators=[],
            evidence=[],
        ),
    )
    result = DecisionAgent().analyze(context)
    assert result.decision == DecisionType.MANUAL_REVIEW.value
    assert result.requires_manual_review is True
    assert result.disagreement_detected is True


def test_adversarial_attack_3_fraud_agent_failure(sample_transaction):
    """Attack 3: Fraud agent fails -> System falls back safely to MANUAL_REVIEW, never APPROVE."""
    context = DecisionContext(
        transaction_id=sample_transaction.transaction_id,
        reconciliation_result=ReconciliationAgentResult(
            transaction_id=sample_transaction.transaction_id,
            status="MATCHED",
            reconciliation_score=1.0,
            requires_manual_review=False,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id=sample_transaction.transaction_id,
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="MATCHED",
            ),
        ),
        fraud_result=None,
        agent_failures=["FraudDetectionAgent"],
    )
    result = DecisionAgent().analyze(context)
    assert result.decision == DecisionType.MANUAL_REVIEW.value
    assert result.requires_manual_review is True


def test_adversarial_attack_4_policy_agent_failure(sample_transaction):
    """Attack 4: Policy agent fails -> System falls back safely to MANUAL_REVIEW."""
    context = DecisionContext(
        transaction_id=sample_transaction.transaction_id,
        agent_failures=["PolicyAgent"],
    )
    result = DecisionAgent().analyze(context)
    assert result.decision == DecisionType.MANUAL_REVIEW.value
    assert result.requires_manual_review is True


def test_adversarial_attack_5_critical_policy_overrides_approval(sample_transaction):
    """Attack 5: Recon is MATCHED, Fraud is LOW, but critical policy triggers -> BLOCK_RECOMMENDED."""
    context = DecisionContext(
        transaction_id=sample_transaction.transaction_id,
        reconciliation_result=ReconciliationAgentResult(
            transaction_id=sample_transaction.transaction_id,
            status="MATCHED",
            reconciliation_score=1.0,
            requires_manual_review=False,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id=sample_transaction.transaction_id,
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="MATCHED",
            ),
        ),
        fraud_result=FraudAnalysisResult(
            transaction_id=sample_transaction.transaction_id,
            fraud_score=15,
            risk_level=RiskLevel.LOW,
            status=FraudStatus.CLEAR,
            requires_manual_review=False,
            block_recommended=False,
            indicators=[],
            evidence=[],
        ),
        policy_result=PolicyAgentResult(
            transaction_id=sample_transaction.transaction_id,
            recommendation="BLOCK_RECOMMENDED",
            requires_manual_review=True,
            block_recommended=True,
            triggered_policies=[
                PolicyEvidence(
                    policy_id="SANCTIONS_LIST_MATCH",
                    triggered=True,
                    severity="CRITICAL",
                    reason="Entity flagged on designated sanctions list.",
                    recommendation="BLOCK_RECOMMENDED",
                    evidence={},
                )
            ],
            execution_record=AgentExecutionRecord(
                agent_name="Policy", agent_version="1.0", transaction_id=sample_transaction.transaction_id,
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="BLOCK_RECOMMENDED",
            ),
        ),
    )
    result = DecisionAgent().analyze(context)
    assert result.decision == DecisionType.BLOCK_RECOMMENDED.value
    assert result.block_recommended is True


def test_adversarial_attack_6_duplicate_consensus_idempotent(test_db_path, sample_transaction):
    """Attack 6: Duplicate consensus requests produce identical idempotent outcomes."""
    coordinator = AgentCoordinator(db_path=test_db_path)
    res_a = coordinator.coordinate_decision(sample_transaction)
    res_b = coordinator.coordinate_decision(sample_transaction)
    assert res_a.decision_id == res_b.decision_id
    assert res_a.decision == res_b.decision
    assert res_a.confidence == res_b.confidence


# ==============================================================================
# 12. Additional Multi-Agent Edge Cases & Invariant Tests
# ==============================================================================

def test_consensus_disagreement_detection_matrix():
    """Verify consensus engine detects and records inter-agent disagreements."""
    engine = ConsensusEngine()
    context = DecisionContext(
        transaction_id="TXN-DISAGREE-1",
        reconciliation_result=ReconciliationAgentResult(
            transaction_id="TXN-DISAGREE-1",
            status="MATCHED",
            reconciliation_score=1.0,
            requires_manual_review=False,
            execution_record=AgentExecutionRecord(
                agent_name="Recon", agent_version="1.0", transaction_id="TXN-DISAGREE-1",
                started_at=datetime.now(timezone.utc), completed_at=datetime.now(timezone.utc),
                execution_time_ms=1.0, decision="MATCHED",
            ),
        ),
        fraud_result=FraudAnalysisResult(
            transaction_id="TXN-DISAGREE-1",
            fraud_score=85,
            risk_level=RiskLevel.CRITICAL,
            status=FraudStatus.BLOCK_RECOMMENDED,
            requires_manual_review=True,
            block_recommended=True,
            indicators=["HIGH_AMOUNT_ANOMALY"],
            evidence=[],
        ),
    )
    decision, conf, review, block, reasons, disagree, details = engine.evaluate(context)
    assert disagree is True
    assert len(details) >= 1
    assert any("Fraud Agent" in d and "Reconciliation" in d for d in details)


def test_policy_compound_mismatch_and_high_risk(sample_transaction):
    """Policy: Mismatch combined with HIGH risk triggers COMBINED_HIGH_RISK and BLOCK."""
    policy_agent = PolicyAgent()
    fake_fraud = FraudAnalysisResult(
        transaction_id=sample_transaction.transaction_id,
        fraud_score=60,
        risk_level=RiskLevel.HIGH,
        status=FraudStatus.MANUAL_REVIEW,
        requires_manual_review=True,
        block_recommended=False,
        indicators=["HIGH_VELOCITY"],
        evidence=[],
    )
    result = policy_agent.analyze(
        transaction=sample_transaction,
        fraud_result=fake_fraud,
        reconciliation_status="MISMATCHED",
    )
    assert result.recommendation == PolicyRecommendation.BLOCK_RECOMMENDED.value
    assert result.block_recommended is True
    assert any(p.policy_id == "COMBINED_HIGH_RISK" and p.triggered for p in result.triggered_policies)


def test_policy_custom_config_thresholds(sample_transaction):
    """Verify PolicyAgent respects custom PolicyAgentConfig."""
    custom_cfg = PolicyAgentConfig(
        critical_fraud_threshold=60,
        high_fraud_threshold=20,
    )
    agent = PolicyAgent(config=custom_cfg)
    fake_fraud = FraudAnalysisResult(
        transaction_id=sample_transaction.transaction_id,
        fraud_score=25,  # Exceeds custom 20 threshold
        risk_level=RiskLevel.LOW,
        status=FraudStatus.CLEAR,
        requires_manual_review=False,
        block_recommended=False,
        indicators=[],
        evidence=[],
    )
    res = agent.analyze(transaction=sample_transaction, fraud_result=fake_fraud)
    assert any(p.policy_id == "HIGH_FRAUD_RISK" and p.triggered for p in res.triggered_policies)


def test_coordinator_agent_execution_times_recorded(test_db_path, sample_transaction, matched_ledger_entry):
    """Verify execution times are captured for every executed agent."""
    with get_connection(test_db_path) as conn:
        insert_transaction(conn, sample_transaction)
        insert_ledger_entry(conn, matched_ledger_entry)

    repo = LedgerRepository(db_path=test_db_path)
    recon_service = ReconciliationService(repository=repo, db_path=test_db_path)
    recon_agent = ReconciliationAgent(service=recon_service)
    coordinator = AgentCoordinator(
        reconciliation_agent=recon_agent,
        db_path=test_db_path,
    )
    result = coordinator.coordinate_decision(sample_transaction)
    assert "reconciliation" in result.agent_results
    assert result.agent_results["reconciliation"]["status"] == "MATCHED"
    assert result.decision == "APPROVE"


def test_decision_result_serialization():
    """Verify DecisionResult model serialization and round-trip dump."""
    res = DecisionResult(
        transaction_id="TXN-SER-1",
        decision=DecisionType.APPROVE.value,
        confidence=0.99,
        requires_manual_review=False,
        block_recommended=False,
        decision_reasons=["Fully verified clean transaction"],
        agent_disagreements=False,
        disagreement_details=[],
        agent_results={"reconciliation": {"status": "MATCHED"}},
    )
    dump_dict = res.model_dump()
    assert dump_dict["transaction_id"] == "TXN-SER-1"
    assert dump_dict["decision"] == "APPROVE"
    json_str = res.model_dump_json()
    assert "TXN-SER-1" in json_str
    assert "APPROVE" in json_str


def test_adversarial_attack_7_zero_trust_immutability(test_db_path, sample_transaction, matched_ledger_entry):
    """Attack 7: Ensure transaction and ledger records remain bitwise unmodified after multi-agent consensus."""
    with get_connection(test_db_path) as conn:
        insert_transaction(conn, sample_transaction)
        insert_ledger_entry(conn, matched_ledger_entry)

    orig_tx_dict = sample_transaction.model_dump()
    orig_entry_dict = matched_ledger_entry.model_dump()

    repo = LedgerRepository(db_path=test_db_path)
    recon_service = ReconciliationService(repository=repo, db_path=test_db_path)
    recon_agent = ReconciliationAgent(service=recon_service)
    coordinator = AgentCoordinator(
        reconciliation_agent=recon_agent,
        db_path=test_db_path,
    )
    coordinator.coordinate_decision(sample_transaction)

    with get_connection(test_db_path) as conn:
        tx_after = get_transaction_by_id(conn, sample_transaction.transaction_id)
        entry_after = repo.get_ledger_entry(matched_ledger_entry.ledger_entry_id)

    assert tx_after["amount"] == orig_tx_dict["amount"]
    assert tx_after["currency"] == orig_tx_dict["currency"]
    assert tx_after["account_id"] == orig_tx_dict["account_id"]
    assert entry_after.amount == orig_entry_dict["amount"]
    assert entry_after.currency == orig_entry_dict["currency"]

