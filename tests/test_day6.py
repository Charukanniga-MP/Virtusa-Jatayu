"""Comprehensive Test Suite for Day 6: Fraud Detection Agent.

Tests cover:
- Pydantic schema validation & bounds
- Feature extraction determinism
- Independent rule evaluations
- Risk engine scoring & anti-double-counting safeguards
- FraudDetectionAgent determinism & classifications
- Event integration & workflow progression
- Database persistence & idempotency
- REST API endpoints
- Adversarial attack scenarios & zero-trust financial safety guarantees
"""

from datetime import datetime, timezone, timedelta
from decimal import Decimal
import os
import pytest
from pydantic import ValidationError
from fastapi.testclient import TestClient

from database.database import (
    get_connection,
    init_db,
    insert_transaction,
    insert_fraud_analysis,
    get_fraud_analysis_by_transaction_id,
    get_fraud_metrics,
)
from fraud_detection.features import FeatureExtractionConfig, FeatureExtractor, FraudFeatures
from fraud_detection.fraud_result import (
    FraudAnalysisResult,
    FraudStatus,
    RiskLevel,
    RuleEvidence,
    RuleSeverity,
)
from fraud_detection.fraud_service import FraudService
from fraud_detection.profile import AccountProfile, AccountProfileStore
from fraud_detection.risk_engine import FraudRiskEngine, FraudRiskThresholds
from fraud_detection.rules import (
    RuleAccountBehaviorDeviation,
    RuleHighAmountAnomaly,
    RuleHighVelocity,
    RuleRapidRepeatedTransaction,
    RuleReconciliationAnomaly,
    RuleRepeatedFailedActivity,
    RuleUnusualTransactionTime,
    get_default_fraud_rules,
)
from agents.fraud_agent.fraud_agent import FraudDetectionAgent
from kafka.local_event_publisher import LocalEventPublisher
from main import app
from orchestrator.event_audit import EventAuditTrail
from orchestrator.event_orchestrator import Event, EventOrchestrator
from orchestrator.event_types import EventType
from orchestrator.processing_pipeline import ProcessingPipeline
from orchestrator.workflow_state import StageStatus, WorkflowTracker
from reconciliation.models import Transaction
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus


# ==========================================
# FIXTURES
# ==========================================

@pytest.fixture
def temp_db(tmp_path):
    db_file = str(tmp_path / "test_day6.db")
    init_db(db_file)
    return db_file


@pytest.fixture
def sample_transaction():
    return Transaction(
        transaction_id="TXN-600001",
        account_id="ACC-200001",
        transaction_type="PAYMENT",
        amount=150.00,
        currency="USD",
        transaction_timestamp=datetime(2026, 10, 6, 14, 30, 0, tzinfo=timezone.utc),
        source_system="WEB_PORTAL",
        destination_system="INTERNAL_LEDGER",
        reference_id="REF-600001",
    )


@pytest.fixture
def baseline_profile():
    return AccountProfile(
        account_id="ACC-200001",
        transaction_count=20,
        average_amount=Decimal("200.00"),
        max_amount=Decimal("1000.00"),
        min_amount=Decimal("15.00"),
        average_interval_seconds=3600.0,
        common_transaction_hours=[10, 11, 12, 13, 14, 15, 16],
        common_currencies=["USD"],
        common_transaction_directions=["PAYMENT", "DEBIT"],
    )


# ==========================================
# 1. MODEL VALIDATION TESTS (5 tests)
# ==========================================

def test_valid_fraud_result():
    res = FraudAnalysisResult(
        transaction_id="TXN-001",
        fraud_score=45,
        risk_level=RiskLevel.MEDIUM,
        status=FraudStatus.ANALYZED,
        requires_manual_review=False,
        block_recommended=False,
    )
    assert res.fraud_score == 45
    assert res.risk_level == RiskLevel.MEDIUM
    assert res.status == FraudStatus.ANALYZED


def test_fraud_score_negative_rejected():
    with pytest.raises(ValidationError):
        FraudAnalysisResult(
            transaction_id="TXN-001",
            fraud_score=-5,
            risk_level=RiskLevel.LOW,
            status=FraudStatus.CLEAR,
        )


def test_fraud_score_exceeding_100_rejected():
    with pytest.raises(ValidationError):
        FraudAnalysisResult(
            transaction_id="TXN-001",
            fraud_score=105,
            risk_level=RiskLevel.CRITICAL,
            status=FraudStatus.BLOCK_RECOMMENDED,
        )


def test_valid_risk_levels():
    assert RiskLevel.LOW.value == "LOW"
    assert RiskLevel.MEDIUM.value == "MEDIUM"
    assert RiskLevel.HIGH.value == "HIGH"
    assert RiskLevel.CRITICAL.value == "CRITICAL"


def test_invalid_enum_rejected():
    with pytest.raises(ValidationError):
        FraudAnalysisResult(
            transaction_id="TXN-001",
            fraud_score=20,
            risk_level="EXTREME_DANGER",  # type: ignore
            status=FraudStatus.CLEAR,
        )


# ==========================================
# 2. FEATURE EXTRACTION TESTS (6 tests)
# ==========================================

def test_feature_amount_anomaly(sample_transaction, baseline_profile):
    extractor = FeatureExtractor()
    # Baseline avg is 200.00; set amount to 1000.00 (ratio 5.0x)
    tx = sample_transaction.model_copy(update={"amount": 1000.00})
    features = extractor.extract_features(tx, baseline_profile)
    assert features.amount_deviation_ratio == 5.0
    assert features.is_amount_anomalous is True


def test_feature_velocity_burst(sample_transaction, baseline_profile):
    extractor = FeatureExtractor(config=FeatureExtractionConfig(velocity_window_seconds=60))
    base_time = sample_transaction.transaction_timestamp
    recent = [
        sample_transaction.model_copy(update={
            "transaction_id": f"TXN-PAST-{i}",
            "transaction_timestamp": base_time - timedelta(seconds=i * 10)
        })
        for i in range(1, 5)
    ]
    features = extractor.extract_features(sample_transaction, baseline_profile, recent_transactions=recent)
    assert features.velocity_count_in_window == 4
    assert features.is_high_velocity is True


def test_feature_rapid_repeated(sample_transaction, baseline_profile):
    extractor = FeatureExtractor()
    base_time = sample_transaction.transaction_timestamp
    recent = [
        sample_transaction.model_copy(update={
            "transaction_id": f"TXN-REP-{i}",
            "amount": sample_transaction.amount,  # identical amount
            "transaction_timestamp": base_time - timedelta(seconds=i * 20)
        })
        for i in range(1, 3)
    ]
    features = extractor.extract_features(sample_transaction, baseline_profile, recent_transactions=recent)
    assert features.rapid_repeated_count == 2
    assert features.is_rapid_repeated is True


def test_feature_unusual_hour(sample_transaction, baseline_profile):
    extractor = FeatureExtractor(config=FeatureExtractionConfig(business_start_hour=6, business_end_hour=22))
    # Transaction at 03:00 AM
    night_tx = sample_transaction.model_copy(
        update={"transaction_timestamp": datetime(2026, 10, 6, 3, 0, 0, tzinfo=timezone.utc)}
    )
    features = extractor.extract_features(night_tx, baseline_profile)
    assert features.transaction_hour == 3
    assert features.is_unusual_hour is True


def test_feature_account_behavior_deviation(sample_transaction, baseline_profile):
    extractor = FeatureExtractor()
    # Uncommon currency EUR, uncommon direction WITHDRAWAL, amount 2500 (> 1.5 * max 1000)
    dev_tx = sample_transaction.model_copy(
        update={"currency": "EUR", "transaction_type": "WITHDRAWAL", "amount": 2500.00}
    )
    features = extractor.extract_features(dev_tx, baseline_profile)
    assert features.is_currency_unusual is True
    assert features.is_direction_unusual is True
    assert features.is_above_historical_max is True
    assert features.behavior_deviation_count == 3


def test_feature_reconciliation_anomaly(sample_transaction, baseline_profile):
    extractor = FeatureExtractor()
    recon = ReconciliationResult(
        reconciliation_id="REC-001",
        transaction_id=sample_transaction.transaction_id,
        status=ReconciliationStatus.MISMATCHED,
        score=0.45,
        matched_fields=["account_id"],
        mismatched_fields=["amount"],
        mismatch_reasons=["Amount variance"],
        evidence=[{"note": "Mismatch evidence"}],
    )
    features = extractor.extract_features(sample_transaction, baseline_profile, reconciliation_result=recon)
    assert features.is_reconciliation_anomalous is True
    assert features.reconciliation_status == "MISMATCHED"


# ==========================================
# 3. INDEPENDENT RULE TESTS (7 tests)
# ==========================================

def test_rule_high_amount_anomaly(sample_transaction, baseline_profile):
    rule = RuleHighAmountAnomaly()
    extractor = FeatureExtractor()

    # Normal ratio (1.0x) -> triggered=False, contribution=0
    f_norm = extractor.extract_features(sample_transaction.model_copy(update={"amount": 200.00}), baseline_profile)
    ev_norm = rule.evaluate(f_norm)
    assert ev_norm.triggered is False
    assert ev_norm.score_contribution == 0

    # 4.0x ratio -> contribution=10
    f_med = extractor.extract_features(sample_transaction.model_copy(update={"amount": 800.00}), baseline_profile)
    ev_med = rule.evaluate(f_med)
    assert ev_med.triggered is True
    assert ev_med.score_contribution == 10

    # 12.0x ratio -> contribution=25
    f_crit = extractor.extract_features(sample_transaction.model_copy(update={"amount": 2400.00}), baseline_profile)
    ev_crit = rule.evaluate(f_crit)
    assert ev_crit.triggered is True
    assert ev_crit.score_contribution == 25


def test_rule_high_velocity(sample_transaction, baseline_profile):
    rule = RuleHighVelocity()
    extractor = FeatureExtractor()
    base_time = sample_transaction.transaction_timestamp

    # 4 transactions in window -> contribution=12
    recent = [
        sample_transaction.model_copy(update={
            "transaction_id": f"TX-{i}",
            "transaction_timestamp": base_time - timedelta(seconds=i * 5)
        })
        for i in range(1, 5)
    ]
    f = extractor.extract_features(sample_transaction, baseline_profile, recent_transactions=recent)
    ev = rule.evaluate(f)
    assert ev.triggered is True
    assert ev.score_contribution == 12


def test_rule_rapid_repeated(sample_transaction, baseline_profile):
    rule = RuleRapidRepeatedTransaction()
    extractor = FeatureExtractor()
    base_time = sample_transaction.transaction_timestamp

    # 3 identical amounts -> contribution=15
    recent = [
        sample_transaction.model_copy(update={
            "transaction_id": f"TX-{i}",
            "amount": sample_transaction.amount,
            "transaction_timestamp": base_time - timedelta(seconds=i * 10)
        })
        for i in range(1, 4)
    ]
    f = extractor.extract_features(sample_transaction, baseline_profile, recent_transactions=recent)
    ev = rule.evaluate(f)
    assert ev.triggered is True
    assert ev.score_contribution == 15


def test_rule_unusual_time(sample_transaction, baseline_profile):
    rule = RuleUnusualTransactionTime()
    extractor = FeatureExtractor()
    night_tx = sample_transaction.model_copy(
        update={"transaction_timestamp": datetime(2026, 10, 6, 2, 30, 0, tzinfo=timezone.utc)}
    )
    f = extractor.extract_features(night_tx, baseline_profile)
    ev = rule.evaluate(f)
    assert ev.triggered is True
    assert ev.score_contribution == 10


def test_rule_account_behavior_deviation(sample_transaction, baseline_profile):
    rule = RuleAccountBehaviorDeviation()
    extractor = FeatureExtractor()

    # 2 deviations -> contribution=15
    dev_tx = sample_transaction.model_copy(update={"currency": "JPY", "transaction_type": "WITHDRAWAL"})
    f = extractor.extract_features(dev_tx, baseline_profile)
    ev = rule.evaluate(f)
    assert ev.triggered is True
    assert ev.score_contribution == 15


def test_rule_reconciliation_anomaly(sample_transaction, baseline_profile):
    rule = RuleReconciliationAnomaly()
    extractor = FeatureExtractor()

    recon_mismatch = ReconciliationResult(
        reconciliation_id="REC-M",
        transaction_id="TX-1",
        status=ReconciliationStatus.MISMATCHED,
        score=0.4,
        matched_fields=[],
        mismatched_fields=[],
        mismatch_reasons=[],
        evidence=[{"note": "mismatch"}],
    )
    f = extractor.extract_features(sample_transaction, baseline_profile, reconciliation_result=recon_mismatch)
    ev = rule.evaluate(f)
    assert ev.triggered is True
    assert ev.score_contribution == 10

    recon_ambig = ReconciliationResult(
        reconciliation_id="REC-A",
        transaction_id="TX-1",
        status=ReconciliationStatus.AMBIGUOUS,
        score=0.8,
        matched_fields=[],
        mismatched_fields=[],
        mismatch_reasons=[],
        evidence=[{"note": "ambiguous"}],
    )
    f_ambig = extractor.extract_features(sample_transaction, baseline_profile, reconciliation_result=recon_ambig)
    ev_ambig = rule.evaluate(f_ambig)
    assert ev_ambig.triggered is True
    assert ev_ambig.score_contribution == 7


def test_rule_repeated_failed_activity(sample_transaction, baseline_profile):
    rule = RuleRepeatedFailedActivity()
    extractor = FeatureExtractor()
    f = extractor.extract_features(sample_transaction, baseline_profile, prior_failed_count=3)
    ev = rule.evaluate(f)
    assert ev.triggered is True
    assert ev.score_contribution == 10


# ==========================================
# 4. RISK ENGINE & THRESHOLD TESTS (8 tests)
# ==========================================

def test_risk_score_low_boundary():
    engine = FraudRiskEngine()
    features = FraudFeatures(
        transaction_id="TXN-0",
        account_id="ACC-0",
        amount=Decimal("100.00"),
        currency="USD",
        transaction_timestamp="2026-10-06T12:00:00Z",
        historical_avg_amount=Decimal("100.00"),
        amount_deviation_ratio=1.0,
        is_amount_anomalous=False,
        velocity_count_in_window=0,
        is_high_velocity=False,
        rapid_repeated_count=0,
        is_rapid_repeated=False,
        transaction_hour=12,
        is_unusual_hour=False,
        is_currency_unusual=False,
        is_direction_unusual=False,
        is_above_historical_max=False,
        behavior_deviation_count=0,
    )
    res = engine.evaluate(features)
    assert res.fraud_score == 0
    assert res.risk_level == RiskLevel.LOW
    assert res.requires_manual_review is False
    assert res.block_recommended is False


def test_risk_score_medium_threshold():
    engine = FraudRiskEngine()
    # Amount anomaly 18 + unusual time 10 = 28 -> MEDIUM (25-49)
    features = FraudFeatures(
        transaction_id="TXN-MED",
        account_id="ACC-0",
        amount=Decimal("1200.00"),
        currency="USD",
        transaction_timestamp="2026-10-06T03:00:00Z",
        historical_avg_amount=Decimal("200.00"),
        amount_deviation_ratio=6.0,  # +18
        is_amount_anomalous=True,
        velocity_count_in_window=0,
        is_high_velocity=False,
        rapid_repeated_count=0,
        is_rapid_repeated=False,
        transaction_hour=3,
        is_unusual_hour=True,  # +10
        is_currency_unusual=False,
        is_direction_unusual=False,
        is_above_historical_max=False,
        behavior_deviation_count=0,
    )
    res = engine.evaluate(features)
    assert res.fraud_score == 28
    assert res.risk_level == RiskLevel.MEDIUM
    assert res.requires_manual_review is False
    assert res.block_recommended is False


def test_risk_score_high_threshold():
    engine = FraudRiskEngine()
    # Velocity 25 + repeated 15 + unusual time 10 + recon 10 = 60 -> HIGH (50-74)
    features = FraudFeatures(
        transaction_id="TXN-HIGH",
        account_id="ACC-0",
        amount=Decimal("200.00"),
        currency="USD",
        transaction_timestamp="2026-10-06T03:00:00Z",
        historical_avg_amount=Decimal("200.00"),
        amount_deviation_ratio=1.0,
        is_amount_anomalous=False,
        velocity_count_in_window=8,  # +25
        is_high_velocity=True,
        rapid_repeated_count=3,  # +15
        is_rapid_repeated=True,
        transaction_hour=3,  # +10
        is_unusual_hour=True,
        is_currency_unusual=False,
        is_direction_unusual=False,
        is_above_historical_max=False,
        behavior_deviation_count=0,
        reconciliation_status="MISMATCHED",  # +10
        is_reconciliation_anomalous=True,
    )
    res = engine.evaluate(features)
    assert res.fraud_score == 60
    assert res.risk_level == RiskLevel.HIGH
    assert res.requires_manual_review is True
    assert res.block_recommended is False


def test_risk_score_critical_threshold():
    engine = FraudRiskEngine()
    # Amount 25 + velocity 25 + repeated 15 + unusual time 10 + recon 10 = 85 -> CRITICAL (>=75)
    features = FraudFeatures(
        transaction_id="TXN-CRIT",
        account_id="ACC-0",
        amount=Decimal("5000.00"),
        currency="USD",
        transaction_timestamp="2026-10-06T03:00:00Z",
        historical_avg_amount=Decimal("200.00"),
        amount_deviation_ratio=25.0,  # +25
        is_amount_anomalous=True,
        velocity_count_in_window=8,  # +25
        is_high_velocity=True,
        rapid_repeated_count=3,  # +15
        is_rapid_repeated=True,
        transaction_hour=3,  # +10
        is_unusual_hour=True,
        is_currency_unusual=False,
        is_direction_unusual=False,
        is_above_historical_max=False,
        behavior_deviation_count=0,
        reconciliation_status="MISMATCHED",  # +10
        is_reconciliation_anomalous=True,
    )
    res = engine.evaluate(features)
    assert res.fraud_score == 85
    assert res.risk_level == RiskLevel.CRITICAL
    assert res.requires_manual_review is True
    assert res.block_recommended is True


def test_anti_double_counting_safeguard():
    engine = FraudRiskEngine()
    # Anomaly amount (+25) and behavior deviation with ONLY historical ceiling (+8)
    features = FraudFeatures(
        transaction_id="TXN-DC",
        account_id="ACC-0",
        amount=Decimal("5000.00"),
        currency="USD",
        transaction_timestamp="2026-10-06T14:00:00Z",
        historical_avg_amount=Decimal("200.00"),
        amount_deviation_ratio=25.0,  # +25
        is_amount_anomalous=True,
        velocity_count_in_window=0,
        is_high_velocity=False,
        rapid_repeated_count=0,
        is_rapid_repeated=False,
        transaction_hour=14,
        is_unusual_hour=False,
        is_currency_unusual=False,
        is_direction_unusual=False,
        is_above_historical_max=True,
        behavior_deviation_count=1,  # without safeguard would add 8
    )
    res = engine.evaluate(features)
    # The redundant +8 from behavior deviation should be discounted
    assert res.fraud_score == 25
    assert "HIGH_AMOUNT_ANOMALY" in res.indicators


def test_score_capped_at_100():
    engine = FraudRiskEngine()
    # All rules trigger simultaneously
    features = FraudFeatures(
        transaction_id="TXN-ALL",
        account_id="ACC-0",
        amount=Decimal("10000.00"),
        currency="JPY",
        transaction_timestamp="2026-10-06T03:00:00Z",
        historical_avg_amount=Decimal("100.00"),
        amount_deviation_ratio=100.0,
        is_amount_anomalous=True,
        velocity_count_in_window=10,
        is_high_velocity=True,
        rapid_repeated_count=5,
        is_rapid_repeated=True,
        transaction_hour=3,
        is_unusual_hour=True,
        is_currency_unusual=True,
        is_direction_unusual=True,
        is_above_historical_max=True,
        behavior_deviation_count=3,
        reconciliation_status="MISMATCHED",
        is_reconciliation_anomalous=True,
        failed_prior_activity_count=5,
        is_repeated_failure=True,
    )
    res = engine.evaluate(features)
    assert res.fraud_score <= 100
    assert res.fraud_score == 100
    assert res.risk_level == RiskLevel.CRITICAL


def test_custom_thresholds():
    custom_thresholds = FraudRiskThresholds(low_max=10, medium_max=30, high_max=60)
    engine = FraudRiskEngine(thresholds=custom_thresholds)
    # Score 18 would normally be LOW, but with low_max=10 it becomes MEDIUM
    features = FraudFeatures(
        transaction_id="TXN-CUST",
        account_id="ACC-0",
        amount=Decimal("1200.00"),
        currency="USD",
        transaction_timestamp="2026-10-06T14:00:00Z",
        historical_avg_amount=Decimal("200.00"),
        amount_deviation_ratio=6.0,  # +18
        is_amount_anomalous=True,
        velocity_count_in_window=0,
        is_high_velocity=False,
        rapid_repeated_count=0,
        is_rapid_repeated=False,
        transaction_hour=14,
        is_unusual_hour=False,
        is_currency_unusual=False,
        is_direction_unusual=False,
        is_above_historical_max=False,
        behavior_deviation_count=0,
    )
    res = engine.evaluate(features)
    assert res.fraud_score == 18
    assert res.risk_level == RiskLevel.MEDIUM


def test_clear_status_when_zero_score():
    engine = FraudRiskEngine()
    features = FraudFeatures(
        transaction_id="TXN-ZERO",
        account_id="ACC-0",
        amount=Decimal("50.00"),
        currency="USD",
        transaction_timestamp="2026-10-06T12:00:00Z",
        historical_avg_amount=Decimal("50.00"),
        amount_deviation_ratio=1.0,
        is_amount_anomalous=False,
        velocity_count_in_window=0,
        is_high_velocity=False,
        rapid_repeated_count=0,
        is_rapid_repeated=False,
        transaction_hour=12,
        is_unusual_hour=False,
        is_currency_unusual=False,
        is_direction_unusual=False,
        is_above_historical_max=False,
        behavior_deviation_count=0,
    )
    res = engine.evaluate(features)
    assert res.status == FraudStatus.CLEAR


# ==========================================
# 5. AGENT DETERMINISM & EXECUTION (5 tests)
# ==========================================

def test_agent_normal_transaction(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    res = agent.analyze_transaction(sample_transaction, profile=baseline_profile)
    assert res.fraud_score == 0
    assert res.risk_level == RiskLevel.LOW
    assert res.requires_manual_review is False
    assert res.block_recommended is False


def test_agent_suspicious_transaction(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    # 4x amount -> ratio 4.0x (+10)
    tx = sample_transaction.model_copy(update={"amount": 800.00})
    res = agent.analyze_transaction(tx, profile=baseline_profile)
    assert res.fraud_score >= 10
    assert "HIGH_AMOUNT_ANOMALY" in res.indicators


def test_agent_high_risk_transaction(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    base_time = sample_transaction.transaction_timestamp
    tx = sample_transaction.model_copy(update={
        "amount": 2500.00,
        "transaction_timestamp": base_time.replace(hour=3)
    })
    # Velocity burst relative to tx (+18) + unusual time (+10) + amount anomaly (+25) = 53 -> HIGH
    recent = [
        sample_transaction.model_copy(update={
            "transaction_id": f"TX-{i}",
            "transaction_timestamp": tx.transaction_timestamp - timedelta(seconds=i * 5)
        })
        for i in range(1, 6)
    ]
    res = agent.analyze_transaction(tx, profile=baseline_profile, recent_transactions=recent)
    assert res.risk_level == RiskLevel.HIGH
    assert res.requires_manual_review is True
    assert res.block_recommended is False


def test_agent_critical_transaction(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    base_time = sample_transaction.transaction_timestamp
    tx = sample_transaction.model_copy(update={
        "amount": 3000.00,
        "transaction_timestamp": base_time.replace(hour=3)
    })
    # Velocity burst (+25) + repeated (+15) + amount (+25) + night (+10) + failed (+10) = 85 -> CRITICAL
    recent = [
        sample_transaction.model_copy(update={
            "transaction_id": f"TX-{i}",
            "amount": 3000.00,
            "transaction_timestamp": tx.transaction_timestamp - timedelta(seconds=i * 5)
        })
        for i in range(1, 9)
    ]
    res = agent.analyze_transaction(
        tx,
        profile=baseline_profile,
        recent_transactions=recent,
        prior_failed_count=3,
    )
    assert res.risk_level == RiskLevel.CRITICAL
    assert res.requires_manual_review is True
    assert res.block_recommended is True


def test_agent_deterministic_reproducibility(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    res1 = agent.analyze_transaction(sample_transaction, profile=baseline_profile)
    res2 = agent.analyze_transaction(sample_transaction, profile=baseline_profile)
    assert res1.fraud_score == res2.fraud_score
    assert res1.risk_level == res2.risk_level
    assert res1.indicators == res2.indicators
    assert len(res1.evidence) == len(res2.evidence)


# ==========================================
# 6. PROFILE RESOLUTION & GENERATION (2 tests)
# ==========================================

def test_profile_generation_from_transactions(sample_transaction):
    store = AccountProfileStore()
    tx1 = sample_transaction.model_copy(update={"amount": 100.00})
    tx2 = sample_transaction.model_copy(update={"amount": 300.00})
    profile = store.build_profile_from_transactions(sample_transaction.account_id, [tx1, tx2])
    assert profile.transaction_count == 2
    assert profile.average_amount == Decimal("200.00")
    assert profile.max_amount == Decimal("300.00")
    assert profile.min_amount == Decimal("100.00")


def test_profile_default_fallback():
    store = AccountProfileStore()
    profile = store.get_or_create_default_profile("ACC-UNKNOWN")
    assert profile.account_id == "ACC-UNKNOWN"
    assert profile.average_amount == Decimal("5000.00")


# ==========================================
# 7. EVENT INTEGRATION & WORKFLOW (4 tests)
# ==========================================

def test_fraud_service_emits_event(sample_transaction, baseline_profile, temp_db):
    tracker = WorkflowTracker()
    orchestrator = EventOrchestrator(workflow_tracker=tracker)
    publisher = LocalEventPublisher(orchestrator)
    service = FraudService(publisher=publisher, workflow_tracker=tracker, db_path=temp_db)

    received_events = []
    orchestrator.register_handler(
        EventType.FRAUD_ANALYSIS_COMPLETED.value,
        lambda ev: received_events.append(ev) or {"status": "OK"},
    )

    res = service.analyze_transaction(sample_transaction, correlation_id="CORR-EV-1")
    assert len(received_events) == 1
    ev = received_events[0]
    assert ev.event_type == EventType.FRAUD_ANALYSIS_COMPLETED.value
    assert ev.correlation_id == "CORR-EV-1"
    assert ev.payload["fraud_score"] == res.fraud_score
    assert ev.payload["risk_level"] == res.risk_level.value


def test_fraud_service_masks_account_in_event(sample_transaction, temp_db):
    orchestrator = EventOrchestrator()
    publisher = LocalEventPublisher(orchestrator)
    service = FraudService(publisher=publisher, db_path=temp_db)

    event_captured = []
    orchestrator.register_handler(
        EventType.FRAUD_ANALYSIS_COMPLETED.value,
        lambda ev: event_captured.append(ev) or {"status": "OK"},
    )

    service.analyze_transaction(sample_transaction)
    assert len(event_captured) == 1
    masked_acc = event_captured[0].payload["masked_account_id"]
    assert "ACC-200001" not in masked_acc
    assert "****" in masked_acc


def test_workflow_state_updated_to_completed(sample_transaction, temp_db):
    tracker = WorkflowTracker()
    corr_id = "CORR-WF-FRAUD"
    tracker.create_workflow(corr_id)

    orchestrator = EventOrchestrator(workflow_tracker=tracker)
    publisher = LocalEventPublisher(orchestrator)
    service = FraudService(publisher=publisher, workflow_tracker=tracker, db_path=temp_db)

    service.analyze_transaction(sample_transaction, correlation_id=corr_id)
    wf = tracker.get_workflow(corr_id)
    assert wf.stages["fraud_analysis"] == StageStatus.COMPLETED.value


def test_on_fraud_requested_callback(sample_transaction, temp_db):
    tracker = WorkflowTracker()
    orchestrator = EventOrchestrator(workflow_tracker=tracker)
    publisher = LocalEventPublisher(orchestrator)
    service = FraudService(publisher=publisher, workflow_tracker=tracker, db_path=temp_db)

    event = Event(
        event_type=EventType.FRAUD_ANALYSIS_REQUESTED.value,
        source="test",
        correlation_id="CORR-CB-1",
        causation_id="EV-PARENT",
        payload={
            "transaction_id": sample_transaction.transaction_id,
            "account_id": sample_transaction.account_id,
            "amount": sample_transaction.amount,
            "currency": sample_transaction.currency,
            "destination_system": "INTERNAL_LEDGER",
            "source_system": "API",
        },
    )
    resp = service.on_fraud_requested(event)
    assert resp["status"] == "COMPLETED"
    assert resp["stage"] == "FRAUD_ANALYSIS"
    assert "fraud_score" in resp


# ==========================================
# 8. DATABASE PERSISTENCE & METRICS (3 tests)
# ==========================================

def test_db_save_and_retrieve_fraud_analysis(sample_transaction, temp_db):
    service = FraudService(db_path=temp_db)
    res = service.analyze_transaction(sample_transaction)
    stored = service.get_fraud_analysis(sample_transaction.transaction_id)
    assert stored is not None
    assert stored["transaction_id"] == sample_transaction.transaction_id
    assert stored["fraud_score"] == res.fraud_score
    assert stored["risk_level"] == res.risk_level.value


def test_db_fraud_metrics_aggregation(sample_transaction, baseline_profile, temp_db):
    service = FraudService(db_path=temp_db)
    # 1 LOW transaction
    service.analyze_transaction(sample_transaction, profile=baseline_profile)
    # 1 HIGH transaction (amount anomaly 25 + night 10 + velocity 18 = 53 -> HIGH)
    tx_high = sample_transaction.model_copy(update={
        "transaction_id": "TXN-HIGH-METRIC",
        "amount": 2500.00,
        "transaction_timestamp": sample_transaction.transaction_timestamp.replace(hour=3)
    })
    recent_burst = [
        sample_transaction.model_copy(update={
            "transaction_id": f"TX-BURST-{i}",
            "transaction_timestamp": tx_high.transaction_timestamp - timedelta(seconds=i * 5)
        })
        for i in range(1, 6)
    ]
    service.analyze_transaction(tx_high, profile=baseline_profile, recent_transactions=recent_burst)

    metrics = service.get_metrics()
    assert metrics["total_analyzed"] == 2
    assert metrics["low_count"] >= 1
    assert metrics["high_count"] >= 1
    assert metrics["average_score"] > 0


def test_fraud_analysis_idempotency(sample_transaction, temp_db):
    service = FraudService(db_path=temp_db)
    res1 = service.analyze_transaction(sample_transaction)
    res2 = service.analyze_transaction(sample_transaction)
    assert res1.fraud_score == res2.fraud_score
    # In SQLite, latest row is retrieved
    retrieved = service.get_fraud_analysis(sample_transaction.transaction_id)
    assert retrieved is not None
    assert retrieved["fraud_score"] == res1.fraud_score


# ==========================================
# 9. REST API ENDPOINTS (3 tests)
# ==========================================

def test_api_post_fraud_analyze(sample_transaction, temp_db):
    # Insert transaction first
    conn = get_connection(temp_db)
    insert_transaction(conn, sample_transaction)
    conn.close()

    # Configure app fraud service to use temp_db
    from main import fraud_service
    original_db = fraud_service.db_path
    fraud_service.db_path = temp_db

    client = TestClient(app)
    try:
        response = client.post(
            "/fraud/analyze",
            json={"transaction_id": sample_transaction.transaction_id},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["transaction_id"] == sample_transaction.transaction_id
        assert "fraud_score" in data
        assert "risk_level" in data
        assert "requires_manual_review" in data
    finally:
        fraud_service.db_path = original_db


def test_api_post_fraud_analyze_not_found():
    client = TestClient(app)
    response = client.post("/fraud/analyze", json={"transaction_id": "TXN-NONEXISTENT-9999"})
    assert response.status_code == 404


def test_api_get_fraud_metrics():
    client = TestClient(app)
    response = client.get("/fraud/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "total_analyzed" in data
    assert "average_score" in data


# ==========================================
# 10. ADVERSARIAL & SAFETY TESTS (7 tests)
# ==========================================

def test_adversarial_huge_amount(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    huge_tx = sample_transaction.model_copy(update={"amount": 10_000_000.00})
    res = agent.analyze_transaction(huge_tx, profile=baseline_profile)
    assert res.fraud_score >= 25
    assert "HIGH_AMOUNT_ANOMALY" in res.indicators


def test_adversarial_velocity_burst_attack(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    base_time = sample_transaction.transaction_timestamp
    recent = [
        sample_transaction.model_copy(update={
            "transaction_id": f"BURST-{i}",
            "transaction_timestamp": base_time - timedelta(seconds=i * 2)
        })
        for i in range(1, 15)
    ]
    res = agent.analyze_transaction(sample_transaction, profile=baseline_profile, recent_transactions=recent)
    assert "HIGH_TRANSACTION_VELOCITY" in res.indicators
    assert res.fraud_score >= 25


def test_adversarial_repeated_identical_transactions(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    base_time = sample_transaction.transaction_timestamp
    recent = [
        sample_transaction.model_copy(update={
            "transaction_id": f"DUP-{i}",
            "amount": sample_transaction.amount,
            "transaction_timestamp": base_time - timedelta(seconds=i * 10)
        })
        for i in range(1, 5)
    ]
    res = agent.analyze_transaction(sample_transaction, profile=baseline_profile, recent_transactions=recent)
    assert "RAPID_REPEATED_TRANSACTION" in res.indicators


def test_adversarial_unknown_account_graceful_handling(sample_transaction):
    agent = FraudDetectionAgent()
    unknown_tx = sample_transaction.model_copy(update={"account_id": "ACC-NEVER-SEEN-BEFORE"})
    res = agent.analyze_transaction(unknown_tx)  # no profile supplied
    assert res is not None
    assert 0 <= res.fraud_score <= 100


def test_adversarial_missing_reconciliation_result(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    res = agent.analyze_transaction(sample_transaction, profile=baseline_profile, reconciliation_result=None)
    assert res is not None
    # RuleReconciliationAnomaly should simply not trigger
    assert "RECONCILIATION_ANOMALY" not in res.indicators


def test_safety_transaction_fields_unmodified(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    orig_amount = sample_transaction.amount
    orig_acc = sample_transaction.account_id
    orig_curr = sample_transaction.currency

    agent.analyze_transaction(sample_transaction, profile=baseline_profile)

    assert sample_transaction.amount == orig_amount
    assert sample_transaction.account_id == orig_acc
    assert sample_transaction.currency == orig_curr


def test_safety_reconciliation_result_unmodified(sample_transaction, baseline_profile):
    agent = FraudDetectionAgent()
    recon = ReconciliationResult(
        reconciliation_id="REC-SAFE",
        transaction_id=sample_transaction.transaction_id,
        status=ReconciliationStatus.MATCHED,
        score=1.0,
        matched_fields=["all"],
        mismatched_fields=[],
        mismatch_reasons=[],
        evidence=[{"note": "perfect"}],
    )
    agent.analyze_transaction(sample_transaction, profile=baseline_profile, reconciliation_result=recon)
    assert recon.status == ReconciliationStatus.MATCHED
    assert recon.score == 1.0
