"""Fraud Detection Agent for ZT-LedgerGuard.

Autonomous, deterministic intelligence agent coordinating profile resolution,
feature extraction, rule evaluation, and explainable risk grading.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional

from agents.base_agent import AgentExecutionRecord, BaseAgent
from fraud_detection.features import FeatureExtractionConfig, FeatureExtractor
from fraud_detection.fraud_result import FraudAnalysisResult
from fraud_detection.profile import AccountProfile, AccountProfileStore
from fraud_detection.risk_engine import FraudRiskEngine, FraudRiskThresholds
from reconciliation.models import Transaction
from reconciliation.reconciliation_result import ReconciliationResult

logger = logging.getLogger("agents.fraud_agent")


class FraudDetectionAgent(BaseAgent):
    """Deterministic, explainable fraud detection agent."""

    agent_name: str = "FraudDetectionAgent"
    agent_version: str = "1.0.0"

    def __init__(
        self,
        profile_store: Optional[AccountProfileStore] = None,
        feature_extractor: Optional[FeatureExtractor] = None,
        risk_engine: Optional[FraudRiskEngine] = None,
        agent_name: Optional[str] = None,
        agent_version: Optional[str] = None,
    ) -> None:
        super().__init__(
            agent_name=agent_name or "FraudDetectionAgent",
            agent_version=agent_version or "1.0.0",
        )
        self.profile_store = profile_store or AccountProfileStore()
        self.feature_extractor = feature_extractor or FeatureExtractor()
        self.risk_engine = risk_engine or FraudRiskEngine()

    def analyze_transaction(
        self,
        transaction: Transaction,
        profile: Optional[AccountProfile] = None,
        recent_transactions: Optional[List[Transaction]] = None,
        reconciliation_result: Optional[ReconciliationResult] = None,
        prior_failed_count: int = 0,
        correlation_id: Optional[str] = None,
    ) -> FraudAnalysisResult:
        """Execute full deterministic fraud evaluation for a transaction."""
        started_at = datetime.now(timezone.utc)

        # 1. Resolve Account Profile
        if profile is None:
            profile = self.profile_store.get_or_create_default_profile(
                account_id=transaction.account_id,
                baseline_currency=transaction.currency,
            )

        # 2. Extract Features
        features = self.feature_extractor.extract_features(
            transaction=transaction,
            profile=profile,
            recent_transactions=recent_transactions,
            reconciliation_result=reconciliation_result,
            prior_failed_count=prior_failed_count,
        )

        # 3. Evaluate Rules & Calculate Risk
        result = self.risk_engine.evaluate(
            features=features,
            correlation_id=correlation_id,
        )

        completed_at = datetime.now(timezone.utc)

        # Attach execution record if supported
        evidence_dicts: List[Dict[str, Any]] = [
            ev.model_dump() if hasattr(ev, "model_dump") else dict(ev)
            for ev in result.evidence
        ]
        exec_record = self.create_execution_record(
            transaction_id=transaction.transaction_id,
            decision=result.risk_level.value if hasattr(result.risk_level, "value") else str(result.risk_level),
            started_at=started_at,
            completed_at=completed_at,
            evidence=evidence_dicts,
            correlation_id=correlation_id,
            success=True,
        )
        setattr(result, "execution_record", exec_record)

        logger.info(
            "[FraudAgent] Evaluated TX %s -> Score: %d (%s), Status: %s, ManualReview: %s, Block: %s",
            transaction.transaction_id,
            result.fraud_score,
            result.risk_level.value if hasattr(result.risk_level, "value") else str(result.risk_level),
            result.status.value if hasattr(result.status, "value") else str(result.status),
            result.requires_manual_review,
            result.block_recommended,
        )
        return result

    def analyze(
        self,
        transaction: Transaction,
        profile: Optional[AccountProfile] = None,
        recent_transactions: Optional[List[Transaction]] = None,
        reconciliation_result: Optional[ReconciliationResult] = None,
        prior_failed_count: int = 0,
        correlation_id: Optional[str] = None,
    ) -> FraudAnalysisResult:
        """BaseAgent interface alias to analyze_transaction."""
        return self.analyze_transaction(
            transaction=transaction,
            profile=profile,
            recent_transactions=recent_transactions,
            reconciliation_result=reconciliation_result,
            prior_failed_count=prior_failed_count,
            correlation_id=correlation_id,
        )
