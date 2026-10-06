"""Fraud Detection Agent for ZT-LedgerGuard.

Autonomous, deterministic intelligence agent coordinating profile resolution,
feature extraction, rule evaluation, and explainable risk grading.
"""

import logging
from typing import List, Optional
from fraud_detection.features import FeatureExtractionConfig, FeatureExtractor
from fraud_detection.fraud_result import FraudAnalysisResult
from fraud_detection.profile import AccountProfile, AccountProfileStore
from fraud_detection.risk_engine import FraudRiskEngine, FraudRiskThresholds
from reconciliation.models import Transaction
from reconciliation.reconciliation_result import ReconciliationResult

logger = logging.getLogger("agents.fraud_agent")


class FraudDetectionAgent:
    """Deterministic, explainable fraud detection agent."""

    def __init__(
        self,
        profile_store: Optional[AccountProfileStore] = None,
        feature_extractor: Optional[FeatureExtractor] = None,
        risk_engine: Optional[FraudRiskEngine] = None,
    ) -> None:
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

        logger.info(
            "[FraudAgent] Evaluated TX %s -> Score: %d (%s), Status: %s, ManualReview: %s, Block: %s",
            transaction.transaction_id,
            result.fraud_score,
            result.risk_level.value,
            result.status.value,
            result.requires_manual_review,
            result.block_recommended,
        )
        return result
