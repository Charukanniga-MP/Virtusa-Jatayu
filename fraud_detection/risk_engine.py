"""Deterministic Fraud Risk Scoring Engine for ZT-LedgerGuard.

Aggregates independent rule scores, enforces double-counting safeguards,
applies configurable thresholds, and maps scores to risk levels and review recommendations.
"""

from typing import List, Optional
from pydantic import BaseModel, Field

from fraud_detection.features import FraudFeatures
from fraud_detection.fraud_result import (
    FraudAnalysisResult,
    FraudStatus,
    RiskLevel,
    RuleEvidence,
)
from fraud_detection.rules import BaseFraudRule, get_default_fraud_rules


class FraudRiskThresholds(BaseModel):
    """Configurable boundaries for risk levels."""

    low_max: int = Field(default=24, ge=0, le=100)
    medium_max: int = Field(default=49, ge=0, le=100)
    high_max: int = Field(default=74, ge=0, le=100)
    # 75 to 100 is CRITICAL


class FraudRiskEngine:
    """Stateless, deterministic risk aggregation and classification engine."""

    def __init__(
        self,
        thresholds: Optional[FraudRiskThresholds] = None,
        rules: Optional[List[BaseFraudRule]] = None,
    ) -> None:
        self.thresholds = thresholds or FraudRiskThresholds()
        self.rules = rules or get_default_fraud_rules()

    def evaluate(
        self,
        features: FraudFeatures,
        correlation_id: Optional[str] = None,
    ) -> FraudAnalysisResult:
        """Execute rules, apply anti-double-counting safeguards, and calculate final risk score."""
        evidence_list: List[RuleEvidence] = []
        raw_score = 0
        triggered_indicators: List[str] = []

        amount_anomaly_triggered = False
        behavior_dev_triggered = False
        behavior_dev_evidence: Optional[RuleEvidence] = None

        for rule in self.rules:
            evidence = rule.evaluate(features)
            evidence_list.append(evidence)

            if evidence.triggered:
                triggered_indicators.append(evidence.rule_id)
                raw_score += evidence.score_contribution

                if evidence.rule_id == "HIGH_AMOUNT_ANOMALY":
                    amount_anomaly_triggered = True
                elif evidence.rule_id == "ACCOUNT_BEHAVIOR_DEVIATION":
                    behavior_dev_triggered = True
                    behavior_dev_evidence = evidence

        # Anti-Double-Counting Safeguard:
        # If both HIGH_AMOUNT_ANOMALY and ACCOUNT_BEHAVIOR_DEVIATION triggered,
        # check if ACCOUNT_BEHAVIOR_DEVIATION was driven solely by amount exceeding max.
        # If so, discount the redundant contribution to avoid inflating the score for the same signal.
        if amount_anomaly_triggered and behavior_dev_triggered and behavior_dev_evidence:
            deviations = behavior_dev_evidence.evidence.get("deviations", [])
            # If the only deviation was historical max ceiling, or if multiple deviations include it:
            if "Exceeds historical maximum ceiling" in deviations:
                if len(deviations) == 1:
                    # Fully redundant with HIGH_AMOUNT_ANOMALY: damp by deducting its contribution
                    discount = behavior_dev_evidence.score_contribution
                    raw_score -= discount
                    behavior_dev_evidence.reason += (
                        " [Note: Redundant score discounted to avoid double-counting with HIGH_AMOUNT_ANOMALY]"
                    )
                else:
                    # Partial overlap: reduce by 5 points to prevent inflation
                    discount = 5
                    raw_score -= discount
                    behavior_dev_evidence.reason += (
                        " [Note: Partial overlap discounted by 5 points to avoid double-counting]"
                    )

        # Cap score between 0 and 100
        final_score = max(0, min(100, raw_score))

        # Map to RiskLevel
        if final_score <= self.thresholds.low_max:
            risk_level = RiskLevel.LOW
            status = FraudStatus.CLEAR if final_score == 0 else FraudStatus.ANALYZED
            requires_manual_review = False
            block_recommended = False
        elif final_score <= self.thresholds.medium_max:
            risk_level = RiskLevel.MEDIUM
            status = FraudStatus.ANALYZED
            requires_manual_review = False
            block_recommended = False
        elif final_score <= self.thresholds.high_max:
            risk_level = RiskLevel.HIGH
            status = FraudStatus.MANUAL_REVIEW
            requires_manual_review = True
            block_recommended = False
        else:
            risk_level = RiskLevel.CRITICAL
            status = FraudStatus.BLOCK_RECOMMENDED
            requires_manual_review = True
            block_recommended = True

        return FraudAnalysisResult(
            transaction_id=features.transaction_id,
            fraud_score=final_score,
            risk_level=risk_level,
            status=status,
            requires_manual_review=requires_manual_review,
            block_recommended=block_recommended,
            indicators=triggered_indicators,
            evidence=evidence_list,
            correlation_id=correlation_id,
        )
