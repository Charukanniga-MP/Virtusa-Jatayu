"""Independent Deterministic Fraud Rules for ZT-LedgerGuard.

Each rule evaluates a specific domain pattern against extracted features,
producing an explainable RuleEvidence with numerical score contribution.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from fraud_detection.features import FraudFeatures
from fraud_detection.fraud_result import RuleEvidence, RuleSeverity


class BaseFraudRule(ABC):
    """Abstract base class for independent deterministic fraud rules."""

    rule_id: str
    max_score: int

    @abstractmethod
    def evaluate(self, features: FraudFeatures) -> RuleEvidence:
        """Evaluate the rule against extracted features and return explainable evidence."""
        pass


class RuleHighAmountAnomaly(BaseFraudRule):
    """Detects transactions significantly exceeding historical account baseline."""

    rule_id = "HIGH_AMOUNT_ANOMALY"
    max_score = 25

    def evaluate(self, features: FraudFeatures) -> RuleEvidence:
        ratio = features.amount_deviation_ratio
        if ratio < 3.0:
            return RuleEvidence(
                rule_id=self.rule_id,
                triggered=False,
                score_contribution=0,
                severity=RuleSeverity.INFO,
                reason="Transaction amount is within normal historical account bounds.",
                evidence={
                    "current_amount": str(features.amount),
                    "historical_average": str(features.historical_avg_amount),
                    "deviation_ratio": ratio,
                },
            )

        # Graduated scoring
        if ratio < 5.0:
            contribution = 10
            severity = RuleSeverity.MEDIUM
        elif ratio < 10.0:
            contribution = 18
            severity = RuleSeverity.HIGH
        else:
            contribution = 25
            severity = RuleSeverity.CRITICAL

        return RuleEvidence(
            rule_id=self.rule_id,
            triggered=True,
            score_contribution=contribution,
            severity=severity,
            reason=f"Transaction amount is {ratio:.1f}x higher than historical average baseline.",
            evidence={
                "current_amount": str(features.amount),
                "historical_average": str(features.historical_avg_amount),
                "deviation_ratio": ratio,
            },
        )


class RuleHighVelocity(BaseFraudRule):
    """Detects transaction bursts exceeding expected frequency limits."""

    rule_id = "HIGH_TRANSACTION_VELOCITY"
    max_score = 25

    def evaluate(self, features: FraudFeatures) -> RuleEvidence:
        count = features.velocity_count_in_window
        if count < 3:
            return RuleEvidence(
                rule_id=self.rule_id,
                triggered=False,
                score_contribution=0,
                severity=RuleSeverity.INFO,
                reason="Transaction frequency is within expected velocity thresholds.",
                evidence={"velocity_count": count, "window_seconds": 60},
            )

        if count < 5:
            contribution = 12
            severity = RuleSeverity.MEDIUM
        elif count < 8:
            contribution = 18
            severity = RuleSeverity.HIGH
        else:
            contribution = 25
            severity = RuleSeverity.CRITICAL

        return RuleEvidence(
            rule_id=self.rule_id,
            triggered=True,
            score_contribution=contribution,
            severity=severity,
            reason=f"Detected high velocity burst of {count} transactions within short time window.",
            evidence={"velocity_count": count, "window_seconds": 60},
        )


class RuleRapidRepeatedTransaction(BaseFraudRule):
    """Detects rapid repeated transactions with identical amount and account."""

    rule_id = "RAPID_REPEATED_TRANSACTION"
    max_score = 15

    def evaluate(self, features: FraudFeatures) -> RuleEvidence:
        count = features.rapid_repeated_count
        if count < 2:
            return RuleEvidence(
                rule_id=self.rule_id,
                triggered=False,
                score_contribution=0,
                severity=RuleSeverity.INFO,
                reason="No suspicious duplicate transactions detected in rapid succession.",
                evidence={"repeated_count": count},
            )

        contribution = 10 if count == 2 else 15
        severity = RuleSeverity.HIGH if count >= 3 else RuleSeverity.MEDIUM

        return RuleEvidence(
            rule_id=self.rule_id,
            triggered=True,
            score_contribution=contribution,
            severity=severity,
            reason=f"Identified {count} identical-amount transactions executed in rapid succession.",
            evidence={"repeated_count": count, "amount": str(features.amount)},
        )


class RuleUnusualTransactionTime(BaseFraudRule):
    """Flags transactions executed outside standard business or daylight hours."""

    rule_id = "UNUSUAL_TRANSACTION_TIME"
    max_score = 10

    def evaluate(self, features: FraudFeatures) -> RuleEvidence:
        if not features.is_unusual_hour:
            return RuleEvidence(
                rule_id=self.rule_id,
                triggered=False,
                score_contribution=0,
                severity=RuleSeverity.INFO,
                reason="Transaction occurred during standard operational business hours.",
                evidence={"transaction_hour": features.transaction_hour},
            )

        return RuleEvidence(
            rule_id=self.rule_id,
            triggered=True,
            score_contribution=10,
            severity=RuleSeverity.LOW,
            reason=f"Transaction executed at hour {features.transaction_hour:02d}:00 outside standard business operating hours.",
            evidence={
                "transaction_hour": features.transaction_hour,
                "business_window": features.context.get("business_hours", "06:00-22:00"),
            },
        )


class RuleAccountBehaviorDeviation(BaseFraudRule):
    """Detects deviations in currency, transaction direction, or historical ceiling."""

    rule_id = "ACCOUNT_BEHAVIOR_DEVIATION"
    max_score = 15

    def evaluate(self, features: FraudFeatures) -> RuleEvidence:
        dev_count = features.behavior_deviation_count
        if dev_count == 0:
            return RuleEvidence(
                rule_id=self.rule_id,
                triggered=False,
                score_contribution=0,
                severity=RuleSeverity.INFO,
                reason="Transaction attributes match account baseline profile.",
                evidence={"deviations": []},
            )

        dev_list = []
        if features.is_currency_unusual:
            dev_list.append(f"Uncommon currency: {features.currency}")
        if features.is_direction_unusual:
            dev_list.append("Uncommon transaction direction")
        if features.is_above_historical_max:
            dev_list.append("Exceeds historical maximum ceiling")

        contribution = 8 if dev_count == 1 else 15
        severity = RuleSeverity.HIGH if dev_count >= 2 else RuleSeverity.MEDIUM

        return RuleEvidence(
            rule_id=self.rule_id,
            triggered=True,
            score_contribution=contribution,
            severity=severity,
            reason=f"Transaction exhibits {dev_count} behavioral deviation(s) from account baseline: {', '.join(dev_list)}.",
            evidence={"deviation_count": dev_count, "deviations": dev_list},
        )


class RuleReconciliationAnomaly(BaseFraudRule):
    """Correlates reconciliation failure or ambiguity with fraud risk."""

    rule_id = "RECONCILIATION_ANOMALY"
    max_score = 10

    def evaluate(self, features: FraudFeatures) -> RuleEvidence:
        if not features.is_reconciliation_anomalous:
            return RuleEvidence(
                rule_id=self.rule_id,
                triggered=False,
                score_contribution=0,
                severity=RuleSeverity.INFO,
                reason="Reconciliation status indicates valid or pending ledger entry.",
                evidence={"reconciliation_status": features.reconciliation_status},
            )

        status = features.reconciliation_status
        if status in ("MISMATCHED", "NOT_FOUND"):
            contribution = 10
            severity = RuleSeverity.MEDIUM
        elif status == "AMBIGUOUS":
            contribution = 7
            severity = RuleSeverity.MEDIUM
        else:  # PARTIAL_MATCH
            contribution = 4
            severity = RuleSeverity.LOW

        return RuleEvidence(
            rule_id=self.rule_id,
            triggered=True,
            score_contribution=contribution,
            severity=severity,
            reason=f"Ledger reconciliation returned anomalous status: {status}.",
            evidence={
                "reconciliation_status": status,
                "reconciliation_score": features.reconciliation_score,
            },
        )


class RuleRepeatedFailedActivity(BaseFraudRule):
    """Detects prior sequential validation or processing failures."""

    rule_id = "REPEATED_FAILED_ACTIVITY"
    max_score = 10

    def evaluate(self, features: FraudFeatures) -> RuleEvidence:
        count = features.failed_prior_activity_count
        if count < 2:
            return RuleEvidence(
                rule_id=self.rule_id,
                triggered=False,
                score_contribution=0,
                severity=RuleSeverity.INFO,
                reason="No repeated transaction failure patterns detected.",
                evidence={"failed_count": count},
            )

        contribution = 6 if count == 2 else 10
        severity = RuleSeverity.MEDIUM

        return RuleEvidence(
            rule_id=self.rule_id,
            triggered=True,
            score_contribution=contribution,
            severity=severity,
            reason=f"Detected {count} prior transaction or reconciliation failures for account.",
            evidence={"failed_count": count},
        )


def get_default_fraud_rules() -> List[BaseFraudRule]:
    """Return the ordered suite of default deterministic fraud rules."""
    return [
        RuleHighAmountAnomaly(),
        RuleHighVelocity(),
        RuleRapidRepeatedTransaction(),
        RuleUnusualTransactionTime(),
        RuleAccountBehaviorDeviation(),
        RuleReconciliationAnomaly(),
        RuleRepeatedFailedActivity(),
    ]
