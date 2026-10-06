"""Deterministic Fraud Feature Extraction for ZT-LedgerGuard.

Extracts auditable, numerical, and contextual behavioral signals from transactions,
account profiles, and reconciliation statuses without black-box ML.
"""

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field

from fraud_detection.profile import AccountProfile
from reconciliation.models import Transaction
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus


class FeatureExtractionConfig(BaseModel):
    """Configurable parameters for feature extraction thresholds."""

    velocity_window_seconds: int = Field(default=60, ge=1)
    rapid_repeat_window_seconds: int = Field(default=120, ge=1)
    business_start_hour: int = Field(default=6, ge=0, le=23)
    business_end_hour: int = Field(default=22, ge=0, le=23)
    amount_anomaly_ratio_threshold: float = Field(default=3.0, ge=1.0)


class FraudFeatures(BaseModel):
    """Container for deterministic extracted fraud signals."""

    transaction_id: str
    account_id: str
    amount: Decimal
    currency: str
    transaction_timestamp: str

    # Feature 1: Amount Anomaly
    historical_avg_amount: Decimal
    amount_deviation_ratio: float
    is_amount_anomalous: bool

    # Feature 2: Velocity
    velocity_count_in_window: int
    is_high_velocity: bool

    # Feature 3: Rapid Repeated Transactions
    rapid_repeated_count: int
    is_rapid_repeated: bool

    # Feature 4: Unusual Transaction Time
    transaction_hour: int
    is_unusual_hour: bool

    # Feature 5: Account Behavior Deviation
    is_currency_unusual: bool
    is_direction_unusual: bool
    is_above_historical_max: bool
    behavior_deviation_count: int

    # Feature 6: Reconciliation Anomaly
    reconciliation_status: Optional[str] = None
    reconciliation_score: Optional[float] = None
    is_reconciliation_anomalous: bool = False

    # Feature 7: Failed / Repeated Activity
    failed_prior_activity_count: int = 0
    is_repeated_failure: bool = False

    # Raw context dictionary
    context: Dict[str, Any] = Field(default_factory=dict)


def parse_timestamp_safe(ts_input: Union[str, datetime]) -> datetime:
    """Safely parse ISO timestamp string or return datetime."""
    if isinstance(ts_input, datetime):
        return ts_input
    try:
        clean_ts = str(ts_input).replace("Z", "+00:00")
        return datetime.fromisoformat(clean_ts)
    except Exception:
        return datetime.now(timezone.utc)


class FeatureExtractor:
    """Deterministic extractor of behavioral and contextual fraud features."""

    def __init__(self, config: Optional[FeatureExtractionConfig] = None) -> None:
        self.config = config or FeatureExtractionConfig()

    def extract_features(
        self,
        transaction: Transaction,
        profile: AccountProfile,
        recent_transactions: Optional[List[Transaction]] = None,
        reconciliation_result: Optional[ReconciliationResult] = None,
        prior_failed_count: int = 0,
    ) -> FraudFeatures:
        """Extract all 7 canonical fraud features deterministically."""
        tx_amount = Decimal(str(transaction.amount)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_EVEN
        )
        tx_dt = parse_timestamp_safe(transaction.transaction_timestamp)
        tx_hour = tx_dt.hour

        # 1. Amount Anomaly
        avg_amt = (
            profile.average_amount
            if profile.average_amount > Decimal("0")
            else Decimal("5000.00")
        )
        deviation_ratio = float(tx_amount / avg_amt)
        is_amount_anomalous = deviation_ratio >= self.config.amount_anomaly_ratio_threshold

        # 2. Velocity (Transactions within configured time window)
        history = recent_transactions or []
        velocity_count = 0
        rapid_repeated_count = 0

        # Filter transactions matching this account
        account_history = [
            tx
            for tx in history
            if tx.account_id == transaction.account_id
            and tx.transaction_id != transaction.transaction_id
        ]

        for past_tx in account_history:
            past_dt = parse_timestamp_safe(past_tx.transaction_timestamp)
            delta_seconds = abs((tx_dt - past_dt).total_seconds())

            # Velocity window check
            if delta_seconds <= self.config.velocity_window_seconds:
                velocity_count += 1

            # Rapid repeated check (same amount, within repeat window)
            past_amount = Decimal(str(past_tx.amount)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_EVEN
            )
            if delta_seconds <= self.config.rapid_repeat_window_seconds and past_amount == tx_amount:
                rapid_repeated_count += 1

        is_high_velocity = velocity_count >= 3
        is_rapid_repeated = rapid_repeated_count >= 2

        # 4. Unusual Transaction Time
        is_unusual_hour = (
            tx_hour < self.config.business_start_hour or tx_hour >= self.config.business_end_hour
        )

        # 5. Account Behavior Deviation
        is_currency_unusual = (
            transaction.currency.upper() not in [c.upper() for c in profile.common_currencies]
            if profile.common_currencies
            else False
        )
        is_direction_unusual = (
            transaction.transaction_type.upper()
            not in [d.upper() for d in profile.common_transaction_directions]
            if profile.common_transaction_directions
            else False
        )
        is_above_historical_max = tx_amount > (profile.max_amount * Decimal("1.5"))

        deviation_count = (
            (1 if is_currency_unusual else 0)
            + (1 if is_direction_unusual else 0)
            + (1 if is_above_historical_max else 0)
        )

        # 6. Reconciliation Anomaly
        recon_status = None
        recon_score = None
        is_recon_anomalous = False

        if reconciliation_result:
            recon_status = (
                reconciliation_result.status.value
                if hasattr(reconciliation_result.status, "value")
                else str(reconciliation_result.status)
            )
            recon_score = reconciliation_result.score
            if recon_status in (
                "MISMATCHED",
                "NOT_FOUND",
                "AMBIGUOUS",
                "PARTIAL_MATCH",
            ):
                is_recon_anomalous = True

        # 7. Failed / Repeated Activity
        is_repeated_failure = prior_failed_count >= 2

        return FraudFeatures(
            transaction_id=transaction.transaction_id,
            account_id=transaction.account_id,
            amount=tx_amount,
            currency=transaction.currency.upper(),
            transaction_timestamp=tx_dt.isoformat(),
            historical_avg_amount=avg_amt,
            amount_deviation_ratio=round(deviation_ratio, 2),
            is_amount_anomalous=is_amount_anomalous,
            velocity_count_in_window=velocity_count,
            is_high_velocity=is_high_velocity,
            rapid_repeated_count=rapid_repeated_count,
            is_rapid_repeated=is_rapid_repeated,
            transaction_hour=tx_hour,
            is_unusual_hour=is_unusual_hour,
            is_currency_unusual=is_currency_unusual,
            is_direction_unusual=is_direction_unusual,
            is_above_historical_max=is_above_historical_max,
            behavior_deviation_count=deviation_count,
            reconciliation_status=recon_status,
            reconciliation_score=recon_score,
            is_reconciliation_anomalous=is_recon_anomalous,
            failed_prior_activity_count=prior_failed_count,
            is_repeated_failure=is_repeated_failure,
            context={
                "profile_tx_count": profile.transaction_count,
                "profile_max_amount": str(profile.max_amount),
                "business_hours": f"{self.config.business_start_hour:02d}:00-{self.config.business_end_hour:02d}:00",
            },
        )
