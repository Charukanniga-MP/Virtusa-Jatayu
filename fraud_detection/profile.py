"""Deterministic Account Behavioral Profiles for ZT-LedgerGuard.

Provides lightweight, statistical behavioral profiles of banking accounts
without non-deterministic machine learning models.
"""

from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Dict, List, Optional, Set
from pydantic import BaseModel, Field

from reconciliation.models import Transaction


class AccountProfile(BaseModel):
    """Statistical historical profile of an account's normal transaction activity."""

    account_id: str
    transaction_count: int = Field(default=0, ge=0)
    average_amount: Decimal = Field(default=Decimal("5000.00"))
    max_amount: Decimal = Field(default=Decimal("25000.00"))
    min_amount: Decimal = Field(default=Decimal("10.00"))
    average_interval_seconds: float = Field(default=3600.0, ge=0.0)
    common_transaction_hours: List[int] = Field(
        default_factory=lambda: list(range(8, 20))  # Standard business hours (08:00 to 19:00)
    )
    common_currencies: List[str] = Field(default_factory=lambda: ["USD"])
    common_transaction_directions: List[str] = Field(
        default_factory=lambda: ["DEBIT", "PAYMENT", "TRANSFER"]
    )


class AccountProfileStore:
    """Thread-safe store and deterministic generator for account profiles."""

    def __init__(self) -> None:
        self._profiles: Dict[str, AccountProfile] = {}

    def get_profile(self, account_id: str) -> Optional[AccountProfile]:
        """Retrieve existing cached profile."""
        return self._profiles.get(account_id)

    def set_profile(self, profile: AccountProfile) -> None:
        """Register or update an account profile."""
        self._profiles[profile.account_id] = profile

    def build_profile_from_transactions(
        self, account_id: str, transactions: List[Transaction]
    ) -> AccountProfile:
        """Deterministically compute an account profile from historical transactions."""
        account_txs = [tx for tx in transactions if tx.account_id == account_id]
        if not account_txs:
            default_profile = AccountProfile(account_id=account_id)
            self._profiles[account_id] = default_profile
            return default_profile

        count = len(account_txs)
        amounts = [Decimal(str(tx.amount)) for tx in account_txs]
        total_amount = sum(amounts)
        avg_amount = (total_amount / Decimal(count)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_EVEN
        )
        max_amt = max(amounts).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
        min_amt = min(amounts).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)

        # Parse timestamps to compute intervals and common hours
        timestamps: List[datetime] = []
        hours_set: Set[int] = set()
        currencies_set: Set[str] = set()
        directions_set: Set[str] = set()

        for tx in account_txs:
            try:
                dt = datetime.fromisoformat(tx.transaction_timestamp.replace("Z", "+00:00"))
                timestamps.append(dt)
                hours_set.add(dt.hour)
            except Exception:
                pass
            currencies_set.add(tx.currency.upper())
            directions_set.add(tx.transaction_type.upper())

        # Sort timestamps to calculate average interval
        timestamps.sort()
        if len(timestamps) > 1:
            intervals = [
                (timestamps[i] - timestamps[i - 1]).total_seconds()
                for i in range(1, len(timestamps))
            ]
            avg_interval = sum(intervals) / len(intervals)
        else:
            avg_interval = 3600.0

        profile = AccountProfile(
            account_id=account_id,
            transaction_count=count,
            average_amount=avg_amount,
            max_amount=max_amt,
            min_amount=min_amt,
            average_interval_seconds=max(1.0, float(avg_interval)),
            common_transaction_hours=sorted(list(hours_set)) if hours_set else list(range(8, 20)),
            common_currencies=sorted(list(currencies_set)) if currencies_set else ["USD"],
            common_transaction_directions=sorted(list(directions_set))
            if directions_set
            else ["DEBIT"],
        )
        self._profiles[account_id] = profile
        return profile

    def get_or_create_default_profile(
        self,
        account_id: str,
        baseline_avg: Optional[Decimal] = None,
        baseline_currency: str = "USD",
    ) -> AccountProfile:
        """Return existing profile or create a default baseline profile."""
        if account_id in self._profiles:
            return self._profiles[account_id]

        avg = baseline_avg or Decimal("5000.00")
        profile = AccountProfile(
            account_id=account_id,
            transaction_count=1,
            average_amount=avg,
            max_amount=avg * Decimal("5"),
            min_amount=Decimal("10.00"),
            average_interval_seconds=3600.0,
            common_transaction_hours=list(range(8, 20)),
            common_currencies=[baseline_currency.upper()],
            common_transaction_directions=["DEBIT", "TRANSFER", "PAYMENT"],
        )
        self._profiles[account_id] = profile
        return profile

    def clear(self) -> None:
        """Clear cached profiles."""
        self._profiles.clear()
