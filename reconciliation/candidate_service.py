"""Candidate Discovery Service for Ledger Reconciliation.

Coordinates candidate discovery from persistent ledger storage and executes
fuzzy candidate ranking without making authoritative financial decisions.
Maintains clean architectural boundaries:
Repository -> CandidateService -> FuzzyMatcher -> Ranked Candidates
"""

from __future__ import annotations

import logging
from typing import List, Optional

from reconciliation.fuzzy_matcher import FuzzyCandidateMatcher
from reconciliation.fuzzy_result import CandidateDiscoveryResult
from reconciliation.ledger_repository import LedgerRepository
from reconciliation.models import LedgerEntry, Transaction

logger = logging.getLogger("reconciliation.candidate_service")


class CandidateService:
    """Service discovering potential ledger entry candidates for a transaction."""

    def __init__(
        self,
        repository: Optional[LedgerRepository] = None,
        matcher: Optional[FuzzyCandidateMatcher] = None,
        amount_search_window: float = 50.0,
        max_repository_candidates: int = 100,
    ) -> None:
        """Initialize candidate discovery service with repository and fuzzy matcher."""
        self.repository = repository or LedgerRepository()
        self.matcher = matcher or FuzzyCandidateMatcher()
        self.amount_search_window = amount_search_window
        self.max_repository_candidates = max_repository_candidates

    def discover_candidates(
        self,
        transaction: Transaction,
    ) -> CandidateDiscoveryResult:
        """Locate candidate ledger entries and rank them using the fuzzy matcher.

        Args:
            transaction: Input banking transaction.

        Returns:
            CandidateDiscoveryResult containing ranked candidates and selection status.
        """
        logger.debug(
            "Discovering candidates for tx=%s (ref=%s, acc=%s, amt=%.2f)",
            transaction.transaction_id,
            transaction.reference_id,
            transaction.account_id,
            transaction.amount,
        )

        # 1. Fetch potential candidate pool from repository
        candidates: List[LedgerEntry] = self.repository.find_potential_candidates(
            transaction=transaction,
            amount_tolerance=self.amount_search_window,
            limit=self.max_repository_candidates,
        )

        # 2. Execute fuzzy candidate ranking
        discovery_result: CandidateDiscoveryResult = self.matcher.rank_candidates(
            transaction=transaction,
            candidates=candidates,
        )

        logger.info(
            "Candidate discovery for tx=%s: found=%d, status=%s, top=%s (review_required=%s)",
            transaction.transaction_id,
            discovery_result.candidate_count,
            discovery_result.selection_status,
            discovery_result.top_candidate.ledger_entry_id if discovery_result.top_candidate else "NONE",
            discovery_result.requires_manual_review,
        )

        return discovery_result
