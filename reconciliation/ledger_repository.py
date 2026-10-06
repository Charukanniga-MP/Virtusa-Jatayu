"""Repository Layer for Core Ledger Entries and Reconciliation Results.

Decouples SQL persistence from the deterministic reconciliation engine.
Provides lookup mechanisms by ledger_entry_id, transaction_id, and external reference_id.
"""

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional

from database.database import (
    get_all_reconciliation_results,
    get_connection,
    get_ledger_entries_by_reference_id,
    get_ledger_entries_by_transaction_id,
    get_ledger_entry_by_id,
    get_reconciliation_result_by_id,
    init_db,
    insert_ledger_entry,
    insert_reconciliation_result,
)
from reconciliation.models import LedgerEntry
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus

logger = logging.getLogger("reconciliation.ledger_repository")


class LedgerRepository:
    """Repository managing persistence and retrieval of ledger entries and reconciliation results."""

    def __init__(self, db_path: Optional[str] = None) -> None:
        """Initialize repository with optional SQLite database file path."""
        self.db_path = db_path
        # Ensure schema exists on initialization
        init_db(self.db_path)

    # --------------------------------------------------------------------------
    # Ledger Entry Operations
    # --------------------------------------------------------------------------
    def save_ledger_entry(self, entry: LedgerEntry) -> None:
        """Persist or replace a LedgerEntry."""
        with get_connection(self.db_path) as conn:
            insert_ledger_entry(conn, entry)
        logger.debug("Saved ledger entry %s for tx %s", entry.ledger_entry_id, entry.transaction_id)

    def get_ledger_entry(self, ledger_entry_id: str) -> Optional[LedgerEntry]:
        """Retrieve a LedgerEntry by its primary key."""
        with get_connection(self.db_path) as conn:
            row = get_ledger_entry_by_id(conn, ledger_entry_id)
            if not row:
                return None
            return self._row_to_ledger_entry(row)

    def find_by_transaction_id(self, transaction_id: str) -> Optional[LedgerEntry]:
        """Find the primary candidate LedgerEntry associated with a transaction ID."""
        with get_connection(self.db_path) as conn:
            rows = get_ledger_entries_by_transaction_id(conn, transaction_id)
            if not rows:
                return None
            # Return the first matching ledger candidate
            return self._row_to_ledger_entry(rows[0])

    def find_all_by_transaction_id(self, transaction_id: str) -> List[LedgerEntry]:
        """Find all candidate LedgerEntry records associated with a transaction ID."""
        with get_connection(self.db_path) as conn:
            rows = get_ledger_entries_by_transaction_id(conn, transaction_id)
            return [self._row_to_ledger_entry(r) for r in rows]

    def find_by_reference_id(self, reference_id: str) -> Optional[LedgerEntry]:
        """Find candidate LedgerEntry matching an external reference ID."""
        if not reference_id:
            return None
        with get_connection(self.db_path) as conn:
            rows = get_ledger_entries_by_reference_id(conn, reference_id)
            if not rows:
                return None
            return self._row_to_ledger_entry(rows[0])

    # --------------------------------------------------------------------------
    # Reconciliation Result Operations
    # --------------------------------------------------------------------------
    def save_reconciliation_result(self, result: ReconciliationResult) -> None:
        """Persist a ReconciliationResult audit record."""
        payload = {
            "reconciliation_id": result.reconciliation_id,
            "transaction_id": result.transaction_id,
            "ledger_entry_id": result.ledger_entry_id,
            "correlation_id": result.correlation_id,
            "status": result.status,
            "score": result.score,
            "matched_fields": json.dumps(result.matched_fields),
            "mismatched_fields": json.dumps(result.mismatched_fields),
            "mismatch_reasons": json.dumps(result.mismatch_reasons),
            "evidence": json.dumps(result.evidence),
            "timestamp": result.timestamp.isoformat(),
        }
        with get_connection(self.db_path) as conn:
            insert_reconciliation_result(conn, payload)
        logger.info(
            "Saved reconciliation result %s (status=%s, score=%.2f) for tx %s",
            result.reconciliation_id,
            result.status,
            result.score,
            result.transaction_id,
        )

    def get_reconciliation_result(self, reconciliation_id: str) -> Optional[ReconciliationResult]:
        """Retrieve a ReconciliationResult by its reconciliation_id."""
        with get_connection(self.db_path) as conn:
            row = get_reconciliation_result_by_id(conn, reconciliation_id)
            if not row:
                return None
            return self._row_to_reconciliation_result(row)

    def get_all_results(self) -> List[ReconciliationResult]:
        """Retrieve all stored reconciliation results."""
        with get_connection(self.db_path) as conn:
            rows = get_all_reconciliation_results(conn)
            return [self._row_to_reconciliation_result(r) for r in rows]

    def get_metrics(self) -> Dict[str, Any]:
        """Compute aggregated reconciliation metrics from stored results."""
        results = self.get_all_results()
        total = len(results)
        if total == 0:
            return {
                "total": 0,
                "matched": 0,
                "partial_match": 0,
                "mismatched": 0,
                "not_found": 0,
                "average_score": 0.0,
            }

        matched = sum(1 for r in results if r.status == ReconciliationStatus.MATCHED.value)
        partial = sum(1 for r in results if r.status == ReconciliationStatus.PARTIAL_MATCH.value)
        mismatched = sum(1 for r in results if r.status == ReconciliationStatus.MISMATCHED.value)
        not_found = sum(1 for r in results if r.status == ReconciliationStatus.NOT_FOUND.value)
        avg_score = round(sum(r.score for r in results) / total, 4)

        return {
            "total": total,
            "matched": matched,
            "partial_match": partial,
            "mismatched": mismatched,
            "not_found": not_found,
            "average_score": avg_score,
        }

    # --------------------------------------------------------------------------
    # Row Deserialization Helpers
    # --------------------------------------------------------------------------
    @staticmethod
    def _row_to_ledger_entry(row: Dict[str, Any]) -> LedgerEntry:
        ts = row["timestamp"]
        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts)
        else:
            dt = ts

        return LedgerEntry(
            ledger_entry_id=row["ledger_entry_id"],
            transaction_id=row["transaction_id"],
            account_id=row["account_id"],
            amount=float(row["amount"]),
            currency=row["currency"],
            debit_credit=row["debit_credit"],
            reference_id=row.get("reference_id"),
            timestamp=dt,
            source_system=row["source_system"],
        )

    @staticmethod
    def _row_to_reconciliation_result(row: Dict[str, Any]) -> ReconciliationResult:
        ts = row["timestamp"]
        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts)
        else:
            dt = ts

        matched = json.loads(row["matched_fields"]) if isinstance(row["matched_fields"], str) else row["matched_fields"]
        mismatched = json.loads(row["mismatched_fields"]) if isinstance(row["mismatched_fields"], str) else row["mismatched_fields"]
        reasons = json.loads(row["mismatch_reasons"]) if isinstance(row["mismatch_reasons"], str) else row["mismatch_reasons"]
        evidence = json.loads(row["evidence"]) if row.get("evidence") and isinstance(row["evidence"], str) else (row.get("evidence") or [])

        return ReconciliationResult(
            reconciliation_id=row["reconciliation_id"],
            transaction_id=row["transaction_id"],
            ledger_entry_id=row.get("ledger_entry_id"),
            correlation_id=row.get("correlation_id"),
            status=row["status"],
            score=float(row["score"]),
            matched_fields=matched,
            mismatched_fields=mismatched,
            mismatch_reasons=reasons,
            evidence=evidence,
            timestamp=dt,
        )
