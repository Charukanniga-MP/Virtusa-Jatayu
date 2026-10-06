"""Audit Integrity Service for ZT-LedgerGuard.

Zero-Trust Principle:
Bridges domain audit events with the cryptographic tamper-evident HashChain.
Every audit event is sanitized (PII masked), canonically hashed, linked to
the previous block's digest, and persisted to SQLite in an append-only ledger.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from database.database import (
    get_connection,
    get_audit_chain_count,
    get_audit_chain_records,
    get_latest_audit_chain_record,
    insert_audit_chain_record,
)
from security.hash_chain import (
    ChainIntegrityErrorType,
    ChainVerificationResult,
    HashChain,
    HashChainBlock,
)
from security.hashing import hash_json
from security.pii_protection import sanitize_payload
from security.security_config import GENESIS_PREVIOUS_HASH
from security.security_events import get_security_metrics_store

logger = logging.getLogger("security.audit_integrity")


class AuditIntegrityService:
    """Manages append-only cryptographic audit logging and tamper verification."""

    def __init__(
        self,
        db_path: Optional[str] = None,
        chain: Optional[HashChain] = None,
        metrics_store=None,
    ) -> None:
        self.db_path = db_path
        self.chain = chain or HashChain()
        self.metrics_store = metrics_store or get_security_metrics_store()

        # If db_path is provided, synchronize in-memory chain from SQLite
        if self.db_path:
            self._sync_chain_from_db()

    def _sync_chain_from_db(self) -> None:
        """Load persisted blocks from SQLite into memory on initialization."""
        try:
            with get_connection(self.db_path) as conn:
                db_records = get_audit_chain_records(conn)
            if db_records:
                blocks = [
                    HashChainBlock(
                        sequence_number=r["sequence_number"],
                        event_id=r["event_id"],
                        event_type=r["event_type"],
                        timestamp=r["timestamp"],
                        payload_hash=r["payload_hash"],
                        previous_hash=r["previous_hash"],
                        event_hash=r["event_hash"],
                        sanitized_payload=r.get("sanitized_payload"),
                    )
                    for r in db_records
                ]
                self.chain._blocks = blocks
                logger.info("[AuditIntegrityService] Loaded %d blocks from SQLite audit_chain.", len(blocks))
        except Exception as e:
            logger.warning("[AuditIntegrityService] Failed to sync chain from SQLite: %s", e)

    def record_audit_event(
        self,
        event_id: str,
        event_type: str,
        payload: Dict[str, Any],
        timestamp: Optional[str] = None,
    ) -> HashChainBlock:
        """Sanitize, hash, and cryptographically link an audit event to the ledger."""
        self.metrics_store.record_security_check()

        # 1. Zero-Trust PII Sanitization
        sanitized_payload = sanitize_payload(payload, deep=True)
        self.metrics_store.record_pii_sanitization()

        # 2. Canonical Payload Hashing
        payload_hash = hash_json(sanitized_payload)

        # 3. Append to In-Memory Hash Chain
        block = self.chain.append_block(
            event_id=event_id,
            event_type=event_type,
            payload_hash=payload_hash,
            timestamp=timestamp,
            sanitized_payload=sanitized_payload,
        )

        # 4. Persist to SQLite Append-Only Table
        if self.db_path:
            try:
                with get_connection(self.db_path) as conn:
                    insert_audit_chain_record(
                        conn,
                        {
                            "sequence_number": block.sequence_number,
                            "event_id": block.event_id,
                            "event_type": block.event_type,
                            "timestamp": block.timestamp,
                            "payload_hash": block.payload_hash,
                            "previous_hash": block.previous_hash,
                            "event_hash": block.event_hash,
                            "sanitized_payload": block.sanitized_payload,
                            "integrity_status": "VALID",
                        },
                    )
            except Exception as e:
                logger.error("[AuditIntegrityService] Failed to persist block %d to DB: %s", block.sequence_number, e)

        return block

    def verify_chain(self) -> ChainVerificationResult:
        """Perform end-to-end cryptographic verification of the audit ledger."""
        self.metrics_store.record_integrity_check()

        # If DB is configured, read directly from SQLite for ground-truth verification
        if self.db_path:
            try:
                with get_connection(self.db_path) as conn:
                    db_records = get_audit_chain_records(conn)
                blocks_to_verify = [
                    HashChainBlock(
                        sequence_number=r["sequence_number"],
                        event_id=r["event_id"],
                        event_type=r["event_type"],
                        timestamp=r["timestamp"],
                        payload_hash=r["payload_hash"],
                        previous_hash=r["previous_hash"],
                        event_hash=r["event_hash"],
                        sanitized_payload=r.get("sanitized_payload"),
                    )
                    for r in db_records
                ]
            except Exception as e:
                logger.error("[AuditIntegrityService] Failed to query SQLite audit_chain: %s", e)
                blocks_to_verify = self.chain.get_blocks()
        else:
            blocks_to_verify = self.chain.get_blocks()

        result = HashChain.verify_chain(blocks_to_verify)

        if not result.valid:
            self.metrics_store.record_integrity_failure()
            logger.critical(
                "[AuditIntegrityService] CRYPTOGRAPHIC INTEGRITY VIOLATION at seq %s: %s (%s)",
                result.first_invalid_sequence,
                result.error_type,
                result.details,
            )
        else:
            logger.info("[AuditIntegrityService] Verified %d blocks: 100%% INTACT.", result.events_checked)

        return result


# Global default service instance
_DEFAULT_INTEGRITY_SERVICE = AuditIntegrityService()


def get_audit_integrity_service(db_path: Optional[str] = None) -> AuditIntegrityService:
    """Access or instantiate the AuditIntegrityService."""
    if db_path and _DEFAULT_INTEGRITY_SERVICE.db_path != db_path:
        return AuditIntegrityService(db_path=db_path)
    return _DEFAULT_INTEGRITY_SERVICE
