"""Event Audit Trail for Financial Auditability and Traceability.

Records complete lifecycle audit records for every event passing through the system:
- event_id
- event_type
- correlation_id
- causation_id
- source
- timestamp
- processing_status (RECEIVED, PROCESSING, COMPLETED, FAILED, DUPLICATE)
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
import threading

from pydantic import BaseModel, Field


class AuditStatus(str, Enum):
    """Processing status categories for event audit records."""

    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DUPLICATE = "DUPLICATE"


class AuditRecord(BaseModel):
    """Immutable audit record representing an event transition."""

    event_id: str
    event_type: str
    correlation_id: str
    causation_id: Optional[str] = None
    source: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    processing_status: str
    error: Optional[str] = None
    details: Dict[str, Any] = Field(default_factory=dict)
    sequence_number: Optional[int] = Field(default=None, description="Cryptographic hash-chain sequence")
    event_hash: Optional[str] = Field(default=None, description="Cryptographic block digest")
    previous_hash: Optional[str] = Field(default=None, description="Predecessor block digest")
    payload_hash: Optional[str] = Field(default=None, description="Digest of sanitized payload")


class EventAuditTrail:
    """Thread-safe in-memory audit log for event operations with optional cryptographic integrity."""

    def __init__(self, integrity_service: Optional[Any] = None) -> None:
        self._lock = threading.RLock()
        self._records: List[AuditRecord] = []
        self._by_correlation: Dict[str, List[AuditRecord]] = {}
        self.integrity_service = integrity_service

    def set_integrity_service(self, integrity_service: Any) -> None:
        """Attach or update the cryptographic audit integrity service."""
        with self._lock:
            self.integrity_service = integrity_service

    def record(
        self,
        event_id: str,
        event_type: str,
        correlation_id: str,
        source: str,
        processing_status: str,
        causation_id: Optional[str] = None,
        error: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> AuditRecord:
        """Record an event state transition in the audit log."""
        with self._lock:
            seq_num = None
            ev_hash = None
            prev_hash = None
            pay_hash = None

            # If cryptographic integrity service is attached, record to hash chain
            if self.integrity_service is not None:
                try:
                    payload_to_hash = dict(details or {})
                    payload_to_hash.update({
                        "event_id": event_id,
                        "event_type": str(event_type),
                        "correlation_id": correlation_id,
                        "processing_status": str(processing_status),
                    })
                    block = self.integrity_service.record_audit_event(
                        event_id=event_id,
                        event_type=str(event_type),
                        payload=payload_to_hash,
                    )
                    seq_num = block.sequence_number
                    ev_hash = block.event_hash
                    prev_hash = block.previous_hash
                    pay_hash = block.payload_hash
                except Exception:
                    pass

            rec = AuditRecord(
                event_id=event_id,
                event_type=str(event_type),
                correlation_id=correlation_id,
                causation_id=causation_id,
                source=source,
                processing_status=str(processing_status),
                error=error,
                details=details or {},
                sequence_number=seq_num,
                event_hash=ev_hash,
                previous_hash=prev_hash,
                payload_hash=pay_hash,
            )
            self._records.append(rec)
            if correlation_id not in self._by_correlation:
                self._by_correlation[correlation_id] = []
            self._by_correlation[correlation_id].append(rec)
            return rec

    def get_event_history(self, correlation_id: str) -> List[Dict[str, Any]]:
        """Retrieve complete chronological event history for a correlation ID."""
        with self._lock:
            records = self._by_correlation.get(correlation_id, [])
            return [rec.model_dump() for rec in records]

    def get_all_records(self) -> List[AuditRecord]:
        """Return all audit records across all workflows."""
        with self._lock:
            return list(self._records)

    def reset(self) -> None:
        """Reset internal audit log."""
        with self._lock:
            self._records.clear()
            self._by_correlation.clear()
