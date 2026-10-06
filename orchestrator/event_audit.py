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


class EventAuditTrail:
    """Thread-safe in-memory audit log for event operations."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._records: List[AuditRecord] = []
        self._by_correlation: Dict[str, List[AuditRecord]] = {}

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
            rec = AuditRecord(
                event_id=event_id,
                event_type=str(event_type),
                correlation_id=correlation_id,
                causation_id=causation_id,
                source=source,
                processing_status=str(processing_status),
                error=error,
                details=details or {},
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
