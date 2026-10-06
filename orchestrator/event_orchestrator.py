"""Event Model and Production-Grade Event Orchestrator Pipeline.

Provides:
- Enriched Event model with correlation_id, causation_id, and retry_count
- Decoupled handler registration and unregistration
- Automatic event idempotency (duplicate prevention)
- Bounded retry mechanism with sanitized error logging
- Seamless integration with WorkflowTracker and EventAuditTrail
- Full backward compatibility with Day 1 & Day 2 contracts
"""

import asyncio
import inspect
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Union
from pydantic import BaseModel, Field, field_validator

from orchestrator.event_types import EventType
from security.security_utils import mask_account_id, sanitize_log_message

logger = logging.getLogger("orchestrator.event_orchestrator")


class Event(BaseModel):
    """Canonical Financial Domain Event Model."""

    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Globally unique identifier for this event instance",
    )
    event_type: str = Field(..., description="Canonical event type name or topic")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when event was emitted",
    )
    source: str = Field(..., description="Originating component, agent, or service")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Structured event payload")
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Workflow correlation ID shared across all related events",
    )
    causation_id: Optional[str] = Field(
        default=None,
        description="Identifier of the direct parent event that triggered this event",
    )
    retry_count: int = Field(
        default=0,
        ge=0,
        description="Number of delivery/processing retry attempts made",
    )

    @field_validator("event_id", "event_type", "source", "correlation_id")
    @classmethod
    def validate_non_empty(cls, value: str, info) -> str:
        """Validate that essential metadata fields are not blank."""
        if not value or not str(value).strip():
            raise ValueError(f"Field '{info.field_name}' cannot be empty.")
        return str(value).strip()

    model_config = {
        "json_schema_extra": {
            "example": {
                "event_id": "evt-77f98a21-9988",
                "event_type": "TRANSACTION_RECEIVED",
                "timestamp": "2026-10-06T10:30:00Z",
                "source": "api_gateway",
                "payload": {"transaction_id": "TXN-000001", "amount": 2500.50},
                "correlation_id": "corr-3321-bbca",
                "causation_id": "evt-root-1100",
                "retry_count": 0,
            }
        }
    }


EventHandler = Any


class EventOrchestrator:
    """Production-grade In-Memory Event Orchestrator.

    Coordinates event routing, idempotent deduplication, bounded retries,
    workflow status tracking, and audit logging.
    """

    def __init__(
        self,
        max_retries: Optional[int] = None,
        workflow_tracker: Optional[Any] = None,
        audit_trail: Optional[Any] = None,
    ) -> None:
        self.max_retries = (
            max_retries
            if max_retries is not None
            else int(os.getenv("MAX_EVENT_RETRIES", "3"))
        )
        self.workflow_tracker = workflow_tracker
        self.audit_trail = audit_trail

        self._lock = threading.RLock()
        self._handlers: Dict[str, List[EventHandler]] = {}
        self._processed_event_ids: Set[str] = set()

        self._metrics: Dict[str, int] = {
            "events_received": 0,
            "events_processed": 0,
            "events_failed": 0,
            "events_retried": 0,
            "events_duplicate": 0,
            "handlers_registered": 0,
            "workflows_started": 0,
            "workflows_completed": 0,
            "workflows_failed": 0,
            # Backward compatibility with Day 1 & Day 2 metrics
            "events_published": 0,
            "events_handled": 0,
            "events_rejected": 0,
            "unknown_events": 0,
            "processing_errors": 0,
        }

    def register_handler(self, event_type: str, handler: EventHandler) -> None:
        """Register an event handler for a specific event type."""
        clean_type = str(event_type).strip()
        if not clean_type:
            raise ValueError("event_type cannot be empty.")

        with self._lock:
            if clean_type not in self._handlers:
                self._handlers[clean_type] = []
            self._handlers[clean_type].append(handler)
            self._metrics["handlers_registered"] += 1

        name = getattr(handler, "name", getattr(handler, "__name__", repr(handler)))
        logger.info("Registered handler %s for event type '%s'", name, clean_type)

    def unregister_handler(self, event_type: str, handler: EventHandler) -> bool:
        """Unregister an event handler from a specific event type."""
        clean_type = str(event_type).strip()
        with self._lock:
            if clean_type in self._handlers and handler in self._handlers[clean_type]:
                self._handlers[clean_type].remove(handler)
                self._metrics["handlers_registered"] = max(
                    0, self._metrics["handlers_registered"] - 1
                )
                logger.info("Unregistered handler for event type '%s'", clean_type)
                return True
        return False

    def publish(self, event: Event) -> List[Any]:
        """Publish an event through the orchestrator.

        Enforces:
        1. Metrics recording
        2. Idempotency checks (duplicates are recorded and safely ignored)
        3. Audit trail logging
        4. Bounded retries upon handler failure
        5. Graceful unknown event handling
        """
        with self._lock:
            self._metrics["events_received"] += 1
            self._metrics["events_published"] += 1

        # 1. Idempotency check: Reject duplicate event IDs
        with self._lock:
            if event.event_id in self._processed_event_ids:
                self._metrics["events_duplicate"] += 1
                logger.warning(
                    "Duplicate event %s detected (type=%s). Skipping execution.",
                    event.event_id,
                    event.event_type,
                )
                if self.audit_trail:
                    self.audit_trail.record(
                        event_id=event.event_id,
                        event_type=str(event.event_type),
                        correlation_id=event.correlation_id,
                        causation_id=event.causation_id,
                        source=event.source,
                        processing_status="DUPLICATE",
                        details={"message": "Duplicate event ignored"},
                    )
                return [{"status": "DUPLICATE", "event_id": event.event_id}]

        # 2. Record RECEIVED in audit trail
        if self.audit_trail:
            self.audit_trail.record(
                event_id=event.event_id,
                event_type=str(event.event_type),
                correlation_id=event.correlation_id,
                causation_id=event.causation_id,
                source=event.source,
                processing_status="RECEIVED",
            )

        logger.info(
            "Event dispatched: id=%s type=%s source=%s correlation_id=%s causation_id=%s retry=%d",
            event.event_id,
            event.event_type,
            event.source,
            event.correlation_id,
            event.causation_id,
            event.retry_count,
        )

        handlers = list(self._handlers.get(str(event.event_type), []))

        if not handlers:
            with self._lock:
                self._metrics["unknown_events"] += 1
            logger.warning(
                "No registered handlers for event_type='%s' (event_id=%s). Event safely deferred.",
                event.event_type,
                event.event_id,
            )
            with self._lock:
                self._processed_event_ids.add(event.event_id)
            return []

        results: List[Any] = []

        for handler in handlers:
            handler_name = getattr(
                handler, "name", getattr(handler, "__name__", repr(handler))
            )
            handler_success = False

            # Bounded retry loop for individual handler invocation
            while event.retry_count <= self.max_retries and not handler_success:
                try:
                    # Invoke handler (supports BaseEventHandler, sync callable, or async callable)
                    result = self._execute_handler(handler, event)
                    handler_success = True
                    results.append(result)

                    with self._lock:
                        self._metrics["events_processed"] += 1
                        self._metrics["events_handled"] += 1

                    if self.audit_trail:
                        self.audit_trail.record(
                            event_id=event.event_id,
                            event_type=str(event.event_type),
                            correlation_id=event.correlation_id,
                            causation_id=event.causation_id,
                            source=event.source,
                            processing_status="COMPLETED",
                            details={"handler": handler_name},
                        )

                except Exception as exc:
                    sanitized_error = sanitize_log_message(str(exc))

                    if event.retry_count < self.max_retries:
                        event.retry_count += 1
                        with self._lock:
                            self._metrics["events_retried"] += 1
                        logger.warning(
                            "Handler %s failed on event %s (attempt %d/%d): %s. Retrying...",
                            handler_name,
                            event.event_id,
                            event.retry_count,
                            self.max_retries,
                            sanitized_error,
                        )
                        if self.audit_trail:
                            self.audit_trail.record(
                                event_id=event.event_id,
                                event_type=str(event.event_type),
                                correlation_id=event.correlation_id,
                                causation_id=event.causation_id,
                                source=event.source,
                                processing_status="PROCESSING",
                                error=sanitized_error,
                                details={"retry_count": event.retry_count},
                            )
                    else:
                        # Max retries exhausted
                        with self._lock:
                            self._metrics["events_failed"] += 1
                            self._metrics["processing_errors"] += 1
                        logger.error(
                            "Handler %s permanently failed for event %s after %d retries: %s",
                            handler_name,
                            event.event_id,
                            event.retry_count,
                            sanitized_error,
                        )

                        if self.audit_trail:
                            self.audit_trail.record(
                                event_id=event.event_id,
                                event_type=str(event.event_type),
                                correlation_id=event.correlation_id,
                                causation_id=event.causation_id,
                                source=event.source,
                                processing_status="FAILED",
                                error=sanitized_error,
                                details={"handler": handler_name, "final_retry": event.retry_count},
                            )

                        if self.workflow_tracker:
                            self.workflow_tracker.mark_failed(
                                correlation_id=event.correlation_id,
                                error=sanitized_error,
                                failed_stage=str(event.event_type),
                            )

                        results.append({"error": sanitized_error, "handler": handler_name})
                        break

        # Mark event as processed in idempotency set
        with self._lock:
            self._processed_event_ids.add(event.event_id)

        return results

    def _execute_handler(self, handler: Any, event: Event) -> Any:
        """Execute handler supporting class-based, callable, sync, and async variants."""
        # 1. BaseEventHandler class instance
        if hasattr(handler, "handle") and hasattr(handler, "can_handle"):
            if not handler.can_handle(event):
                return None
            fn = handler.handle
        else:
            fn = handler

        # 2. Asynchronous coroutine handler execution
        if inspect.iscoroutinefunction(fn):
            try:
                loop = asyncio.get_running_loop()
                return asyncio.run_coroutine_threadsafe(fn(event), loop).result()
            except RuntimeError:
                return asyncio.run(fn(event))

        # 3. Synchronous callable execution
        result = fn(event)
        if inspect.iscoroutine(result):
            return asyncio.run(result)
        return result

    def get_registered_event_types(self) -> List[str]:
        """Return list of event types that currently have registered handlers."""
        with self._lock:
            return list(self._handlers.keys())

    def get_metrics(self) -> Dict[str, int]:
        """Return a copy of the current orchestrator execution metrics."""
        with self._lock:
            metrics = dict(self._metrics)
            if self.workflow_tracker:
                workflows = self.workflow_tracker.get_all_workflows()
                metrics["workflows_started"] = len(workflows)
                metrics["workflows_completed"] = sum(
                    1 for w in workflows if w.status == "COMPLETED"
                )
                metrics["workflows_failed"] = sum(
                    1 for w in workflows if w.status == "FAILED"
                )
            return metrics

    def reset_metrics(self) -> None:
        """Reset internal metrics back to initial zero counters."""
        with self._lock:
            for key in self._metrics:
                self._metrics[key] = 0
            self._processed_event_ids.clear()


# ==============================================================================
# Baseline Handlers for Architectural Verification
# ==============================================================================

def placeholder_reconciliation_handler(event: Event) -> Dict[str, Any]:
    """Day 1 placeholder handler for reconciliation pipeline."""
    logger.info("[Reconciliation Placeholder] Received event %s", event.event_id)
    return {"status": "ACK_RECONCILIATION_PLACEHOLDER", "event_id": event.event_id}


def placeholder_fraud_handler(event: Event) -> Dict[str, Any]:
    """Day 1 placeholder handler for fraud pipeline."""
    logger.info("[Fraud Triage Placeholder] Received event %s", event.event_id)
    return {"status": "ACK_FRAUD_PLACEHOLDER", "event_id": event.event_id}


def transaction_event_handler(event: Event) -> Dict[str, Any]:
    """Standard Day 2 event handler for TRANSACTION_RECEIVED events."""
    payload = event.payload or {}
    tx_id = payload.get("transaction_id", "UNKNOWN")
    masked_acc = payload.get("masked_account_id", "********0000")

    logger.info(
        "Transaction event processed: tx_id=%s account=%s event_id=%s correlation_id=%s",
        tx_id,
        masked_acc,
        event.event_id,
        event.correlation_id,
    )
    return {
        "status": "PROCESSED",
        "transaction_id": tx_id,
        "event_id": event.event_id,
        "correlation_id": event.correlation_id,
    }
