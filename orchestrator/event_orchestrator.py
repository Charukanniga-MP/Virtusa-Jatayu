"""Event Model and Local Event Orchestrator Foundation.

Provides an in-memory event bus and routing mechanism for Day 1 development.
Designed to be seamlessly replaced with Apache Kafka consumers/producers in Day 2/3
without altering domain agent handler signatures.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger("orchestrator.event_orchestrator")


class Event(BaseModel):
    """Canonical Domain Event Model.

    Encapsulates state changes and messaging across system components.
    """

    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Globally unique identifier for the event",
    )
    event_type: str = Field(..., description="Canonical category/topic of the event")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when event was produced",
    )
    source: str = Field(..., description="System, service, or agent that produced the event")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Structured event payload")
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Distributed tracing correlation identifier",
    )

    @field_validator("event_id", "event_type", "source", "correlation_id")
    @classmethod
    def validate_non_empty(cls, value: str, info) -> str:
        """Validate that essential event metadata fields are non-empty."""
        if not value or not str(value).strip():
            raise ValueError(f"Field '{info.field_name}' cannot be empty.")
        return str(value).strip()

    model_config = {
        "json_schema_extra": {
            "example": {
                "event_id": "evt-77f98a21-9988",
                "event_type": "TRANSACTION_INGESTED",
                "timestamp": "2026-10-06T10:30:00Z",
                "source": "api_gateway",
                "payload": {"transaction_id": "TX-10001", "amount": 2500.50},
                "correlation_id": "corr-3321-bbca",
            }
        }
    }


EventHandler = Callable[[Event], Any]


class EventOrchestrator:
    """Local in-memory Event Orchestrator.

    Facilitates decoupled event dispatching and routing.
    In Phase 1, replaces distributed message brokers (e.g. Kafka) with local routing.
    """

    def __init__(self) -> None:
        self._handlers: Dict[str, List[EventHandler]] = {}

    def register_handler(self, event_type: str, handler: EventHandler) -> None:
        """Register a callback handler for a specific event type.

        Args:
            event_type: Canonical event name (e.g., 'TRANSACTION_INGESTED').
            handler: Callable taking an Event instance as argument.
        """
        clean_type = event_type.strip()
        if not clean_type:
            raise ValueError("event_type cannot be empty.")

        if clean_type not in self._handlers:
            self._handlers[clean_type] = []

        self._handlers[clean_type].append(handler)
        logger.info("Registered handler %s for event type '%s'", getattr(handler, "__name__", repr(handler)), clean_type)

    def publish(self, event: Event) -> List[Any]:
        """Publish an event to all registered handlers for its type.

        Logs event metadata safely (avoiding PII exposure in logs).
        Handles unknown event types gracefully without raising fatal errors.

        Args:
            event: The Event instance to dispatch.

        Returns:
            A list containing the return values of invoked handlers.
        """
        # Security rule: Log event ID, type, and correlation ID without unmasked account details
        logger.info(
            "Event dispatched: id=%s type=%s source=%s correlation_id=%s",
            event.event_id,
            event.event_type,
            event.source,
            event.correlation_id,
        )

        handlers = self._handlers.get(event.event_type)

        if not handlers:
            logger.warning(
                "No registered handlers for event_type='%s' (event_id=%s). Event safely dropped/deferred.",
                event.event_type,
                event.event_id,
            )
            return []

        results: List[Any] = []
        for handler in handlers:
            handler_name = getattr(handler, "__name__", repr(handler))
            try:
                result = handler(event)
                results.append(result)
            except Exception as exc:
                logger.error(
                    "Error executing handler %s for event %s (type=%s): %s",
                    handler_name,
                    event.event_id,
                    event.event_type,
                    exc,
                    exc_info=True,
                )
                results.append({"error": str(exc), "handler": handler_name})

        return results

    def get_registered_event_types(self) -> List[str]:
        """Return list of event types that have active handlers."""
        return list(self._handlers.keys())


# Placeholder handlers for Day 1 architectural validation
def placeholder_reconciliation_handler(event: Event) -> Dict[str, Any]:
    """Day 1 placeholder handler for reconciliation pipeline."""
    logger.info("[Reconciliation Placeholder] Received event %s", event.event_id)
    return {"status": "ACK_RECONCILIATION_PLACEHOLDER", "event_id": event.event_id}


def placeholder_fraud_handler(event: Event) -> Dict[str, Any]:
    """Day 1 placeholder handler for fraud pipeline."""
    logger.info("[Fraud Triage Placeholder] Received event %s", event.event_id)
    return {"status": "ACK_FRAUD_PLACEHOLDER", "event_id": event.event_id}
