"""Event Publisher Abstraction and Local Publisher Implementation.

Provides an abstract EventPublisher interface that decouples event production from the
underlying broker infrastructure.
On Day 2, LocalEventPublisher routes events to the in-memory EventOrchestrator.
In subsequent phases (Days 3+), KafkaEventPublisher can be dropped in without changing
any generator, service, or agent business logic.
"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional
import uuid

from orchestrator.event_orchestrator import Event, EventOrchestrator
from reconciliation.models import Transaction
from security.security_utils import mask_account_id

# Canonical Event Types
EVENT_TYPE_TRANSACTION_RECEIVED = "TRANSACTION_RECEIVED"


class EventPublisher(ABC):
    """Abstract Base Class for event publishers."""

    @abstractmethod
    def publish(self, event: Event) -> Any:
        """Publish an event to the underlying messaging system or orchestrator.

        Args:
            event: Canonical domain Event to emit.

        Returns:
            Publisher-specific result or acknowledgment list.
        """
        pass

    def close(self) -> None:
        """Close any open connections or resources (no-op for local)."""
        pass


class LocalEventPublisher(EventPublisher):
    """Local in-memory event publisher delegating to the EventOrchestrator.

    Simulates message publishing semantics on developer workstations without
    requiring external Kafka brokers, Zookeeper, or container runtimes.
    """

    def __init__(self, orchestrator: Optional[EventOrchestrator] = None) -> None:
        """Initialize the local event publisher with an orchestrator instance.

        Args:
            orchestrator: EventOrchestrator instance. If None, instantiates a local one.
        """
        self.orchestrator = orchestrator if orchestrator is not None else EventOrchestrator()

    def publish(self, event: Event) -> List[Any]:
        """Send an event directly to the in-memory EventOrchestrator."""
        return self.orchestrator.publish(event)


def create_transaction_event(
    transaction: Transaction,
    source: str = "transaction_ingestion_service",
    correlation_id: Optional[str] = None,
) -> Event:
    """Helper utility to construct a canonical TRANSACTION_RECEIVED event.

    Args:
        transaction: Validated domain Transaction instance.
        source: Originating service or producer identifier.
        correlation_id: Optional correlation tracking ID (auto-generated if omitted).

    Returns:
        Canonical Event ready for publishing.
    """
    masked_acc = mask_account_id(transaction.account_id)
    payload = {
        "transaction_id": transaction.transaction_id,
        "masked_account_id": masked_acc,
        "transaction_type": transaction.transaction_type,
        "amount": transaction.amount,
        "currency": transaction.currency,
        "transaction_timestamp": transaction.transaction_timestamp.isoformat(),
        "source_system": transaction.source_system,
        "destination_system": transaction.destination_system,
        "reference_id": transaction.reference_id,
    }

    return Event(
        event_type=EVENT_TYPE_TRANSACTION_RECEIVED,
        source=source,
        payload=payload,
        correlation_id=correlation_id or str(uuid.uuid4()),
    )
