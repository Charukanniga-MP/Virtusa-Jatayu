"""Kafka Streaming Module.

Day 1 & Day 2 use local in-memory event dispatching abstractions.
Distributed Kafka streaming integration is scheduled for Day 3+.
"""

from kafka.local_event_publisher import (
    EventPublisher,
    LocalEventPublisher,
    create_transaction_event,
    EVENT_TYPE_TRANSACTION_RECEIVED,
)

__all__ = [
    "EventPublisher",
    "LocalEventPublisher",
    "create_transaction_event",
    "EVENT_TYPE_TRANSACTION_RECEIVED",
]

