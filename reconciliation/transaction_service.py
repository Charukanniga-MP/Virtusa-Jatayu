"""Transaction Ingestion and Coordination Service.

Coordinates:
1. Transaction receiving and schema validation
2. SQLite database persistence
3. Event creation with correlation tracing
4. Decoupled event publishing through the EventPublisher abstraction

Decoupled by design: contains no fraud scoring or reconciliation business logic.
"""

import logging
from typing import Any, Dict, Iterable, List, Optional, Union
from pydantic import ValidationError

from database.database import save_transaction
from kafka.local_event_publisher import (
    EventPublisher,
    LocalEventPublisher,
    create_transaction_event,
)
from reconciliation.models import Transaction
from security.security_utils import mask_account_id

logger = logging.getLogger("reconciliation.transaction_service")


class TransactionService:
    """Service orchestrating transaction ingestion, persistence, and event emission."""

    def __init__(
        self,
        publisher: Optional[EventPublisher] = None,
        db_path: Optional[str] = None,
    ) -> None:
        """Initialize TransactionService with a publisher and database connection target.

        Args:
            publisher: EventPublisher abstraction (defaults to LocalEventPublisher).
            db_path: Optional custom SQLite database target path (e.g., for tests).
        """
        self.publisher = publisher or LocalEventPublisher()
        self.db_path = db_path

    def process_transaction(
        self,
        transaction: Union[Transaction, Dict[str, Any]],
        correlation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Ingest, validate, store, and publish a financial transaction.

        Args:
            transaction: Transaction instance or raw dict payload to validate.
            correlation_id: Optional distributed correlation trace ID.

        Returns:
            Dict containing ingestion acceptance confirmation and event metadata.

        Raises:
            ValidationError: If input dict fails Pydantic schema validation.
            Exception: If storage or event emission encounters a fatal error.
        """
        # 1. Validation
        if not isinstance(transaction, Transaction):
            tx_obj = Transaction(**transaction)
        else:
            tx_obj = transaction

        masked_acc = mask_account_id(tx_obj.account_id)
        logger.info(
            "Ingesting transaction %s for account %s (amount: %.2f %s)",
            tx_obj.transaction_id,
            masked_acc,
            tx_obj.amount,
            tx_obj.currency,
        )

        # 2. Database Persistence
        save_transaction(tx_obj, db_path=self.db_path)

        # 3. Canonical Event Creation
        event = create_transaction_event(
            transaction=tx_obj,
            source="transaction_service",
            correlation_id=correlation_id,
        )

        # 4. Decoupled Event Publishing
        self.publisher.publish(event)

        # 5. Return Clean Response
        return {
            "status": "accepted",
            "transaction_id": tx_obj.transaction_id,
            "event_type": event.event_type,
            "event_id": event.event_id,
            "correlation_id": event.correlation_id,
        }

    def process_batch(
        self,
        transactions: Iterable[Union[Transaction, Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Ingest a batch of transactions and collect summary statistics.

        Args:
            transactions: Iterable of Transaction instances or dicts.

        Returns:
            Summary dict with generated, accepted, and rejected counts.
        """
        total = 0
        accepted = 0
        rejected = 0
        errors: List[Dict[str, Any]] = []

        for item in transactions:
            total += 1
            try:
                self.process_transaction(item)
                accepted += 1
            except ValidationError as ve:
                rejected += 1
                logger.warning("Batch transaction rejected due to validation failure: %s", ve)
                errors.append({"error": str(ve)})
            except Exception as exc:
                rejected += 1
                logger.error("Batch transaction failed during processing: %s", exc)
                errors.append({"error": str(exc)})

        return {
            "status": "completed",
            "generated": total,
            "accepted": accepted,
            "rejected": rejected,
            "errors": errors if errors else None,
        }
