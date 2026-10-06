"""Event Handler Interface and Base Implementations.

Defines the contract for decoupled domain event handlers.
Supports both object-oriented EventHandler implementations and callable functional handlers.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, Union
import logging

from orchestrator.event_types import EventType
from security.security_utils import mask_account_id

logger = logging.getLogger("orchestrator.handlers")


class BaseEventHandler(ABC):
    """Abstract interface for decoupled event handlers."""

    @property
    def name(self) -> str:
        """Name of the handler for logging and telemetry."""
        return self.__class__.__name__

    @abstractmethod
    def can_handle(self, event: Any) -> bool:
        """Determine whether this handler is capable of processing the given event.

        Args:
            event: Event instance to evaluate.

        Returns:
            True if the handler can process this event, False otherwise.
        """
        pass

    @abstractmethod
    def handle(self, event: Any) -> Any:
        """Process the event and return the execution result.

        Args:
            event: Event instance to process.

        Returns:
            Handler-specific execution result or acknowledgment.
        """
        pass


class FunctionalEventHandler(BaseEventHandler):
    """Adapter wrapping a standard Python callable into the BaseEventHandler interface."""

    def __init__(
        self,
        func: Callable[[Any], Any],
        target_event_type: Optional[str] = None,
        name: Optional[str] = None,
    ) -> None:
        self._func = func
        self._target_event_type = target_event_type
        self._name = name or getattr(func, "__name__", "functional_handler")

    @property
    def name(self) -> str:
        return self._name

    def can_handle(self, event: Any) -> bool:
        if self._target_event_type is None:
            return True
        return str(event.event_type) == str(self._target_event_type)

    def handle(self, event: Any) -> Any:
        return self._func(event)


# ==============================================================================
# Placeholder Handlers for Day 3 Orchestration Pipeline
# ==============================================================================

class ValidationHandler(BaseEventHandler):
    """Day 3 Placeholder: validates transaction payload and triggers downstream checks."""

    def can_handle(self, event: Any) -> bool:
        return str(event.event_type) == EventType.TRANSACTION_RECEIVED.value

    def handle(self, event: Any) -> Dict[str, Any]:
        tx_id = event.payload.get("transaction_id", "UNKNOWN")
        masked_acc = event.payload.get("masked_account_id", "********0000")
        logger.info("[ValidationHandler] Validating transaction %s for %s", tx_id, masked_acc)
        return {
            "stage": "VALIDATION",
            "status": "COMPLETED",
            "transaction_id": tx_id,
            "correlation_id": event.correlation_id,
        }


class ReconciliationRequestHandler(BaseEventHandler):
    """Day 3 Placeholder: prepares ledger matching request."""

    def can_handle(self, event: Any) -> bool:
        return str(event.event_type) in (
            EventType.TRANSACTION_VALIDATED.value,
            EventType.TRANSACTION_RECONCILIATION_REQUESTED.value,
        )

    def handle(self, event: Any) -> Dict[str, Any]:
        tx_id = event.payload.get("transaction_id", "UNKNOWN")
        logger.info("[ReconciliationRequestHandler] Requesting reconciliation for %s", tx_id)
        return {
            "stage": "RECONCILIATION",
            "status": "COMPLETED",
            "reconciled": True,
            "transaction_id": tx_id,
            "correlation_id": event.correlation_id,
        }


class FraudAnalysisRequestHandler(BaseEventHandler):
    """Day 3 Placeholder: requests fraud anomaly triage."""

    def can_handle(self, event: Any) -> bool:
        return str(event.event_type) in (
            EventType.TRANSACTION_VALIDATED.value,
            EventType.FRAUD_ANALYSIS_REQUESTED.value,
        )

    def handle(self, event: Any) -> Dict[str, Any]:
        tx_id = event.payload.get("transaction_id", "UNKNOWN")
        logger.info("[FraudAnalysisRequestHandler] Requesting fraud triage for %s", tx_id)
        return {
            "stage": "FRAUD_ANALYSIS",
            "status": "COMPLETED",
            "risk_score": 0.05,
            "flagged": False,
            "transaction_id": tx_id,
            "correlation_id": event.correlation_id,
        }


class RiskAnalysisRequestHandler(BaseEventHandler):
    """Day 3 Placeholder: evaluates multi-agent composite risk."""

    def can_handle(self, event: Any) -> bool:
        return str(event.event_type) in (
            EventType.FRAUD_ANALYSIS_COMPLETED.value,
            EventType.RISK_ANALYSIS_REQUESTED.value,
        )

    def handle(self, event: Any) -> Dict[str, Any]:
        tx_id = event.payload.get("transaction_id", "UNKNOWN")
        logger.info("[RiskAnalysisRequestHandler] Evaluating aggregate risk for %s", tx_id)
        return {
            "stage": "RISK_ANALYSIS",
            "status": "COMPLETED",
            "overall_risk": "LOW",
            "transaction_id": tx_id,
            "correlation_id": event.correlation_id,
        }


class FinalDecisionHandler(BaseEventHandler):
    """Day 3 Placeholder: determines final transaction disposition."""

    def can_handle(self, event: Any) -> bool:
        return str(event.event_type) in (
            EventType.RISK_ANALYSIS_COMPLETED.value,
            EventType.TRANSACTION_APPROVED.value,
        )

    def handle(self, event: Any) -> Dict[str, Any]:
        tx_id = event.payload.get("transaction_id", "UNKNOWN")
        logger.info("[FinalDecisionHandler] Approving transaction %s", tx_id)
        return {
            "stage": "FINAL_DECISION",
            "disposition": "APPROVED",
            "transaction_id": tx_id,
            "correlation_id": event.correlation_id,
        }
