"""Main Application Entry Point for ZT-LedgerGuard.

Day 1 Foundation:
- Minimal FastAPI service
- Health check endpoint (/health)
- Transaction validation endpoint (/transactions/validate)
- Structured logging with Zero-Trust sensitive data masking
- Safe environment variable loading
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from data.transaction_generator import TransactionGenerator
from database.database import init_db
from kafka.local_event_publisher import (
    EVENT_TYPE_TRANSACTION_RECEIVED,
    LocalEventPublisher,
)
from orchestrator.event_orchestrator import (
    Event,
    EventOrchestrator,
    placeholder_fraud_handler,
    placeholder_reconciliation_handler,
    transaction_event_handler,
)
from reconciliation.models import Transaction
from reconciliation.transaction_service import TransactionService
from security.security_utils import mask_account_id

# 1. Environment Loading
load_dotenv()

APP_NAME = os.getenv("APP_NAME", "ZT-LedgerGuard")
APP_ENV = os.getenv("APP_ENV", "development")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# 2. Structured Logging Configuration (timestamp, level, module, message)
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("main")

# 3. Global Orchestrator & Publisher Instances
orchestrator = EventOrchestrator()
publisher = LocalEventPublisher(orchestrator)
transaction_service = TransactionService(publisher=publisher)


class SimulationRequest(BaseModel):
    """Request model for synthetic transaction stream simulation."""

    count: int = Field(
        default=10,
        ge=1,
        le=1000,
        description="Number of synthetic transactions to simulate (1 to 1000).",
    )
    seed: Optional[int] = Field(
        default=None,
        description="Optional seed for deterministic reproducibility.",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management: initialize DB and register handlers."""
    logger.info("Starting %s in %s mode...", APP_NAME, APP_ENV)

    # Initialize SQLite database schema
    init_db()
    logger.info("Database schema verified.")

    # Register baseline handlers
    orchestrator.register_handler("TRANSACTION_INGESTED", placeholder_reconciliation_handler)
    orchestrator.register_handler("TRANSACTION_INGESTED", placeholder_fraud_handler)
    orchestrator.register_handler(EVENT_TYPE_TRANSACTION_RECEIVED, transaction_event_handler)
    logger.info("Event orchestrator initialized with baseline handlers.")

    yield

    logger.info("Shutting down %s...", APP_NAME)


# 4. FastAPI Application Initialization
app = FastAPI(
    title=APP_NAME,
    version="0.2.0",
    description="Zero-Trust AI Ledger Reconciliation & Real-Time Fraud Triage Platform (Day 2 Streaming Foundation)",
    lifespan=lifespan,
)


@app.get(
    "/health",
    tags=["System"],
    summary="Service Health Check",
    status_code=status.HTTP_200_OK,
)
def health_check() -> Dict[str, str]:
    """Return current service health status."""
    return {
        "status": "healthy",
        "service": APP_NAME,
    }


@app.post(
    "/transactions/validate",
    tags=["Transactions"],
    summary="Validate Transaction Schema",
    status_code=status.HTTP_200_OK,
)
def validate_transaction(transaction: Transaction) -> Dict[str, Any]:
    """Validate an incoming transaction against core Pydantic data rules (Day 1 baseline)."""
    masked_account = mask_account_id(transaction.account_id)
    logger.info(
        "Validated transaction %s for account %s (amount: %.2f %s)",
        transaction.transaction_id,
        masked_account,
        transaction.amount,
        transaction.currency,
    )

    # Dispatch ingestion event through orchestrator
    event = Event(
        event_type="TRANSACTION_INGESTED",
        source="api.transactions.validate",
        payload={
            "transaction_id": transaction.transaction_id,
            "masked_account_id": masked_account,
            "amount": transaction.amount,
            "currency": transaction.currency,
        },
    )
    orchestrator.publish(event)

    return {
        "status": "valid",
        "transaction_id": transaction.transaction_id,
        "message": "Transaction passed schema and validation checks.",
    }


@app.post(
    "/transactions",
    tags=["Transactions"],
    summary="Ingest and Publish Transaction",
    status_code=status.HTTP_200_OK,
)
def ingest_transaction(transaction: Transaction) -> Dict[str, Any]:
    """Ingest a validated transaction: saves to SQLite, creates event, and publishes to orchestrator."""
    try:
        result = transaction_service.process_transaction(transaction)
        return {
            "status": result["status"],
            "transaction_id": result["transaction_id"],
            "event_type": result["event_type"],
        }
    except Exception as exc:
        logger.error("Failed to process transaction %s: %s", transaction.transaction_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Transaction processing failure.",
        )


@app.post(
    "/transactions/simulate",
    tags=["Simulation"],
    summary="Simulate Synthetic Transaction Stream",
    status_code=status.HTTP_200_OK,
)
def simulate_transactions(request: SimulationRequest) -> Dict[str, Any]:
    """Generate and ingest a stream of synthetic transactions with bounded limits (1-1000)."""
    if request.count < 1 or request.count > 1000:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Count must be between 1 and 1000.",
        )

    generator = TransactionGenerator(seed=request.seed, auto_sequence=True)
    transactions = generator.generate_transactions(count=request.count)

    summary = transaction_service.process_batch(transactions)

    return {
        "status": summary["status"],
        "generated": summary["generated"],
        "accepted": summary["accepted"],
        "rejected": summary["rejected"],
    }


@app.get(
    "/events/metrics",
    tags=["Metrics"],
    summary="Get Event Orchestration Metrics",
    status_code=status.HTTP_200_OK,
)
def get_event_metrics() -> Dict[str, int]:
    """Return live execution metrics from the event orchestrator."""
    return orchestrator.get_metrics()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)

