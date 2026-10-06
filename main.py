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
from typing import Any, Dict

from dotenv import load_dotenv
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from database.database import init_db
from orchestrator.event_orchestrator import (
    Event,
    EventOrchestrator,
    placeholder_fraud_handler,
    placeholder_reconciliation_handler,
)
from reconciliation.models import Transaction
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

# 3. Global Orchestrator Instance for In-Memory Dispatching
orchestrator = EventOrchestrator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management: initialize DB and register handlers."""
    logger.info("Starting %s in %s mode...", APP_NAME, APP_ENV)

    # Initialize SQLite database schema
    init_db()
    logger.info("Database schema verified.")

    # Register foundational placeholder handlers
    orchestrator.register_handler("TRANSACTION_INGESTED", placeholder_reconciliation_handler)
    orchestrator.register_handler("TRANSACTION_INGESTED", placeholder_fraud_handler)
    logger.info("Event orchestrator initialized with baseline handlers.")

    yield

    logger.info("Shutting down %s...", APP_NAME)


# 4. FastAPI Application Initialization
app = FastAPI(
    title=APP_NAME,
    version="0.1.0",
    description="Zero-Trust AI Ledger Reconciliation & Real-Time Fraud Triage Platform (Day 1 Foundation)",
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
    """Validate an incoming transaction against core Pydantic data rules.

    Does NOT execute fraud triage or reconciliation (planned for Days 4-6).
    Enforces zero-trust logging by masking the account ID.
    """
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
