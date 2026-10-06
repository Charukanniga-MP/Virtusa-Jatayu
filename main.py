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
import uuid
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
from orchestrator.event_audit import EventAuditTrail
from orchestrator.event_orchestrator import (
    Event,
    EventOrchestrator,
    placeholder_fraud_handler,
    placeholder_reconciliation_handler,
    transaction_event_handler,
)
from orchestrator.processing_pipeline import ProcessingPipeline
from orchestrator.workflow_state import WorkflowTracker
from agents.agent_coordinator import AgentCoordinator
from agents.decision_agent.decision_agent import DecisionAgent
from agents.fraud_agent.fraud_agent import FraudDetectionAgent
from agents.policy_agent.policy_agent import PolicyAgent
from agents.reconciliation_agent.reconciliation_agent import ReconciliationAgent
from database.database import (
    get_connection,
    get_decision_metrics,
    get_decision_result_by_transaction_id,
    get_transaction_by_id,
    init_db,
)
from fraud_detection.fraud_service import FraudService
from reconciliation.engine import ReconciliationEngine
from reconciliation.ledger_repository import LedgerRepository
from reconciliation.models import Transaction
from reconciliation.reconciliation_service import ReconciliationService
from reconciliation.transaction_service import TransactionService
from security import (
    AuditIntegrityService,
    get_audit_integrity_service,
    get_security_gateway,
    get_security_metrics_store,
    mask_account_id,
    sanitize_payload,
)

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

# 3. Global Orchestrator, Tracker, Audit, Reconciliation, and Pipeline Singletons
workflow_tracker = WorkflowTracker()
audit_trail = EventAuditTrail()
# Day 8: Cryptographic Audit Integrity Service
audit_integrity_service = get_audit_integrity_service(db_path=os.getenv("DATABASE_PATH", "zt_ledgerguard.db"))
audit_trail.set_integrity_service(audit_integrity_service)
security_gateway = get_security_gateway()
security_metrics_store = get_security_metrics_store()

orchestrator = EventOrchestrator(
    workflow_tracker=workflow_tracker,
    audit_trail=audit_trail,
)
publisher = LocalEventPublisher(orchestrator)
transaction_service = TransactionService(publisher=publisher)
ledger_repository = LedgerRepository()
reconciliation_engine = ReconciliationEngine()
reconciliation_service = ReconciliationService(
    repository=ledger_repository,
    engine=reconciliation_engine,
    publisher=publisher,
    workflow_tracker=workflow_tracker,
)
fraud_agent = FraudDetectionAgent()
fraud_service = FraudService(
    agent=fraud_agent,
    publisher=publisher,
    workflow_tracker=workflow_tracker,
)

# Day 7: Multi-Agent Consensus Singletons
reconciliation_agent = ReconciliationAgent(service=reconciliation_service)
policy_agent = PolicyAgent()
decision_agent = DecisionAgent()
agent_coordinator = AgentCoordinator(
    reconciliation_agent=reconciliation_agent,
    fraud_agent=fraud_agent,
    policy_agent=policy_agent,
    decision_agent=decision_agent,
    publisher=publisher,
    workflow_tracker=workflow_tracker,
)

pipeline = ProcessingPipeline(
    orchestrator=orchestrator,
    workflow_tracker=workflow_tracker,
    reconciliation_service=reconciliation_service,
    fraud_service=fraud_service,
    agent_coordinator=agent_coordinator,
)


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


class EventProcessRequest(BaseModel):
    """Request model for manual event ingestion and processing."""

    event_type: str
    source: str = "API"
    payload: Dict[str, Any] = Field(default_factory=dict)
    event_id: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


class ReconciliationRunRequest(BaseModel):
    """Request model for invoking ledger reconciliation on a transaction."""

    transaction_id: str = Field(..., description="Target transaction ID to reconcile against the ledger")


class CandidateDiscoveryRequest(BaseModel):
    """Request model for fuzzy candidate discovery and ranking."""

    transaction_id: str = Field(..., description="Target transaction ID to discover candidate ledger entries for")


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
    version="0.3.0",
    description="Zero-Trust AI Ledger Reconciliation & Real-Time Fraud Triage Platform (Day 3 Orchestration Pipeline)",
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
    "/workflows/{correlation_id}",
    tags=["Workflows"],
    summary="Get Workflow State",
    status_code=status.HTTP_200_OK,
)
def get_workflow_state(correlation_id: str) -> Dict[str, Any]:
    """Retrieve current execution state and stage breakdown for a correlation ID."""
    workflow = workflow_tracker.get_workflow(correlation_id)
    if not workflow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workflow not found for correlation_id: {correlation_id}",
        )
    return {
        "correlation_id": workflow.correlation_id,
        "status": workflow.status,
        "stages": workflow.stages,
        "error": workflow.error,
        "updated_at": workflow.updated_at.isoformat(),
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


@app.get(
    "/events/{correlation_id}",
    tags=["Audit"],
    summary="Get Event Audit History",
    status_code=status.HTTP_200_OK,
)
def get_workflow_event_history(correlation_id: str) -> Dict[str, Any]:
    """Retrieve chronological audit trail of all events for a given correlation ID."""
    history = audit_trail.get_event_history(correlation_id)
    return {
        "correlation_id": correlation_id,
        "event_count": len(history),
        "events": history,
    }


@app.post(
    "/events/process",
    tags=["Events"],
    summary="Ingest and Route Event",
    status_code=status.HTTP_200_OK,
)
def process_event_endpoint(request: EventProcessRequest) -> Dict[str, Any]:
    """Accept an arbitrary domain event and dispatch through orchestrator pipeline."""
    event = Event(
        event_type=request.event_type,
        source=request.source,
        payload=request.payload,
        event_id=request.event_id or str(uuid.uuid4()),
        correlation_id=request.correlation_id or str(uuid.uuid4()),
        causation_id=request.causation_id,
    )
    orchestrator.publish(event)
    return {
        "status": "processed",
        "event_id": event.event_id,
        "correlation_id": event.correlation_id,
    }


@app.post(
    "/reconciliation/run",
    tags=["Reconciliation"],
    summary="Run Deterministic Ledger Reconciliation",
    status_code=status.HTTP_200_OK,
)
def run_reconciliation_endpoint(request: ReconciliationRunRequest) -> Dict[str, Any]:
    """Execute deterministic ledger reconciliation for a transaction."""
    result = reconciliation_service.reconcile_by_transaction_id(request.transaction_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transaction '{request.transaction_id}' not found.",
        )
    return result.model_dump()


@app.post(
    "/reconciliation/candidates",
    tags=["Reconciliation"],
    summary="Discover and Rank Ledger Candidates",
    status_code=status.HTTP_200_OK,
)
def discover_candidates_endpoint(request: CandidateDiscoveryRequest) -> Dict[str, Any]:
    """Execute fuzzy candidate discovery and ranking for a transaction."""
    discovery = reconciliation_service.discover_candidates_by_transaction_id(request.transaction_id)
    if discovery is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transaction '{request.transaction_id}' not found.",
        )
    return discovery.model_dump()


@app.get(
    "/reconciliation/candidates/{transaction_id}",
    tags=["Reconciliation"],
    summary="Get Ranked Candidates for a Transaction",
    status_code=status.HTTP_200_OK,
)
def get_candidates_endpoint(transaction_id: str) -> Dict[str, Any]:
    """Retrieve ranked candidate ledger entries and explainable similarity evidence."""
    discovery = reconciliation_service.discover_candidates_by_transaction_id(transaction_id)
    if discovery is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transaction '{transaction_id}' not found.",
        )
    return discovery.model_dump()


@app.get(
    "/reconciliation/metrics",
    tags=["Reconciliation"],
    summary="Get Ledger Reconciliation Metrics",
    status_code=status.HTTP_200_OK,
)
def get_reconciliation_metrics_endpoint() -> Dict[str, Any]:
    """Retrieve aggregate reconciliation statistics."""
    return reconciliation_service.get_metrics()


@app.get(
    "/reconciliation/{reconciliation_id}",
    tags=["Reconciliation"],
    summary="Get Reconciliation Result",
    status_code=status.HTTP_200_OK,
)
def get_reconciliation_result_endpoint(reconciliation_id: str) -> Dict[str, Any]:
    """Retrieve explainable reconciliation result and evidence by reconciliation ID."""
    result = ledger_repository.get_reconciliation_result(reconciliation_id)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Reconciliation result '{reconciliation_id}' not found.",
        )
    return result.model_dump()


class FraudAnalysisRequest(BaseModel):
    """Request model for on-demand fraud analysis of a transaction."""

    transaction_id: str = Field(description="Unique transaction ID to evaluate for fraud.")


@app.post(
    "/fraud/analyze",
    tags=["Fraud Triage"],
    summary="Analyze Transaction Fraud Risk",
    status_code=status.HTTP_200_OK,
)
def analyze_transaction_endpoint(request: FraudAnalysisRequest) -> Dict[str, Any]:
    """Execute deterministic fraud triage and risk scoring for a transaction."""
    result = fraud_service.analyze_by_transaction_id(request.transaction_id)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transaction '{request.transaction_id}' not found for fraud analysis.",
        )
    return {
        "transaction_id": result.transaction_id,
        "fraud_score": result.fraud_score,
        "risk_level": result.risk_level.value,
        "status": result.status.value,
        "requires_manual_review": result.requires_manual_review,
        "block_recommended": result.block_recommended,
        "indicators": result.indicators,
        "evidence": [e.model_dump() for e in result.evidence],
    }


@app.get(
    "/fraud/metrics",
    tags=["Fraud Triage"],
    summary="Get Fraud Triage Metrics",
    status_code=status.HTTP_200_OK,
)
def get_fraud_metrics_endpoint() -> Dict[str, Any]:
    """Retrieve aggregate fraud assessment and triage metrics."""
    return fraud_service.get_metrics()


@app.get(
    "/fraud/{transaction_id}",
    tags=["Fraud Triage"],
    summary="Get Fraud Analysis Result",
    status_code=status.HTTP_200_OK,
)
def get_fraud_analysis_endpoint(transaction_id: str) -> Dict[str, Any]:
    """Retrieve stored fraud analysis and explainable evidence by transaction ID."""
    analysis = fraud_service.get_fraud_analysis(transaction_id)
    if not analysis:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Fraud analysis for transaction '{transaction_id}' not found.",
        )
    return analysis


# ==============================================================================
# Multi-Agent Coordination & Consensus Endpoints (Day 7)
# ==============================================================================

class DecisionAnalyzeRequest(BaseModel):
    """Request model for on-demand multi-agent consensus decision."""

    transaction_id: str = Field(description="Unique transaction ID to evaluate across multi-agent consensus.")


@app.post(
    "/decision/analyze",
    tags=["Multi-Agent Consensus"],
    summary="Run Multi-Agent Decision Consensus",
    status_code=status.HTTP_200_OK,
)
def analyze_decision_endpoint(request: DecisionAnalyzeRequest) -> Dict[str, Any]:
    """Execute independent specialized agents and combine into authoritative consensus disposition."""
    with get_connection(agent_coordinator.db_path) as conn:
        row = get_transaction_by_id(conn, request.transaction_id)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transaction '{request.transaction_id}' not found for decision analysis.",
        )
    tx = Transaction(**row)
    result = agent_coordinator.coordinate_decision(tx)
    return result.model_dump()



@app.get(
    "/decision/metrics",
    tags=["Multi-Agent Consensus"],
    summary="Get Multi-Agent Consensus Metrics",
    status_code=status.HTTP_200_OK,
)
def get_decision_metrics_endpoint() -> Dict[str, Any]:
    """Retrieve aggregate multi-agent consensus decisions and agreement metrics."""
    with get_connection(agent_coordinator.db_path) as conn:
        return get_decision_metrics(conn)


@app.get(
    "/decision/{transaction_id}",
    tags=["Multi-Agent Consensus"],
    summary="Get Multi-Agent Decision Result",
    status_code=status.HTTP_200_OK,
)
def get_decision_result_endpoint(transaction_id: str) -> Dict[str, Any]:
    """Retrieve stored multi-agent consensus disposition and explainable audit trail."""
    with get_connection(agent_coordinator.db_path) as conn:
        decision = get_decision_result_by_transaction_id(conn, transaction_id)
    if not decision:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Decision for transaction '{transaction_id}' not found.",
        )
    return decision


# ==============================================================================
# Day 8: Zero-Trust Security & Audit Verification Endpoints
# ==============================================================================

@app.get(
    "/security/metrics",
    tags=["Security & Integrity"],
    summary="Get Zero-Trust Security Metrics",
    status_code=status.HTTP_200_OK,
)
def get_security_metrics_endpoint() -> Dict[str, Any]:
    """Retrieve telemetry metrics for Zero-Trust validation, sanitization, and integrity checks."""
    return security_metrics_store.get_metrics()


@app.get(
    "/security/audit/verify",
    tags=["Security & Integrity"],
    summary="Verify Tamper-Evident Audit Hash Chain",
    status_code=status.HTTP_200_OK,
)
def verify_audit_chain_endpoint() -> Dict[str, Any]:
    """Execute end-to-end cryptographic verification of the append-only audit ledger."""
    result = audit_integrity_service.verify_chain()
    return result.model_dump()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)


