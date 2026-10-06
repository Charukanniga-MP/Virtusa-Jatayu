# ZT-LedgerGuard

> **Zero-Trust AI Ledger Reconciliation & Real-Time Fraud Triage Platform**  
> *Targeted at Virtusa Jatayu Season 6 — Forward Deployment Engineering (FDE)*  
> *Domain: FinTech / Banking Systems*

---

## The Banking Problem

Modern enterprise banking and payment infrastructures face fundamental operational challenges:

1. **Multi-Rail Ingestion Discrepancies:** Financial transactions arrive continuously across fragmented channels (ACH, Fedwire, SWIFT, SEPA, real-time card rails, and mobile APIs).
2. **Ledger Timing & Mismatch Gaps:** General ledger accounting entries often post asynchronously, creating timing deltas, out-of-order batches, and reconciliation friction.
3. **Rigid Rule Engines Miss Complex Anomalies:** Legacy fraud detection relies predominantly on static rule trees that fail to detect multi-stage velocity attacks, account takeovers, and cross-channel anomalies.
4. **Irreplaceable Legacy Core-Banking Systems:** Critical core-banking mainframes and legacy batch architectures cannot be ripped and replaced overnight without catastrophic operational risk.
5. **Stringent Security, Privacy, and Audit Requirements:** Strict compliance mandates (PCI-DSS, GDPR, SOX) demand zero-trust least privilege, masking of sensitive personal and financial identifiers, and immutable auditability.

---

## Proposed Solution

**ZT-LedgerGuard** is an enterprise-grade platform designed to solve these challenges through:

- **Event-Driven Transaction Processing:** High-throughput decoupled streaming of all financial interactions.
- **AI-Assisted Ledger Reconciliation:** Exact double-entry matching coupled with intelligent fuzzy algorithms for out-of-order posting.
- **Real-Time Fraud Triage:** Low-latency multi-feature scoring and anomaly categorization.
- **Multi-Agent Coordination:** Collaborative consensus between Reconciliation, Fraud, and Risk agents to determine transaction dispositions.
- **Zero-Trust Communication:** Strict least-privilege boundaries, cryptographic ledger verification, and tokenized data access.
- **Sensitive-Data Protection:** Automatic masking of account numbers and PII across logs, telemetry, and distributed traces.
- **Legacy-System Integration & Shadow Deployment:** Non-disruptive dual-run shadow mode validating AI agent decisions against legacy host outputs.
- **Self-Healing Capabilities:** Automated resolution workflows and proposed reversals for transient accounting mismatches.

> **Note:** The advanced features above (Kafka clustering, machine learning fraud scoring, multi-agent consensus, differential privacy, and shadow mode comparison) are planned milestones across the 15-day development roadmap and are **not** fully implemented on Day 1.

---

## Current Status: Day 1, Day 2 & Day 3 Completed (63/63 Tests Passing)

### Day 1 — Foundation & Architecture
- **Project Architecture & Layout:** Modular package structure with isolated agent directories, orchestrator, legacy simulator, and database layers.
- **Core Domain Models:** Robust Pydantic models for `Transaction`, `LedgerEntry`, and `Event` with strict validation (positive amounts, ISO 3-letter currency codes, non-empty fields).
- **In-Memory Event Orchestrator:** Local event router supporting dynamic handler registration, decoupled publishing, and graceful handling of unknown event types.
- **SQLite Database Foundation:** Lightweight, safe database initialization with idempotent schema creation and parameterized queries.
- **Security & Data Hygiene Foundation:** Zero-trust masking utilities (`mask_account_id`, `mask_sensitive_value`, `sanitize_log_message`) ensuring account identifiers are masked in application logs.
- **Legacy Banking Simulator:** Deterministic host simulator (`LegacyBankingSystem`) providing baseline approval (`00`) and limit/restriction rejection codes (`51`/`05`).
- **FastAPI Core Service:** Minimal web service providing `/health` and `/transactions/validate` endpoints with structured logging.

### Day 2 — Transaction Streaming Foundation
- **Synthetic Transaction Generation:** Deterministic, reproducible stream generator (`TransactionGenerator`) with seed support, multi-rail types (`TRANSFER`, `PAYMENT`, `CARD_PAYMENT`, `WITHDRAWAL`, `DEPOSIT`, `INTERNATIONAL_TRANSFER`), and controlled currencies (`USD`, `EUR`, `GBP`, `INR`).
- **Local Event Streaming:** Decoupled `EventPublisher` abstract base class and `LocalEventPublisher` implementing in-memory event dispatching to `EventOrchestrator`.
- **Kafka-Ready Architecture:** Designed for seamless drop-in replacement with `KafkaEventPublisher` in Days 3+ without modifying generator, service, or agent business logic.
- **Transaction Ingestion Service:** `TransactionService` coordinating schema validation, idempotent SQLite persistence, canonical `TRANSACTION_RECEIVED` event creation, and publisher emission.
- **REST Endpoints:** `POST /transactions`, `POST /transactions/simulate`, `GET /events/metrics`.
- **CLI Simulation Utility:** `scripts/run_transaction_stream.py` for terminal-based batch/stream transaction generation.

### Day 3 — Event-Driven Orchestration & Processing Pipeline
- **Centralized Event Types:** Strongly typed `EventType` enum in `orchestrator/event_types.py` defining canonical event transitions (`TRANSACTION_RECEIVED`, `TRANSACTION_VALIDATED`, `TRANSACTION_RECONCILIATION_REQUESTED`, `TRANSACTION_RECONCILED`, `FRAUD_ANALYSIS_REQUESTED`, `FRAUD_ANALYSIS_COMPLETED`, `RISK_ANALYSIS_REQUESTED`, `RISK_ANALYSIS_COMPLETED`, `TRANSACTION_APPROVED`, `TRANSACTION_FLAGGED`, `TRANSACTION_REJECTED`, `PROCESSING_FAILED`).
- **Enhanced Event Envelope:** Domain `Event` model containing `event_id`, `event_type`, `correlation_id`, `causation_id`, `source`, `timestamp`, `retry_count`, and `payload`.
- **Handler Interface & Dynamic Registry:** Extensible handler pattern (`BaseEventHandler`, `FunctionalEventHandler`) with `register_handler`, `unregister_handler`, multi-handler execution per event type, and support for both synchronous and asynchronous coroutines without blocking.
- **Decoupled Processing Pipeline:** `ProcessingPipeline` in `orchestrator/processing_pipeline.py` chaining the banking event flow from ingestion through validation, reconciliation request, fraud analysis request, risk analysis request, to final disposition decision without hardcoding intelligence.
- **Workflow State Tracking:** Thread-safe `WorkflowTracker` in `orchestrator/workflow_state.py` tracking end-to-end lifecycle per `correlation_id` across distinct stage states (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`).
- **Event Idempotency:** Guaranteed deduplication tracking processed event IDs (`_processed_event_ids`); identical event replays are recorded as `DUPLICATE` without re-executing handlers.
- **Bounded Retries & Failure Handling:** Automatic retry loop bounded by `MAX_EVENT_RETRIES` (default: 3). Exceptions are caught gracefully, sanitized to prevent sensitive data leakage, logged to the audit trail, and workflows marked as `FAILED` without crashing the application.
- **Financial Event Audit Trail:** `EventAuditTrail` in `orchestrator/event_audit.py` maintaining in-memory chronological transition records with querying via `get_event_history(correlation_id)`.
- **REST Endpoints Added:**
  - `GET /workflows/{correlation_id}`: Query workflow state, active stage progression, and status.
  - `GET /events/{correlation_id}`: Retrieve chronological audit history for a given correlation ID.
  - `POST /events/process`: Manual domain event ingestion and orchestrator routing.
  - Extended `GET /events/metrics`: Live metrics for `events_received`, `events_processed`, `events_failed`, `events_retried`, `events_duplicate`, `workflows_started`, `workflows_completed`, `workflows_failed`.
- **Automated Test Suite:** 63/63 passing tests (18 Day 1 + 20 Day 2 + 25 Day 3).
- **100-Transaction Benchmark:** 100 transactions processed in 0.23 seconds (3,944 events/sec) with 100% workflow completion, 0 failures, and 5 duplicates caught and ignored.

---

## Technology Stack

Minimal, lightweight libraries verified on Windows without Docker or external dependencies:

- **Python 3.10+** (Windows-native execution)
- **FastAPI** (Minimal REST API framework)
- **Uvicorn** (ASGI server)
- **Pydantic v2** (Strict schema validation and data modeling)
- **python-dotenv** (Safe environment variable management)
- **SQLite3** (Standard library embedded relational database with parameterized queries)
- **Pandas** (Tabular data support foundation)
- **pytest** & **HTTPX** (Automated test runner and API client)

*Docker, Kubernetes, and Apache Kafka are deliberately NOT required for Day 1 or Day 2.*


---

## Running the Project Locally (Windows)

All instructions below are tested and verified for **Windows PowerShell** or **Command Prompt**.

### 1. Create Python Virtual Environment
```powershell
python -m venv .venv
```

### 2. Activate Virtual Environment
```powershell
# PowerShell
.\.venv\Scripts\Activate.ps1

# Or Command Prompt (cmd.exe)
.\.venv\Scripts\activate.bat
```

### 3. Install Dependencies
```powershell
pip install -r requirements.txt
```

### 4. Configure Environment
```powershell
Copy-Item .env.example .env
```

### 5. Run the Automated Test Suite
```powershell
pytest -v
```

### 6. Start the FastAPI Application
```powershell
python main.py
```
Or with Uvicorn CLI:
```powershell
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

### 7. Test API Endpoints

**Health Check:**
```powershell
curl http://127.0.0.1:8000/health
```
Response:
```json
{
  "status": "healthy",
  "service": "ZT-LedgerGuard"
}
```

**Validate Transaction:**
```powershell
curl -X POST http://127.0.0.1:8000/transactions/validate `
  -H "Content-Type: application/json" `
  -d '{\"transaction_id\": \"TX-10001\", \"account_id\": \"123456789012\", \"transaction_type\": \"TRANSFER\", \"amount\": 2500.50, \"currency\": \"USD\", \"transaction_timestamp\": \"2026-10-06T10:30:00Z\", \"source_system\": \"CORE_API\", \"destination_system\": \"ACH_GATEWAY\"}'
```
**Ingest and Stream Transaction (Day 2):**
```powershell
curl -X POST http://127.0.0.1:8000/transactions `
  -H "Content-Type: application/json" `
  -d '{\"transaction_id\": \"TXN-000101\", \"account_id\": \"ACC-100012\", \"transaction_type\": \"PAYMENT\", \"amount\": 150.00, \"currency\": \"USD\", \"transaction_timestamp\": \"2026-10-06T12:00:00Z\", \"source_system\": \"MOBILE_APP\", \"destination_system\": \"ACH_NETWORK\"}'
```
Response:
```json
{
  "status": "accepted",
  "transaction_id": "TXN-000101",
  "event_type": "TRANSACTION_RECEIVED"
}
```

**Simulate Synthetic Stream (Day 2):**
```powershell
curl -X POST http://127.0.0.1:8000/transactions/simulate `
  -H "Content-Type: application/json" `
  -d '{\"count\": 10, \"seed\": 42}'
```
Response:
```json
{
  "status": "completed",
  "generated": 10,
  "accepted": 10,
  "rejected": 0
}
```

**Query Live Event Metrics (Day 2):**
```powershell
curl http://127.0.0.1:8000/events/metrics
```
Response:
```json
{
  "events_published": 10,
  "events_handled": 10,
  "events_rejected": 0,
  "unknown_events": 0,
  "processing_errors": 0
}
```

### 8. Run CLI Transaction Stream Simulation
```powershell
python scripts/run_transaction_stream.py --count 20 --seed 42
```
Output:
```text
ZT-LedgerGuard Transaction Stream
---------------------------------
Generated: 20
Accepted: 20
Rejected: 0
Events Published: 20
Events Handled: 20
---------------------------------
```

```

---

## Day 3 — Event-Driven Orchestration

Day 3 introduces a production-style, decoupled event orchestration pipeline with correlation tracing, parent causation lineage, bounded retries, idempotent deduplication, and financial auditability.

```text
Transaction Ingestion (API / Generator)
               │
               ▼
      TRANSACTION_RECEIVED ──[Validation Handler]──► Workflow State: PROCESSING
               │
               ▼
      TRANSACTION_VALIDATED ──[Coordinator]
         ┌─────┴────────────────┐
         ▼                      ▼
TRANSACTION_RECONCILIATION   FRAUD_ANALYSIS
      _REQUESTED               _REQUESTED
         │                      │
         ▼                      ▼
TRANSACTION_RECONCILED       FRAUD_ANALYSIS_COMPLETED
                                │
                                ▼
                             RISK_ANALYSIS_REQUESTED
                                │
                                ▼
                             RISK_ANALYSIS_COMPLETED
                                │
                                ▼
                             TRANSACTION_APPROVED ──► Workflow State: COMPLETED
```

### Architecture Pillars

1. **Centralized Event Types (`orchestrator/event_types.py`):**
   - Canonical `EventType` enum guarantees zero magic strings across producers, orchestrators, and handlers:
     - `TRANSACTION_RECEIVED`, `TRANSACTION_VALIDATED`, `TRANSACTION_RECONCILIATION_REQUESTED`, `TRANSACTION_RECONCILED`
     - `FRAUD_ANALYSIS_REQUESTED`, `FRAUD_ANALYSIS_COMPLETED`, `RISK_ANALYSIS_REQUESTED`, `RISK_ANALYSIS_COMPLETED`
     - `TRANSACTION_APPROVED`, `TRANSACTION_FLAGGED`, `TRANSACTION_REJECTED`, `PROCESSING_FAILED`

2. **Event Envelope & Tracing Lineage (`orchestrator/event_orchestrator.py`):**
   - **`correlation_id`:** Globally unique token tracking the entire lifecycle of a transaction across all stages and agents.
   - **`causation_id`:** Pointer to the immediate parent `event_id` that triggered the current event, enabling distributed debugging and causality graphs.
   - **`retry_count`:** Integer tracking invocation attempts per event.
   - **`timestamp`:** UTC ISO timestamp of event creation.

3. **Handler Registry & Extensibility (`orchestrator/handlers.py`):**
   - Pluggable `BaseEventHandler` abstract interface and `FunctionalEventHandler` adapter.
   - Multi-handler support: Multiple independent handlers can bind to the same event type.
   - Non-blocking execution: Seamlessly supports both synchronous functions and asynchronous coroutines.
   - Dynamic binding via `orchestrator.register_handler()` and `orchestrator.unregister_handler()`.

4. **Workflow State Tracking (`orchestrator/workflow_state.py`):**
   - Thread-safe `WorkflowTracker` maintains end-to-end lifecycle status (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`).
   - Granular stage tracking: `validation`, `reconciliation`, `fraud_analysis`, `risk_analysis`, `final_decision`.
   - Accessible via REST endpoint `GET /workflows/{correlation_id}`.

5. **Financial Event Idempotency:**
   - In-memory event deduplication (`_processed_event_ids`).
   - Replay attacks or duplicate broker deliveries are trapped, marked as `DUPLICATE` in the audit log, and return immediately without executing handlers.

6. **Bounded Retries & Graceful Failure Handling:**
   - Configurable `MAX_EVENT_RETRIES` (default: 3).
   - Failed handler executions trigger exponential/sequential bounded retries.
   - Permanent failures transition the event to `FAILED`, record sanitized diagnostic messages in the audit trail, and transition the workflow to `FAILED` without crashing the service.

7. **Financial Event Audit Trail (`orchestrator/event_audit.py`):**
   - In-memory immutable `AuditRecord` journal logging every state transition (`RECEIVED`, `PROCESSING`, `COMPLETED`, `FAILED`, `DUPLICATE`).
   - Inspectable via `GET /events/{correlation_id}`.

### Day 3 API Endpoints

- **`GET /workflows/{correlation_id}`:** Retrieve real-time workflow status and stage breakdown.
- **`GET /events/{correlation_id}`:** Retrieve chronological audit log for a correlation ID.
- **`POST /events/process`:** Manually inject a domain event into the orchestrator pipeline.
- **`GET /events/metrics`:** Query live event and workflow counters (`events_received`, `events_processed`, `events_failed`, `events_retried`, `events_duplicate`, `workflows_started`, `workflows_completed`).

### Running the 100-Transaction Benchmark
```powershell
python scripts/benchmark_day3.py
```

---

## Directory Structure

```
ZT-LedgerGuard/
│
├── agents/
│   ├── __init__.py
│   ├── reconciliation_agent/
│   ├── fraud_agent/
│   ├── risk_agent/
│   └── privacy_agent/
│
├── orchestrator/
│   ├── __init__.py
│   ├── event_orchestrator.py       # Event bus, idempotency, bounded retries
│   ├── event_types.py              # Canonical EventType enum
│   ├── handlers.py                 # BaseEventHandler & placeholder handlers
│   ├── workflow_state.py           # Thread-safe WorkflowTracker & stages
│   ├── event_audit.py              # Immutable EventAuditTrail
│   └── processing_pipeline.py      # End-to-end transaction coordinator
│
├── kafka/
│   ├── __init__.py
│   └── local_event_publisher.py    # LocalEventPublisher & create_transaction_event
│
├── reconciliation/
│   ├── __init__.py
│   ├── models.py                   # Transaction & LedgerEntry Pydantic models
│   └── transaction_service.py      # TransactionService ingestion coordinator
│
├── fraud_detection/
│   ├── __init__.py
│   └── models.py
│
├── security/
│   ├── __init__.py
│   └── security_utils.py           # PII masking & zero-trust log sanitization
│
├── legacy_simulation/
│   ├── __init__.py
│   └── legacy_system.py
│
├── database/
│   ├── __init__.py
│   └── database.py                 # SQLite persistence & queries
│
├── data/
│   ├── __init__.py
│   └── transaction_generator.py    # Deterministic synthetic transaction generator
│
├── tests/
│   ├── __init__.py
│   ├── test_foundation.py          # Day 1 test suite (18 tests)
│   ├── test_day2.py                # Day 2 test suite (20 tests)
│   └── test_day3.py                # Day 3 test suite (25 tests)
│
├── scripts/
│   ├── __init__.py
│   ├── run_transaction_stream.py   # Day 2 streaming CLI
│   └── benchmark_day3.py           # Day 3 100-transaction benchmark
│
├── docs/
│   ├── architecture.md
│   └── development_plan.md
│
├── .env.example
├── .gitignore
├── requirements.txt
├── README.md
```

---

## Roadmap

Detailed 15-day implementation milestones are documented in [docs/development_plan.md](docs/development_plan.md).  
Architecture components and design status are detailed in [docs/architecture.md](docs/architecture.md).
