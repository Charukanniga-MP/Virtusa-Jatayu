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

## Current Status: Day 1 & Day 2 Completed

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
  > **Note: Kafka is not yet enabled.** Day 2 uses a local event publisher as the Kafka-compatible foundation without requiring Docker, Zookeeper, or external brokers.
- **Transaction Ingestion Service:** `TransactionService` coordinating schema validation, idempotent SQLite persistence, canonical `TRANSACTION_RECEIVED` event creation, and publisher emission.
- **REST Endpoints:**
  - `POST /transactions`: Single transaction ingestion and event publishing.
  - `POST /transactions/simulate`: Bounded synthetic stream simulation (1 to 1000 transactions).
  - `GET /events/metrics`: Live event execution counters (`events_published`, `events_handled`, `unknown_events`).
- **CLI Simulation Utility:** `scripts/run_transaction_stream.py` for terminal-based batch/stream transaction generation with zero-trust masked account outputs.
- **Automated Test Suite:** 38/38 passing tests (18 Day 1 foundation tests + 20 Day 2 streaming tests).

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

---


## Directory Structure

```
ZT-LedgerGuard/
│
├── agents/
│   ├── __init__.py
│   ├── reconciliation_agent/
│   │   └── __init__.py
│   ├── fraud_agent/
│   │   └── __init__.py
│   ├── risk_agent/
│   │   └── __init__.py
│   └── privacy_agent/
│       └── __init__.py
│
├── orchestrator/
│   ├── __init__.py
│   └── event_orchestrator.py
│
├── kafka/
│   └── __init__.py
│
├── reconciliation/
│   ├── __init__.py
│   └── models.py
│
├── fraud_detection/
│   ├── __init__.py
│   └── models.py
│
├── security/
│   ├── __init__.py
│   └── security_utils.py
│
├── legacy_simulation/
│   ├── __init__.py
│   └── legacy_system.py
│
├── shadow_deployment/
│   └── __init__.py
│
├── dashboard/
│   └── __init__.py
│
├── database/
│   ├── __init__.py
│   └── database.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── sample/
│
├── tests/
│   ├── __init__.py
│   └── test_foundation.py
│
├── scripts/
│   └── __init__.py
│
├── docs/
│   ├── architecture.md
│   └── development_plan.md
│
├── .env.example
├── .gitignore
├── requirements.txt
├── README.md
└── main.py
```

---

## Roadmap

Detailed 15-day implementation milestones are documented in [docs/development_plan.md](docs/development_plan.md).  
Architecture components and design status are detailed in [docs/architecture.md](docs/architecture.md).
