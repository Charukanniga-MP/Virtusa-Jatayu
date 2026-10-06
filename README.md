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

## Current Status: Days 1, 2, 3, 4, 5, 6 & 7 Completed (241/241 Tests Passing)

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
- **Centralized Event Types:** Strongly typed `EventType` enum in `orchestrator/event_types.py` defining canonical event transitions.
- **Enhanced Event Envelope:** Domain `Event` model containing `event_id`, `event_type`, `correlation_id`, `causation_id`, `source`, `timestamp`, `retry_count`, and `payload`.
- **Handler Interface & Dynamic Registry:** Extensible handler pattern (`BaseEventHandler`, `FunctionalEventHandler`) with non-blocking async execution.
- **Decoupled Processing Pipeline:** `ProcessingPipeline` in `orchestrator/processing_pipeline.py` chaining the banking event flow from ingestion through validation, reconciliation request, fraud analysis request, risk analysis request, to final disposition decision without hardcoding intelligence.
- **Workflow State Tracking:** Thread-safe `WorkflowTracker` in `orchestrator/workflow_state.py` tracking end-to-end lifecycle per `correlation_id` across distinct stage states (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`).
- **Event Idempotency & Bounded Retries:** Guaranteed deduplication tracking processed event IDs (`_processed_event_ids`) and bounded retry handling.
- **Financial Event Audit Trail:** `EventAuditTrail` in `orchestrator/event_audit.py` maintaining in-memory chronological transition records with querying via `get_event_history(correlation_id)`.

### Day 4 — Intelligent Ledger Reconciliation Engine
- **Deterministic & Explainable Financial Matching:** Zero LLM, zero ML heuristic reconciliation engine (`reconciliation/engine.py` and `reconciliation/rules.py`) comparing incoming transactions with core ledger entries.
- **7 Deterministic Rules:** Transaction ID, Reference ID, Account ID, Amount (`Decimal`), Currency (ISO 4217), Timestamp Tolerance (default 300s), and Logical Debit/Credit Direction.
- **Weighted Match Scoring:** Configurable scoring weights totaling 100% (ID/Reference: 30%, Account: 20%, Amount: 25%, Currency: 10%, Timestamp: 10%, Direction: 5%) outputting normalized score between `0.0` and `1.0`.
- **Controlled Status Classification:** Discrete statuses (`MATCHED`, `PARTIAL_MATCH`, `MISMATCHED`, `NOT_FOUND`).
- **Strict Decimal Money Comparison:** Eliminated floating-point IEEE-754 drift using Python `Decimal` with banker's rounding for currency comparisons.
- **Explainable Evidence Generation:** Every rule produces transparent human-auditable evidence dictionaries stating compared values, status, and exact delta (e.g. amount variance or timestamp delta).
- **Decoupled Repository & Generator:** `LedgerRepository` abstraction managing SQLite persistence for general ledger entries and reconciliation records. `LedgerGenerator` producing synthetic, deterministic ledger entries across exact, partial, mismatched, and missing scenarios.
- **End-to-End Event Integration:** Automated handler responding to `TRANSACTION_RECONCILIATION_REQUESTED` and emitting `TRANSACTION_RECONCILED` with correlation and causation tracking.

### Day 5 — Fuzzy Ledger Reconciliation & Candidate Matching
- **Fuzzy Candidate Discovery Layer:** Deterministic `FuzzyCandidateMatcher` ranking potential ledger entries without making authoritative financial decisions.
- **Field Normalization:** Canonical normalization (`normalize_reference`, `normalize_transaction_id`, `normalize_account_id`, `normalize_currency`) preserving sensitive account digits.
- **Hard Constraints Enforcement:** Incompatible accounts or currencies immediately trigger candidate rejection, preventing false matches.
- **Ambiguity Detection & Manual Review:** Competing candidates within `ambiguity_margin` (0.03) are classified as `AMBIGUOUS` with `requires_manual_review = True`.
- **Two-Stage Reconciliation Pipeline:** Stage 1 (Candidate Discovery) -> Stage 2 (Deterministic Verification Engine).
- **New Events & Endpoints:** `FUZZY_CANDIDATE_IDENTIFIED` event, `POST /reconciliation/candidates`, and `GET /reconciliation/candidates/{transaction_id}`.
- **10,000-Record Benchmark:** 10,000 transactions processed in 3.25 seconds (~3,073 records/second).

### Day 6 — Autonomous Deterministic Fraud Detection Agent
- **Autonomous Intelligence Layer:** Explainable, deterministic `FraudDetectionAgent` (`agents/fraud_agent/fraud_agent.py`) analyzing transactions downstream of reconciliation without LLM dependencies, Gemini, Ollama, LangChain, Kafka, or Docker.
- **7 Deterministic Feature Extractors:** Amount anomaly, transaction velocity burst (60s), rapid repeated transactions (120s), unusual transaction time (22:00–06:00), account behavior deviation, reconciliation anomaly, and repeated failed activity.
- **7 Independent Fraud Rules & Transparent Evidence:**
  - `HIGH_AMOUNT_ANOMALY` (25 pts)
  - `HIGH_TRANSACTION_VELOCITY` (25 pts)
  - `RAPID_REPEATED_TRANSACTION` (15 pts)
  - `UNUSUAL_TRANSACTION_TIME` (10 pts)
  - `ACCOUNT_BEHAVIOR_DEVIATION` (15 pts)
  - `RECONCILIATION_ANOMALY` (10 pts)
  - `REPEATED_FAILED_ACTIVITY` (10 pts)
- **Anti-Double-Counting Safeguards:** Dampens correlated ceiling spikes when amount anomalies are already triggered by the exact same value.
- **Bounded Scoring & Risk Tiers:** Risk score strictly bounded in `[0, 100]`:
  - `LOW` (0–24): Normal operations
  - `MEDIUM` (25–49): Audited baseline anomaly
  - `HIGH` (50–74): Flagged for human triage (`requires_manual_review = True`)
  - `CRITICAL` (75–100): High threat (`requires_manual_review = True`, `block_recommended = True`)
- **Safety Guarantees:** Never modifies amounts, account numbers, currencies, ledger balances, or reconciliation outcomes; never deletes transactions.
- **Event Pipeline Integration:** Handler listening for `FRAUD_ANALYSIS_REQUESTED` and emitting `FRAUD_ANALYSIS_COMPLETED` with masked account IDs.
- **Database Persistence & REST APIs:** `fraud_analyses` table, `POST /fraud/analyze`, `GET /fraud/{transaction_id}`, and `GET /fraud/metrics`.
- **Automated Test Suite:** 195/195 passing tests.
- **10,000-Record Benchmark:** 10,000 transactions processed in 2.18 seconds (~4,588 tx/second, 0.22 ms/tx).

### Day 7 — Multi-Agent Coordination & Risk Decision Consensus
- **4 Specialized Domain Agents (`agents/`):**
  - `ReconciliationAgent` (`agents/reconciliation_agent/reconciliation_agent.py`): Wraps Day 5 two-stage fuzzy/deterministic reconciliation into standardized `BaseAgent` contract.
  - `FraudDetectionAgent` (`agents/fraud_agent/fraud_agent.py`): 7-feature deterministic risk grader implementing `BaseAgent` metadata and auditable `AgentExecutionRecord`.
  - `PolicyAgent` (`agents/policy_agent/policy_agent.py`): Evaluates governance and regulatory policy rules (`CRITICAL_FRAUD_RISK`, `HIGH_FRAUD_RISK`, `RECONCILIATION_AMBIGUITY`, `RECONCILIATION_MISMATCH`, `MISSING_LEDGER_ENTRY`, `COMBINED_HIGH_RISK`, `TRANSACTION_LIMIT_BREACH`).
  - `DecisionAgent` & `ConsensusEngine` (`agents/decision_agent/decision_agent.py`): Centralized consensus arbitrator resolving conflicting agent recommendations deterministically.
- **Deterministic Consensus Precedence Hierarchy:**
  - Strict priority order: `BLOCK_RECOMMENDED > MANUAL_REVIEW > APPROVE`.
  - Consensus Rule 1: Critical fraud score ($\ge 75$) mandates `BLOCK_RECOMMENDED` overriding clean reconciliation match.
  - Consensus Rule 2: Elevated fraud score ($50 \le \text{score} < 75$) mandates `MANUAL_REVIEW`.
  - Consensus Rule 3: Ambiguous candidate match mandates `MANUAL_REVIEW`.
  - Consensus Rule 4: Missing ledger entry mandates `MANUAL_REVIEW`.
  - Consensus Rule 5: Ledger mismatch coupled with elevated fraud mandates `BLOCK_RECOMMENDED` (Compound Risk).
  - Consensus Rule 6: Matched ledger, low fraud ($<25$), clear policy yields `APPROVE`.
- **Agreement Confidence Scoring & Disagreement Detection:**
  - Confidence score strictly bounded in `[0.0, 1.0]`. Unanimous alignment scores `0.95–1.00`, inter-agent divergence scores `0.70–0.85`, and agent execution failures score `0.50–0.65`.
  - Full disagreement tracking records specific posture divergences (e.g., Reconciliation matched, but Fraud Agent detected CRITICAL threat).
- **Zero-Trust Safety Safeguards:**
  - **Fail-Safe Invariant:** `Agent Failure != Approval`. Any unhandled agent exception automatically falls back to `MANUAL_REVIEW` with penalized confidence.
  - **Zero-Mutation Invariant:** Transaction amount, account, currency, and individual agent outcomes are strictly immutable.
- **Coordinator & Persistence Orchestration (`agents/agent_coordinator.py`):**
  - Manages sequential execution across all 4 agents, idempotency caching, masked audit event emissions (`CONSENSUS_DECISION_REQUESTED`, `CONSENSUS_DECISION_COMPLETED`), and workflow state tracking.
  - SQLite persistence in indexed `decision_results` table.
- **REST APIs:**
  - `POST /decision/analyze`: Run multi-agent consensus coordination for a transaction.
  - `GET /decision/metrics`: Aggregated disposition breakdown, average confidence, disagreement count, and agent performance.
  - `GET /decision/{transaction_id}`: Retrieve authoritative decision record and audit explanations.
- **Interactive Demo & Performance Benchmark:**
  - `scripts/demo_day7_consensus.py`: Interactive verification across all 5 core consensus scenarios.
  - `scripts/benchmark_day7.py`: 10,000-record benchmark (1,153s, 8.67 tx/s across 4 agents with SQLite persistence, 75.37% APPROVE, 24.63% MANUAL_REVIEW, 0% BLOCK_RECOMMENDED, Average Confidence: 0.9475, 0 system failures).
- **Test Suite Verification:** 241/241 passing tests (46 Day 7 tests, 0 regressions across Days 1–6).

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

## Day 4 — Intelligent Ledger Reconciliation

Day 4 delivers the first real financial intelligence module: an explainable, deterministic ledger reconciliation engine that deterministically matches incoming transactions against general ledger accounting entries with zero non-deterministic dependencies.

```text
Transaction Ingestion
         │
         ▼
TRANSACTION_RECONCILIATION_REQUESTED
         │
         ▼
   Ledger Repository (SQLite Lookup: by tx_id / reference_id)
         │
         ▼
   Reconciliation Engine (Deterministic Rules & Weighted Scoring)
         ├───────────────────────────────────┐
         ▼                                   ▼
Match / Mismatch Evaluation         Explainable Evidence Generation
         │                                   │
         └─────────────────┬─────────────────┘
                           ▼
                  ReconciliationResult
                 (Status, Score, Fields)
                           │
                           ▼
               Save to SQLite Audit Store
                           │
                           ▼
                 TRANSACTION_RECONCILED
                           │
                           ▼
                  Workflow Stage Update (reconciliation -> COMPLETED)
```

### Deterministic Financial Architecture Pillars

1. **Deterministic Matching (Zero LLMs / Zero ML):**
   - Financial ledger matching requires exact explainability, repeatability, and legal auditability.
   - Strictly avoids non-deterministic LLMs, prompt drift, and opaque black-box classifiers.
   - Evaluates 7 core banking criteria: Transaction ID, Reference ID, Account ID, Decimal Amount, ISO 4217 Currency, Timestamp Tolerance, and Logical Debit/Credit Direction.

2. **Weighted Reconciliation Scoring:**
   - Transparent, configurable scoring matrix summing to exactly 1.0 (100%):
     - **Transaction ID / Reference:** 30%
     - **Account ID:** 20%
     - **Amount:** 25%
     - **Currency:** 10%
     - **Timestamp Tolerance:** 10%
     - **Debit/Credit Direction:** 5%
   - Normalized score bounded within `[0.0, 1.0]`.
   - Discrete status classification:
     - `MATCHED`: Score >= 0.95 with 0 mismatched fields.
     - `PARTIAL_MATCH`: Score >= 0.50 (e.g. amount variance or timestamp delta).
     - `MISMATCHED`: Score < 0.50 (compound field discrepancies).
     - `NOT_FOUND`: Candidate ledger record absent from core repository.

3. **Decimal Financial Precision (No Floating-Point Equality):**
   - IEEE 754 standard floating-point representation (`float`) introduces precision drift (e.g. `0.1 + 0.2 != 0.3`).
   - All monetary fields in `reconciliation/rules.py` are converted to Python `Decimal` with banker's rounding (`ROUND_HALF_EVEN`) to 2 decimal places before equality checks.

4. **Timestamp Tolerance Window:**
   - General ledger postings often experience asynchronous network/batch latency relative to transaction authorization.
   - Configurable tolerance window (default: 300 seconds / 5 minutes). Postings within this window are evaluated as a match; outliers trigger a timestamp mismatch record.

5. **Mismatch Detection & Reasoning:**
   - Explicit failure categorization: captures specific mismatch reasons such as `Amount mismatch: tx=1500.00 USD, ledger=1400.00 USD (diff: 100.00 USD)`, `Currency mismatch`, `Account mismatch`, and `Timestamp outside tolerance`.

6. **Explainable Evidence Generation:**
   - Generates auditable JSON evidence blocks for every evaluated rule:
     ```json
     {
       "field": "amount",
       "match": true,
       "message": "Exact amount match: 1500.00 USD"
     }
     ```

7. **Clean Architectural Separation:**
   ```text
   Models (ReconciliationResult, Transaction, LedgerEntry)
     ↓
   Rules (Deterministic evaluators & Decimal conversions)
     ↓
   Engine (Stateless ReconciliationEngine)
     ↓
   Repository (LedgerRepository SQLite abstraction)
     ↓
   Service (ReconciliationService coordinator & event publisher)
     ↓
   Event Orchestrator (TRANSACTION_RECONCILED emission & workflow update)
     ↓
   API (FastAPI endpoints)
   ```

### Day 4 REST API Endpoints

- **`POST /reconciliation/run`:** Trigger reconciliation for an ingested transaction ID.
  ```json
  // Request
  { "transaction_id": "TXN-000001" }

  // Response
  {
    "reconciliation_id": "84fc90f7-9204-4e48-8dfa-8aa47b998dfc",
    "transaction_id": "TXN-000001",
    "ledger_entry_id": "LEDGER-000001",
    "status": "MATCHED",
    "score": 1.0,
    "matched_fields": ["transaction_id", "account_id", "amount", "currency", "timestamp", "direction"],
    "mismatched_fields": [],
    "mismatch_reasons": [],
    "evidence": [...]
  }
  ```
- **`GET /reconciliation/{reconciliation_id}`:** Retrieve stored reconciliation result and audit trail evidence.
- **`GET /reconciliation/metrics`:** Query live reconciliation telemetry (`total`, `matched`, `partial_match`, `mismatched`, `not_found`, `average_score`).

### Running the 1,000-Record Reconciliation Benchmark
```powershell
python scripts/benchmark_day4.py
```

---

## Day 5 — Fuzzy Ledger Reconciliation

Day 5 introduces a deterministic fuzzy candidate-matching layer that identifies candidate general ledger entries even when payment rail metadata contains formatting variations, minor fee discrepancies, or timestamp posting latency.

```text
                 Transaction
                      |
                      v
              Candidate Discovery
                      |
                      v
               Hard Constraints (Account & Currency)
                      |
                      v
              Fuzzy Candidate Ranker
                      |
          +-----------+-----------+
          |                       |
          v                       v
      Clear Winner            Ambiguous (delta < margin)
          |                       |
          v                       v
 Deterministic Engine      Manual Review Required
          |
          v
 Reconciliation Result
```

> **CRITICAL FINANCIAL SAFETY PRINCIPLE:**  
> **Fuzzy matching is used only for candidate discovery. Final financial reconciliation remains deterministic and explainable.**  
> The fuzzy candidate ranker never alters ledger values, never overrides double-entry principles, and never directly marks a transaction as `MATCHED`. Only the deterministic reconciliation engine produces authoritative financial decisions.

### Architectural Pillars

1. **Why Exact Matching is Insufficient:**
   - In production banking, reference codes are often entered across multiple rails with formatting drift (`PAY-ABC-123` vs `pay abc 123` or `PAY_ABC.123`).
   - Minor processing fees or cent differences (e.g. $0.01 tolerance) and network posting latencies (settlement within a 15-minute window) prevent naive exact queries from locating candidate ledger entries.

2. **Field Normalization (`reconciliation/normalization.py`):**
   - **Reference Normalization:** Strips leading/trailing whitespace, converts to uppercase, and collapses separators (`[\s\-_.]+`).
   - **Transaction ID Normalization:** Strips whitespace and normalizes case while preserving domain structure.
   - **Account ID Normalization:** Strips outer whitespace and case-folds standard prefixes, but **strictly preserves all meaningful digits**. No digits are ever truncated from sensitive financial account numbers.
   - **Currency Normalization:** Normalizes to standard 3-letter ISO 4217 uppercase.

3. **Configurable Candidate Scoring (`reconciliation/fuzzy_matcher.py`):**
   - Evaluates field-level similarity using normalized `difflib.SequenceMatcher`, strict `Decimal` numerical differences, and linear timestamp decay:
     - **Reference Similarity:** 30%
     - **Account Match:** 25%
     - **Decimal Amount Similarity:** 25%
     - **Timestamp Proximity (900s window):** 15%
     - **Currency Match:** 5%
   - Normalized candidate score is bounded within `[0.0, 1.0]`.
   - Confidence levels:
     - `HIGH`: Score >= 0.90
     - `MEDIUM`: Score >= 0.75
     - `LOW`: Score >= 0.50
     - `REJECTED`: Score < 0.50 or hard constraint violation.

4. **Non-Negotiable Hard Constraints:**
   - Account identifier mismatch or currency mismatch immediately triggers candidate `REJECTED` (score: `0.0`).
   - Prevents dangerous false matches where an unrelated account with an identical reference string or amount would otherwise be selected.

5. **Ambiguity Detection & Manual Review Flag:**
   - If the difference between the top-ranked candidate and the runner-up candidate is within the configurable `ambiguity_margin` (default: `0.03`), candidate selection is flagged as `AMBIGUOUS`.
   - The transaction is held for human compliance triage: `requires_manual_review = True`.
   - Ambiguous candidates are never automatically reconciled or approved.

6. **Two-Stage Reconciliation Architecture:**
   - **Stage 1 (Discovery):** Queries repository pools, evaluates field similarity, enforces hard constraints, and ranks top candidates.
   - **Stage 2 (Verification):** Passes the selected top candidate to the existing deterministic `ReconciliationEngine` to compute the authoritative `ReconciliationResult`.

### Day 5 REST API Endpoints

- **`POST /reconciliation/candidates`:** Discover and rank candidate ledger entries for a transaction.
  ```json
  // Request
  { "transaction_id": "TXN-000001" }

  // Response
  {
    "transaction_id": "TXN-000001",
    "candidate_count": 2,
    "selection_status": "SELECTED",
    "top_candidate": {
      "ledger_entry_id": "LEDGER-000001",
      "candidate_score": 0.96,
      "confidence_level": "HIGH"
    },
    "candidates": [...],
    "requires_manual_review": false
  }
  ```
- **`GET /reconciliation/candidates/{transaction_id}`:** Retrieve full candidate ranking, field similarity scores, and explainable evidence.

### Day 6 REST API Endpoints

- **`POST /fraud/analyze`:** On-demand deterministic fraud evaluation for a transaction.
  ```json
  // Request
  { "transaction_id": "TXN-000001" }

  // Response
  {
    "transaction_id": "TXN-000001",
    "fraud_score": 60.0,
    "risk_level": "HIGH",
    "status": "MANUAL_REVIEW",
    "requires_manual_review": true,
    "block_recommended": false,
    "indicators": ["HIGH_AMOUNT_ANOMALY", "RECONCILIATION_ANOMALY"],
    "evidence": [
      {
        "rule_id": "HIGH_AMOUNT_ANOMALY",
        "score_contribution": 25.0,
        "reason": "Transaction amount 75000.00 significantly exceeds baseline average 5000.00 (15.00x).",
        "evidence": { "amount": 75000.0, "average_amount": 5000.0, "ratio": 15.0 }
      }
    ],
    "analysis_timestamp": "2026-10-06T12:00:00Z",
    "model_version": "1.0.0-deterministic"
  }
  ```
- **`GET /fraud/{transaction_id}`:** Retrieve persisted fraud analysis record and transparent rule-by-rule evidence.
- **`GET /fraud/metrics`:** Aggregated operational fraud metrics:
  ```json
  {
    "total_analyzed": 10000,
    "low_count": 7616,
    "medium_count": 1526,
    "high_count": 323,
    "critical_count": 535,
    "manual_review_count": 858,
    "block_recommended_count": 535,
    "average_score": 15.41
  }
  ```

### Running Performance Benchmarks & Demos
```powershell
# Day 5: 10,000-Record Fuzzy Candidate Reconciliation Benchmark
python scripts/benchmark_day5.py

# Day 6: 10,000-Record Fraud Detection Agent Benchmark
python scripts/benchmark_day6.py

# Day 7: 5-Scenario Interactive Multi-Agent Consensus Demonstration
python scripts/demo_day7_consensus.py

# Day 7: 10,000-Record Multi-Agent Consensus Coordination Benchmark
python scripts/benchmark_day7.py
```

---

## Directory Structure

```
ZT-LedgerGuard/
│
├── agents/
│   ├── __init__.py
│   ├── base_agent.py               # BaseAgent ABC & AgentExecutionRecord dataclass
│   ├── agent_coordinator.py        # AgentCoordinator (sequential execution, idempotency, events)
│   ├── reconciliation_agent/
│   │   ├── __init__.py
│   │   └── reconciliation_agent.py # ReconciliationAgent wrapping two-stage reconciliation
│   ├── fraud_agent/
│   │   ├── __init__.py
│   │   └── fraud_agent.py          # FraudDetectionAgent implementing BaseAgent
│   ├── policy_agent/
│   │   ├── __init__.py
│   │   └── policy_agent.py         # PolicyAgent governance & regulatory compliance rules
│   └── decision_agent/
│       ├── __init__.py
│       └── decision_agent.py       # DecisionAgent, ConsensusEngine & DecisionPrecedence
│
├── orchestrator/
│   ├── __init__.py
│   ├── event_orchestrator.py       # Event bus, idempotency, bounded retries
│   ├── event_types.py              # Canonical EventType enum (incl. CONSENSUS_DECISION_*)
│   ├── handlers.py                 # BaseEventHandler & handler registry
│   ├── workflow_state.py           # Thread-safe WorkflowTracker & consensus stages
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
│   ├── reconciliation_result.py    # ReconciliationResult model & ReconciliationStatus enum
│   ├── rules.py                    # 7 deterministic rules, weights & Decimal arithmetic
│   ├── engine.py                   # Stateless ReconciliationEngine & scoring
│   ├── normalization.py            # Reference, account, and currency safe normalization
│   ├── fuzzy_result.py             # FuzzyCandidate model, ConfidenceLevel & SelectionStatus
│   ├── fuzzy_matcher.py            # FuzzyCandidateMatcher with weights & ambiguity detection
│   ├── candidate_service.py        # CandidateService discovery & ranking coordinator
│   ├── ledger_repository.py        # LedgerRepository SQLite persistence & candidate queries
│   ├── reconciliation_service.py   # Two-Stage ReconciliationService & event dispatcher
│   └── transaction_service.py      # TransactionService ingestion coordinator
│
├── fraud_detection/
│   ├── __init__.py
│   ├── fraud_result.py             # FraudAnalysisResult, RiskLevel, FraudStatus Pydantic models
│   ├── features.py                 # 7 deterministic feature extractors
│   ├── rules.py                    # 7 independent fraud evaluation rules & evidence
│   ├── risk_engine.py              # RiskEngine, anti-double-counting safeguards, scoring
│   ├── profile.py                  # AccountProfile & AccountProfileStore baseline tracking
│   ├── fraud_service.py            # FraudService coordinator, SQLite persistence & events
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
│   └── database.py                 # SQLite persistence, decision_results & metrics queries
│
├── data/
│   ├── __init__.py
│   ├── transaction_generator.py    # Deterministic synthetic transaction & fraud scenarios
│   └── ledger_generator.py         # Synthetic core ledger generator & Day 5 scenarios
│
├── tests/
│   ├── __init__.py
│   ├── test_foundation.py          # Day 1 test suite (18 tests)
│   ├── test_day2.py                # Day 2 test suite (20 tests)
│   ├── test_day3.py                # Day 3 test suite (25 tests)
│   ├── test_day4.py                # Day 4 test suite (35 tests)
│   ├── test_day5.py                # Day 5 test suite (47 tests)
│   ├── test_day6.py                # Day 6 test suite (50 tests)
│   └── test_day7.py                # Day 7 test suite (46 tests)
│
├── scripts/
│   ├── __init__.py
│   ├── run_transaction_stream.py   # Day 2 streaming CLI
│   ├── benchmark_day3.py           # Day 3 100-transaction benchmark
│   ├── benchmark_day4.py           # Day 4 1,000-record reconciliation benchmark
│   ├── benchmark_day5.py           # Day 5 10,000-record fuzzy candidate benchmark
│   ├── benchmark_day6.py           # Day 6 10,000-record fraud detection benchmark
│   ├── demo_day7_consensus.py      # Day 7 5-scenario interactive consensus demonstration
│   └── benchmark_day7.py           # Day 7 10,000-record multi-agent consensus benchmark
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
