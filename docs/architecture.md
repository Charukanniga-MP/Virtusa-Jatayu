# ZT-LedgerGuard Architecture Specification

**Project:** ZT-LedgerGuard — Zero-Trust AI Ledger Reconciliation & Real-Time Fraud Triage Platform  
**Target:** Virtusa Jatayu Season 6 — Forward Deployment Engineering (FDE)  
**Domain:** FinTech / Banking Core Systems  

---

## 1. High-Level System Architecture Diagram

```
                    Transaction Sources
                           |
                           v
                  Event Ingestion Layer
                           |
                           v
                  Event Orchestrator
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
   Reconciliation      Fraud Agent      Risk Agent
       Agent                |                |
          |                 +-------+--------+
          |                         |
          +------------+------------+
                       v
                Decision Engine
                       |
             +---------+---------+
             |                   |
             v                   v
        Legacy System       Investigation
        / Shadow Mode          Queue
```

---

## 2. Day 2 Transaction Ingestion & Streaming Flow

```
   Transaction Source (Synthetic Generator / REST API)
                         |
                         v
             Transaction Generator / API
                         |
                         v
               Transaction Validator (Pydantic)
                         |
                         v
                SQLite Persistence
                         |
                         v
             Event Publisher (EventPublisher ABC)
                         |
                         v
                 LocalEventPublisher
                         |
                         v
                  Event Orchestrator
                         |
             +-----------+-----------+
             |                       |
             v                       v
     Logging Handler         Future Domain Agents
 (transaction_event_handler)       (Day 3+)
```

---

## 3. Day 3 Event-Driven Orchestration Architecture

Day 3 introduces autonomous handler routing, distributed correlation tracking, parent causation lineage, bounded retries, event deduplication, workflow state machines, and immutable audit logs.

```text
                     Event Orchestrator
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
          Handler A     Handler B     Handler C
              |             |             |
              +-------------+-------------+
                            |
                     Workflow State
                            |
                     Event Audit Trail
```

### Canonical Event Model Specification

```text
Event
 ├── event_id          (UUID v4 unique message identifier)
 ├── event_type        (Strongly typed EventType enum token)
 ├── correlation_id    (End-to-end transaction lifecycle identifier)
 ├── causation_id      (Parent event_id that triggered this event)
 ├── timestamp         (UTC ISO timestamp of event emission)
 ├── retry_count       (Bounded retry counter, 0..MAX_EVENT_RETRIES)
 └── payload           (Sanitized domain dictionary payload)
```

### Event Lifecycle & Pipeline Flow

```text
Transaction Ingestion (API / Generator)
               │
               ▼
      TRANSACTION_RECEIVED ──► [ValidationHandler] ──► Workflow: PROCESSING
               │
               ▼
      TRANSACTION_VALIDATED ──► [Pipeline Coordinator]
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
                             TRANSACTION_APPROVED ──► Workflow: COMPLETED
```

---

## 4. Day 4 Intelligent Ledger Reconciliation Architecture

Day 4 introduces an explainable, deterministic ledger reconciliation engine that evaluates whether an authorized banking transaction corresponds to an entry in the core general ledger.

```text
Transaction
     |
     v
Reconciliation Request
     |
     v
Ledger Repository
     |
     v
Reconciliation Engine
     |
     +-------------------+
     |                   |
     v                   v
Match Evaluation     Evidence
     |
     v
Reconciliation Result
     |
     v
TRANSACTION_RECONCILED
```

### Deterministic Financial Guarantee
**The financial decision engine is deterministic and does not depend on an LLM.**

Core banking systems demand absolute reproducibility, sub-millisecond execution times, zero hallucination risk, and auditable calculation logic for financial regulators (SOX, Basel III, PCI-DSS). The reconciliation engine operates via stateless mathematical and rule evaluations with no machine learning models or probabilistic prompt inferences.

### Multi-Rule Weighted Evaluation
The engine evaluates seven deterministic banking rules with configurable weights summing to 100%:

| Rule | Evaluated Fields | Weight | Deterministic Criteria |
| :--- | :--- | :--- | :--- |
| **Rule 1 & 2** | `transaction_id`, `reference_id` | 30% | Strong match if either direct ID or reference ID aligns. |
| **Rule 3** | `account_id` | 20% | Exact equality between transaction account and ledger account. |
| **Rule 4** | `amount` | 25% | Exact financial equality using `Decimal` arithmetic. |
| **Rule 5** | `currency` | 10% | Strict ISO 4217 code equality. |
| **Rule 6** | `timestamp` | 10% | Posting time delta within configurable tolerance (`TIMESTAMP_TOLERANCE_SECONDS = 300`). |
| **Rule 7** | `direction` | 5% | Logical compatibility between transaction type and ledger debit/credit side. |

### Strict Decimal Money Representation
Floating-point arithmetic (IEEE 754) is strictly prohibited for monetary values due to fractional binary conversion anomalies (`0.1 + 0.2 != 0.3`). All amounts in the reconciliation engine are parsed to Python `Decimal` objects quantized to two decimal places with banker's rounding (`ROUND_HALF_EVEN`) before comparison and variance calculation.

---

## 5. Day 5 Fuzzy Ledger Reconciliation & Intelligent Candidate Matching

Day 5 introduces a deterministic fuzzy candidate-matching layer that identifies likely ledger entries when transaction and ledger records are not exact matches (due to reference formatting drift, minor payment processing fee tolerances, or timestamp network latencies).

### Architecture Flow

```text
                 Transaction
                      |
                      v
              Candidate Discovery
                      |
                      v
               Hard Constraints
                      |
                      v
              Fuzzy Candidate Ranker
                      |
          +-----------+-----------+
          |                       |
          v                       v
      Clear Winner            Ambiguous
          |                       |
          v                       v
 Deterministic Engine      Manual Review
          |
          v
 Reconciliation Result
```

### Critical Financial Safety Principle
> **Fuzzy matching is used only for candidate discovery. Final financial reconciliation remains deterministic and explainable.**
> The fuzzy candidate matcher is strictly a discovery mechanism. It never alters ledger records, never adjusts financial values, and never directly marks a transaction as `MATCHED`. The authoritative financial decision is always evaluated by the deterministic `ReconciliationEngine`.

### Key Design Pillars

1. **Safe Normalization (`reconciliation/normalization.py`):**
   - Reference codes: Whitespace and punctuation (`[\s\-_.]+`) stripped, converted to uppercase (`PAY-ABC-123` -> `PAYABC123`).
   - Account numbers: Leading/trailing whitespace trimmed, prefixes normalized, but **all meaningful digits are strictly preserved**. Digits are never stripped or altered.
   - ISO 4217 Currency: Normalized to uppercase 3-letter codes.

2. **Multi-Field Similarity Scoring (`reconciliation/fuzzy_matcher.py`):**
   - Configurable weights: Reference (30%), Account (25%), Amount (25%), Timestamp Window (15%), Currency (5%).
   - Numerical Decimal comparison for amounts (absolute and relative tolerance).
   - Timestamp decay over configurable window (`FUZZY_TIMESTAMP_WINDOW_SECONDS = 900`).

3. **Non-Negotiable Hard Constraints:**
   - Incompatible account IDs or incompatible currencies are hard-rejected (`REJECTED`, candidate score = 0.0). Even a 100% identical reference ID cannot override an account or currency mismatch.

4. **Ambiguity Detection & Human Review:**
   - If `top_score - second_score < AMBIGUITY_MARGIN (0.03)`, candidate selection status is marked as `AMBIGUOUS`.
   - The transaction is flagged with `requires_manual_review = True` and held for human compliance review.

5. **Two-Stage Reconciliation Pipeline:**
   - **Stage 1 (Discovery):** `CandidateService` retrieves candidate pool from indexed SQLite queries and ranks them using `FuzzyCandidateMatcher`, emitting `FUZZY_CANDIDATE_IDENTIFIED`.
   - **Stage 2 (Financial Verification):** Selected top candidate is passed to `ReconciliationEngine` for final deterministic verification (`MATCHED`, `PARTIAL_MATCH`, `MISMATCHED`, or `NOT_FOUND`).

---

## 6. Day 6 Fraud Detection Agent Architecture

Day 6 introduces the autonomous, deterministic, and explainable **Fraud Detection Agent** (`agents/fraud_agent/fraud_agent.py`), adding a real-time risk intelligence layer downstream of transaction validation and reconciliation.

### Complete Transaction & Intelligence Flow

```text
Transaction
    ↓
Validation
    ↓
Fuzzy Candidate Discovery
    ↓
Deterministic Reconciliation
    ↓
Fraud Feature Extraction
    ↓
Fraud Rule Engine
    ↓
Risk Engine
    ↓
Fraud Detection Agent
    ↓
Risk Assessment
    ↓
Manual Review / Future Decision Layer
```

### Event-Driven Orchestration Flow

```text
TRANSACTION_RECEIVED
        ↓
VALIDATED
        ↓
RECONCILIATION_REQUESTED
        ↓
CANDIDATE_DISCOVERY (Stage 1)
        ↓
FUZZY_CANDIDATE_IDENTIFIED
        ↓
DETERMINISTIC_RECONCILIATION (Stage 2)
        ↓
RECONCILIATION_COMPLETED
        ↓
FRAUD_ANALYSIS_REQUESTED
        ↓
FraudDetectionAgent
        ↓
FRAUD_ANALYSIS_COMPLETED
        ↓
Risk Decision
        ↓
Final Decision
```

### Safety & Integrity Guarantees

1. **Non-Mutating Intelligence:** The Fraud Detection Agent **never** modifies transaction amount, account ID, currency, ledger balances, or reconciliation outcomes.
2. **Zero External Dependencies:** 100% deterministic local computation; zero LLMs (no Gemini, Ollama, LangChain), zero external APIs, zero Docker, zero Kafka required.
3. **No Automatic Destructive Actions:** Even for `CRITICAL` risk transactions, the agent only sets `block_recommended = True` and `requires_manual_review = True`; it never deletes transactions or initiates unauthorized money movements.
4. **Anti-Double-Counting Safeguards:** Dampens correlated ceiling spikes when amount anomalies are already triggered by the exact same transaction value.

### Fraud Rules & Maximum Score Weights

| Rule Identifier | Max Weight | Evaluation Logic | Explainable Evidence |
| :--- | :---: | :--- | :--- |
| `HIGH_AMOUNT_ANOMALY` | 25 | Compares transaction amount to account historical baseline average. | Baseline average, transaction amount, deviation multiplier. |
| `HIGH_TRANSACTION_VELOCITY` | 25 | Evaluates transaction count within sliding window (default 60s). | Window duration, transaction count, burst threshold. |
| `RAPID_REPEATED_TRANSACTION` | 15 | Detects identical amount transactions within short interval (120s). | Repetition count, identical amount, time interval. |
| `UNUSUAL_TRANSACTION_TIME` | 10 | Identifies transactions outside business hours (22:00–06:00 UTC). | Transaction timestamp, hour of day, configured window. |
| `ACCOUNT_BEHAVIOR_DEVIATION`| 15 | Compares transaction against account ceiling and currency patterns. | Historical maximum, deviation ratio, currency match. |
| `RECONCILIATION_ANOMALY` | 10 | Integrates reconciliation result (`MISMATCHED`, `AMBIGUOUS`). | Reconciliation status, match score, mismatch reasons. |
| `REPEATED_FAILED_ACTIVITY` | 10 | Checks prior validation/reconciliation failure counts. | Failure count, threshold limit. |

### Configurable Risk Tiers

- **`LOW` (0–24):** Normal operations; `requires_manual_review = False`, `block_recommended = False`.
- **`MEDIUM` (25–49):** Low-to-moderate anomaly; preserved audit evidence; `requires_manual_review = False`, `block_recommended = False`.
- **`HIGH` (50–74):** Elevated risk; `requires_manual_review = True`, `block_recommended = False`.
- **`CRITICAL` (75–100):** Severe threat pattern; `requires_manual_review = True`, `block_recommended = True`.

---

## 7. Component Status Matrix (Days 1–6 vs. Planned)

| Component | Status | Day 1–6 Implementation Scope | Planned Full Scope (Days 7–15) |
| :--- | :--- | :--- | :--- |
| **Transaction Sources** | **IMPLEMENTED (Day 2)** | Deterministic synthetic generator `TransactionGenerator` with multi-channel, multi-currency, and Day 6 fraud scenario simulation. | Continuous streaming load injector with anomaly burst modes. |
| **Event Ingestion Layer** | **IMPLEMENTED (Day 2)** | `TransactionService` coordinating validation, SQLite persistence, and decoupled event publishing via `POST /transactions` and `POST /transactions/simulate`. | Distributed streaming ingestion gateways with TLS token authentication. |
| **Event Publisher** | **IMPLEMENTED (Day 2)** | `EventPublisher` ABC with concrete `LocalEventPublisher` routing to `EventOrchestrator`. | `KafkaEventPublisher` implementing the same `EventPublisher` interface with zero code refactoring needed. |
| **Event Orchestrator** | **IMPLEMENTED (Day 3)** | Decoupled event bus with dynamic handler registry, idempotency deduplication (`_processed_event_ids`), bounded retries (`MAX_EVENT_RETRIES`), sync/async non-blocking handler support, and full audit trail. | Distributed Apache Kafka topics, consumer groups, partition management, DLQs. |
| **Workflow State Tracker** | **IMPLEMENTED (Day 3, 6)** | Thread-safe `WorkflowTracker` tracking status and stage breakdowns (`validation`, `reconciliation`, `fraud_analysis`, `risk_analysis`, `final_decision`). | Persistent Redis / distributed state cache with timeout monitors. |
| **Event Audit Trail** | **IMPLEMENTED (Day 3)** | Thread-safe `EventAuditTrail` capturing immutable records with statuses (`RECEIVED`, `PROCESSING`, `COMPLETED`, `FAILED`, `DUPLICATE`) and causation lineage. | Append-only cryptographically signed ledger audit log (Day 8). |
| **Processing Pipeline** | **IMPLEMENTED (Day 3-6)** | `ProcessingPipeline` chaining canonical banking event stages, dynamically invoking two-stage `ReconciliationService` and `FraudService`. | Full multi-agent consensus evaluation pipeline. |
| **Reconciliation Engine** | **IMPLEMENTED (Day 4-5)** | Two-stage architecture: `CandidateService` + `FuzzyCandidateMatcher` (Stage 1) and deterministic `ReconciliationEngine` (Stage 2) with 7 banking rules, Decimal arithmetic, explainability, ambiguity detection, and manual review flags. | Multi-account split reconciliation and automated reversal generation (Day 10). |
| **Fraud Agent** | **IMPLEMENTED (Day 6)** | Autonomous, deterministic `FraudDetectionAgent`, 7 feature extractors, 7 rules, profile baselining, anti-double-counting safeguards, risk scoring (0-100), SQLite persistence, and REST APIs (`POST /fraud/analyze`, `GET /fraud/{id}`, `GET /fraud/metrics`). | Multi-agent risk consensus (Day 7), optional ML embedding scoring. |
| **Risk Agent** | **PLANNED** | Directory initialized (`agents/risk_agent`). | Multi-agent risk profiling & aggregate score weighting (Day 7). |
| **Decision Engine** | **PLANNED** | Placeholder disposition handler (`TRANSACTION_APPROVED`). | Multi-agent consensus engine and automated disposition logic (Day 7). |
| **Legacy System Simulator** | **IMPLEMENTED (Day 1)** | Deterministic `LegacyBankingSystem` simulating host approvals/rejections. | Full Shadow Deployment dual-run engine and discrepancy analysis (Day 9). |
| **Investigation Queue** | **IMPLEMENTED (Day 5, 6)**| Flagged via `requires_manual_review = True` on ambiguous reconciliation and HIGH/CRITICAL fraud analyses. | Alert queuing, operator review console, and case management (Day 11). |
| **Security Layer** | **IMPLEMENTED (Day 1-6)** | Account ID masking (`mask_account_id`), zero-trust log sanitization, safe normalization preserving account digits. | Mutual TLS, cryptographic signing, API gateway policies (Day 8). |
| **Privacy Layer** | **IMPLEMENTED (Day 1-6)** | Zero-trust logging rule: strict ban on raw PII / account numbers in logs, fuzzy evidence, and fraud event payloads. | Differential privacy queries and cryptographic data tokenization (Day 8). |
| **Database** | **IMPLEMENTED (Day 1-6)** | Local SQLite engine with indexed tables (`transactions`, `ledger_entries`, `reconciliation_results`, `fraud_analyses`), migration support, and aggregated metrics queries. | PostgreSQL / TimescaleDB support with high-throughput indexing (Day 8+). |
| **Monitoring & Telemetry** | **IMPLEMENTED (Day 3-6)** | Structured logging + live execution metrics, audit traces, reconciliation metrics, and fraud metrics (`GET /fraud/metrics`). | Distributed OpenTelemetry tracing and Prometheus metric exporters (Day 12). |
| **Dashboard** | **PLANNED** | Directory initialized (`dashboard/`). | Real-time operator triage console and reconciliation metrics UI (Day 11). |

---

## 8. Data Flow & Zero-Trust Boundary

### Implemented Flow:
1. **Transaction Submission:** A client submits a JSON payload to `POST /transactions` or `POST /transactions/simulate`.
2. **Schema & Business Validation:** Pydantic `Transaction` validates fields, positive amounts, 3-letter currency codes, and non-empty IDs.
3. **Zero-Trust Sanitization:** Account identifiers are masked via `mask_account_id()` prior to output to system logs and event audit records.
4. **Local Event Dispatch:** The transaction is converted into an in-memory `Event` and routed via `EventOrchestrator` to registered handlers.
5. **Candidate Discovery (Stage 1):** `CandidateService` retrieves potential candidates from indexed SQLite tables and ranks them via `FuzzyCandidateMatcher`. Emits `FUZZY_CANDIDATE_IDENTIFIED`.
6. **Financial Verification (Stage 2):** Selected candidate is evaluated by deterministic `ReconciliationEngine`. Emits `TRANSACTION_RECONCILED`.
7. **Fraud Risk Analysis:** `FraudService` invokes `FraudDetectionAgent` extracting 7 features, evaluating 7 deterministic rules, and emitting `FRAUD_ANALYSIS_COMPLETED`.
8. **Database Storage:** Tables `transactions`, `ledger_entries`, `reconciliation_results`, and `fraud_analyses` are maintained in SQLite with parameterized queries preventing SQL injection.

---

## 9. Architectural Decisions

1. **No Docker / Local-First Python:** Entire foundation is executable on standard Windows workstations using native Python virtual environments.
2. **Decoupled Event Orchestration:** The `EventOrchestrator` defines an interface identical to pub/sub brokers, allowing drop-in Kafka replacement without refactoring downstream agents.
3. **Two-Stage Reconciliation Separation:** Fuzzy matching is strictly confined to candidate discovery. Authoritative financial reconciliation decisions are reserved for the deterministic engine.
4. **Deterministic & Explainable Fraud Intelligence:** Zero LLM dependency; risk decisions are 100% reproducible, explainable, and auditable.
5. **Data Hygiene First:** Logging never prints raw account numbers; zero-trust principles are applied across all stages.


