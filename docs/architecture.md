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

## 7. Day 7 Multi-Agent Coordination & Risk Decision Consensus

Day 7 establishes an explainable, deterministic multi-agent orchestration architecture uniting 4 specialized domain agents under a centralized Consensus Engine.

```text
                    Incoming Transaction
                             │
                             ▼
                    [AgentCoordinator]
       ┌─────────────────────┼─────────────────────┐
       ▼                     ▼                     ▼
[ReconciliationAgent] [FraudDetectionAgent]   [PolicyAgent]
 (Candidate Discovery   (7 Features, 7 Rules,  (Compliance, Limits,
 & Deterministic Verif) Baseline Deviations)    Compound Risks)
       │                     │                     │
       └─────────────────────┬─────────────────────┘
                             ▼
                      DecisionContext
                             │
                             ▼
                    [ConsensusEngine]
              (Precedence: BLOCK > REVIEW > APPROVE)
                             │
                             ▼
                      DecisionResult
        (Authoritative Disposition, Confidence 0.0-1.0,
         Disagreements, Triggered Policies, SQLite & Audit)
```

### Specialized Domain Agents

1. **`ReconciliationAgent` (`agents/reconciliation_agent/reconciliation_agent.py`):**
   - Implements `BaseAgent` contract (`analyze()`, `create_execution_record()`).
   - Wraps Day 5 two-stage reconciliation (`CandidateService` discovery + `ReconciliationEngine` verification).
   - Produces structured `ReconciliationAgentResult` with status (`MATCHED`, `AMBIGUOUS`, `MISMATCHED`, `NOT_FOUND`), score, and manual review flag.

2. **`FraudDetectionAgent` (`agents/fraud_agent/fraud_agent.py`):**
   - Implements `BaseAgent` contract.
   - Evaluates 7 canonical features against 7 independent deterministic rules with profile baselining.
   - Categorizes risk into `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` with explainable evidence.

3. **`PolicyAgent` (`agents/policy_agent/policy_agent.py`):**
   - Evaluates governance, regulatory compliance, and composite risk patterns.
   - Policies evaluated:
     - `CRITICAL_FRAUD_RISK`: Mandates `BLOCK_RECOMMENDED` if fraud score $\ge 75$.
     - `HIGH_FRAUD_RISK`: Mandates `MANUAL_REVIEW` if fraud score $\ge 50$.
     - `RECONCILIATION_AMBIGUITY`: Mandates `MANUAL_REVIEW` if candidates are ambiguous.
     - `RECONCILIATION_MISMATCH`: Mandates `MANUAL_REVIEW` on ledger discrepancy.
     - `MISSING_LEDGER_ENTRY`: Mandates `MANUAL_REVIEW` if ledger entry is `NOT_FOUND`.
     - `COMBINED_HIGH_RISK`: Escalates to `BLOCK_RECOMMENDED` on ledger mismatch + elevated fraud score ($\ge 50$).
     - `TRANSACTION_LIMIT_BREACH`: Flags amounts exceeding single-transaction ceiling ($1,000,000 INR).

4. **`DecisionAgent` & `ConsensusEngine` (`agents/decision_agent/decision_agent.py`):**
   - Central arbitrator reconciling individual agent postures into an authoritative final disposition.
   - Applies strict deterministic precedence and confidence scoring.

### Centralized Consensus Precedence & Rule Hierarchy

```text
               ┌───────────────────────┐
               │   BLOCK_RECOMMENDED   │  Highest Precedence (Priority 3)
               └───────────┬───────────┘
                           │
               ┌───────────▼───────────┐
               │     MANUAL_REVIEW     │  Intermediate Precedence (Priority 2)
               └───────────┬───────────┘
                           │
               ┌───────────▼───────────┐
               │        APPROVE        │  Lowest Precedence (Priority 1)
               └───────────────────────┘
```

| Rule | Trigger Condition | Consensus Disposition | Precedence Priority | Rationale |
| :--- | :--- | :--- | :---: | :--- |
| **Rule 1** | Fraud Score $\ge 75$ (`CRITICAL`) | `BLOCK_RECOMMENDED` | **Highest (3)** | Severe fraud risk overrides clean reconciliation match. |
| **Rule 5** | Ledger Mismatch + Fraud Score $\ge 50$ | `BLOCK_RECOMMENDED` | **Highest (3)** | Compound risk: unverified ledger + elevated fraud threat. |
| **Policy Mandate** | Policy recommendation `BLOCK_RECOMMENDED` | `BLOCK_RECOMMENDED` | **Highest (3)** | Governance policy breach mandates immediate freeze. |
| **Rule 2** | Elevated Fraud ($50 \le \text{score} < 75$) | `MANUAL_REVIEW` | **Medium (2)** | Suspicious anomaly requires compliance triage. |
| **Rule 3** | Reconciliation Status `AMBIGUOUS` | `MANUAL_REVIEW` | **Medium (2)** | Fuzzy candidate ambiguity cannot be auto-approved. |
| **Rule 4** | Reconciliation Status `NOT_FOUND` | `MANUAL_REVIEW` | **Medium (2)** | Missing ledger entry requires human investigation. |
| **Mismatch** | Reconciliation Status `MISMATCHED` | `MANUAL_REVIEW` | **Medium (2)** | Discrepancy requires accounting operator reconciliation. |
| **Fail-Safe** | Any Agent Execution Failure | `MANUAL_REVIEW` | **Medium (2)** | Zero-Trust: Agent Failure != Approval. |
| **Rule 6** | Matched Ledger + Low Fraud ($<25$) + Policy Clean | `APPROVE` | **Lowest (1)** | Full alignment across all agents with zero anomalies. |
| **Rule 7** | Matched Ledger + Moderate Fraud ($25 \le \text{score} < 50$) | `APPROVE` | **Lowest (1)** | Verified ledger match approves with preserved audit evidence. |

### Deterministic Agreement Confidence Formula

The consensus confidence score is deterministically calculated and strictly bounded within `[0.50, 1.0]`:
- **Base Agreement Score:** `1.00`
- **Agent Failure Penalty:** $-0.35$ (drops base confidence to $\le 0.65$)
- **Inter-Agent Disagreement Penalty:** $-0.20$ (drops base confidence to $\le 0.80$)
- **Borderline Reconciliation Score Penalty:** $-0.05$ (if match score $< 0.85$) or $-0.10$ (if `PARTIAL_MATCH`)
- **Boundary Fraud Score Penalty:** $-0.05$ (if fraud score is on decision edge: $[45, 55]$ or $[70, 78]$)
- **Bounded Clamping:** $\text{Confidence} = \max(0.50, \min(1.0, \text{Score}))$

### Zero-Trust Safety & Immutability Invariants

1. **Agent Failure != Approval:** If an agent encounters an unhandled exception or RPC timeout, the coordinator records the failure, downgrades confidence, and forces `MANUAL_REVIEW`.
2. **Strict Financial Immutability:** No agent or coordinator is permitted to mutate transaction amounts, account numbers, currencies, or reconciliation status.
3. **Audit Trail Integrity:** Account numbers are masked (`mask_account_id`) across all consensus events (`CONSENSUS_DECISION_REQUESTED`, `CONSENSUS_DECISION_COMPLETED`).
4. **Idempotent Decision Cache:** Duplicate analysis requests for the same transaction return cached results idempotently.

---

## 8. Component Status Matrix (Days 1–7 vs. Planned)

| Component | Status | Day 1–7 Implementation Scope | Planned Full Scope (Days 8–15) |
| :--- | :--- | :--- | :--- |
| **Transaction Sources** | **IMPLEMENTED (Day 2)** | Synthetic generator `TransactionGenerator` with multi-channel, multi-currency, and fraud scenario generation. | Continuous streaming load injector with anomaly burst modes. |
| **Event Ingestion Layer** | **IMPLEMENTED (Day 2)** | `TransactionService` coordinating validation, SQLite persistence, and decoupled event publishing via `POST /transactions`. | Distributed streaming ingestion gateways with TLS token authentication. |
| **Event Publisher** | **IMPLEMENTED (Day 2)** | `EventPublisher` ABC with concrete `LocalEventPublisher` routing to `EventOrchestrator`. | `KafkaEventPublisher` implementing the same `EventPublisher` interface. |
| **Event Orchestrator** | **IMPLEMENTED (Day 3)** | Decoupled event bus with dynamic handler registry, idempotency deduplication, bounded retries (`MAX_EVENT_RETRIES`), sync/async handler support. | Distributed Apache Kafka topics, consumer groups, partition management, DLQs. |
| **Workflow State Tracker** | **IMPLEMENTED (Day 3, 6, 7)**| Thread-safe `WorkflowTracker` tracking status and stage breakdowns (`validation`, `reconciliation`, `fraud_analysis`, `consensus_decision`). | Persistent Redis / distributed state cache with timeout monitors. |
| **Event Audit Trail** | **IMPLEMENTED (Day 3)** | Thread-safe `EventAuditTrail` capturing immutable records with causation lineage. | Append-only cryptographically signed ledger audit log (Day 8). |
| **Reconciliation Agent** | **IMPLEMENTED (Day 4, 5, 7)** | Standardized `ReconciliationAgent` wrapping two-stage candidate discovery (`CandidateService` + `FuzzyCandidateMatcher`) and deterministic verification (`ReconciliationEngine`). | Multi-account split reconciliation and automated reversal generation (Day 10). |
| **Fraud Detection Agent**| **IMPLEMENTED (Day 6, 7)** | Autonomous `FraudDetectionAgent` extracting 7 features, evaluating 7 rules, baselining account profiles, and generating `AgentExecutionRecord`. | Optional ML embedding scoring and graph fraud rings (Day 12). |
| **Policy Agent** | **IMPLEMENTED (Day 7)** | `PolicyAgent` evaluating critical fraud, high fraud, ambiguity, mismatch, missing ledger, compound risk, and transaction limits. | Dynamic regulatory rule reloading and external policy bundles (Day 8). |
| **Decision Agent & Consensus**| **IMPLEMENTED (Day 7)** | Centralized `DecisionAgent` & `ConsensusEngine` implementing strict precedence (`BLOCK > REVIEW > APPROVE`), confidence scoring, and disagreement detection. | Dynamic committee voting and multi-jurisdiction rule trees (Day 12). |
| **Agent Coordinator** | **IMPLEMENTED (Day 7)** | `AgentCoordinator` orchestrating all 4 domain agents, idempotency caching, masked audit event emissions, and SQLite `decision_results` persistence. | Distributed distributed actor-model agent coordinator (Day 12). |
| **Legacy System Simulator** | **IMPLEMENTED (Day 1)** | Deterministic `LegacyBankingSystem` simulating host approvals/rejections. | Full Shadow Deployment dual-run engine and discrepancy analysis (Day 9). |
| **Investigation Queue** | **IMPLEMENTED (Day 5-7)** | Flagged via `requires_manual_review = True` on ambiguous reconciliation, high/critical fraud, policy triggers, or agent failures. | Operator review console, and case management (Day 11). |
| **Security Layer** | **IMPLEMENTED (Day 1-8)** | Zero-Trust Security Gateway, input bounds, trust boundaries (stripping fake client decisions), safe error handling. | Mutual TLS, hardware key stores, external HSM integration (Day 14). |
| **Privacy & PII Layer** | **IMPLEMENTED (Day 1-8)** | 3-tier PII classification, irreversible masking (`mask_*`), deterministic surrogate tokenization (`TokenVault`), recursive payload sanitization. | Differential privacy and encrypted fields at rest (Day 13). |
| **Audit Integrity** | **IMPLEMENTED (Day 8)** | Tamper-evident SHA-256 hash-chain ledger, append-only SQLite `audit_chain`, mathematical integrity verification (`verify_chain`). | External Merkle timestamping and notary witness nodes (Day 14). |
| **Database** | **IMPLEMENTED (Day 1-8)** | SQLite with indexed tables (`transactions`, `ledger_entries`, `reconciliation_results`, `fraud_analyses`, `decision_results`, `audit_chain`). | PostgreSQL / TimescaleDB support with high-throughput partitioning. |
| **REST APIs** | **IMPLEMENTED (Day 1-8)** | Ingestion, Reconciliation, Candidates, Fraud, Consensus Decision, Security Metrics (`GET /security/metrics`), and Audit Verification (`GET /security/audit/verify`). | Full OpenAPI 3.1 gateway and gRPC endpoints (Day 14). |
| **Dashboard** | **PLANNED** | Directory initialized (`dashboard/`). | Real-time operator triage console and reconciliation metrics UI (Day 11). |

---

## 9. Data Flow & Zero-Trust Multi-Agent Boundary

### Implemented End-to-End Flow:
1. **Untrusted Request Ingestion:** Request arrives at `SecurityGateway` (verifies structural bounds, monetary limits, payload size <= 1MB, ISO currencies).
2. **Trust Boundary Scrubbing:** Client-injected decisions (`decision`, `fraud_score`, `risk_level`, `confidence`) are stripped immediately.
3. **Canonical Transaction Hashing:** `hash_transaction()` computes SHA-256 digest covering normalized banking fields and tokenized account surrogate.
4. **Local Event Dispatch:** In-memory `Event` routed via `EventOrchestrator` with masked account IDs.
5. **Reconciliation Agent:** Discovers candidate ledger entries via `CandidateService` and verifies via `ReconciliationEngine`.
6. **Fraud Detection Agent:** Extracts 7 features, compares with account profile baseline, evaluates 7 independent fraud rules.
7. **Policy Agent:** Evaluates compliance rules, single transaction ceilings, and compound risk.
8. **Consensus Engine:** Combines agent postures, resolves precedence (`BLOCK > REVIEW > APPROVE`), calculates agreement confidence.
9. **Decision Integrity Hash:** Canonical hash digest generated for the final consensus verdict.
10. **Tamper-Evident Hash Chain:** `AuditIntegrityService` sanitizes payload, computes canonical payload hash, cryptographically links to previous block's digest, and appends to SQLite `audit_chain` ledger.

---

## 10. Architectural Decisions

1. **No Docker / Local-First Python:** Entire foundation is executable on standard Windows workstations using native Python virtual environments.
2. **Decoupled Event Orchestration:** The `EventOrchestrator` defines an interface identical to pub/sub brokers, allowing drop-in Kafka replacement without refactoring downstream agents.
3. **Two-Stage Reconciliation Separation:** Fuzzy matching is strictly confined to candidate discovery. Authoritative financial reconciliation decisions are reserved for the deterministic engine.
4. **Deterministic & Explainable Intelligence:** Zero LLM dependency; risk and consensus decisions are 100% reproducible, explainable, and auditable.
5. **Centralized Precedence Arbitration:** Conflicting agent postures are resolved deterministically using `BLOCK_RECOMMENDED > MANUAL_REVIEW > APPROVE`.
6. **Fail-Safe Invariant:** `Agent Failure != Approval` ensures unhandled downstream errors fall back to human compliance triage.
7. **Never Trust Client Decisions:** Client-supplied risk or disposition fields are stripped at trust boundaries; authoritative verdicts are recomputed server-side.
8. **Append-Only Audit Immutability:** Audit records are append-only; no `UPDATE` or `DELETE` queries exist for audit history. Corrections are recorded as new events.

---

## 11. Day 8 Zero-Trust Security & Audit Integrity Specification

### Architecture Diagram

```text
                    UNTRUSTED REQUEST
                           │
                           ▼
                 ┌───────────────────┐
                 │ Security Gateway  │
                 └─────────┬─────────┘
                           │
                 ┌─────────┼─────────┐
                 ▼         ▼         ▼
              Validate    PII      Tokenize
                         Protect
                 │         │         │
                 └─────────┼─────────┘
                           ▼
                    Business Logic
                           │
                           ▼
                   Decision Engine
                           │
                           ▼
                  Canonical Payload
                           │
                           ▼
                     SHA-256 Hash
                           │
                           ▼
                    Hash Chain Audit
                           │
                           ▼
                    Integrity Check
```

### PII Classification Taxonomy

| Level | Classification | Attributes | Handling & Storage Policy |
| :--- | :--- | :--- | :--- |
| **1** | **PUBLIC** | `transaction_id`, `amount`, `currency`, `transaction_type`, `timestamp`, `risk_level` | Visible in logs, telemetry, APIs, and audit blocks. |
| **2** | **SENSITIVE** | `account_id`, `customer_id` | Masked in logs/events (`ACC-*****6789`). Surrogate tokenized for internal joins. |
| **3** | **HIGHLY SENSITIVE** | `card_number`, `bank_account_number`, `government_id`, `phone_number`, `email` | Deeply masked (`************5678`, `c****@domain.com`). Never stored in cleartext. |

### Technical Distinction: Masking vs Tokenization vs Hashing vs Encryption

- **Visual Masking:** Irreversible redaction replacing middle/identifying characters with asterisks (e.g. `ACC-*****6789`). Used for logs, public API outputs, and audit events.
- **Deterministic Tokenization:** Reversible surrogate identifiers (`tok_<hex16>`) generated via HMAC-SHA256 with vault isolation (`TokenVault`). Enables authorized internal correlation without exposing raw identifiers.
- **Canonical Hashing:** One-way SHA-256 digest of key-order-independent, whitespace-stripped canonical JSON (`canonicalize_json`). Used for cryptographic proof of integrity and tamper detection.
- **Encryption:** Reversible confidentiality ciphertext using symmetric or asymmetric secret keys. (Not conflated with tokenization).

### Cryptographic Hash-Chain Ledger

```text
Genesis Previous Hash: 0000000000000000000000000000000000000000000000000000000000000000
       │
       ▼
Block 1: [Seq: 1, Event: EVT-001, PayloadHash: P1, PrevHash: GENESIS, EventHash: H1]
       │
       ▼
Block 2: [Seq: 2, Event: EVT-002, PayloadHash: P2, PrevHash: H1,      EventHash: H2]
       │
       ▼
Block 3: [Seq: 3, Event: EVT-003, PayloadHash: P3, PrevHash: H2,      EventHash: H3]
       │
       ▼
...
```

### Security Threat Matrix & Protection Verification

| Threat | Protection Mechanism | Verification Test | Result |
| :--- | :--- | :--- | :---: |
| **PII Data Leakage** | Deep recursive masking (`sanitize_payload`), surrogate tokenization (`TokenVault`) | `test_sanitize_payload_deep_non_mutating`, `test_mask_*` | **PROTECTED** |
| **Audit Payload Tampering** | Canonical payload hash verification against stored block | `test_attack_1_audit_payload_tampering` | **DETECTED** (`PAYLOAD_HASH_MISMATCH`) |
| **Block Header Tampering** | Recomputed event hash verification (`compute_event_hash`) | `test_attack_2_event_hash_tampering` | **DETECTED** (`EVENT_HASH_MISMATCH`) |
| **Previous Hash Alteration**| Sequential hash-link verification | `test_attack_3_previous_hash_tampering` | **DETECTED** (`PREVIOUS_HASH_MISMATCH`) |
| **Audit Event Deletion** | Monotonic sequence continuity check | `test_attack_4_event_deletion` | **DETECTED** (`SEQUENCE_GAP`) |
| **Event Reordering** | Previous hash linkage and monotonic sequence check | `test_attack_5_event_reordering` | **DETECTED** (`SEQUENCE_GAP` / `DUPLICATE`) |
| **Fake Client Decision** | Trust boundary stripping of client-provided decision fields | `test_trust_boundary_strips_fake_decision` | **NEUTRALIZED** (Server Computed) |
| **Fake Fraud Score Injection**| Trust boundary stripping of client-provided fraud score | `test_trust_boundary_strips_fake_fraud_score` | **NEUTRALIZED** (Server Computed) |
| **Oversized Input Attack** | Security Gateway size limiter (`MAX_REQUEST_SIZE_BYTES = 1 MB`) | `test_gateway_oversized_payload` | **REJECTED** (`REQUEST_SIZE_EXCEEDED`) |
| **SQL Injection / Malformed ID**| Regex structural validation (`^[a-zA-Z0-9_-]{3,64}$`) | `test_gateway_invalid_characters_transaction_id` | **REJECTED** (Validation Failure) |
| **Sensitive Error Leakage** | Safe sanitized error responses; suppressed stack traces & paths | `test_api_no_raw_pii_leakage_in_error` | **PROTECTED** (Zero stack traces) |
| **Transaction Mutation** | Transaction integrity hash (`hash_transaction`) | `test_hash_transaction_sensitivity` | **PROTECTED** (Hash mismatch on edit) |

### Zero-Trust Security Failure Policy

If cryptographic audit integrity verification fails or an audit chain exhibits tampering:
- The system **NEVER** silently falls back to approval.
- The system automatically triggers the safe state: `MANUAL_REVIEW`.
- A high-priority security event `AUDIT_INTEGRITY_VERIFICATION_FAILED` is logged to the security event bus.
- Operators are immediately alerted through the security verification telemetry endpoint (`GET /security/audit/verify`).


