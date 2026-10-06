# ZT-LedgerGuard: 15-Day Engineering Roadmap

Target: **Virtusa Jatayu Season 6 — Forward Deployment Engineering (FDE)**  
Domain: **FinTech / Core Banking & Payment Systems**

This roadmap outlines the iterative engineering milestones for **ZT-LedgerGuard (Zero-Trust AI Ledger Reconciliation & Real-Time Fraud Triage Platform)**.

---

### DAY 1: Foundation and Architecture *(Completed)*
- Project structure setup and Windows-native Python virtual environment.
- Core Pydantic domain models: `Transaction`, `LedgerEntry`, and `Event`.
- In-memory `EventOrchestrator` foundation with decoupled handler registration.
- SQLite database foundation with idempotent table creation and parameterized access.
- Zero-Trust security hygiene rules and account masking utilities (`mask_account_id`).
- Deterministic `LegacyBankingSystem` simulator.
- Minimal FastAPI service with `/health` and `/transactions/validate` endpoints.
- 100% passing test suite across 12 foundation validation criteria.

---

### DAY 2: Transaction Generator and Event Streaming Foundation *(Completed)*
- Realistic synthetic transaction generator (`TransactionGenerator`) with deterministic seed support.
- Multi-currency, multi-channel payment rail simulations (ACH, Fedwire, SWIFT, SEPA, Cards).
- Event streaming abstractions (`EventPublisher` ABC and `LocalEventPublisher`) decoupling business services from Kafka.
- Canonical `TRANSACTION_RECEIVED` event model with correlation tracking and masked payload logging.
- `TransactionService` coordinating ingestion, Pydantic schema validation, SQLite persistence, and decoupled publishing.
- FastAPI REST endpoints: `POST /transactions`, `POST /transactions/simulate`, and `GET /events/metrics`.
- CLI simulation utility `scripts/run_transaction_stream.py`.
- Event execution metrics tracking (`events_published`, `events_handled`, `unknown_events`).
- 100% test coverage with 38 passing tests (18 Day 1 + 20 Day 2).

---

### DAY 3: Event-Driven Orchestration & Processing Pipeline *(Completed)*
- Canonical `EventType` enum in `orchestrator/event_types.py` defining standard financial event vocabulary.
- Enhanced `Event` envelope supporting `correlation_id`, parent `causation_id`, and `retry_count`.
- Extensible `BaseEventHandler` interface and dynamic handler registry (`register_handler`, `unregister_handler`, multi-handler).
- Asynchronous-ready design seamlessly executing sync functions and async coroutines without blocking.
- Decoupled `ProcessingPipeline` chaining validation, reconciliation, fraud analysis, risk analysis, and final decision stages.
- Thread-safe `WorkflowTracker` in `orchestrator/workflow_state.py` tracking distributed stage state per correlation ID.
- Event deduplication and idempotency preventing duplicate event execution (`_processed_event_ids`).
- Bounded retry mechanism with configurable `MAX_EVENT_RETRIES` (default: 3) and graceful exception handling.
- Financial `EventAuditTrail` recording chronological state transitions (`RECEIVED`, `PROCESSING`, `COMPLETED`, `FAILED`, `DUPLICATE`).
- REST endpoints: `GET /workflows/{correlation_id}`, `GET /events/{correlation_id}`, `POST /events/process`, `GET /events/metrics`.
- 100% test pass rate with 63 total tests (18 Day 1 + 20 Day 2 + 25 Day 3).
- 100-transaction performance benchmark (905 events, 0 failures, 5 duplicates caught, 3,944 events/sec).

---

### DAY 4: Ledger Reconciliation Engine *(Completed)*
- Deterministic, explainable financial reconciliation engine comparing banking transactions with core ledger entries (zero LLM / zero ML).
- Seven deterministic evaluation rules: Transaction ID, Reference ID, Account ID, Decimal Amount, ISO 4217 Currency, Timestamp Tolerance (300s window), and Debit/Credit Direction.
- Strict Python `Decimal` money comparisons with banker's rounding to eliminate floating-point arithmetic errors.
- Weighted scoring system with normalized score between 0.0 and 1.0 (ID/Reference: 30%, Account: 20%, Amount: 25%, Currency: 10%, Timestamp: 10%, Direction: 5%).
- Controlled status classifications: `MATCHED`, `PARTIAL_MATCH`, `MISMATCHED`, and `NOT_FOUND`.
- Explainable evidence generation attaching human-readable verification records and exact variance messages.
- Decoupled `LedgerRepository` abstraction with SQLite persistence for ledger entries and reconciliation results.
- `LedgerGenerator` synthetic data generator producing deterministic exact, partial, mismatched, and missing scenarios.
- Event pipeline integration: `ReconciliationService` listening for `TRANSACTION_RECONCILIATION_REQUESTED` and emitting `TRANSACTION_RECONCILED` with correlation and causation tracking.
- Workflow stage state tracking updating `reconciliation` stage from `IN_PROGRESS` to `COMPLETED` / `FAILED`.
- REST endpoints: `POST /reconciliation/run`, `GET /reconciliation/{reconciliation_id}`, and `GET /reconciliation/metrics`.
- 100% test pass rate with 98 total tests (18 Day 1 + 20 Day 2 + 25 Day 3 + 35 Day 4).
- 1,000-record benchmark completed at ~4,800 records/sec throughput.

---

### DAY 5: Fuzzy Ledger Reconciliation & Intelligent Candidate Matching *(Completed)*
- Deterministic, explainable fuzzy candidate matching layer strictly decoupled from final financial verification.
- Safe field normalization in `reconciliation/normalization.py`: reference strings stripped and uppercased, currencies ISO 4217 validated, account numbers strictly preserved without altering meaningful digits.
- Configurable candidate scoring in `reconciliation/fuzzy_matcher.py` with `FuzzyMatchWeights` (Reference: 30%, Account: 25%, Decimal Amount: 25%, Timestamp Proximity: 15%, Currency: 5%).
- Non-negotiable hard constraints: mismatched account numbers or incompatible currencies immediately rejected with zero score, preventing unauthorized candidate matches.
- Ambiguity detection with configurable `AMBIGUITY_MARGIN` (0.03): competing candidates flag selection as `AMBIGUOUS` and set `requires_manual_review = True`.
- Two-stage reconciliation workflow: Stage 1 Candidate Discovery via `CandidateService` -> `FUZZY_CANDIDATE_IDENTIFIED` event -> Stage 2 Authoritative Financial Verification via deterministic `ReconciliationEngine`.
- Enriched `ReconciliationStatus` with `AMBIGUOUS` state and `requires_manual_review` boolean flag.
- Extended SQLite persistence with indexed candidate lookup methods (`find_candidates_by_account`, `find_candidates_by_reference`, `find_candidates_by_amount`, `find_candidates_by_time_window`).
- REST endpoints: `POST /reconciliation/candidates` and `GET /reconciliation/candidates/{transaction_id}`.
- 47 new comprehensive tests in `tests/test_day5.py` covering normalization, scoring, hard constraints, ambiguity, repository, workflow integration, APIs, and adversarial attacks (145/145 total suite passing).
- 10,000-record benchmark in `scripts/benchmark_day5.py` achieving 3,073+ records/sec throughput.

---

### DAY 6: Fraud Detection Agent *(Completed)*
- Autonomous, deterministic, and explainable Fraud Detection Agent (`agents/fraud_agent/fraud_agent.py`) operating without external LLM dependencies, Gemini, Ollama, LangChain, Kafka, or Docker.
- Deterministic feature extraction engine (`fraud_detection/features.py`) computing 7 canonical features:
  1. Amount anomaly (ratio to account historical baseline average).
  2. Transaction velocity (windowed transaction burst count in seconds).
  3. Rapid repeated transactions (identical amount and short interval clustering).
  4. Unusual transaction time (configurable off-hours detection, default 22:00–06:00).
  5. Account behavior deviation (historical ceiling/multiplier deviation).
  6. Reconciliation anomaly integration (`MISMATCHED`, `NOT_FOUND`, `AMBIGUOUS`, `PARTIAL_MATCH`).
  7. Repeated failed activity (prior failed validation/reconciliation counts).
- Account profile baseline engine (`fraud_detection/profile.py`) tracking historical transaction counts, average amounts, maximum ceilings, common hours, currencies, and directions without machine learning.
- Rule engine (`fraud_detection/rules.py`) with 7 independent deterministic rules generating transparent `RuleEvidence`:
  - `HIGH_AMOUNT_ANOMALY` (max weight: 25)
  - `HIGH_TRANSACTION_VELOCITY` (max weight: 25)
  - `RAPID_REPEATED_TRANSACTION` (max weight: 15)
  - `UNUSUAL_TRANSACTION_TIME` (max weight: 10)
  - `ACCOUNT_BEHAVIOR_DEVIATION` (max weight: 15)
  - `RECONCILIATION_ANOMALY` (max weight: 10)
  - `REPEATED_FAILED_ACTIVITY` (max weight: 10)
- Risk engine (`fraud_detection/risk_engine.py`) with anti-double-counting safeguards (prevents artificial score inflation on correlated amount anomaly spikes), capped risk score (0–100), and configurable risk tiers:
  - `LOW` (0–24): `requires_manual_review = False`, `block_recommended = False`
  - `MEDIUM` (25–49): `requires_manual_review = False`, `block_recommended = False`
  - `HIGH` (50–74): `requires_manual_review = True`, `block_recommended = False`
  - `CRITICAL` (75–100): `requires_manual_review = True`, `block_recommended = True` (transaction freeze recommendation without modifying or deleting data)
- Structured Pydantic result model (`fraud_detection/fraud_result.py`) validating strict [0, 100] bounds.
- Event pipeline integration: `FraudService` listening for `FRAUD_ANALYSIS_REQUESTED` and emitting `FRAUD_ANALYSIS_COMPLETED` with account number masking and causation tracking.
- Database persistence: `fraud_analyses` table in SQLite with indexing and aggregation queries.
- REST API endpoints:
  - `POST /fraud/analyze`: On-demand fraud evaluation.
  - `GET /fraud/{transaction_id}`: Retrieve stored fraud analysis result and explainable evidence.
  - `GET /fraud/metrics`: Aggregated fraud triage metrics (total analyzed, risk tier counts, manual review count, average score).
- 50 new comprehensive unit, integration, event, and adversarial tests (`tests/test_day6.py`), achieving 195/195 total passing tests.
- 10,000-record performance benchmark (`scripts/benchmark_day6.py`) achieving ~4,588 transactions/second (0.22 ms/tx) locally.

---

### DAY 7: Multi-Agent Coordination & Risk Decision Consensus *(Completed)*
- Standardized `BaseAgent` abstract class and auditable `AgentExecutionRecord` tracking execution latency, audit evidence, and parent correlation IDs.
- Four specialized domain agents:
  1. `ReconciliationAgent`: Two-stage fuzzy/deterministic candidate discovery and verification.
  2. `FraudDetectionAgent`: 7-feature deterministic risk grader with account profile baselining.
  3. `PolicyAgent`: Regulatory and business rule evaluator (`CRITICAL_FRAUD_RISK`, `HIGH_FRAUD_RISK`, `RECONCILIATION_AMBIGUITY`, `RECONCILIATION_MISMATCH`, `MISSING_LEDGER_ENTRY`, `COMBINED_HIGH_RISK`, `TRANSACTION_LIMIT_BREACH`).
  4. `DecisionAgent` & `ConsensusEngine`: Centralized arbitrator enforcing deterministic priority order: `BLOCK_RECOMMENDED > MANUAL_REVIEW > APPROVE`.
- Deterministic Consensus Rules:
  - Rule 1: Critical fraud ($\ge 75$) mandates `BLOCK_RECOMMENDED` overriding clean reconciliation match.
  - Rule 2: Elevated fraud ($50 \le \text{score} < 75$) mandates `MANUAL_REVIEW`.
  - Rule 3: Ambiguous candidate match mandates `MANUAL_REVIEW`.
  - Rule 4: Missing ledger entry (`NOT_FOUND`) mandates `MANUAL_REVIEW`.
  - Rule 5: Ledger mismatch coupled with elevated fraud mandates `BLOCK_RECOMMENDED` (Compound Risk).
  - Rule 6: Clean transaction (matched, low fraud, clear policy) yields `APPROVE`.
  - Rule 7: Moderate risk with clean reconciliation approves with preserved audit trail.
- Agreement confidence scoring formula bounded deterministically in $[0.50, 1.0]$.
- Multi-agent disagreement detection tracking posture divergences (e.g., Recon APPROVE vs. Fraud BLOCK).
- Zero-Trust Safeguards:
  - Fail-Safe Invariant: `Agent Failure != Approval` forces `MANUAL_REVIEW` and downgrades confidence if an agent fails.
  - Financial Immutability: Transaction amounts, accounts, currencies, and agent outputs are strictly non-mutating.
- `AgentCoordinator` orchestrator managing idempotency, masked event emissions (`CONSENSUS_DECISION_REQUESTED`, `CONSENSUS_DECISION_COMPLETED`), and workflow stage updates.
- SQLite persistence in indexed `decision_results` table.
- REST API endpoints: `POST /decision/analyze`, `GET /decision/metrics`, `GET /decision/{transaction_id}`.
- 46 new unit, integration, and adversarial tests (`tests/test_day7.py`) with 241/241 total passing tests across the entire test suite.
- 10,000-record benchmark (`scripts/benchmark_day7.py`): 10,000 transactions processed in 1,153s through all 4 agents with full SQLite writes and event logging, 75.37% APPROVE, 24.63% MANUAL_REVIEW, 0% BLOCK_RECOMMENDED, Average Confidence: 0.9475, 0 system failures.
- 5-Scenario interactive consensus demo (`scripts/demo_day7_consensus.py`) passing all verification assertions.

---

### DAY 8: Zero-Trust Security and PII Protection
- Cryptographic verification of ledger entries (hash-chain / Merkle ledger audits).
- Differential privacy mechanisms and homomorphic / tokenized identifier queries.
- Least-privilege agent communication boundaries and mutual identity verification.

---

### DAY 9: Legacy Banking Simulation and Shadow Deployment
- Shadow deployment pipeline: streaming production-grade transactions simultaneously to legacy host and ZT-LedgerGuard.
- Discrepancy comparator analyzing legacy batch outputs vs. real-time AI triage.
- Dual-run regression reporting and drift metrics.

---

### DAY 10: Self-Healing and Failure Recovery
- Automated remediation workflows for transient ledger reconciliation failures.
- Auto-reversal generation and accounting adjustment proposal pipeline.
- Network split / transient failure circuit breakers.

---

### DAY 11: Real-Time Dashboard
- Operations & triage control dashboard.
- Live stream visualization of transactions, ledger matches, anomalies, and triage queues.
- Manual audit review tools for human-in-the-loop compliance officers.

---

### DAY 12: Integration and End-to-End Workflow
- Full integration testing linking ingestion, event bus, multi-agent evaluation, legacy shadow mode, and dashboard.
- Stress testing under simulated burst traffic and concurrent payment rails.

---

### DAY 13: Security and Failure Testing
- Adversarial simulation: ledger tampering, malicious transaction spoofing, race conditions.
- Chaos engineering: dropped events, delayed ledger postings, out-of-order batches.
- Red team audit of zero-trust boundaries and masking efficacy.

---

### DAY 14: Deployment Preparation and Documentation
- Production packaging and cross-platform installation scripts.
- Operational runbooks, API documentation, architectural compliance proofs.
- Virtusa Jatayu competition submission artifacts.

---

### DAY 15: Demo, Presentation, and Final Polishing
- Live demonstration scenarios: real-time fraud catch, auto-reconciliation, legacy shadow diffing.
- Executive architecture slide deck and video demonstration.
- Final code audit and benchmark validation.
