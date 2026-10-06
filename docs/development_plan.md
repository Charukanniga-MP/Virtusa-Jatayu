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

### DAY 6: Fraud Detection Agent
- Real-time fraud triage agent architecture.
- Feature extraction: velocity bursts, geolocation hops, anomaly patterns.
- Rule engine combined with preliminary machine-learning anomaly scoring.
- Triage prioritization: Low, Medium, High, Immediate Freeze.

---

### DAY 7: Multi-Agent Coordination
- Multi-agent consensus protocol between Reconciliation Agent, Fraud Agent, and Risk Agent.
- Conflict resolution engine: balancing ledger compliance with fraud freezing.
- Unified transaction disposition and decision engine.

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
