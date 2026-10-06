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

## 3. Component Status Matrix (Day 1 & Day 2 vs. Planned)

| Component | Status | Day 1-2 Implementation Scope | Planned Full Scope (Days 3–15) |
| :--- | :--- | :--- | :--- |
| **Transaction Sources** | **IMPLEMENTED (Day 2)** | Deterministic synthetic generator `TransactionGenerator` with multi-channel and multi-currency simulation. | Continuous streaming load injector with anomaly burst modes. |
| **Event Ingestion Layer** | **IMPLEMENTED (Day 2)** | `TransactionService` coordinating validation, SQLite persistence, and decoupled event publishing via `POST /transactions` and `POST /transactions/simulate`. | Distributed streaming ingestion gateways with TLS token authentication. |
| **Event Publisher** | **IMPLEMENTED (Day 2)** | `EventPublisher` ABC with concrete `LocalEventPublisher` routing to `EventOrchestrator`. | `KafkaEventPublisher` implementing the same `EventPublisher` interface with zero code refactoring needed. |
| **Event Orchestrator** | **IMPLEMENTED (Day 1-2)** | In-memory `EventOrchestrator` with typed event registration, execution metrics (`events_published`, `events_handled`). | Distributed Apache Kafka topics, consumer groups, partition management, DLQs. |
| **Reconciliation Agent** | **PLANNED** | Placeholder acknowledgment handler and validated `LedgerEntry` model. | Deterministic matching (Day 4) and fuzzy reconciliation algorithms (Day 5). |
| **Fraud Agent** | **PLANNED** | Placeholder handler and schema foundation in `fraud_detection/models.py`. | Real-time feature extraction, velocity checking, ML anomaly scoring (Day 6). |
| **Risk Agent** | **PLANNED** | Directory initialized (`agents/risk_agent`). | Multi-agent risk profiling & aggregate score weighting (Day 7). |
| **Decision Engine** | **PLANNED** | Direct API response contract. | Multi-agent consensus engine and automated disposition logic (Day 7). |
| **Legacy System Simulator** | **IMPLEMENTED (Day 1)** | Deterministic `LegacyBankingSystem` simulating host approvals/rejections. | Full Shadow Deployment dual-run engine and discrepancy analysis (Day 9). |
| **Investigation Queue** | **PLANNED** | Not implemented today. | Alert queuing, operator review hooks, and case management (Day 11). |
| **Security Layer** | **IMPLEMENTED (Day 1)** | Account ID masking (`mask_account_id`), log sanitization, zero-trust log hygiene. | Mutual TLS, cryptographic signing, API gateway policies (Day 8). |
| **Privacy Layer** | **PLANNED** | Zero-trust logging rule: strict ban on raw PII / account numbers in logs. | Differential privacy queries and cryptographic data tokenization (Day 8). |
| **Database** | **IMPLEMENTED (Day 1-2)** | Local SQLite engine with idempotent schemas and sequence auto-detection for transactions & ledger records. | PostgreSQL / TimescaleDB support with high-throughput indexing (Day 8+). |
| **Monitoring & Telemetry** | **IMPLEMENTED (Day 2)** | Structured logging + live `EventOrchestrator` execution metrics (`GET /events/metrics`). | Distributed OpenTelemetry tracing and Prometheus metric exporters (Day 12). |
| **Dashboard** | **PLANNED** | Directory initialized (`dashboard/`). | Real-time operator triage console and reconciliation metrics UI (Day 11). |


---

## 3. Data Flow & Zero-Trust Boundary

### Day 1 Implemented Flow:
1. **Transaction Submission:** A client submits a JSON payload to `POST /transactions/validate`.
2. **Schema & Business Validation:** Pydantic `Transaction` validates fields, positive amounts, 3-letter currency codes, and non-empty IDs.
3. **Zero-Trust Sanitization:** Account identifiers are masked via `mask_account_id()` prior to output to system logs.
4. **Local Event Dispatch:** The transaction is converted into an in-memory `Event` and routed via `EventOrchestrator` to registered placeholder handlers.
5. **Database Storage:** Tables `transactions` and `ledger_entries` are maintained in SQLite with parameterized queries preventing SQL injection.
6. **Legacy Host Simulation:** `LegacyBankingSystem` provides deterministic reference approvals/rejections (`00` vs `51` / `05`).

---

## 4. Architectural Decisions

1. **No Docker / Local-First Python:** Entire Day 1 foundation is executable on standard Windows workstations using native Python virtual environments.
2. **Decoupled Event Orchestration:** The `EventOrchestrator` defines an interface identical to pub/sub brokers, allowing drop-in Kafka replacement without refactoring downstream agents.
3. **Separation of Transactions and Ledger Entries:** Transaction requests and ledger records are distinct Pydantic models to mirror real-world core banking and double-entry general ledgers.
4. **Data Hygiene First:** Logging never prints raw account numbers; zero-trust principles are applied at the earliest design stage.
