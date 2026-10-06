"""Database Foundation for ZT-LedgerGuard.

Provides lightweight SQLite connection handling, schema creation, and parameterized queries.
Designed for local execution on Windows without Docker or external DB services.
PostgreSQL/TimescaleDB connectors will build upon this contract in later phases.
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from reconciliation.models import LedgerEntry, Transaction


def parse_db_path(database_url: Optional[str] = None) -> str:
    """Extract SQLite filesystem path from DATABASE_URL or environment variable."""
    url = database_url or os.getenv("DATABASE_URL", "sqlite:///zt_ledgerguard.db")
    if url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "")
    return url


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Create a new SQLite database connection with row factory enabled.

    Args:
        db_path: Explicit file path or :memory:. If None, resolved from config.

    Returns:
        sqlite3.Connection configured with sqlite3.Row.
    """
    resolved_path = parse_db_path(db_path) if db_path is not None else parse_db_path()
    conn = sqlite3.connect(resolved_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Optional[str] = None) -> None:
    """Initialize database tables for transactions, ledger entries, and reconciliation results.

    Idempotent schema initialization using IF NOT EXISTS.
    """
    resolved_path = parse_db_path(db_path) if db_path is not None else parse_db_path()
    with sqlite3.connect(resolved_path) as conn:
        cursor = conn.cursor()

        # Transactions table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                transaction_type TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL,
                transaction_timestamp TEXT NOT NULL,
                source_system TEXT NOT NULL,
                destination_system TEXT NOT NULL,
                reference_id TEXT,
                created_at TEXT NOT NULL
            );
            """
        )

        # Ledger entries table (decoupled from transactions table to support async external feeds)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ledger_entries (
                ledger_entry_id TEXT PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL,
                debit_credit TEXT NOT NULL,
                reference_id TEXT,
                timestamp TEXT NOT NULL,
                source_system TEXT NOT NULL
            );
            """
        )

        # Reconciliation results table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS reconciliation_results (
                reconciliation_id TEXT PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                ledger_entry_id TEXT,
                correlation_id TEXT,
                status TEXT NOT NULL,
                score REAL NOT NULL,
                matched_fields TEXT NOT NULL,
                mismatched_fields TEXT NOT NULL,
                mismatch_reasons TEXT NOT NULL,
                evidence TEXT,
                requires_manual_review INTEGER DEFAULT 0,
                timestamp TEXT NOT NULL
            );
            """
        )

        # Dynamic schema migration: add requires_manual_review if missing
        try:
            cursor.execute("ALTER TABLE reconciliation_results ADD COLUMN requires_manual_review INTEGER DEFAULT 0;")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Performance indexes for reconciliation & auditing
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transactions_account_id
            ON transactions(account_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ledger_transaction_id
            ON ledger_entries(transaction_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ledger_reference_id
            ON ledger_entries(reference_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ledger_account_id
            ON ledger_entries(account_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ledger_amount
            ON ledger_entries(amount);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_recon_transaction_id
            ON reconciliation_results(transaction_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_recon_correlation_id
            ON reconciliation_results(correlation_id);
            """
        )

        # Fraud analyses table (Day 6)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS fraud_analyses (
                id TEXT PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                fraud_score INTEGER NOT NULL,
                risk_level TEXT NOT NULL,
                status TEXT NOT NULL,
                requires_manual_review INTEGER NOT NULL,
                block_recommended INTEGER NOT NULL,
                indicators_json TEXT,
                evidence_json TEXT,
                created_at TEXT NOT NULL
            );
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fraud_transaction_id
            ON fraud_analyses(transaction_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fraud_risk_level
            ON fraud_analyses(risk_level);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fraud_created_at
            ON fraud_analyses(created_at);
            """
        )

        # Multi-agent decision results table (Day 7)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS decision_results (
                id TEXT PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                correlation_id TEXT,
                decision TEXT NOT NULL,
                confidence REAL NOT NULL,
                requires_manual_review INTEGER NOT NULL,
                block_recommended INTEGER NOT NULL,
                agent_results_json TEXT,
                policy_results_json TEXT,
                decision_reasons_json TEXT,
                disagreements_json TEXT,
                created_at TEXT NOT NULL
            );
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_decision_transaction_id
            ON decision_results(transaction_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_decision_correlation_id
            ON decision_results(correlation_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_decision_decision
            ON decision_results(decision);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_decision_created_at
            ON decision_results(created_at);
            """
        )

        # Tamper-evident audit hash-chain table (Day 8)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_chain (
                sequence_number INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                previous_hash TEXT NOT NULL,
                event_hash TEXT NOT NULL UNIQUE,
                sanitized_payload TEXT,
                integrity_status TEXT DEFAULT 'VALID',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audit_chain_event_id
            ON audit_chain(event_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audit_chain_event_hash
            ON audit_chain(event_hash);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audit_chain_created_at
            ON audit_chain(created_at);
            """
        )
        conn.commit()


def insert_transaction(conn: sqlite3.Connection, transaction: Transaction) -> None:
    """Insert or replace a validated transaction record using safe parameterized query."""
    query = """
        INSERT OR REPLACE INTO transactions (
            transaction_id, account_id, transaction_type, amount, currency,
            transaction_timestamp, source_system, destination_system,
            reference_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        transaction.transaction_id,
        transaction.account_id,
        transaction.transaction_type,
        transaction.amount,
        transaction.currency,
        transaction.transaction_timestamp.isoformat(),
        transaction.source_system,
        transaction.destination_system,
        transaction.reference_id,
        transaction.created_at.isoformat(),
    )
    with conn:
        conn.execute(query, params)


def get_transaction_by_id(conn: sqlite3.Connection, transaction_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve a transaction record by its unique ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM transactions WHERE transaction_id = ?", (transaction_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def insert_ledger_entry(conn: sqlite3.Connection, entry: LedgerEntry) -> None:
    """Insert or replace a validated ledger entry record using safe parameterized query."""
    query = """
        INSERT OR REPLACE INTO ledger_entries (
            ledger_entry_id, transaction_id, account_id, amount, currency,
            debit_credit, reference_id, timestamp, source_system
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    params = (
        entry.ledger_entry_id,
        entry.transaction_id,
        entry.account_id,
        entry.amount,
        entry.currency,
        entry.debit_credit,
        entry.reference_id,
        entry.timestamp.isoformat(),
        entry.source_system,
    )
    with conn:
        conn.execute(query, params)


def get_ledger_entries_by_transaction_id(conn: sqlite3.Connection, transaction_id: str) -> List[Dict[str, Any]]:
    """Retrieve all ledger entries associated with a transaction ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM ledger_entries WHERE transaction_id = ?", (transaction_id,))
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def save_transaction(transaction: Transaction, db_path: Optional[str] = None) -> None:
    """Save a transaction directly managing connection lifecycle.

    Convenience wrapper around insert_transaction.
    """
    with get_connection(db_path) as conn:
        insert_transaction(conn, transaction)



def get_transaction(transaction_id: str, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Retrieve a transaction by ID directly managing connection lifecycle.

    Convenience wrapper around get_transaction_by_id.
    """
    with get_connection(db_path) as conn:
        return get_transaction_by_id(conn, transaction_id)


def get_max_transaction_counter(db_path: Optional[str] = None) -> int:
    """Find the highest integer sequence number among existing TXN-xxxxxx records."""
    try:
        with get_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT transaction_id FROM transactions WHERE transaction_id LIKE 'TXN-%'")
            rows = cursor.fetchall()
            max_val = 0
            for row in rows:
                tx_id = row["transaction_id"]
                parts = tx_id.split("-")
                if len(parts) >= 2 and parts[1].isdigit():
                    max_val = max(max_val, int(parts[1]))
            return max_val
    except Exception:
        return 0


def get_ledger_entry_by_id(conn: sqlite3.Connection, ledger_entry_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve a single ledger entry by its unique ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM ledger_entries WHERE ledger_entry_id = ?", (ledger_entry_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_ledger_entries_by_reference_id(conn: sqlite3.Connection, reference_id: str) -> List[Dict[str, Any]]:
    """Retrieve all ledger entries matching an external reference ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM ledger_entries WHERE reference_id = ?", (reference_id,))
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def get_ledger_entries_by_account_id(
    conn: sqlite3.Connection,
    account_id: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Retrieve ledger entries matching an account ID."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM ledger_entries WHERE account_id = ? ORDER BY timestamp DESC LIMIT ?",
        (account_id, limit),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def get_ledger_entries_by_amount_range(
    conn: sqlite3.Connection,
    min_amount: float,
    max_amount: float,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Retrieve ledger entries within a numerical amount window."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM ledger_entries WHERE amount BETWEEN ? AND ? ORDER BY timestamp DESC LIMIT ?",
        (min_amount, max_amount, limit),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def get_ledger_entries_by_timestamp_range(
    conn: sqlite3.Connection,
    start_ts: str,
    end_ts: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Retrieve ledger entries posted within a UTC timestamp window."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM ledger_entries WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp DESC LIMIT ?",
        (start_ts, end_ts, limit),
    )
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def get_potential_candidate_ledger_entries(
    conn: sqlite3.Connection,
    transaction_id: str,
    reference_id: Optional[str] = None,
    account_id: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Retrieve union of potential candidate ledger entries across ID, reference, account, and amount."""
    cursor = conn.cursor()
    candidates: Dict[str, Dict[str, Any]] = {}

    # 1. Exact transaction_id
    cursor.execute("SELECT * FROM ledger_entries WHERE transaction_id = ?", (transaction_id,))
    for row in cursor.fetchall():
        candidates[row["ledger_entry_id"]] = dict(row)

    # 2. Reference match if available
    if reference_id and len(candidates) < limit:
        cursor.execute("SELECT * FROM ledger_entries WHERE reference_id = ?", (reference_id,))
        for row in cursor.fetchall():
            candidates[row["ledger_entry_id"]] = dict(row)

    # 3. Account ID match
    if account_id and len(candidates) < limit:
        remaining = limit - len(candidates)
        cursor.execute(
            "SELECT * FROM ledger_entries WHERE account_id = ? ORDER BY timestamp DESC LIMIT ?",
            (account_id, remaining),
        )
        for row in cursor.fetchall():
            candidates[row["ledger_entry_id"]] = dict(row)

    # 4. Amount window match if specified and limit not reached
    if min_amount is not None and max_amount is not None and len(candidates) < limit:
        remaining = limit - len(candidates)
        cursor.execute(
            "SELECT * FROM ledger_entries WHERE amount BETWEEN ? AND ? ORDER BY timestamp DESC LIMIT ?",
            (min_amount, max_amount, remaining),
        )
        for row in cursor.fetchall():
            candidates[row["ledger_entry_id"]] = dict(row)

    return list(candidates.values())[:limit]


def insert_reconciliation_result(conn: sqlite3.Connection, result_data: Dict[str, Any]) -> None:
    """Insert or replace a reconciliation result record."""
    query = """
        INSERT OR REPLACE INTO reconciliation_results (
            reconciliation_id, transaction_id, ledger_entry_id, correlation_id,
            status, score, matched_fields, mismatched_fields, mismatch_reasons,
            evidence, requires_manual_review, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        result_data["reconciliation_id"],
        result_data["transaction_id"],
        result_data.get("ledger_entry_id"),
        result_data.get("correlation_id"),
        result_data["status"],
        result_data["score"],
        result_data["matched_fields"],
        result_data["mismatched_fields"],
        result_data["mismatch_reasons"],
        result_data.get("evidence"),
        1 if result_data.get("requires_manual_review") else 0,
        result_data["timestamp"],
    )
    with conn:
        conn.execute(query, params)


def get_reconciliation_result_by_id(conn: sqlite3.Connection, reconciliation_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve a reconciliation result by its unique ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM reconciliation_results WHERE reconciliation_id = ?", (reconciliation_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_all_reconciliation_results(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Retrieve all reconciliation results for audit and metric computation."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM reconciliation_results ORDER BY timestamp DESC")
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def insert_fraud_analysis(conn: sqlite3.Connection, analysis_data: Dict[str, Any]) -> None:
    """Insert or replace a fraud analysis record."""
    query = """
        INSERT OR REPLACE INTO fraud_analyses (
            id, transaction_id, fraud_score, risk_level, status,
            requires_manual_review, block_recommended, indicators_json,
            evidence_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        analysis_data["id"],
        analysis_data["transaction_id"],
        int(analysis_data["fraud_score"]),
        analysis_data["risk_level"],
        analysis_data["status"],
        1 if analysis_data.get("requires_manual_review") else 0,
        1 if analysis_data.get("block_recommended") else 0,
        analysis_data.get("indicators_json", "[]"),
        analysis_data.get("evidence_json", "[]"),
        analysis_data["created_at"],
    )
    with conn:
        conn.execute(query, params)


def get_fraud_analysis_by_transaction_id(
    conn: sqlite3.Connection, transaction_id: str
) -> Optional[Dict[str, Any]]:
    """Retrieve the most recent fraud analysis for a transaction."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM fraud_analyses WHERE transaction_id = ? ORDER BY created_at DESC LIMIT 1",
        (transaction_id,),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


def get_fraud_analysis_by_id(
    conn: sqlite3.Connection, analysis_id: str
) -> Optional[Dict[str, Any]]:
    """Retrieve a fraud analysis record by its primary ID."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM fraud_analyses WHERE id = ?", (analysis_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_all_fraud_analyses(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Retrieve all fraud analyses ordered by timestamp."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM fraud_analyses ORDER BY created_at DESC")
    rows = cursor.fetchall()
    return [dict(row) for row in rows]


def get_fraud_metrics(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Calculate aggregated fraud assessment metrics."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            COUNT(*) as total_analyzed,
            SUM(CASE WHEN risk_level = 'LOW' THEN 1 ELSE 0 END) as low_count,
            SUM(CASE WHEN risk_level = 'MEDIUM' THEN 1 ELSE 0 END) as medium_count,
            SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) as high_count,
            SUM(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END) as critical_count,
            SUM(CASE WHEN requires_manual_review = 1 THEN 1 ELSE 0 END) as manual_review_count,
            SUM(CASE WHEN block_recommended = 1 THEN 1 ELSE 0 END) as block_recommended_count,
            AVG(fraud_score) as average_score
        FROM fraud_analyses
        """
    )
    row = cursor.fetchone()
    if not row or row["total_analyzed"] == 0:
        return {
            "total_analyzed": 0,
            "low_count": 0,
            "medium_count": 0,
            "high_count": 0,
            "critical_count": 0,
            "manual_review_count": 0,
            "block_recommended_count": 0,
            "average_score": 0.0,
        }
    return {
        "total_analyzed": row["total_analyzed"],
        "low_count": row["low_count"] or 0,
        "medium_count": row["medium_count"] or 0,
        "high_count": row["high_count"] or 0,
        "critical_count": row["critical_count"] or 0,
        "manual_review_count": row["manual_review_count"] or 0,
        "block_recommended_count": row["block_recommended_count"] or 0,
        "average_score": round(float(row["average_score"] or 0.0), 2),
    }


# ==============================================================================
# Multi-Agent Decision Results Persistence & Retrieval (Day 7)
# ==============================================================================

def insert_decision_result(conn: sqlite3.Connection, decision_data: Dict[str, Any]) -> None:
    """Insert or replace a multi-agent decision outcome in SQLite."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR REPLACE INTO decision_results (
            id,
            transaction_id,
            correlation_id,
            decision,
            confidence,
            requires_manual_review,
            block_recommended,
            agent_results_json,
            policy_results_json,
            decision_reasons_json,
            disagreements_json,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            decision_data["id"],
            decision_data["transaction_id"],
            decision_data.get("correlation_id"),
            decision_data["decision"],
            float(decision_data["confidence"]),
            1 if decision_data["requires_manual_review"] else 0,
            1 if decision_data["block_recommended"] else 0,
            json.dumps(decision_data.get("agent_results", {})),
            json.dumps(decision_data.get("triggered_policies", [])),
            json.dumps(decision_data.get("decision_reasons", [])),
            json.dumps(decision_data.get("disagreements", [])),
            decision_data.get("created_at") or decision_data.get("decision_timestamp"),
        ),
    )
    conn.commit()


def get_decision_result_by_transaction_id(
    conn: sqlite3.Connection, transaction_id: str
) -> Optional[Dict[str, Any]]:
    """Retrieve the latest multi-agent decision for a transaction ID."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM decision_results
        WHERE transaction_id = ?
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        (transaction_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    d = dict(row)
    if d.get("agent_results_json"):
        d["agent_results"] = json.loads(d["agent_results_json"])
    if d.get("policy_results_json"):
        d["triggered_policies"] = json.loads(d["policy_results_json"])
    if d.get("decision_reasons_json"):
        d["decision_reasons"] = json.loads(d["decision_reasons_json"])
    if d.get("disagreements_json"):
        d["disagreements"] = json.loads(d["disagreements_json"])
    d["requires_manual_review"] = bool(d["requires_manual_review"])
    d["block_recommended"] = bool(d["block_recommended"])
    return d


def get_decision_metrics(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Calculate aggregated decision metrics across multi-agent consensus evaluations."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            COUNT(*) as total_decisions,
            SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END) as approved_count,
            SUM(CASE WHEN decision = 'MANUAL_REVIEW' THEN 1 ELSE 0 END) as manual_review_count,
            SUM(CASE WHEN decision = 'BLOCK_RECOMMENDED' THEN 1 ELSE 0 END) as block_recommended_count,
            AVG(confidence) as avg_confidence,
            SUM(CASE WHEN disagreements_json IS NOT NULL AND disagreements_json != '[]' THEN 1 ELSE 0 END) as disagreement_count
        FROM decision_results
        """
    )
    row = cursor.fetchone()
    if not row or row["total_decisions"] == 0:
        return {
            "total_decisions": 0,
            "approved": 0,
            "manual_review": 0,
            "block_recommended": 0,
            "average_confidence": 0.0,
            "agent_disagreements": 0,
        }
    return {
        "total_decisions": row["total_decisions"],
        "approved": row["approved_count"] or 0,
        "manual_review": row["manual_review_count"] or 0,
        "block_recommended": row["block_recommended_count"] or 0,
        "average_confidence": round(float(row["avg_confidence"] or 0.0), 4),
        "agent_disagreements": row["disagreement_count"] or 0,
    }


# ==============================================================================
# Tamper-Evident Audit Hash-Chain Operations (Day 8)
# Zero-Trust Invariant: Append-only ledger; NO UPDATE or DELETE queries exposed.
# ==============================================================================

def insert_audit_chain_record(conn: sqlite3.Connection, record: Dict[str, Any]) -> None:
    """Insert a cryptographically linked audit chain record into SQLite.

    IMMUTABILITY INVARIANT:
    Records are append-only. Overwriting or updating existing audit blocks is forbidden.
    """
    cursor = conn.cursor()
    query = """
    INSERT INTO audit_chain (
        sequence_number,
        event_id,
        event_type,
        timestamp,
        payload_hash,
        previous_hash,
        event_hash,
        sanitized_payload,
        integrity_status
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    sanitized_json = (
        json.dumps(record["sanitized_payload"], sort_keys=True, separators=(",", ":"))
        if record.get("sanitized_payload") is not None
        else None
    )
    cursor.execute(
        query,
        (
            record["sequence_number"],
            record["event_id"],
            record["event_type"],
            record["timestamp"],
            record["payload_hash"],
            record["previous_hash"],
            record["event_hash"],
            sanitized_json,
            record.get("integrity_status", "VALID"),
        ),
    )
    conn.commit()


def get_audit_chain_records(
    conn: sqlite3.Connection,
    limit: Optional[int] = None,
    start_sequence: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Retrieve audit chain records ordered strictly by sequence_number ASC."""
    cursor = conn.cursor()
    query = """
    SELECT
        sequence_number,
        event_id,
        event_type,
        timestamp,
        payload_hash,
        previous_hash,
        event_hash,
        sanitized_payload,
        integrity_status,
        created_at
    FROM audit_chain
    """
    params = []
    if start_sequence is not None:
        query += " WHERE sequence_number >= ?"
        params.append(start_sequence)
    query += " ORDER BY sequence_number ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()
    results = []
    for row in rows:
        d = dict(row)
        if d.get("sanitized_payload"):
            try:
                d["sanitized_payload"] = json.loads(d["sanitized_payload"])
            except Exception:
                pass
        results.append(d)
    return results


def get_latest_audit_chain_record(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    """Retrieve the latest block in the audit chain."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            sequence_number,
            event_id,
            event_type,
            timestamp,
            payload_hash,
            previous_hash,
            event_hash,
            sanitized_payload,
            integrity_status,
            created_at
        FROM audit_chain
        ORDER BY sequence_number DESC
        LIMIT 1;
        """
    )
    row = cursor.fetchone()
    if not row:
        return None
    d = dict(row)
    if d.get("sanitized_payload"):
        try:
            d["sanitized_payload"] = json.loads(d["sanitized_payload"])
        except Exception:
            pass
    return d


def get_audit_chain_count(conn: sqlite3.Connection) -> int:
    """Return the total number of blocks in the audit chain."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM audit_chain;")
    row = cursor.fetchone()
    return row[0] if row else 0


def get_audit_chain_record_by_event_id(conn: sqlite3.Connection, event_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve a specific audit block by its event_id."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            sequence_number,
            event_id,
            event_type,
            timestamp,
            payload_hash,
            previous_hash,
            event_hash,
            sanitized_payload,
            integrity_status,
            created_at
        FROM audit_chain
        WHERE event_id = ?;
        """,
        (event_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    d = dict(row)
    if d.get("sanitized_payload"):
        try:
            d["sanitized_payload"] = json.loads(d["sanitized_payload"])
        except Exception:
            pass
    return d

