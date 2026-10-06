"""Database Foundation for ZT-LedgerGuard.

Provides lightweight SQLite connection handling, schema creation, and parameterized queries.
Designed for local execution on Windows without Docker or external DB services.
PostgreSQL/TimescaleDB connectors will build upon this contract in later phases.
"""

from __future__ import annotations

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
                timestamp TEXT NOT NULL
            );
            """
        )

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


def insert_reconciliation_result(conn: sqlite3.Connection, result_data: Dict[str, Any]) -> None:
    """Insert or replace a reconciliation result record."""
    query = """
        INSERT OR REPLACE INTO reconciliation_results (
            reconciliation_id, transaction_id, ledger_entry_id, correlation_id,
            status, score, matched_fields, mismatched_fields, mismatch_reasons,
            evidence, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


