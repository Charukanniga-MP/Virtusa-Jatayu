"""Comprehensive Foundation Test Suite for ZT-LedgerGuard (Day 1).

Covers all 12 mandated test criteria:
1. Valid transaction
2. Invalid transaction amount
3. Invalid currency
4. Missing required field
5. Valid ledger entry
6. Event creation
7. Event routing
8. Unknown event handling
9. Account masking
10. Database initialization
11. Legacy system simulator
12. Health endpoint

All tests execute locally on Windows without Docker, Kafka, or external services.
"""

import os
import sqlite3
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from database.database import (
    get_connection,
    get_transaction_by_id,
    init_db,
    insert_ledger_entry,
    insert_transaction,
)
from legacy_simulation.legacy_system import LegacyBankingSystem
from main import app
from orchestrator.event_orchestrator import Event, EventOrchestrator
from reconciliation.models import LedgerEntry, Transaction
from security.security_utils import (
    mask_account_id,
    mask_sensitive_value,
    sanitize_log_message,
)


# ==============================================================================
# 1. Valid Transaction Test
# ==============================================================================
def test_valid_transaction():
    """Verify that a compliant transaction initializes without errors."""
    tx = Transaction(
        transaction_id="TX-10001",
        account_id="ACC-123456789012",
        transaction_type="TRANSFER",
        amount=1500.75,
        currency="USD",
        transaction_timestamp=datetime(2026, 10, 6, 10, 0, 0, tzinfo=timezone.utc),
        source_system="CORE_BANKING_API",
        destination_system="ACH_NETWORK",
        reference_id="REF-9988",
    )
    assert tx.transaction_id == "TX-10001"
    assert tx.amount == 1500.75
    assert tx.currency == "USD"
    assert tx.account_id == "ACC-123456789012"
    assert tx.created_at is not None


# ==============================================================================
# 2. Invalid Transaction Amount Test
# ==============================================================================
def test_invalid_transaction_amount():
    """Verify that negative transaction amounts trigger validation errors."""
    with pytest.raises(ValidationError) as exc_info:
        Transaction(
            transaction_id="TX-10002",
            account_id="ACC-123456789012",
            transaction_type="PAYMENT",
            amount=-50.00,  # Negative amount must fail
            currency="USD",
            transaction_timestamp=datetime.now(timezone.utc),
            source_system="ATM_NETWORK",
            destination_system="CORE_BANKING",
        )
    assert "greater than or equal to 0" in str(exc_info.value)


# ==============================================================================
# 3. Invalid Currency Test
# ==============================================================================
@pytest.mark.parametrize("invalid_currency", ["usd", "US", "USDD", "123", "US$", ""])
def test_invalid_currency(invalid_currency):
    """Verify that non-ISO 3-letter uppercase currency codes fail validation."""
    with pytest.raises(ValidationError) as exc_info:
        Transaction(
            transaction_id="TX-10003",
            account_id="ACC-123456789012",
            transaction_type="DEPOSIT",
            amount=100.00,
            currency=invalid_currency,
            transaction_timestamp=datetime.now(timezone.utc),
            source_system="BRANCH_TELLER",
            destination_system="CORE_BANKING",
        )
    assert "Currency must be a valid 3-letter uppercase" in str(exc_info.value)


# ==============================================================================
# 4. Missing Required Field Test
# ==============================================================================
def test_missing_required_field():
    """Verify that omitted required fields or blank values raise ValidationError."""
    # Omitted field
    with pytest.raises(ValidationError):
        Transaction(
            # transaction_id omitted
            account_id="ACC-123456789012",
            transaction_type="TRANSFER",
            amount=500.0,
            currency="USD",
            transaction_timestamp=datetime.now(timezone.utc),
            source_system="PORTAL",
            destination_system="GATEWAY",
        )

    # Empty whitespace string
    with pytest.raises(ValidationError):
        Transaction(
            transaction_id="   ",
            account_id="ACC-123456789012",
            transaction_type="TRANSFER",
            amount=500.0,
            currency="USD",
            transaction_timestamp=datetime.now(timezone.utc),
            source_system="PORTAL",
            destination_system="GATEWAY",
        )


# ==============================================================================
# 5. Valid Ledger Entry Test
# ==============================================================================
def test_valid_ledger_entry():
    """Verify that ledger entry model validates fields and debit/credit sides."""
    entry = LedgerEntry(
        ledger_entry_id="LED-9001",
        transaction_id="TX-10001",
        account_id="ACC-123456789012",
        amount=1500.75,
        currency="USD",
        debit_credit="debit",  # Case-insensitive, should normalize to DEBIT
        reference_id="REF-GL-11",
        timestamp=datetime.now(timezone.utc),
        source_system="GENERAL_LEDGER",
    )
    assert entry.ledger_entry_id == "LED-9001"
    assert entry.debit_credit == "DEBIT"
    assert entry.amount == 1500.75

    # Invalid debit/credit value
    with pytest.raises(ValidationError) as exc_info:
        LedgerEntry(
            ledger_entry_id="LED-9002",
            transaction_id="TX-10001",
            account_id="ACC-123456789012",
            amount=100.0,
            currency="USD",
            debit_credit="INVALID_SIDE",
            timestamp=datetime.now(timezone.utc),
            source_system="GENERAL_LEDGER",
        )
    assert "must be either 'DEBIT' or 'CREDIT'" in str(exc_info.value)


# ==============================================================================
# 6. Event Creation Test
# ==============================================================================
def test_event_creation():
    """Verify event construction with UUID generation and payload encapsulation."""
    event = Event(
        event_type="TRANSACTION_POSTED",
        source="test_suite",
        payload={"tx_id": "TX-10001", "amount": 250.0},
    )
    assert event.event_id is not None
    assert len(event.event_id) > 0
    assert event.correlation_id is not None
    assert event.event_type == "TRANSACTION_POSTED"
    assert event.payload["tx_id"] == "TX-10001"
    assert event.timestamp is not None


# ==============================================================================
# 7. Event Routing Test
# ==============================================================================
def test_event_routing():
    """Verify that orchestrator routes events to registered handlers."""
    orchestrator = EventOrchestrator()
    received_events = []

    def sample_handler(evt: Event):
        received_events.append(evt)
        return "PROCESSED_SUCCESS"

    orchestrator.register_handler("TEST_EVENT", sample_handler)

    test_event = Event(
        event_type="TEST_EVENT",
        source="unit_test",
        payload={"key": "value"},
    )
    results = orchestrator.publish(test_event)

    assert len(received_events) == 1
    assert received_events[0].event_id == test_event.event_id
    assert results == ["PROCESSED_SUCCESS"]


# ==============================================================================
# 8. Unknown Event Handling Test
# ==============================================================================
def test_unknown_event_handling():
    """Verify that unpublished/unknown event types do not raise fatal errors."""
    orchestrator = EventOrchestrator()
    orphan_event = Event(
        event_type="UNREGISTERED_EVENT_TYPE",
        source="unknown_producer",
        payload={},
    )
    # Must execute safely without throwing exceptions
    results = orchestrator.publish(orphan_event)
    assert results == []


# ==============================================================================
# 9. Account Masking Test
# ==============================================================================
def test_account_masking():
    """Verify zero-trust account identifier masking for logs and telemetry."""
    # Standard 12-digit account number
    masked = mask_account_id("123456789012")
    assert masked == "********9012"
    assert len(masked) == 12

    # Short account numbers
    assert mask_account_id("1234") == "***4"
    assert mask_account_id("12") == "*2"
    assert mask_account_id("") == ""
    assert mask_account_id(None) == ""

    # Arbitrary log message sanitization
    raw_log = "Processing payment for account 987654321098 from gateway."
    sanitized = sanitize_log_message(raw_log)
    assert "987654321098" not in sanitized
    assert "********1098" in sanitized


# ==============================================================================
# 10. Database Initialization Test
# ==============================================================================
def test_database_initialization(tmp_path):
    """Verify SQLite database table creation, insertion, and query retrieval."""
    test_db_path = str(tmp_path / "test_zt_ledger.db")
    init_db(test_db_path)

    conn = get_connection(test_db_path)
    cursor = conn.cursor()

    # Verify tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {row["name"] for row in cursor.fetchall()}
    assert "transactions" in tables
    assert "ledger_entries" in tables

    # Test parameterized transaction insert and fetch
    tx = Transaction(
        transaction_id="TX-DB-001",
        account_id="ACC-998877665544",
        transaction_type="PAYMENT",
        amount=750.25,
        currency="EUR",
        transaction_timestamp=datetime(2026, 10, 6, 11, 0, 0, tzinfo=timezone.utc),
        source_system="WEB_BANKING",
        destination_system="SEPA_RAILS",
        reference_id="REF-SEPA-1",
    )
    insert_transaction(conn, tx)

    fetched = get_transaction_by_id(conn, "TX-DB-001")
    assert fetched is not None
    assert fetched["transaction_id"] == "TX-DB-001"
    assert fetched["amount"] == 750.25
    assert fetched["currency"] == "EUR"

    # Test ledger entry insert
    entry = LedgerEntry(
        ledger_entry_id="LED-DB-001",
        transaction_id="TX-DB-001",
        account_id="ACC-998877665544",
        amount=750.25,
        currency="EUR",
        debit_credit="DEBIT",
        reference_id="REF-SEPA-1",
        timestamp=datetime.now(timezone.utc),
        source_system="CORE_GL",
    )
    insert_ledger_entry(conn, entry)

    cursor.execute("SELECT * FROM ledger_entries WHERE ledger_entry_id = ?", ("LED-DB-001",))
    row = cursor.fetchone()
    assert row is not None
    assert row["debit_credit"] == "DEBIT"

    conn.close()


# ==============================================================================
# 11. Legacy System Simulator Test
# ==============================================================================
def test_legacy_system_simulator():
    """Verify deterministic processing and rejections in the legacy banking host."""
    legacy = LegacyBankingSystem(host_id="TEST-CORE-HOST")

    # Normal valid transaction -> APPROVED
    tx_normal = Transaction(
        transaction_id="TX-LEG-01",
        account_id="ACC-112233445566",
        transaction_type="TRANSFER",
        amount=5000.00,
        currency="USD",
        transaction_timestamp=datetime.now(timezone.utc),
        source_system="ATM",
        destination_system="CORE",
    )
    resp_normal = legacy.process_transaction(tx_normal)
    assert resp_normal["status"] == "PROCESSED"
    assert resp_normal["response_code"] == "00"
    assert resp_normal["reason"] == "APPROVED"
    assert resp_normal["authorization_code"].startswith("AUTH-")

    # Transaction exceeding host limit ceiling -> REJECTED
    tx_over_limit = Transaction(
        transaction_id="TX-LEG-02",
        account_id="ACC-112233445566",
        transaction_type="TRANSFER",
        amount=2_000_000.00,
        currency="USD",
        transaction_timestamp=datetime.now(timezone.utc),
        source_system="WIRE",
        destination_system="CORE",
    )
    resp_over = legacy.process_transaction(tx_over_limit)
    assert resp_over["status"] == "REJECTED"
    assert resp_over["response_code"] == "51"
    assert resp_over["reason"] == "EXCEEDS_LEGACY_LIMIT"

    # Transaction on restricted account -> REJECTED
    tx_restricted = Transaction(
        transaction_id="TX-LEG-03",
        account_id="RESTRICTED-ACC-99",
        transaction_type="WITHDRAWAL",
        amount=100.00,
        currency="USD",
        transaction_timestamp=datetime.now(timezone.utc),
        source_system="BRANCH",
        destination_system="CORE",
    )
    resp_restricted = legacy.process_transaction(tx_restricted)
    assert resp_restricted["status"] == "REJECTED"
    assert resp_restricted["response_code"] == "05"
    assert resp_restricted["reason"] == "ACCOUNT_RESTRICTED"


# ==============================================================================
# 12. Health Endpoint & API Validation Test
# ==============================================================================
def test_health_endpoint():
    """Verify that FastAPI /health endpoint returns HTTP 200 and expected payload."""
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "ZT-LedgerGuard"


def test_api_transaction_validation():
    """Verify FastAPI /transactions/validate endpoint with valid & invalid bodies."""
    with TestClient(app) as client:
        # Valid payload
        valid_payload = {
            "transaction_id": "TX-API-001",
            "account_id": "123456789012",
            "transaction_type": "TRANSFER",
            "amount": 1250.00,
            "currency": "USD",
            "transaction_timestamp": "2026-10-06T10:00:00Z",
            "source_system": "MOBILE_APP",
            "destination_system": "CORE",
            "reference_id": "EXT-991",
        }
        resp = client.post("/transactions/validate", json=valid_payload)
        assert resp.status_code == 200
        assert resp.json()["status"] == "valid"
        assert resp.json()["transaction_id"] == "TX-API-001"

        # Invalid payload (negative amount)
        invalid_payload = dict(valid_payload, amount=-10.0)
        resp_invalid = client.post("/transactions/validate", json=invalid_payload)
        assert resp_invalid.status_code == 422
