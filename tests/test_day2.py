"""Comprehensive Test Suite for Day 2: Transaction Generator & Event Streaming.

Covers all 20 required Day 2 validation criteria:
1. Generates requested number of transactions
2. Generated transactions pass Pydantic validation
3. Transaction IDs are unique
4. Same seed produces same sequence
5. Different seeds produce different sequences
6. Account IDs are synthetic
7. Transaction event is created correctly
8. Event contains correlation ID
9. Local publisher sends event to orchestrator
10. Event handler receives transaction event
11. Generated transaction is stored
12. Stored transaction can be retrieved
13. Unknown transaction returns appropriate result
14. POST /transactions works
15. Invalid transaction is rejected
16. POST /transactions/simulate works
17. Simulation count limit works
18. Invalid simulation count is rejected
19. Published event count increments
20. Handled event count increments
"""

from datetime import datetime, timezone
import pytest
from fastapi.testclient import TestClient

from data.transaction_generator import TransactionGenerator
from database.database import (
    get_transaction,
    init_db,
    save_transaction,
)
from kafka.local_event_publisher import (
    EVENT_TYPE_TRANSACTION_RECEIVED,
    LocalEventPublisher,
    create_transaction_event,
)
from main import app
from orchestrator.event_orchestrator import (
    Event,
    EventOrchestrator,
    transaction_event_handler,
)
from reconciliation.models import Transaction
from reconciliation.transaction_service import TransactionService


# ==============================================================================
# Transaction Generator Tests (1 - 6)
# ==============================================================================
def test_generator_generates_requested_count():
    """1. Verify that the generator produces the exact requested number of transactions."""
    generator = TransactionGenerator(seed=101)
    tx_stream = list(generator.generate_transactions(count=25))
    assert len(tx_stream) == 25


def test_generator_transactions_pass_validation():
    """2. Verify that all generated transactions comply with the Transaction Pydantic model."""
    generator = TransactionGenerator(seed=202)
    for tx in generator.generate_transactions(count=15):
        assert isinstance(tx, Transaction)
        assert tx.amount >= 0.0
        assert len(tx.currency) == 3
        assert tx.currency.isupper()
        assert tx.transaction_timestamp.tzinfo is not None
        assert len(tx.source_system) > 0
        assert len(tx.destination_system) > 0


def test_generator_transaction_ids_are_unique():
    """3. Verify that transaction IDs within a generated stream are completely unique."""
    generator = TransactionGenerator(seed=303)
    transactions = list(generator.generate_transactions(count=50))
    tx_ids = [tx.transaction_id for tx in transactions]
    assert len(tx_ids) == len(set(tx_ids))


def test_generator_same_seed_produces_same_sequence():
    """4. Verify that deterministic seeds produce identical transaction sequences."""
    gen1 = TransactionGenerator(seed=42)
    gen2 = TransactionGenerator(seed=42)

    stream1 = list(gen1.generate_transactions(count=10))
    stream2 = list(gen2.generate_transactions(count=10))

    for tx1, tx2 in zip(stream1, stream2):
        assert tx1.transaction_id == tx2.transaction_id
        assert tx1.account_id == tx2.account_id
        assert tx1.amount == tx2.amount
        assert tx1.currency == tx2.currency
        assert tx1.transaction_type == tx2.transaction_type


def test_generator_different_seeds_produce_different_sequences():
    """5. Verify that different seeds produce divergent transaction sequences."""
    gen1 = TransactionGenerator(seed=100)
    gen2 = TransactionGenerator(seed=200)

    stream1 = list(gen1.generate_transactions(count=10))
    stream2 = list(gen2.generate_transactions(count=10))

    amounts1 = [tx.amount for tx in stream1]
    amounts2 = [tx.amount for tx in stream2]
    assert amounts1 != amounts2


def test_generator_account_ids_are_synthetic():
    """6. Verify that account IDs adhere to the synthetic ACC-100xxx convention."""
    generator = TransactionGenerator(seed=505, account_pool_size=10)
    for tx in generator.generate_transactions(count=20):
        assert tx.account_id.startswith("ACC-")
        account_num = tx.account_id.replace("ACC-", "")
        assert account_num.isdigit()
        assert int(account_num) >= 100000


# ==============================================================================
# Event Publishing Tests (7 - 10)
# ==============================================================================
def test_transaction_event_created_correctly():
    """7. Verify canonical TRANSACTION_RECEIVED event structure and payload."""
    tx = Transaction(
        transaction_id="TXN-999901",
        account_id="ACC-100001",
        transaction_type="TRANSFER",
        amount=340.50,
        currency="USD",
        transaction_timestamp=datetime.now(timezone.utc),
        source_system="CORE_BANKING",
        destination_system="ACH_NETWORK",
    )
    event = create_transaction_event(tx)
    assert event.event_type == EVENT_TYPE_TRANSACTION_RECEIVED
    assert event.payload["transaction_id"] == "TXN-999901"
    assert "masked_account_id" in event.payload
    assert event.payload["amount"] == 340.50


def test_event_contains_correlation_id():
    """8. Verify that created events have non-empty correlation IDs."""
    tx = Transaction(
        transaction_id="TXN-999902",
        account_id="ACC-100002",
        transaction_type="PAYMENT",
        amount=120.00,
        currency="EUR",
        transaction_timestamp=datetime.now(timezone.utc),
        source_system="POS_TERMINAL",
        destination_system="INTERNAL_LEDGER",
    )
    event = create_transaction_event(tx, correlation_id="CORR-ABC-123")
    assert event.correlation_id == "CORR-ABC-123"

    auto_event = create_transaction_event(tx)
    assert auto_event.correlation_id is not None
    assert len(auto_event.correlation_id) > 0


def test_local_publisher_sends_to_orchestrator():
    """9. Verify that LocalEventPublisher routes events to the EventOrchestrator."""
    orchestrator = EventOrchestrator()
    received = []

    def handler(evt: Event):
        received.append(evt)
        return "OK"

    orchestrator.register_handler(EVENT_TYPE_TRANSACTION_RECEIVED, handler)
    publisher = LocalEventPublisher(orchestrator)

    test_event = Event(
        event_type=EVENT_TYPE_TRANSACTION_RECEIVED,
        source="test_publisher",
        payload={"transaction_id": "TXN-777"},
    )
    results = publisher.publish(test_event)

    assert len(received) == 1
    assert received[0].event_id == test_event.event_id
    assert results == ["OK"]


def test_event_handler_receives_transaction_event():
    """10. Verify that transaction_event_handler processes the event payload."""
    test_event = Event(
        event_type=EVENT_TYPE_TRANSACTION_RECEIVED,
        source="test_source",
        payload={
            "transaction_id": "TXN-000001",
            "masked_account_id": "********0001",
        },
        correlation_id="CORR-7788",
    )
    result = transaction_event_handler(test_event)
    assert result["status"] == "PROCESSED"
    assert result["transaction_id"] == "TXN-000001"
    assert result["correlation_id"] == "CORR-7788"


# ==============================================================================
# Database Tests (11 - 13)
# ==============================================================================
def test_generated_transaction_stored(tmp_path):
    """11. Verify that generated transactions are persisted to the database."""
    db_file = str(tmp_path / "test_day2.db")
    init_db(db_file)

    generator = TransactionGenerator(seed=606)
    tx = generator.generate_transaction()

    save_transaction(tx, db_path=db_file)
    record = get_transaction(tx.transaction_id, db_path=db_file)

    assert record is not None
    assert record["transaction_id"] == tx.transaction_id


def test_stored_transaction_can_be_retrieved(tmp_path):
    """12. Verify that transaction fields are accurately preserved and retrievable."""
    db_file = str(tmp_path / "test_retrieve.db")
    init_db(db_file)

    tx = Transaction(
        transaction_id="TXN-RET-01",
        account_id="ACC-100099",
        transaction_type="INTERNATIONAL_TRANSFER",
        amount=8950.40,
        currency="GBP",
        transaction_timestamp=datetime(2026, 10, 6, 12, 0, 0, tzinfo=timezone.utc),
        source_system="SWIFT_GATEWAY",
        destination_system="INTERNAL_LEDGER",
        reference_id="REF-RET-01",
    )
    save_transaction(tx, db_path=db_file)
    fetched = get_transaction("TXN-RET-01", db_path=db_file)

    assert fetched is not None
    assert fetched["transaction_id"] == "TXN-RET-01"
    assert fetched["amount"] == 8950.40
    assert fetched["currency"] == "GBP"
    assert fetched["transaction_type"] == "INTERNATIONAL_TRANSFER"


def test_unknown_transaction_returns_none(tmp_path):
    """13. Verify that querying a non-existent transaction ID returns None."""
    db_file = str(tmp_path / "test_none.db")
    init_db(db_file)

    record = get_transaction("NON_EXISTENT_TXN_ID", db_path=db_file)
    assert record is None


# ==============================================================================
# API Ingestion and Simulation Tests (14 - 18)
# ==============================================================================
def test_api_post_transactions_success():
    """14. Verify that POST /transactions accepts and ingests a valid transaction."""
    with TestClient(app) as client:
        payload = {
            "transaction_id": "TXN-API-2001",
            "account_id": "ACC-100045",
            "transaction_type": "PAYMENT",
            "amount": 75.50,
            "currency": "USD",
            "transaction_timestamp": "2026-10-06T12:00:00Z",
            "source_system": "MOBILE_APP",
            "destination_system": "ACH_NETWORK",
            "reference_id": "REF-API-2001",
        }
        resp = client.post("/transactions", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["transaction_id"] == "TXN-API-2001"
        assert data["event_type"] == EVENT_TYPE_TRANSACTION_RECEIVED


def test_api_post_transactions_invalid_rejected():
    """15. Verify that POST /transactions rejects invalid transactions with 422."""
    with TestClient(app) as client:
        invalid_payload = {
            "transaction_id": "TXN-INVALID-01",
            "account_id": "ACC-100045",
            "transaction_type": "PAYMENT",
            "amount": -50.00,  # Negative amount must fail
            "currency": "USD",
            "transaction_timestamp": "2026-10-06T12:00:00Z",
            "source_system": "MOBILE_APP",
            "destination_system": "ACH_NETWORK",
        }
        resp = client.post("/transactions", json=invalid_payload)
        assert resp.status_code == 422


def test_api_post_transactions_simulate_success():
    """16. Verify that POST /transactions/simulate generates and ingests transactions."""
    with TestClient(app) as client:
        resp = client.post("/transactions/simulate", json={"count": 5, "seed": 42})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "completed"
        assert data["generated"] == 5
        assert data["accepted"] == 5
        assert data["rejected"] == 0


def test_api_simulation_count_limits():
    """17. Verify boundary limits for simulation count (1 and 1000)."""
    with TestClient(app) as client:
        # Lower bound = 1
        resp_min = client.post("/transactions/simulate", json={"count": 1, "seed": 1})
        assert resp_min.status_code == 200
        assert resp_min.json()["generated"] == 1

        # Upper bound = 1000
        # (Validate schema boundary acceptance)
        req_body = {"count": 1000, "seed": 1}
        # Validate that the Pydantic validator accepts count=1000 without 422
        from main import SimulationRequest
        req_obj = SimulationRequest(**req_body)
        assert req_obj.count == 1000


def test_api_invalid_simulation_count_rejected():
    """18. Verify that invalid simulation counts (< 1 or > 1000) are rejected."""
    with TestClient(app) as client:
        # Zero count
        resp_zero = client.post("/transactions/simulate", json={"count": 0})
        assert resp_zero.status_code == 422

        # Negative count
        resp_neg = client.post("/transactions/simulate", json={"count": -10})
        assert resp_neg.status_code == 422

        # Exceeds max count limit
        resp_over = client.post("/transactions/simulate", json={"count": 1001})
        assert resp_over.status_code == 422


# ==============================================================================
# Metrics Tests (19 - 20)
# ==============================================================================
def test_metrics_published_count_increments():
    """19. Verify that events_published counter increments upon publishing."""
    orchestrator = EventOrchestrator()
    publisher = LocalEventPublisher(orchestrator)

    assert orchestrator.get_metrics()["events_published"] == 0

    for i in range(5):
        event = Event(
            event_type="DUMMY_METRIC_EVENT",
            source="test_metrics",
            payload={"idx": i},
        )
        publisher.publish(event)

    metrics = orchestrator.get_metrics()
    assert metrics["events_published"] == 5
    assert metrics["unknown_events"] == 5


def test_metrics_handled_count_increments():
    """20. Verify that events_handled counter increments when handlers execute."""
    orchestrator = EventOrchestrator()
    publisher = LocalEventPublisher(orchestrator)

    orchestrator.register_handler(
        EVENT_TYPE_TRANSACTION_RECEIVED,
        transaction_event_handler,
    )

    for i in range(4):
        event = Event(
            event_type=EVENT_TYPE_TRANSACTION_RECEIVED,
            source="test_metrics",
            payload={"transaction_id": f"TXN-M-{i}"},
        )
        publisher.publish(event)

    metrics = orchestrator.get_metrics()
    assert metrics["events_published"] == 4
    assert metrics["events_handled"] == 4
    assert metrics["processing_errors"] == 0
