"""Comprehensive Test Suite for Day 8: Zero-Trust Security, PII Protection & Tamper-Evident Audit.

Validates:
1. PII Classification & Data Masking (Accounts, Cards, Phones, Emails, National IDs)
2. Deterministic Surrogate Tokenization & Vault Lookups
3. Cryptographic Hashing & Canonical JSON Serialization
4. Tamper-Evident Hash-Chain Ledger & Integrity Verification
5. 10 Security Threat & Adversarial Scenarios (Tampering, Deletion, Reordering, Fake Decisions)
6. Zero-Trust Security Gateway & Trust Boundaries
7. Safe Error Handling & Non-Leakage of PII / Filesystem Paths
8. SQLite Audit Chain Append-Only Persistence
9. REST APIs (/security/metrics, /security/audit/verify)
"""

from datetime import datetime, timezone
from decimal import Decimal
import json
import os
import sqlite3
import tempfile
from typing import Any, Dict, List
import pytest
from fastapi.testclient import TestClient

from database.database import (
    get_connection,
    init_db,
    insert_audit_chain_record,
    get_audit_chain_records,
    get_latest_audit_chain_record,
    get_audit_chain_count,
    insert_transaction,
)
from main import app
from orchestrator.event_audit import EventAuditTrail
from orchestrator.event_types import EventType
from reconciliation.models import Transaction
from security.audit_integrity import AuditIntegrityService
from security.hash_chain import (
    ChainIntegrityErrorType,
    ChainVerificationResult,
    HashChain,
    HashChainBlock,
    compute_event_hash,
)
from security.hashing import (
    canonicalize_json,
    hash_data,
    hash_decision,
    hash_event,
    hash_json,
    hash_transaction,
)
from security.pii_protection import (
    PIIClassificationLevel,
    get_field_classification,
    is_sensitive_field,
    mask_account_id,
    mask_card_number,
    mask_email,
    mask_government_id,
    mask_phone_number,
    mask_pii_value,
    mask_sensitive_value,
    sanitize_payload,
    sanitize_text,
)
from security.security_config import (
    GENESIS_PREVIOUS_HASH,
    MAX_REQUEST_SIZE_BYTES,
    MAX_TRANSACTION_AMOUNT,
    MIN_TRANSACTION_AMOUNT,
    TOKEN_PREFIX,
)
from security.security_events import (
    SecurityEventType,
    SecurityMetricsStore,
    get_security_metrics_store,
)
from security.security_gateway import (
    GatewayValidationResult,
    SecurityGateway,
)
from security.tokenization import (
    TokenVault,
    lookup_by_account_token,
    tokenize_account_id,
    tokenize_identifier,
)


@pytest.fixture
def temp_db_path(tmp_path) -> str:
    """Create isolated SQLite database path with initial schema."""
    db_file = str(tmp_path / "test_day8.db")
    init_db(db_file)
    return db_file


@pytest.fixture
def fresh_token_vault() -> TokenVault:
    """Provide clean isolated token vault."""
    return TokenVault(salt="test_salt_day8_isolation")


@pytest.fixture
def fresh_metrics_store() -> SecurityMetricsStore:
    """Provide isolated metrics store."""
    return SecurityMetricsStore()


@pytest.fixture
def sample_transaction() -> Transaction:
    """Sample valid transaction."""
    return Transaction(
        transaction_id="TXN-DAY8-001",
        account_id="ACC-9876543210",
        transaction_type="TRANSFER",
        amount=5000.00,
        currency="INR",
        transaction_timestamp=datetime(2026, 10, 6, 12, 0, 0, tzinfo=timezone.utc),
        source_system="CHANNEL_GATEWAY",
        destination_system="CORE_LEDGER",
        reference_id="REF-SEC-001",
    )


# ==============================================================================
# 1. PII Classification & Data Masking Tests (10 tests)
# ==============================================================================

def test_mask_account_id_with_prefix():
    """Verify account ID with standard prefix masks middle and preserves prefix and last 4."""
    assert mask_account_id("ACC-123456789") == "ACC-*****6789"
    assert mask_account_id("CORP-9876543210") == "CORP-******3210"


def test_mask_account_id_numeric_only():
    """Verify numeric-only account ID masks keeping last 4 digits visible."""
    assert mask_account_id("123456789012") == "********9012"
    assert mask_account_id("9876543210") == "******3210"


def test_mask_account_id_edge_cases():
    """Verify edge cases for account masking: short strings, None, and empty."""
    assert mask_account_id("") == ""
    assert mask_account_id(None) == ""
    assert mask_account_id("12") == "*2"
    assert mask_account_id("1") == "1"


def test_mask_card_number():
    """Verify 16-digit credit/debit card numbers are masked leaving only last 4 digits."""
    assert mask_card_number("9876543212345678") == "************5678"
    assert mask_card_number("9876-5432-1234-5678") == "************5678"
    assert mask_card_number("4111 1111 1111 1234") == "************1234"


def test_mask_phone_number():
    """Verify phone numbers are masked preserving last 4 digits."""
    assert mask_phone_number("9876543210") == "******3210"
    assert mask_phone_number("+919876543210") == "+********3210"
    assert mask_phone_number("123") == "****"


def test_mask_email():
    """Verify email addresses retain first char and domain while masking local part."""
    assert mask_email("charu@example.com") == "c****@example.com"
    assert mask_email("admin@fintech.bank") == "a****@fintech.bank"
    assert mask_email("x@y.z") == "x****@y.z"


def test_mask_government_id():
    """Verify tax/national IDs are masked leaving last 4 characters visible."""
    assert mask_government_id("ABCDE1234F") == "******234F"
    assert mask_government_id("123-45-6789") == "*******6789"


def test_pii_field_classification():
    """Verify field classification matches data taxonomy."""
    assert get_field_classification("account_id") == PIIClassificationLevel.SENSITIVE
    assert get_field_classification("customer_id") == PIIClassificationLevel.SENSITIVE
    assert get_field_classification("card_number") == PIIClassificationLevel.HIGHLY_SENSITIVE
    assert get_field_classification("phone_number") == PIIClassificationLevel.HIGHLY_SENSITIVE
    assert get_field_classification("email") == PIIClassificationLevel.HIGHLY_SENSITIVE
    assert get_field_classification("transaction_id") == PIIClassificationLevel.PUBLIC
    assert get_field_classification("currency") == PIIClassificationLevel.PUBLIC
    assert is_sensitive_field("account_id") is True
    assert is_sensitive_field("currency") is False


def test_sanitize_payload_deep_non_mutating():
    """Verify sanitize_payload recursively masks sensitive keys without mutating input."""
    raw = {
        "transaction_id": "TXN-001",
        "account_id": "ACC-123456789",
        "nested": {
            "customer_id": "CUST-987654321",
            "card_number": "4111111111111234",
            "email": "test@bank.com",
        },
        "public_data": [1, 2, 3],
    }
    sanitized = sanitize_payload(raw)

    # Original remains untouched
    assert raw["account_id"] == "ACC-123456789"
    assert raw["nested"]["card_number"] == "4111111111111234"

    # Sanitized has all sensitive values masked
    assert sanitized["transaction_id"] == "TXN-001"
    assert sanitized["account_id"] == "ACC-*****6789"
    assert sanitized["nested"]["customer_id"] == "CUST-*****4321"
    assert sanitized["nested"]["card_number"] == "************1234"
    assert sanitized["nested"]["email"] == "t****@bank.com"


def test_sanitize_text_freeform():
    """Verify freeform text containing emails, cards, or accounts is sanitized."""
    raw_text = "Contact charu@bank.com regarding card 4111111111111234 or account 123456789012."
    sanitized = sanitize_text(raw_text)
    assert "charu@bank.com" not in sanitized
    assert "4111111111111234" not in sanitized
    assert "123456789012" not in sanitized
    assert "c****@bank.com" in sanitized
    assert "************1234" in sanitized


# ==============================================================================
# 2. Tokenization Tests (6 tests)
# ==============================================================================

def test_tokenization_deterministic(fresh_token_vault):
    """Verify identical raw identifier produces the exact same token."""
    tok1 = fresh_token_vault.tokenize("ACC-9876543210")
    tok2 = fresh_token_vault.tokenize("ACC-9876543210")
    assert tok1 == tok2
    assert tok1.startswith(TOKEN_PREFIX)
    assert len(tok1) == len(TOKEN_PREFIX) + 16


def test_tokenization_different_identifiers_distinct(fresh_token_vault):
    """Verify distinct identifiers produce distinct surrogate tokens (no collisions)."""
    tok1 = fresh_token_vault.tokenize("ACC-1111111111")
    tok2 = fresh_token_vault.tokenize("ACC-2222222222")
    assert tok1 != tok2


def test_tokenization_internal_lookup(fresh_token_vault):
    """Verify internal lookup returns the raw identifier correctly."""
    tok = fresh_token_vault.tokenize("ACC-SECRET-ACCOUNT-99")
    raw = fresh_token_vault.lookup(tok)
    assert raw == "ACC-SECRET-ACCOUNT-99"


def test_tokenization_masked_lookup(fresh_token_vault):
    """Verify lookup_masked returns safe display value."""
    tok = fresh_token_vault.tokenize("ACC-123456789")
    masked = fresh_token_vault.lookup_masked(tok)
    assert masked == "ACC-*****6789"


def test_tokenization_context_scoping(fresh_token_vault):
    """Verify same raw string in different domain contexts yields distinct tokens."""
    tok_acc = fresh_token_vault.tokenize("12345", context="account")
    tok_cust = fresh_token_vault.tokenize("12345", context="customer")
    assert tok_acc != tok_cust


def test_global_tokenize_account_id():
    """Verify top-level module tokenization functions operate deterministically."""
    tok = tokenize_account_id("ACC-GLOBAL-100")
    assert tok.startswith(TOKEN_PREFIX)
    assert lookup_by_account_token(tok) == "ACC-GLOBAL-100"


# ==============================================================================
# 3. Cryptographic Hashing & Canonicalization Tests (6 tests)
# ==============================================================================

def test_hashing_sha256_deterministic():
    """Verify hash_data produces valid 64-character SHA-256 hex string."""
    h = hash_data("test_payload")
    assert len(h) == 64
    assert h == hash_data("test_payload")


def test_canonical_json_key_order_independence():
    """Verify JSON key ordering differences yield identical canonical representation and hash."""
    dict_a = {"alpha": 1, "beta": 2, "nested": {"z": 10, "a": 20}}
    dict_b = {"beta": 2, "alpha": 1, "nested": {"a": 20, "z": 10}}
    canon_a = canonicalize_json(dict_a)
    canon_b = canonicalize_json(dict_b)
    assert canon_a == canon_b
    assert hash_json(dict_a) == hash_json(dict_b)


def test_canonical_json_datetime_normalization():
    """Verify datetimes are normalized deterministically to ISO UTC format."""
    dt = datetime(2026, 10, 6, 15, 30, 0, tzinfo=timezone.utc)
    data = {"timestamp": dt}
    canon = canonicalize_json(data)
    assert "2026-10-06T15:30:00+00:00" in canon


def test_canonical_json_decimal_consistency():
    """Verify Decimals and floats format consistently without IEEE-754 drift."""
    data_dec = {"amount": Decimal("5000.50")}
    data_flt = {"amount": 5000.5}
    assert canonicalize_json(data_dec) == canonicalize_json(data_flt)


def test_hash_transaction_sensitivity(sample_transaction):
    """Verify transaction hash changes when any protected financial field changes."""
    h_orig = hash_transaction(sample_transaction)
    tx_mod_amt = sample_transaction.model_copy(update={"amount": 5001.00})
    tx_mod_curr = sample_transaction.model_copy(update={"currency": "USD"})
    tx_mod_acc = sample_transaction.model_copy(update={"account_id": "ACC-DIFFERENT"})

    assert hash_transaction(tx_mod_amt) != h_orig
    assert hash_transaction(tx_mod_curr) != h_orig
    assert hash_transaction(tx_mod_acc) != h_orig


def test_hash_decision_integrity():
    """Verify decision hash covers core consensus attributes."""
    decision_data = {
        "transaction_id": "TXN-DEC-1",
        "decision": "APPROVE",
        "confidence": 0.95,
        "agent_results": {"fraud": {"score": 10}},
        "triggered_policies": ["CLEAN_CHECK"],
        "decision_reasons": ["All agents agree."],
        "disagreements": [],
    }
    h = hash_decision(decision_data)
    assert len(h) == 64


# ==============================================================================
# 4. Hash Chain Ledger & Tamper Detection Tests (12 tests)
# ==============================================================================

def test_hash_chain_genesis_block():
    """Verify first block in chain links to GENESIS_PREVIOUS_HASH."""
    chain = HashChain()
    block = chain.append_block(
        event_id="EVT-001",
        event_type="TRANSACTION_RECEIVED",
        payload_hash=hash_data("payload_1"),
    )
    assert block.sequence_number == 1
    assert block.previous_hash == GENESIS_PREVIOUS_HASH
    assert len(block.event_hash) == 64


def test_hash_chain_sequential_linking():
    """Verify sequential blocks link cryptographically to predecessor event_hash."""
    chain = HashChain()
    b1 = chain.append_block("EVT-001", "TRANSACTION_RECEIVED", hash_data("p1"))
    b2 = chain.append_block("EVT-002", "TRANSACTION_VALIDATED", hash_data("p2"))
    b3 = chain.append_block("EVT-003", "TRANSACTION_RECONCILED", hash_data("p3"))

    assert b2.sequence_number == 2
    assert b2.previous_hash == b1.event_hash
    assert b3.sequence_number == 3
    assert b3.previous_hash == b2.event_hash


def test_hash_chain_verification_intact():
    """Verify pristine chain of 10 blocks verifies with valid=True."""
    chain = HashChain()
    for i in range(1, 11):
        payload = {"id": i}
        chain.append_block(f"EVT-{i:03d}", "TEST_EVENT", hash_json(payload), sanitized_payload=payload)

    res = HashChain.verify_chain(chain.get_blocks())
    assert res.valid is True
    assert res.events_checked == 10
    assert res.first_invalid_sequence is None
    assert res.error_type is None


def test_attack_1_audit_payload_tampering():
    """Attack 1: Modify payload of block 3 in place -> PAYLOAD_HASH_MISMATCH."""
    chain = HashChain()
    for i in range(1, 6):
        payload = {"data": f"payload_{i}"}
        chain.append_block(f"EVT-{i}", "EVENT", hash_json(payload), sanitized_payload=payload)

    blocks = chain.get_blocks()
    # Tamper with block 3's stored payload without recomputing hash
    blocks[2].sanitized_payload = {"data": "TAMPERED_PAYLOAD_3"}

    res = HashChain.verify_chain(blocks)
    assert res.valid is False
    assert res.first_invalid_sequence == 3
    assert res.error_type == ChainIntegrityErrorType.PAYLOAD_HASH_MISMATCH.value


def test_attack_2_event_hash_tampering():
    """Attack 2: Modify event_hash of block 2 -> EVENT_HASH_MISMATCH."""
    chain = HashChain()
    for i in range(1, 5):
        chain.append_block(f"EVT-{i}", "EVENT", hash_data(f"p_{i}"))

    blocks = chain.get_blocks()
    blocks[1].event_hash = "deadbeef" * 8  # 64-char fake hash

    res = HashChain.verify_chain(blocks)
    assert res.valid is False
    assert res.first_invalid_sequence == 2
    assert res.error_type == ChainIntegrityErrorType.EVENT_HASH_MISMATCH.value


def test_attack_3_previous_hash_tampering():
    """Attack 3: Modify previous_hash of block 4 -> PREVIOUS_HASH_MISMATCH."""
    chain = HashChain()
    for i in range(1, 6):
        chain.append_block(f"EVT-{i}", "EVENT", hash_data(f"p_{i}"))

    blocks = chain.get_blocks()
    blocks[3].previous_hash = "bad_hash" * 8

    res = HashChain.verify_chain(blocks)
    assert res.valid is False
    assert res.first_invalid_sequence == 4
    assert res.error_type == ChainIntegrityErrorType.PREVIOUS_HASH_MISMATCH.value


def test_attack_4_event_deletion():
    """Attack 4: Delete block 3 from the sequence -> SEQUENCE_GAP."""
    chain = HashChain()
    for i in range(1, 6):
        chain.append_block(f"EVT-{i}", "EVENT", hash_data(f"p_{i}"))

    blocks = chain.get_blocks()
    del blocks[2]  # Remove sequence 3

    res = HashChain.verify_chain(blocks)
    assert res.valid is False
    assert res.first_invalid_sequence == 4
    assert res.error_type == ChainIntegrityErrorType.SEQUENCE_GAP.value


def test_attack_5_event_reordering():
    """Attack 5: Swap blocks 2 and 3 -> SEQUENCE_OUT_OF_ORDER or DUPLICATE."""
    chain = HashChain()
    for i in range(1, 6):
        chain.append_block(f"EVT-{i}", "EVENT", hash_data(f"p_{i}"))

    blocks = chain.get_blocks()
    # Swap block 2 and block 3
    blocks[1], blocks[2] = blocks[2], blocks[1]

    res = HashChain.verify_chain(blocks)
    assert res.valid is False
    assert res.error_type in (
        ChainIntegrityErrorType.SEQUENCE_GAP.value,
        ChainIntegrityErrorType.DUPLICATE_SEQUENCE.value,
    )


def test_duplicate_sequence_number_detection():
    """Verify duplicate sequence numbers trigger DUPLICATE_SEQUENCE error."""
    b1 = HashChainBlock(
        sequence_number=1, event_id="E1", event_type="T", timestamp="2026-10-06T12:00:00Z",
        payload_hash=hash_data("p1"), previous_hash=GENESIS_PREVIOUS_HASH,
        event_hash=compute_event_hash(1, "E1", "T", "2026-10-06T12:00:00Z", hash_data("p1"), GENESIS_PREVIOUS_HASH)
    )
    b2_dup = HashChainBlock(
        sequence_number=1, event_id="E2", event_type="T", timestamp="2026-10-06T12:00:01Z",
        payload_hash=hash_data("p2"), previous_hash=b1.event_hash,
        event_hash=compute_event_hash(1, "E2", "T", "2026-10-06T12:00:01Z", hash_data("p2"), b1.event_hash)
    )
    res = HashChain.verify_chain([b1, b2_dup])
    assert res.valid is False
    assert res.error_type == ChainIntegrityErrorType.DUPLICATE_SEQUENCE.value


def test_genesis_previous_hash_mismatch():
    """Verify non-genesis first block triggers GENESIS_HASH_MISMATCH."""
    b1 = HashChainBlock(
        sequence_number=1, event_id="E1", event_type="T", timestamp="2026-10-06T12:00:00Z",
        payload_hash=hash_data("p1"), previous_hash="not_genesis_hash",
        event_hash="fake_hash"
    )
    res = HashChain.verify_chain([b1])
    assert res.valid is False
    assert res.error_type == ChainIntegrityErrorType.GENESIS_HASH_MISMATCH.value


def test_empty_chain_valid():
    """Verify empty chain returns valid=True with 0 checked events."""
    res = HashChain.verify_chain([])
    assert res.valid is True
    assert res.events_checked == 0


# ==============================================================================
# 5. Security Gateway & Trust Boundary Tests (12 tests)
# ==============================================================================

def test_gateway_valid_request(sample_transaction):
    """Verify security gateway approves well-formed transaction payload."""
    gw = SecurityGateway()
    data = sample_transaction.model_dump()
    res = gw.validate_transaction_payload(data)
    assert res.is_valid is True
    assert res.rejection_reason is None
    assert len(res.violations) == 0


def test_gateway_oversized_payload():
    """Attack 9: Payload exceeding MAX_REQUEST_SIZE_BYTES is rejected."""
    gw = SecurityGateway(max_request_size=500)
    oversized_bytes = b'{"data": "' + (b"X" * 600) + b'"}'
    res = gw.validate_request_bytes(oversized_bytes)
    assert res.is_valid is False
    assert res.rejection_reason == "REQUEST_SIZE_EXCEEDED"


def test_gateway_unsupported_media_type():
    """Verify Content-Type other than application/json is rejected."""
    gw = SecurityGateway()
    body = b"<xml>evil</xml>"
    res = gw.validate_request_bytes(body, content_type="text/xml")
    assert res.is_valid is False
    assert res.rejection_reason == "UNSUPPORTED_MEDIA_TYPE"


def test_gateway_malformed_json():
    """Attack 10: Malformed JSON syntax is rejected before domain parsing."""
    gw = SecurityGateway()
    bad_bytes = b'{"transaction_id": "TXN-001", broken_json'
    res = gw.validate_request_bytes(bad_bytes)
    assert res.is_valid is False
    assert res.rejection_reason == "MALFORMED_JSON"


def test_gateway_empty_transaction_id():
    """Verify empty or missing transaction_id triggers rejection."""
    gw = SecurityGateway()
    res = gw.validate_transaction_payload({"transaction_id": "", "account_id": "ACC-1", "amount": 100, "currency": "INR"})
    assert res.is_valid is False
    assert any("transaction_id" in v for v in res.violations)


def test_gateway_invalid_characters_transaction_id():
    """Verify transaction_id with injection characters (SQL/script) is rejected."""
    gw = SecurityGateway()
    res = gw.validate_transaction_payload({
        "transaction_id": "TXN-001; DROP TABLE transactions; --",
        "account_id": "ACC-1",
        "amount": 100,
        "currency": "INR",
    })
    assert res.is_valid is False
    assert any("transaction_id" in v for v in res.violations)


def test_gateway_negative_and_zero_amount():
    """Verify negative or zero monetary amount is rejected."""
    gw = SecurityGateway()
    res1 = gw.validate_transaction_payload({"transaction_id": "TXN-001", "account_id": "ACC-1", "amount": -50.00, "currency": "INR"})
    res2 = gw.validate_transaction_payload({"transaction_id": "TXN-001", "account_id": "ACC-1", "amount": 0.00, "currency": "INR"})
    assert res1.is_valid is False
    assert res2.is_valid is False


def test_gateway_excessive_amount_ceiling():
    """Verify amount exceeding MAX_TRANSACTION_AMOUNT is rejected."""
    gw = SecurityGateway()
    res = gw.validate_transaction_payload({
        "transaction_id": "TXN-001",
        "account_id": "ACC-1",
        "amount": 50000000.00,  # 50 million > 10 million ceiling
        "currency": "INR",
    })
    assert res.is_valid is False
    assert any("ceiling" in v.lower() or "maximum" in v.lower() for v in res.violations)


def test_gateway_unsupported_currency():
    """Verify invalid or non-supported ISO currency code is rejected."""
    gw = SecurityGateway()
    res = gw.validate_transaction_payload({
        "transaction_id": "TXN-001",
        "account_id": "ACC-1",
        "amount": 100,
        "currency": "FAKE_CURRENCY",
    })
    assert res.is_valid is False
    assert any("currency" in v.lower() for v in res.violations)


def test_gateway_malformed_timestamp():
    """Verify malformed timestamp string is rejected."""
    gw = SecurityGateway()
    res = gw.validate_transaction_payload({
        "transaction_id": "TXN-001",
        "account_id": "ACC-1",
        "amount": 100,
        "currency": "INR",
        "transaction_timestamp": "yesterday at noon",
    })
    assert res.is_valid is False
    assert any("timestamp" in v.lower() for v in res.violations)


def test_trust_boundary_strips_fake_decision():
    """Attack 7: Client supplies 'decision': 'APPROVE' -> gateway strips untrusted field."""
    gw = SecurityGateway()
    data = {
        "transaction_id": "TXN-ATTACK-007",
        "account_id": "ACC-9876543210",
        "amount": 5000.00,
        "currency": "INR",
        "decision": "APPROVE",  # Client attempts to dictate disposition
    }
    res = gw.validate_transaction_payload(data)
    assert res.is_valid is True
    assert "decision" not in res.sanitized_data
    assert "decision" in res.untrusted_fields_stripped


def test_trust_boundary_strips_fake_fraud_score():
    """Attack 8: Client supplies 'fraud_score': 0 and 'risk_level': 'LOW' -> stripped."""
    gw = SecurityGateway()
    data = {
        "transaction_id": "TXN-ATTACK-008",
        "account_id": "ACC-9876543210",
        "amount": 5000.00,
        "currency": "INR",
        "fraud_score": 0,
        "risk_level": "LOW",
        "block_recommended": False,
    }
    res = gw.validate_transaction_payload(data)
    assert res.is_valid is True
    assert "fraud_score" not in res.sanitized_data
    assert "risk_level" not in res.sanitized_data
    assert "block_recommended" not in res.sanitized_data
    assert set(res.untrusted_fields_stripped) == {"fraud_score", "risk_level", "block_recommended"}


# ==============================================================================
# 6. Audit Integrity Service & SQLite Persistence Tests (5 tests)
# ==============================================================================

def test_audit_integrity_service_sqlite_append(temp_db_path):
    """Verify AuditIntegrityService records blocks into SQLite audit_chain table."""
    service = AuditIntegrityService(db_path=temp_db_path)
    b1 = service.record_audit_event("E-1", "TRANSACTION_RECEIVED", {"tx_id": "TXN-1", "account_id": "ACC-123456789"})
    b2 = service.record_audit_event("E-2", "TRANSACTION_VALIDATED", {"tx_id": "TXN-1", "account_id": "ACC-123456789"})

    assert b1.sequence_number == 1
    assert b2.sequence_number == 2
    assert b2.previous_hash == b1.event_hash

    # Verify directly in SQLite
    with get_connection(temp_db_path) as conn:
        assert get_audit_chain_count(conn) == 2
        records = get_audit_chain_records(conn)
        assert len(records) == 2
        assert records[0]["event_id"] == "E-1"
        assert records[1]["event_id"] == "E-2"
        # Verify account was masked before DB insertion
        assert records[0]["sanitized_payload"]["account_id"] == "ACC-*****6789"


def test_audit_integrity_service_verify_sqlite_chain(temp_db_path):
    """Verify AuditIntegrityService.verify_chain evaluates database state."""
    service = AuditIntegrityService(db_path=temp_db_path)
    for i in range(1, 6):
        service.record_audit_event(f"EVT-{i}", "EVENT_TYPE", {"index": i})

    res = service.verify_chain()
    assert res.valid is True
    assert res.events_checked == 5


def test_audit_integrity_service_tampered_db_detected(temp_db_path):
    """Verify tampering directly in SQLite audit_chain table is caught by verify_chain."""
    service = AuditIntegrityService(db_path=temp_db_path)
    for i in range(1, 5):
        service.record_audit_event(f"EVT-{i}", "EVENT_TYPE", {"index": i})

    # Tamper with row 2 in SQLite
    with get_connection(temp_db_path) as conn:
        conn.execute("UPDATE audit_chain SET event_hash = 'tampered_hash_value' WHERE sequence_number = 2;")
        conn.commit()

    res = service.verify_chain()
    assert res.valid is False
    assert res.first_invalid_sequence == 2
    assert res.error_type == ChainIntegrityErrorType.EVENT_HASH_MISMATCH.value


def test_audit_trail_integration_with_integrity_service(temp_db_path):
    """Verify EventAuditTrail records cryptographic blocks when integrity service is wired."""
    service = AuditIntegrityService(db_path=temp_db_path)
    audit = EventAuditTrail(integrity_service=service)

    rec1 = audit.record(
        event_id="AUD-01", event_type="TXN_RECEIVED", correlation_id="CORR-1",
        source="TEST", processing_status="RECEIVED", details={"amt": 100}
    )
    rec2 = audit.record(
        event_id="AUD-02", event_type="TXN_VALIDATED", correlation_id="CORR-1",
        source="TEST", processing_status="COMPLETED", details={"amt": 100}
    )

    assert rec1.sequence_number == 1
    assert rec2.sequence_number == 2
    assert rec2.previous_hash == rec1.event_hash
    assert rec1.event_hash is not None


def test_audit_immutability_no_delete_api(temp_db_path):
    """Verify audit_chain enforces append-only semantics (no delete function in DB layer)."""
    import database.database as db_mod
    assert not hasattr(db_mod, "delete_audit_chain_record")
    assert not hasattr(db_mod, "update_audit_chain_record")


# ==============================================================================
# 7. Security Metrics & REST APIs Tests (6 tests)
# ==============================================================================

def test_security_metrics_store():
    """Verify SecurityMetricsStore tracks telemetry counts accurately."""
    store = SecurityMetricsStore()
    store.record_security_check(5)
    store.record_validation_failure(2)
    store.record_pii_sanitization(4)
    store.record_integrity_check(3)
    store.record_integrity_failure(1)
    store.record_policy_violation(2)

    m = store.get_metrics()
    assert m["total_security_checks"] == 5
    assert m["validation_failures"] == 2
    assert m["pii_sanitizations"] == 4
    assert m["integrity_checks"] == 3
    assert m["integrity_failures"] == 1
    assert m["security_policy_violations"] == 2


def test_api_security_metrics():
    """Verify GET /security/metrics endpoint returns metrics dictionary."""
    client = TestClient(app)
    response = client.get("/security/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "total_security_checks" in data
    assert "validation_failures" in data
    assert "pii_sanitizations" in data
    assert "integrity_checks" in data


def test_api_security_audit_verify():
    """Verify GET /security/audit/verify endpoint returns cryptographic verification."""
    client = TestClient(app)
    response = client.get("/security/audit/verify")
    assert response.status_code == 200
    data = response.json()
    assert "valid" in data
    assert "events_checked" in data
    assert isinstance(data["valid"], bool)


def test_api_no_raw_pii_leakage_in_error():
    """Verify invalid transaction query returns safe error without leaking raw internal paths."""
    client = TestClient(app)
    response = client.get("/decision/NON_EXISTENT_TXN_XYZ")
    assert response.status_code == 404
    text = response.text
    assert "Traceback" not in text
    assert "D:\\" not in text
    assert "C:\\" not in text
    assert "sqlite3" not in text.lower()


def test_api_transaction_validation_safe_error():
    """Verify malformed body sent to POST /transactions returns clean 422 without stack trace."""
    client = TestClient(app)
    response = client.post("/transactions", json={"bad_field": 123})
    assert response.status_code == 422
    assert "Traceback" not in response.text


def test_security_events_types_registered():
    """Verify all canonical Day 8 security event types are registered in EventType enum."""
    assert EventType.SECURITY_VALIDATION_FAILED.value == "SECURITY_VALIDATION_FAILED"
    assert EventType.PII_ACCESS_DETECTED.value == "PII_ACCESS_DETECTED"
    assert EventType.AUDIT_INTEGRITY_VERIFICATION_FAILED.value == "AUDIT_INTEGRITY_VERIFICATION_FAILED"
    assert EventType.SECURITY_POLICY_VIOLATION.value == "SECURITY_POLICY_VIOLATION"
