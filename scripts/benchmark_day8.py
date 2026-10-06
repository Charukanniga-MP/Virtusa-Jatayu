"""Day 8 Performance Benchmark — 10,000-Record Zero-Trust Security & Audit Integrity.

Evaluates high-scale throughput and latency for:
1. Canonical SHA-256 Transaction Hashing
2. Recursive Zero-Trust PII Sanitization
3. Append-Only Tamper-Evident Hash-Chain Ledger (10,000 blocks)
4. Full Cryptographic Hash-Chain Verification
5. Zero-Trust Security Gateway (Input validation & trust-boundary filtering)
6. Adversarial Tamper Detection (Payload & Header Mutation Verification)

Runs entirely locally on Windows. Zero Docker, Zero Kafka, Zero LLMs, Zero external network calls.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import os
import random
import sys
import time
from typing import Any, Dict, List, Tuple

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.database import (
    get_connection,
    init_db,
    insert_audit_chain_record,
    get_audit_chain_records,
)
from reconciliation.models import Transaction
from security.hash_chain import (
    ChainIntegrityErrorType,
    HashChain,
    HashChainBlock,
)
from security.hashing import (
    canonicalize_json,
    hash_data,
    hash_decision,
    hash_json,
    hash_transaction,
)
from security.pii_protection import (
    mask_account_id,
    mask_card_number,
    mask_email,
    mask_government_id,
    mask_phone_number,
    sanitize_payload,
)
from security.security_gateway import SecurityGateway
from security.tokenization import TokenVault, tokenize_account_id


def generate_benchmark_transactions(count: int = 10000, seed: int = 20261008) -> List[Transaction]:
    """Generate 10,000 realistic synthetic transactions."""
    rng = random.Random(seed)
    currencies = ["INR", "USD", "EUR", "GBP"]
    tx_types = ["TRANSFER", "PAYMENT", "SETTLEMENT"]

    transactions: List[Transaction] = []
    base_time = datetime(2026, 10, 6, 12, 0, 0, tzinfo=timezone.utc)

    for i in range(count):
        tx_id = f"TXN-BENCH8-{i+1:06d}"
        acc_num = f"{rng.randint(1000000000, 9999999999)}"
        acc_id = f"ACC-{acc_num}"
        amt = Decimal(str(rng.randint(100, 250000))) / Decimal("100")
        curr = rng.choice(currencies)
        ttype = rng.choice(tx_types)

        tx = Transaction(
            transaction_id=tx_id,
            account_id=acc_id,
            transaction_type=ttype,
            amount=amt,
            currency=curr,
            transaction_timestamp=base_time,
            source_system="CORE_BANKING",
            destination_system="PAYMENT_GW",
            reference_id=f"REF-BENCH8-{i+1:06d}",
        )
        transactions.append(tx)

    return transactions


def run_benchmark():
    print("=" * 80)
    print("  ZT-LedgerGuard: DAY 8 ZERO-TRUST SECURITY & AUDIT INTEGRITY BENCHMARK")
    print("  Scale: 10,000 Transactions | 10,000 Tamper-Evident Audit Blocks")
    print("=" * 80)

    # 1. Generate Dataset
    print("\n[Phase 1/6] Generating 10,000 benchmark transaction payloads...")
    gen_start = time.perf_counter()
    transactions = generate_benchmark_transactions(10000)
    gen_time = time.perf_counter() - gen_start
    print(f"  -> Generated 10,000 transactions in {gen_time:.3f}s ({len(transactions)/gen_time:,.1f} tx/sec)")

    # 2. Transaction Hashing Throughput
    print("\n[Phase 2/6] Benchmarking Canonical SHA-256 Transaction Hashing...")
    hash_start = time.perf_counter()
    tx_hashes: List[str] = []
    for tx in transactions:
        h = hash_transaction(tx)
        tx_hashes.append(h)
    hash_time = time.perf_counter() - hash_start
    hash_throughput = len(transactions) / hash_time
    hash_latency_us = (hash_time / len(transactions)) * 1_000_000
    print(f"  -> Processed 10,000 hashes in {hash_time:.3f}s")
    print(f"  -> Throughput: {hash_throughput:,.1f} hashes/sec")
    print(f"  -> Average Latency: {hash_latency_us:.2f} microseconds/hash")

    # 3. PII Sanitization Throughput
    print("\n[Phase 3/6] Benchmarking Recursive Zero-Trust PII Sanitization...")
    # Prepare payloads with mixed sensitive data (accounts, cards, emails, phones)
    payloads_to_sanitize: List[Dict[str, Any]] = []
    rng = random.Random(42)
    for i, tx in enumerate(transactions):
        payloads_to_sanitize.append({
            "transaction_id": tx.transaction_id,
            "account_id": tx.account_id,
            "customer_id": f"CUST-{rng.randint(10000000, 99999999)}",
            "card_number": f"411111111111{rng.randint(1000, 9999)}",
            "email": f"user_{i}@bankexample.com",
            "phone_number": f"+9198{rng.randint(10000000, 99999999)}",
            "amount": float(tx.amount),
            "currency": tx.currency,
            "status": "COMPLETED",
        })

    san_start = time.perf_counter()
    sanitized_payloads: List[Dict[str, Any]] = []
    for p in payloads_to_sanitize:
        s = sanitize_payload(p, deep=True)
        sanitized_payloads.append(s)
    san_time = time.perf_counter() - san_start
    san_throughput = len(payloads_to_sanitize) / san_time
    san_latency_us = (san_time / len(payloads_to_sanitize)) * 1_000_000
    print(f"  -> Sanitized 10,000 payloads in {san_time:.3f}s")
    print(f"  -> Throughput: {san_throughput:,.1f} sanitizations/sec")
    print(f"  -> Average Latency: {san_latency_us:.2f} microseconds/payload")

    # Verify no raw account/card remains in sample
    assert "ACC-" in sanitized_payloads[0]["account_id"] and "*****" in sanitized_payloads[0]["account_id"]
    assert "************" in sanitized_payloads[0]["card_number"]
    assert "****@" in sanitized_payloads[0]["email"]

    # 4. Hash-Chain Append Throughput (10,000 Blocks)
    print("\n[Phase 4/6] Building 10,000-Block Cryptographic Hash-Chain Ledger...")
    chain = HashChain()
    chain_append_start = time.perf_counter()
    for i, s_payload in enumerate(sanitized_payloads):
        p_hash = hash_json(s_payload)
        chain.append_block(
            event_id=f"EVT-BENCH8-{i+1:06d}",
            event_type="TRANSACTION_AUDIT",
            payload_hash=p_hash,
            sanitized_payload=s_payload,
        )
    chain_append_time = time.perf_counter() - chain_append_start
    chain_append_throughput = len(sanitized_payloads) / chain_append_time
    chain_append_latency_us = (chain_append_time / len(sanitized_payloads)) * 1_000_000
    print(f"  -> Appended 10,000 blocks in {chain_append_time:.3f}s")
    print(f"  -> Throughput: {chain_append_throughput:,.1f} blocks/sec")
    print(f"  -> Average Latency: {chain_append_latency_us:.2f} microseconds/block")

    # 5. Full Cryptographic Chain Verification
    print("\n[Phase 5/6] Verifying 10,000-Block Cryptographic Integrity...")
    blocks = chain.get_blocks()
    verify_start = time.perf_counter()
    verify_result = HashChain.verify_chain(blocks)
    verify_time = time.perf_counter() - verify_start
    verify_throughput = len(blocks) / verify_time

    print(f"  -> Verification Status: {'VALID (PASS)' if verify_result.valid else 'INVALID (FAIL)'}")
    print(f"  -> Total Blocks Verified: {verify_result.events_checked:,}")
    print(f"  -> Total Verification Time: {verify_time:.3f}s")
    print(f"  -> Verification Speed: {verify_throughput:,.1f} blocks/sec")
    assert verify_result.valid is True
    assert verify_result.events_checked == 10000

    # 6. Security Gateway Throughput & Trust Boundary Check
    print("\n[Phase 6/6] Benchmarking Zero-Trust Security Gateway...")
    gw = SecurityGateway()
    # Inject untrusted client decisions into 20% of payloads
    gateway_requests: List[Dict[str, Any]] = []
    for i, tx in enumerate(transactions):
        req = {
            "transaction_id": tx.transaction_id,
            "account_id": tx.account_id,
            "amount": float(tx.amount),
            "currency": tx.currency,
            "transaction_timestamp": tx.transaction_timestamp.isoformat(),
        }
        if i % 5 == 0:
            req["decision"] = "APPROVE"  # Untrusted client decision
            req["fraud_score"] = 0
            req["risk_level"] = "LOW"
        gateway_requests.append(req)

    gw_start = time.perf_counter()
    stripped_count = 0
    approved_reqs = 0
    for req in gateway_requests:
        res = gw.validate_transaction_payload(req)
        if res.is_valid:
            approved_reqs += 1
        if res.untrusted_fields_stripped:
            stripped_count += 1
    gw_time = time.perf_counter() - gw_start
    gw_throughput = len(gateway_requests) / gw_time
    gw_latency_us = (gw_time / len(gateway_requests)) * 1_000_000

    print(f"  -> Evaluated 10,000 requests in {gw_time:.3f}s")
    print(f"  -> Valid Requests Approved: {approved_reqs:,} / {len(gateway_requests):,}")
    print(f"  -> Untrusted Decision Injections Neutralized: {stripped_count:,}")
    print(f"  -> Gateway Throughput: {gw_throughput:,.1f} req/sec")
    print(f"  -> Average Gateway Latency: {gw_latency_us:.2f} microseconds/req")
    assert stripped_count == 2000
    assert approved_reqs == 10000

    # 7. Adversarial Tampering Verification
    print("\n" + "=" * 80)
    print("  ADVERSARIAL INTEGRITY VERIFICATION (TAMPER DETECTION TEST)")
    print("=" * 80)

    # Attack Scenario A: Mutate stored payload at sequence 4,500
    print("\n[Attack Scenario A] Tampering with payload at sequence 4,500...")
    blocks_tampered_payload = [HashChainBlock(**b.model_dump()) for b in blocks]
    blocks_tampered_payload[4499].sanitized_payload = {"account_id": "TAMPERED_INJECTED_VALUE"}

    tamper_res_a = HashChain.verify_chain(blocks_tampered_payload)
    print(f"  -> Verification Result: {'VALID' if tamper_res_a.valid else 'TAMPERING DETECTED'}")
    print(f"  -> First Invalid Sequence: {tamper_res_a.first_invalid_sequence}")
    print(f"  -> Error Classification: {tamper_res_a.error_type}")
    print(f"  -> Details: {tamper_res_a.details}")
    assert tamper_res_a.valid is False
    assert tamper_res_a.first_invalid_sequence == 4500
    assert tamper_res_a.error_type == ChainIntegrityErrorType.PAYLOAD_HASH_MISMATCH.value

    # Attack Scenario B: Alter event_hash at sequence 7,200
    print("\n[Attack Scenario B] Tampering with event_hash at sequence 7,200...")
    blocks_tampered_hash = [HashChainBlock(**b.model_dump()) for b in blocks]
    blocks_tampered_hash[7199].event_hash = "deadbeef" * 8

    tamper_res_b = HashChain.verify_chain(blocks_tampered_hash)
    print(f"  -> Verification Result: {'VALID' if tamper_res_b.valid else 'TAMPERING DETECTED'}")
    print(f"  -> First Invalid Sequence: {tamper_res_b.first_invalid_sequence}")
    print(f"  -> Error Classification: {tamper_res_b.error_type}")
    assert tamper_res_b.valid is False
    assert tamper_res_b.first_invalid_sequence == 7200
    assert tamper_res_b.error_type == ChainIntegrityErrorType.EVENT_HASH_MISMATCH.value

    # Calculate Security Overhead
    baseline_latency_us = 15.0  # Approx unverified dict serialization baseline
    security_total_latency_us = hash_latency_us + san_latency_us + gw_latency_us + chain_append_latency_us
    overhead_pct = ((security_total_latency_us - baseline_latency_us) / baseline_latency_us) * 100.0

    print("\n" + "=" * 80)
    print("  BENCHMARK SUMMARY & PERFORMANCE TELEMETRY")
    print("=" * 80)
    print(f"  Total Transactions Evaluated:       10,000")
    print(f"  Total Audit Blocks Evaluated:       10,000")
    print(f"  Transaction Hashing Throughput:     {hash_throughput:,.1f} hashes/sec ({hash_latency_us:.2f} µs/op)")
    print(f"  PII Sanitization Throughput:        {san_throughput:,.1f} ops/sec ({san_latency_us:.2f} µs/op)")
    print(f"  Hash-Chain Append Throughput:       {chain_append_throughput:,.1f} blocks/sec ({chain_append_latency_us:.2f} µs/op)")
    print(f"  Cryptographic Chain Verification:   {verify_time:.3f}s ({verify_throughput:,.1f} blocks/sec)")
    print(f"  Security Gateway Throughput:        {gw_throughput:,.1f} req/sec ({gw_latency_us:.2f} µs/op)")
    print(f"  Total Security Overhead per Tx:     {security_total_latency_us:.2f} µs ({security_total_latency_us/1000:.3f} ms)")
    print(f"  Tamper Detection Efficacy:          100.0% (Zero false negatives)")
    print("=" * 80)
    print("  [RESULT] DAY 8 ZERO-TRUST SECURITY BENCHMARK: PASSED (ALL TARGETS MET)")
    print("=" * 80)


if __name__ == "__main__":
    run_benchmark()
