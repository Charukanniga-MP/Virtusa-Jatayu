"""Day 7 Interactive Demonstration — Multi-Agent Coordination & Risk Decision Consensus.

ZT-LedgerGuard: Zero-Trust AI Ledger Reconciliation & Real-Time Fraud Triage Platform
Virtusa Jatayu Season 6 — Forward Deployment Engineering (FDE)

Demonstrates the 5 mandatory Day 7 consensus scenarios:
  1. Clean Matched Ledger + Low Fraud       -> APPROVE (Confidence >= 0.90)
  2. Matched Ledger + Critical Fraud Risk    -> BLOCK_RECOMMENDED (Precedence Override)
  3. Ambiguous Candidate Match + Low Fraud   -> MANUAL_REVIEW (Consensus Rule 3)
  4. Mismatched Ledger + Elevated Fraud      -> BLOCK_RECOMMENDED (Compound Risk Rule 5)
  5. Simulated Agent Failure (RPC Timeout)   -> MANUAL_REVIEW (Zero-Trust Fail-Safe)

Runs 100% locally: Zero Docker, Zero Kafka, Zero LLMs, Zero external network calls.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
import sys
import tempfile
from typing import List, Optional

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.agent_coordinator import AgentCoordinator
from agents.decision_agent.decision_agent import DecisionType
from agents.fraud_agent.fraud_agent import FraudDetectionAgent
from agents.policy_agent.policy_agent import PolicyAgent
from agents.reconciliation_agent.reconciliation_agent import ReconciliationAgent
from database.database import (
    get_connection,
    init_db,
    insert_ledger_entry,
    insert_transaction,
)
from fraud_detection.profile import AccountProfile, AccountProfileStore
from reconciliation.candidate_service import CandidateService
from reconciliation.ledger_repository import LedgerRepository
from reconciliation.models import LedgerEntry, Transaction
from reconciliation.reconciliation_service import ReconciliationService


def print_banner(text: str) -> None:
    line = "=" * 80
    print(f"\n{line}")
    print(f" {text}")
    print(f"{line}")


def print_section(title: str) -> None:
    print(f"\n--- {title} " + "-" * (75 - len(title)))


def format_currency(amount: Decimal | float, currency: str = "INR") -> str:
    return f"{currency} {Decimal(str(amount)):,.2f}"


def run_demo() -> None:
    print_banner("ZT-LEDGERGUARD DAY 7: MULTI-AGENT COORDINATION & RISK CONSENSUS")
    print(" Architecture: 4 Domain Agents (Recon, Fraud, Policy, Decision/Consensus)")
    print(" Precedence  : BLOCK_RECOMMENDED > MANUAL_REVIEW > APPROVE")
    print(" Zero-Trust  : Agent Failure != Approval -> MANUAL_REVIEW | Zero Mutation Invariant")
    print("=" * 80)

    # Create temporary SQLite environment for pristine demonstration
    temp_dir = tempfile.mkdtemp(prefix="zt_ledgerguard_demo_")
    db_path = os.path.join(temp_dir, "demo_day7.db")
    init_db(db_path)

    # Initialize shared account profile store
    profile_store = AccountProfileStore()
    demo_acc = "ACC-CORP-482910"
    base_time = datetime(2026, 10, 6, 14, 0, 0, tzinfo=timezone.utc)

    # Establish baseline profile: normal transfer ~ 10,000 INR
    profile_store.set_profile(
        AccountProfile(
            account_id=demo_acc,
            transaction_count=120,
            average_amount=Decimal("10000.00"),
            max_amount=Decimal("30000.00"),
            min_amount=Decimal("500.00"),
            average_interval_seconds=3600.0,
            common_transaction_hours=[9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
            common_currencies=["INR"],
            common_transaction_directions=["TRANSFER", "PAYMENT"],
            last_updated=base_time,
        )
    )

    # Helper to construct coordinator
    def make_coordinator(fraud_agent_instance: Optional[FraudDetectionAgent] = None) -> AgentCoordinator:
        repo = LedgerRepository(db_path=db_path)
        cand_svc = CandidateService(repository=repo)
        recon_svc = ReconciliationService(repository=repo, candidate_service=cand_svc, db_path=db_path)
        recon_agent = ReconciliationAgent(service=recon_svc)
        fraud_agent = fraud_agent_instance or FraudDetectionAgent(profile_store=profile_store)
        policy_agent = PolicyAgent()
        return AgentCoordinator(
            reconciliation_agent=recon_agent,
            fraud_agent=fraud_agent,
            policy_agent=policy_agent,
            db_path=db_path,
        )

    # ==========================================================================
    # SCENARIO 1: Clean Matched Ledger + Low Fraud -> APPROVE
    # ==========================================================================
    print_section("SCENARIO 1: Clean Matched Ledger + Low Fraud -> APPROVE")
    print(" Goal: Verify unanimous multi-agent alignment yields authoritative APPROVE with high confidence.")

    tx1 = Transaction(
        transaction_id="TXN-DEMO-001",
        account_id=demo_acc,
        transaction_type="TRANSFER",
        amount=10000.00,
        currency="INR",
        transaction_timestamp=base_time,
        source_system="WEB_BANKING",
        destination_system="CORE_LEDGER",
        reference_id="REF-DEMO-001",
    )
    le1 = LedgerEntry(
        ledger_entry_id="LEDGER-DEMO-001",
        transaction_id="TXN-DEMO-001",
        account_id=demo_acc,
        amount=10000.00,
        currency="INR",
        debit_credit="DEBIT",
        reference_id="REF-DEMO-001",
        timestamp=base_time + timedelta(seconds=2),
        source_system="CORE_BANKING",
    )

    with get_connection(db_path) as conn:
        insert_transaction(conn, tx1)
        insert_ledger_entry(conn, le1)

    coord1 = make_coordinator()
    res1 = coord1.coordinate_decision(tx1)

    print(f" Transaction ID : {tx1.transaction_id} | Amount: {format_currency(tx1.amount)}")
    print(f" Recon Agent    : Status={res1.agent_results.get('reconciliation', {}).get('status')} (Score: {res1.agent_results.get('reconciliation', {}).get('score')})")
    print(f" Fraud Agent    : Risk Level={res1.agent_results.get('fraud', {}).get('risk_level')} (Score: {res1.agent_results.get('fraud', {}).get('score')})")
    print(f" Policy Agent   : Recommendation={res1.agent_results.get('policy', {}).get('recommendation')}")
    print(f" Inter-Agent Conflict : {'YES (Divergence)' if res1.disagreement_detected else 'NONE (Unanimous Agreement)'}")
    print(f" Consensus Decision   : [{res1.decision}] | Confidence: {res1.confidence:.2f}")
    print(f" Primary Rationale    : {res1.decision_reasons[0]}")
    assert res1.decision == DecisionType.APPROVE.value
    assert res1.confidence >= 0.90
    assert not res1.requires_manual_review
    assert not res1.block_recommended

    # ==========================================================================
    # SCENARIO 2: Matched Ledger + Critical Fraud Risk -> BLOCK_RECOMMENDED
    # ==========================================================================
    print_section("SCENARIO 2: Matched Ledger + Critical Fraud Risk -> BLOCK_RECOMMENDED")
    print(" Goal: Verify Precedence Invariant: Critical fraud overrides a 100% clean ledger match.")

    # High amount anomaly: 18x normal profile ($180,000 vs $10,000) at 3:30 AM with multiple prior failures
    tx2_time = datetime(2026, 10, 6, 3, 30, 0, tzinfo=timezone.utc)
    tx2 = Transaction(
        transaction_id="TXN-DEMO-002",
        account_id=demo_acc,
        transaction_type="TRANSFER",
        amount=180000.00,
        currency="INR",
        transaction_timestamp=tx2_time,
        source_system="UNKNOWN_API",
        destination_system="CORE_LEDGER",
        reference_id="REF-DEMO-002",
    )
    le2 = LedgerEntry(
        ledger_entry_id="LEDGER-DEMO-002",
        transaction_id="TXN-DEMO-002",
        account_id=demo_acc,
        amount=180000.00,
        currency="INR",
        debit_credit="DEBIT",
        reference_id="REF-DEMO-002",
        timestamp=tx2_time + timedelta(seconds=1),
        source_system="CORE_BANKING",
    )

    # Create rapid burst of identical transactions at 03:30 AM to simulate credential stuffing / automated theft
    recent_burst = [
        Transaction(
            transaction_id=f"TXN-BURST-{i}",
            account_id=demo_acc,
            transaction_type="TRANSFER",
            amount=180000.00,
            currency="INR",
            transaction_timestamp=tx2_time - timedelta(seconds=i * 8),
            source_system="UNKNOWN_API",
            destination_system="CORE_LEDGER",
            reference_id=f"REF-BURST-{i}",
        )
        for i in range(1, 9)
    ]

    with get_connection(db_path) as conn:
        insert_transaction(conn, tx2)
        insert_ledger_entry(conn, le2)

    coord2 = make_coordinator()
    res2 = coord2.coordinate_decision(tx2, recent_transactions=recent_burst, prior_failed_count=4)

    print(f" Transaction ID : {tx2.transaction_id} | Amount: {format_currency(tx2.amount)} (Off-hours 03:30 AM)")
    print(f" Recon Agent    : Status={res2.agent_results.get('reconciliation', {}).get('status')} (Clean ledger match!)")
    print(f" Fraud Agent    : Risk Level={res2.agent_results.get('fraud', {}).get('risk_level')} (Score: {res2.agent_results.get('fraud', {}).get('score')})")
    print(f" Policy Agent   : Recommendation={res2.agent_results.get('policy', {}).get('recommendation')} | Triggered: {res2.triggered_policies}")
    print(f" Inter-Agent Conflict : {'YES (Detected)' if res2.disagreement_detected else 'NONE'}")
    for d in res2.disagreements:
        print(f"   -> Conflict Detail : {d}")
    print(f" Consensus Decision   : [{res2.decision}] (Overrides Recon APPROVE!)")
    print(f" Block Recommended    : {res2.block_recommended}")
    print(f" Primary Rationale    : {res2.decision_reasons[0]}")
    assert res2.decision == DecisionType.BLOCK_RECOMMENDED.value
    assert res2.block_recommended is True
    assert res2.disagreement_detected is True

    # ==========================================================================
    # SCENARIO 3: Ambiguous Candidate Match + Low Fraud -> MANUAL_REVIEW
    # ==========================================================================
    print_section("SCENARIO 3: Ambiguous Candidate Match + Low Fraud -> MANUAL_REVIEW")
    print(" Goal: Verify Consensus Rule 3: Fuzzy ambiguity mandates manual triage even with 0 fraud score.")

    tx3 = Transaction(
        transaction_id="TXN-DEMO-003",
        account_id=demo_acc,
        transaction_type="PAYMENT",
        amount=9500.00,
        currency="INR",
        transaction_timestamp=base_time + timedelta(minutes=15),
        source_system="MERCHANT_GATEWAY",
        destination_system="CORE_LEDGER",
        reference_id="INV-SPLIT-9500",
    )
    # Seed two competing candidate ledger entries with identical score (ambiguity delta < 0.05)
    cand_a = LedgerEntry(
        ledger_entry_id="LEDGER-CAND-A",
        transaction_id="TXN-DEMO-003-A",
        account_id=demo_acc,
        amount=9500.00,
        currency="INR",
        debit_credit="DEBIT",
        reference_id="INV-SPLIT-9500-1",
        timestamp=base_time + timedelta(minutes=15, seconds=5),
        source_system="CORE_BANKING",
    )
    cand_b = LedgerEntry(
        ledger_entry_id="LEDGER-CAND-B",
        transaction_id="TXN-DEMO-003-B",
        account_id=demo_acc,
        amount=9500.00,
        currency="INR",
        debit_credit="DEBIT",
        reference_id="INV-SPLIT-9500-2",
        timestamp=base_time + timedelta(minutes=15, seconds=6),
        source_system="CORE_BANKING",
    )

    with get_connection(db_path) as conn:
        insert_transaction(conn, tx3)
        insert_ledger_entry(conn, cand_a)
        insert_ledger_entry(conn, cand_b)

    coord3 = make_coordinator()
    res3 = coord3.coordinate_decision(tx3)

    print(f" Transaction ID : {tx3.transaction_id} | Amount: {format_currency(tx3.amount)}")
    print(f" Recon Agent    : Status={res3.agent_results.get('reconciliation', {}).get('status')} (Ambiguous candidate discovery)")
    print(f" Fraud Agent    : Risk Level={res3.agent_results.get('fraud', {}).get('risk_level')} (Score: {res3.agent_results.get('fraud', {}).get('score')} - Completely Benign)")
    print(f" Policy Agent   : Recommendation={res3.agent_results.get('policy', {}).get('recommendation')}")
    print(f" Inter-Agent Conflict : {'YES (Detected)' if res3.disagreement_detected else 'NONE'}")
    for d in res3.disagreements:
        print(f"   -> Conflict Detail : {d}")
    print(f" Consensus Decision   : [{res3.decision}] (Triage mandated)")
    print(f" Requires Review      : {res3.requires_manual_review}")
    print(f" Primary Rationale    : {res3.decision_reasons[0]}")
    assert res3.decision == DecisionType.MANUAL_REVIEW.value
    assert res3.requires_manual_review is True
    assert res3.disagreement_detected is True

    # ==========================================================================
    # SCENARIO 4: Mismatched Ledger + Elevated Fraud -> BLOCK_RECOMMENDED
    # ==========================================================================
    print_section("SCENARIO 4: Mismatched Ledger + Elevated Fraud -> BLOCK_RECOMMENDED")
    print(" Goal: Verify Consensus Rule 5: Compound risk (mismatched entry + elevated fraud) escalates to BLOCK.")

    tx4_time = base_time + timedelta(minutes=30)
    tx4 = Transaction(
        transaction_id="TXN-DEMO-004",
        account_id=demo_acc,
        transaction_type="TRANSFER",
        amount=38000.00,  # Spiked above normal profile
        currency="INR",
        transaction_timestamp=tx4_time,
        source_system="CHANNEL_GATEWAY",
        destination_system="CORE_LEDGER",
        reference_id="REF-DEMO-004",
    )
    # Core banking has a completely mismatched entry ($15,000 vs $38,000)
    le4 = LedgerEntry(
        ledger_entry_id="LEDGER-DEMO-004",
        transaction_id="TXN-DEMO-004",
        account_id=demo_acc,
        amount=15000.00,
        currency="INR",
        debit_credit="DEBIT",
        reference_id="REF-DEMO-004",
        timestamp=tx4_time + timedelta(seconds=1),
        source_system="CORE_BANKING",
    )

    with get_connection(db_path) as conn:
        insert_transaction(conn, tx4)
        insert_ledger_entry(conn, le4)

    coord4 = make_coordinator()
    # Provide rapid velocity to elevate fraud score >= 50
    recent = [
        tx4.model_copy(update={"transaction_id": f"TXN-VEL-{i}", "transaction_timestamp": tx4_time - timedelta(seconds=i*8)})
        for i in range(1, 6)
    ]
    res4 = coord4.coordinate_decision(tx4, recent_transactions=recent, prior_failed_count=2)

    print(f" Transaction ID : {tx4.transaction_id} | Amount: {format_currency(tx4.amount)} vs Ledger: {format_currency(le4.amount)}")
    print(f" Recon Agent    : Status={res4.agent_results.get('reconciliation', {}).get('status')} (Amount Mismatch!)")
    print(f" Fraud Agent    : Risk Level={res4.agent_results.get('fraud', {}).get('risk_level')} (Score: {res4.agent_results.get('fraud', {}).get('score')})")
    print(f" Policy Agent   : Recommendation={res4.agent_results.get('policy', {}).get('recommendation')} | Triggered: {res4.triggered_policies}")
    print(f" Inter-Agent Conflict : {'YES (Detected)' if res4.disagreement_detected else 'NONE'}")
    print(f" Consensus Decision   : [{res4.decision}] (Compound Risk Escalation)")
    print(f" Block Recommended    : {res4.block_recommended}")
    print(f" Primary Rationale    : {res4.decision_reasons[0]}")
    assert res4.decision == DecisionType.BLOCK_RECOMMENDED.value
    assert res4.block_recommended is True

    # ==========================================================================
    # SCENARIO 5: Simulated Agent Failure (RPC Timeout) -> MANUAL_REVIEW
    # ==========================================================================
    print_section("SCENARIO 5: Simulated Agent Failure (RPC Timeout) -> MANUAL_REVIEW")
    print(" Goal: Verify Zero-Trust Safeguard: Agent Failure != Approval. System falls back safely to human triage.")

    tx5 = Transaction(
        transaction_id="TXN-DEMO-005",
        account_id=demo_acc,
        transaction_type="TRANSFER",
        amount=10000.00,
        currency="INR",
        transaction_timestamp=base_time + timedelta(hours=1),
        source_system="WEB_BANKING",
        destination_system="CORE_LEDGER",
        reference_id="REF-DEMO-005",
    )
    le5 = LedgerEntry(
        ledger_entry_id="LEDGER-DEMO-005",
        transaction_id="TXN-DEMO-005",
        account_id=demo_acc,
        amount=10000.00,
        currency="INR",
        debit_credit="DEBIT",
        reference_id="REF-DEMO-005",
        timestamp=base_time + timedelta(hours=1, seconds=2),
        source_system="CORE_BANKING",
    )

    with get_connection(db_path) as conn:
        insert_transaction(conn, tx5)
        insert_ledger_entry(conn, le5)

    # Subclass FraudDetectionAgent to simulate an unhandled network / RPC failure
    class FailingFraudAgent(FraudDetectionAgent):
        def analyze_transaction(self, *args, **kwargs):
            raise TimeoutError("Simulated Fraud Engine Downstream Service RPC Timeout (504)")

    coord5 = make_coordinator(fraud_agent_instance=FailingFraudAgent())
    res5 = coord5.coordinate_decision(tx5)

    print(f" Transaction ID : {tx5.transaction_id} | Amount: {format_currency(tx5.amount)}")
    print(f" Recon Agent    : Status={res5.agent_results.get('reconciliation', {}).get('status')} (Matched)")
    print(f" Fraud Agent    : STATUS=FAILED (Downstream RPC Timeout Simulated)")
    print(f" Inter-Agent Conflict : {'YES (Detected)' if res5.disagreement_detected else 'NONE'}")
    print(f" Consensus Decision   : [{res5.decision}] (Safe fallback)")
    print(f" Confidence Score     : {res5.confidence:.2f} (Penalized due to partial agent failure)")
    print(f" Requires Review      : {res5.requires_manual_review}")
    print(f" Safety Rationale     : {res5.decision_reasons[0]}")
    assert res5.decision == DecisionType.MANUAL_REVIEW.value
    assert res5.requires_manual_review is True
    assert any("FraudDetectionAgent" in r for r in res5.decision_reasons)
    assert res5.confidence < 0.80

    # ==========================================================================
    # SUMMARY
    # ==========================================================================
    print_banner("ALL 5 CONSENSUS SCENARIOS VERIFIED SUCCESSFULLY")
    print("  [1] Clean Match + Low Fraud         -> APPROVE            (Confidence: 1.00)")
    print("  [2] Matched + Critical Fraud Risk   -> BLOCK_RECOMMENDED  (Override Invariant)")
    print("  [3] Ambiguous Match + Low Fraud     -> MANUAL_REVIEW      (Consensus Rule 3)")
    print("  [4] Mismatch + Elevated Fraud       -> BLOCK_RECOMMENDED  (Compound Risk Rule 5)")
    print("  [5] Simulated Agent Failure         -> MANUAL_REVIEW      (Zero-Trust Safeguard)")
    print("================================================================================")


if __name__ == "__main__":
    run_demo()
