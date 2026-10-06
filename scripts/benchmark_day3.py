"""Day 3 Performance Benchmark — Local Event Orchestration Pipeline.

Generates 100 synthetic banking transactions and streams them through the
Day 3 decoupled event-driven orchestration pipeline.

Measures:
- Total transactions processed
- Total events dispatched
- Successful events
- Failed events
- Duplicates handled
- Total processing time
- Event throughput (events/sec)
"""

import os
import sys
import time
import uuid
from typing import Dict, Any

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.transaction_generator import TransactionGenerator
from orchestrator.event_orchestrator import Event, EventOrchestrator
from orchestrator.event_types import EventType
from orchestrator.processing_pipeline import ProcessingPipeline
from orchestrator.workflow_state import WorkflowTracker
from orchestrator.event_audit import EventAuditTrail


def run_benchmark(transaction_count: int = 100) -> Dict[str, Any]:
    print("=" * 70)
    print("ZT-LedgerGuard Day 3: Local Event Orchestration Benchmark")
    print("=" * 70)
    print(f"[*] Initializing pipeline components for {transaction_count} transactions...")

    tracker = WorkflowTracker()
    audit = EventAuditTrail()
    orchestrator = EventOrchestrator(workflow_tracker=tracker, audit_trail=audit)
    pipeline = ProcessingPipeline(orchestrator=orchestrator, workflow_tracker=tracker)

    generator = TransactionGenerator(seed=202610, auto_sequence=True)
    transactions = list(generator.generate_transactions(count=transaction_count))
    print(f"[*] Generated {len(transactions)} synthetic banking transactions.")

    # Track duplicates
    duplicate_events = []

    start_time = time.perf_counter()

    for idx, tx in enumerate(transactions):
        corr_id = f"CORR-BENCH-{idx:05d}"
        tracker.create_workflow(correlation_id=corr_id)

        event = Event(
            event_type=EventType.TRANSACTION_RECEIVED.value,
            source="benchmark_harness",
            payload={
                "transaction_id": tx.transaction_id,
                "account_id": tx.account_id,
                "amount": tx.amount,
                "currency": tx.currency,
                "transaction_type": tx.transaction_type,
            },
            correlation_id=corr_id,
        )

        orchestrator.publish(event)

        # Store every 20th event to test duplicate detection
        if idx % 20 == 0:
            duplicate_events.append(event)

    # Re-publish duplicates to benchmark idempotency
    for dup_event in duplicate_events:
        orchestrator.publish(dup_event)

    elapsed_time = time.perf_counter() - start_time

    metrics = orchestrator.get_metrics()
    completed_workflows = sum(
        1 for w in tracker.get_all_workflows() if w.status == "COMPLETED"
    )

    results = {
        "transactions_generated": transaction_count,
        "workflows_started": metrics["workflows_started"],
        "workflows_completed": completed_workflows,
        "total_events_received": metrics["events_received"],
        "successful_events_processed": metrics["events_processed"],
        "failed_events": metrics["events_failed"],
        "retried_events": metrics["events_retried"],
        "duplicate_events_ignored": metrics["events_duplicate"],
        "total_processing_time_sec": round(elapsed_time, 4),
        "transaction_throughput_tps": round(transaction_count / elapsed_time, 2),
        "event_throughput_eps": round(metrics["events_received"] / elapsed_time, 2),
    }

    print("-" * 70)
    print(f"Total Transactions Processed : {results['transactions_generated']}")
    print(f"Workflows Started / Completed: {results['workflows_started']} / {results['workflows_completed']}")
    print(f"Total Events Received        : {results['total_events_received']}")
    print(f"Successful Events Processed  : {results['successful_events_processed']}")
    print(f"Failed Events                : {results['failed_events']}")
    print(f"Duplicate Events Detected    : {results['duplicate_events_ignored']}")
    print(f"Total Processing Time        : {results['total_processing_time_sec']:.4f} seconds")
    print(f"Transaction Throughput       : {results['transaction_throughput_tps']:.2f} tx/sec")
    print(f"Event Processing Rate        : {results['event_throughput_eps']:.2f} events/sec")
    print("=" * 70)
    print("[OK] Benchmark completed successfully.")
    return results


if __name__ == "__main__":
    run_benchmark(100)
