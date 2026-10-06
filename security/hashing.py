"""Cryptographic Hashing and Canonicalization for ZT-LedgerGuard.

Zero-Trust Principle:
All structured data must be canonically formatted before computing SHA-256
digests. Key insertion order, whitespace, and floating-point variances must
never cause non-deterministic hash divergence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from typing import Any, Dict, Union

from security.security_config import HASH_ALGORITHM
from security.tokenization import tokenize_account_id


def _normalize_value_for_canonicalization(val: Any) -> Any:
    """Recursively normalize data types for deterministic serialization."""
    if isinstance(val, (datetime,)):
        # Normalize to UTC ISO-8601 string with Z
        if val.tzinfo is None:
            val = val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc).isoformat()

    if isinstance(val, Decimal):
        # Format decimal with consistent fixed-point representation
        return f"{val:.2f}"

    if isinstance(val, float):
        # Format float with 2 decimal places to avoid IEEE-754 drift
        return f"{val:.2f}"

    if isinstance(val, dict):
        return {str(k): _normalize_value_for_canonicalization(v) for k, v in sorted(val.items())}

    if isinstance(val, (list, tuple)):
        return [_normalize_value_for_canonicalization(item) for item in val]

    if hasattr(val, "model_dump"):
        return _normalize_value_for_canonicalization(val.model_dump())

    if hasattr(val, "__dict__"):
        return _normalize_value_for_canonicalization(vars(val))

    return val


def canonicalize_json(data: Any) -> str:
    """Produce deterministic canonical JSON string.

    Guarantees:
        1. Keys sorted alphabetically at all nesting levels.
        2. No extraneous whitespace: separators=(',', ':').
        3. Deterministic float and datetime formatting.
        4. Key-order independence: {'a': 1, 'b': 2} == {'b': 2, 'a': 1}.
    """
    normalized = _normalize_value_for_canonicalization(data)
    return json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )


def hash_data(data: Union[str, bytes]) -> str:
    """Compute SHA-256 hexadecimal digest of raw string or bytes."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def hash_json(data: Any) -> str:
    """Canonicalize and compute SHA-256 digest of structured data."""
    canonical_str = canonicalize_json(data)
    return hash_data(canonical_str)


def hash_transaction(transaction: Any) -> str:
    """Compute canonical integrity hash for a financial transaction.

    Covers core non-mutable banking attributes:
    - transaction_id
    - amount (Decimal/2-decimal normalized)
    - currency (ISO 3-letter uppercase)
    - account_token (deterministic surrogate token)
    - transaction_timestamp
    - reference_id
    - transaction_type
    """
    if hasattr(transaction, "model_dump"):
        t_dict = transaction.model_dump()
    elif isinstance(transaction, dict):
        t_dict = dict(transaction)
    else:
        t_dict = vars(transaction)

    # Use tokenized account ID so raw PII is never part of public hash payload
    raw_acc = str(t_dict.get("account_id", ""))
    acc_token = tokenize_account_id(raw_acc) if raw_acc else ""

    amt = t_dict.get("amount", 0.0)
    amt_str = f"{Decimal(str(amt)):.2f}"

    canonical_payload = {
        "transaction_id": str(t_dict.get("transaction_id", "")),
        "amount": amt_str,
        "currency": str(t_dict.get("currency", "")).upper(),
        "account_token": acc_token,
        "transaction_timestamp": str(t_dict.get("transaction_timestamp", "")),
        "reference_id": str(t_dict.get("reference_id", "")),
        "transaction_type": str(t_dict.get("transaction_type", "")).upper(),
    }
    return hash_json(canonical_payload)


def hash_event(event: Any) -> str:
    """Compute canonical integrity hash for an event envelope and payload."""
    if hasattr(event, "model_dump"):
        e_dict = event.model_dump()
    elif isinstance(event, dict):
        e_dict = dict(event)
    else:
        e_dict = vars(event)

    canonical_payload = {
        "event_id": str(e_dict.get("event_id", "")),
        "event_type": str(e_dict.get("event_type", "")),
        "correlation_id": str(e_dict.get("correlation_id", "")),
        "timestamp": str(e_dict.get("timestamp", "")),
        "payload": e_dict.get("payload", {}),
    }
    return hash_json(canonical_payload)


def hash_decision(decision: Any) -> str:
    """Compute canonical integrity hash for a multi-agent consensus decision."""
    if hasattr(decision, "model_dump"):
        d_dict = decision.model_dump()
    elif isinstance(decision, dict):
        d_dict = dict(decision)
    else:
        d_dict = vars(decision)

    canonical_payload = {
        "transaction_id": str(d_dict.get("transaction_id", "")),
        "decision": str(d_dict.get("decision", "")),
        "confidence": round(float(d_dict.get("confidence", 0.0)), 4),
        "agent_results": d_dict.get("agent_results", {}),
        "triggered_policies": sorted(list(d_dict.get("triggered_policies", []))),
        "decision_reasons": sorted(list(d_dict.get("decision_reasons", []))),
        "disagreements": sorted(list(d_dict.get("disagreements", []))),
    }
    return hash_json(canonical_payload)
