"""Tamper-Evident Hash-Chain Ledger for ZT-LedgerGuard.

Zero-Trust Principle:
Audit records form a cryptographically linked, append-only hash chain.
Any silent tampering (payload alteration, record deletion, reordering,
or hash modification) breaks the cryptographic link and is immediately detected.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from security.hashing import hash_data, hash_json
from security.security_config import GENESIS_PREVIOUS_HASH


class ChainIntegrityErrorType(str, Enum):
    """Specific error classifications for hash-chain integrity verification failures."""

    GENESIS_HASH_MISMATCH = "GENESIS_HASH_MISMATCH"
    SEQUENCE_OUT_OF_ORDER = "SEQUENCE_OUT_OF_ORDER"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    DUPLICATE_SEQUENCE = "DUPLICATE_SEQUENCE"
    PREVIOUS_HASH_MISMATCH = "PREVIOUS_HASH_MISMATCH"
    EVENT_HASH_MISMATCH = "EVENT_HASH_MISMATCH"
    PAYLOAD_HASH_MISMATCH = "PAYLOAD_HASH_MISMATCH"


class HashChainBlock(BaseModel):
    """Cryptographically linked block in the tamper-evident audit ledger."""

    sequence_number: int = Field(..., ge=1, description="Strictly monotonic 1-indexed sequence number")
    event_id: str = Field(..., description="Unique event identifier")
    event_type: str = Field(..., description="Canonical event type token")
    timestamp: str = Field(..., description="UTC ISO-8601 creation timestamp")
    payload_hash: str = Field(..., description="SHA-256 digest of canonicalized event payload")
    previous_hash: str = Field(..., description="event_hash of immediate predecessor block, or GENESIS_PREVIOUS_HASH")
    event_hash: str = Field(..., description="SHA-256 digest of block header and previous hash")
    sanitized_payload: Optional[Dict[str, Any]] = Field(default=None, description="Masked/tokenized audit payload")


def compute_event_hash(
    sequence_number: int,
    event_id: str,
    event_type: str,
    timestamp: str,
    payload_hash: str,
    previous_hash: str,
) -> str:
    """Deterministically compute block event_hash from canonical attributes.

    Header components:
    sequence_number|event_id|event_type|timestamp|payload_hash|previous_hash
    """
    raw_header = f"{sequence_number}|{event_id}|{event_type}|{timestamp}|{payload_hash}|{previous_hash}"
    return hash_data(raw_header)


class ChainVerificationResult(BaseModel):
    """Structured, explainable outcome of a hash-chain verification audit."""

    valid: bool = Field(..., description="True if the entire audited chain is cryptographically intact")
    events_checked: int = Field(..., description="Total number of consecutive blocks verified")
    first_invalid_sequence: Optional[int] = Field(default=None, description="Sequence number of first failing block")
    error_type: Optional[str] = Field(default=None, description="Categorical integrity failure type")
    expected_hash: Optional[str] = Field(default=None, description="Cryptographically expected hash")
    actual_hash: Optional[str] = Field(default=None, description="Found/tampered hash value")
    details: Optional[str] = Field(default=None, description="Human-auditable explanation of failure")


class HashChain:
    """In-memory append-only tamper-evident hash-chain manager."""

    def __init__(self, genesis_previous_hash: str = GENESIS_PREVIOUS_HASH) -> None:
        self.genesis_previous_hash: str = genesis_previous_hash
        self._blocks: List[HashChainBlock] = []

    @property
    def current_sequence(self) -> int:
        return len(self._blocks)

    @property
    def latest_block(self) -> Optional[HashChainBlock]:
        return self._blocks[-1] if self._blocks else None

    @property
    def latest_hash(self) -> str:
        return self._blocks[-1].event_hash if self._blocks else self.genesis_previous_hash

    def append_block(
        self,
        event_id: str,
        event_type: str,
        payload_hash: str,
        timestamp: Optional[str] = None,
        sanitized_payload: Optional[Dict[str, Any]] = None,
    ) -> HashChainBlock:
        """Append a new verified audit block to the chain."""
        next_seq = len(self._blocks) + 1
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        prev_hash = self.latest_hash

        ev_hash = compute_event_hash(
            sequence_number=next_seq,
            event_id=event_id,
            event_type=event_type,
            timestamp=ts,
            payload_hash=payload_hash,
            previous_hash=prev_hash,
        )

        block = HashChainBlock(
            sequence_number=next_seq,
            event_id=event_id,
            event_type=event_type,
            timestamp=ts,
            payload_hash=payload_hash,
            previous_hash=prev_hash,
            event_hash=ev_hash,
            sanitized_payload=sanitized_payload,
        )
        self._blocks.append(block)
        return block

    def get_blocks(self) -> List[HashChainBlock]:
        """Return shallow copy of all blocks."""
        return list(self._blocks)

    @staticmethod
    def verify_chain(blocks: List[HashChainBlock]) -> ChainVerificationResult:
        """Cryptographically verify the integrity of a collection of chain blocks.

        Verifies:
        1. Genesis block previous_hash equals GENESIS_PREVIOUS_HASH.
        2. Sequence numbers are strictly monotonic: 1, 2, 3...
        3. No sequence gaps, duplicates, or reorderings.
        4. Every block's previous_hash matches the prior block's event_hash.
        5. Every block's event_hash matches recomputed hash.
        6. If sanitized_payload is present, its hash matches payload_hash.
        """
        if not blocks:
            return ChainVerificationResult(
                valid=True,
                events_checked=0,
                details="Empty audit chain: 0 events checked.",
            )

        expected_seq = 1
        expected_prev_hash = GENESIS_PREVIOUS_HASH

        for idx, block in enumerate(blocks):
            # Check Sequence Continuity
            if block.sequence_number != expected_seq:
                if block.sequence_number < expected_seq:
                    return ChainVerificationResult(
                        valid=False,
                        events_checked=idx,
                        first_invalid_sequence=block.sequence_number,
                        error_type=ChainIntegrityErrorType.DUPLICATE_SEQUENCE.value,
                        details=f"Duplicate or decreasing sequence number {block.sequence_number}; expected {expected_seq}",
                    )
                else:
                    return ChainVerificationResult(
                        valid=False,
                        events_checked=idx,
                        first_invalid_sequence=block.sequence_number,
                        error_type=ChainIntegrityErrorType.SEQUENCE_GAP.value,
                        details=f"Sequence gap detected: found sequence {block.sequence_number}; expected {expected_seq}",
                    )

            # Check Previous Hash Linkage
            if block.previous_hash != expected_prev_hash:
                err_type = (
                    ChainIntegrityErrorType.GENESIS_HASH_MISMATCH.value
                    if expected_seq == 1
                    else ChainIntegrityErrorType.PREVIOUS_HASH_MISMATCH.value
                )
                return ChainVerificationResult(
                    valid=False,
                    events_checked=idx,
                    first_invalid_sequence=block.sequence_number,
                    error_type=err_type,
                    expected_hash=expected_prev_hash,
                    actual_hash=block.previous_hash,
                    details=f"Broken hash link at sequence {block.sequence_number}: expected previous_hash '{expected_prev_hash[:16]}...', got '{block.previous_hash[:16]}...'",
                )

            # Check Block Event Hash Integrity
            recomputed_hash = compute_event_hash(
                sequence_number=block.sequence_number,
                event_id=block.event_id,
                event_type=block.event_type,
                timestamp=block.timestamp,
                payload_hash=block.payload_hash,
                previous_hash=block.previous_hash,
            )
            if block.event_hash != recomputed_hash:
                return ChainVerificationResult(
                    valid=False,
                    events_checked=idx,
                    first_invalid_sequence=block.sequence_number,
                    error_type=ChainIntegrityErrorType.EVENT_HASH_MISMATCH.value,
                    expected_hash=recomputed_hash,
                    actual_hash=block.event_hash,
                    details=f"Event hash tampering detected at sequence {block.sequence_number}: block header altered.",
                )

            # Check Payload Hash if sanitized_payload is attached
            if block.sanitized_payload is not None:
                recomputed_payload_hash = hash_json(block.sanitized_payload)
                if block.payload_hash != recomputed_payload_hash:
                    return ChainVerificationResult(
                        valid=False,
                        events_checked=idx,
                        first_invalid_sequence=block.sequence_number,
                        error_type=ChainIntegrityErrorType.PAYLOAD_HASH_MISMATCH.value,
                        expected_hash=recomputed_payload_hash,
                        actual_hash=block.payload_hash,
                        details=f"Payload tampering detected at sequence {block.sequence_number}: stored payload does not match payload_hash.",
                    )

            expected_seq += 1
            expected_prev_hash = block.event_hash

        return ChainVerificationResult(
            valid=True,
            events_checked=len(blocks),
            details=f"All {len(blocks)} audit blocks verified cryptographically valid.",
        )
