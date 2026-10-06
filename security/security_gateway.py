"""Zero-Trust Security Gateway for ZT-LedgerGuard.

Zero-Trust Principle:
All requests originating from external clients are considered UNTRUSTED.
Input validation, size boundaries, schema conformance, and trust-boundary
sanitization are strictly enforced before payloads reach domain agents.

CRITICAL INVARIANT:
Client-provided decisions (decision, fraud_score, risk_level, confidence,
block_recommended) are NEVER TRUSTED and are stripped immediately.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import json
import re
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from security.pii_protection import sanitize_payload
from security.security_config import (
    ALLOWED_CONTENT_TYPES,
    ALLOWED_CURRENCIES,
    MAX_REQUEST_SIZE_BYTES,
    MAX_TRANSACTION_AMOUNT,
    MIN_TRANSACTION_AMOUNT,
)
from security.security_events import get_security_metrics_store


class GatewayValidationResult(BaseModel):
    """Structured evaluation output of the Zero-Trust Security Gateway."""

    is_valid: bool = Field(..., description="True if the request passes all zero-trust checks")
    sanitized_data: Optional[Dict[str, Any]] = Field(
        default=None, description="Cleaned, sanitized data stripped of untrusted client fields"
    )
    rejection_reason: Optional[str] = Field(default=None, description="Primary cause for rejection")
    violations: List[str] = Field(default_factory=list, description="All policy violations discovered")
    untrusted_fields_stripped: List[str] = Field(
        default_factory=list, description="Client fields that were stripped at the trust boundary"
    )


# Untrusted decision fields that clients must never inject
_UNTRUSTED_CLIENT_FIELDS = {
    "fraud_score",
    "risk_level",
    "decision",
    "confidence",
    "block_recommended",
    "requires_manual_review",
    "reconciliation_status",
    "reconciliation_score",
}

_VALID_ID_REGEX = re.compile(r"^[A-Za-z0-9_\-\.]{3,64}$")


class SecurityGateway:
    """Zero-Trust input validation and trust boundary firewall."""

    def __init__(
        self,
        max_request_size: int = MAX_REQUEST_SIZE_BYTES,
        metrics_store=None,
    ) -> None:
        self.max_request_size = max_request_size
        self.metrics_store = metrics_store or get_security_metrics_store()

    def validate_request_bytes(self, raw_bytes: bytes, content_type: Optional[str] = None) -> GatewayValidationResult:
        """Validate raw request bytes for size limits and content-type compliance."""
        self.metrics_store.record_security_check()

        # 1. Enforce payload size limit
        if len(raw_bytes) > self.max_request_size:
            msg = f"Request size ({len(raw_bytes)} bytes) exceeds maximum permitted limit ({self.max_request_size} bytes)."
            self.metrics_store.record_validation_failure()
            return GatewayValidationResult(
                is_valid=False,
                rejection_reason="REQUEST_SIZE_EXCEEDED",
                violations=[msg],
            )

        # 2. Enforce Content-Type if specified
        if content_type:
            media_type = content_type.split(";")[0].strip().lower()
            if media_type not in ALLOWED_CONTENT_TYPES:
                msg = f"Invalid Content-Type '{content_type}'. Must be application/json."
                self.metrics_store.record_validation_failure()
                return GatewayValidationResult(
                    is_valid=False,
                    rejection_reason="UNSUPPORTED_MEDIA_TYPE",
                    violations=[msg],
                )

        # 3. Parse JSON safely
        try:
            payload = json.loads(raw_bytes.decode("utf-8"))
        except Exception as e:
            self.metrics_store.record_validation_failure()
            return GatewayValidationResult(
                is_valid=False,
                rejection_reason="MALFORMED_JSON",
                violations=[f"Malformed JSON payload: {str(e)}"],
            )

        if not isinstance(payload, dict):
            self.metrics_store.record_validation_failure()
            return GatewayValidationResult(
                is_valid=False,
                rejection_reason="INVALID_BODY_TYPE",
                violations=["Request JSON root must be an object/dict."],
            )

        return self.validate_transaction_payload(payload)

    def validate_transaction_payload(self, data: Dict[str, Any]) -> GatewayValidationResult:
        """Validate transaction schema, enforce boundary sanitization, and strip client decisions."""
        self.metrics_store.record_security_check()
        violations: List[str] = []
        stripped_fields: List[str] = []

        if not isinstance(data, dict):
            self.metrics_store.record_validation_failure()
            return GatewayValidationResult(
                is_valid=False,
                rejection_reason="INVALID_INPUT_TYPE",
                violations=["Payload must be a dictionary object."],
            )

        # Copy data so we do not mutate original
        cleaned_data = dict(data)

        # ----------------------------------------------------------------------
        # 1. TRUST BOUNDARY: Strip Client-Provided Decisions
        # ----------------------------------------------------------------------
        for field in _UNTRUSTED_CLIENT_FIELDS:
            if field in cleaned_data:
                stripped_fields.append(field)
                del cleaned_data[field]
                self.metrics_store.record_policy_violation()

        # ----------------------------------------------------------------------
        # 2. Transaction ID Validation
        # ----------------------------------------------------------------------
        tx_id = cleaned_data.get("transaction_id")
        if not tx_id or not isinstance(tx_id, str) or not tx_id.strip():
            violations.append("Field 'transaction_id' is missing or empty.")
        elif not _VALID_ID_REGEX.match(tx_id.strip()):
            violations.append(
                f"Field 'transaction_id' '{tx_id}' contains invalid characters or length. Must match alphanumeric with dashes/underscores (3-64 chars)."
            )

        # ----------------------------------------------------------------------
        # 3. Account ID Validation
        # ----------------------------------------------------------------------
        acc_id = cleaned_data.get("account_id")
        if not acc_id or not isinstance(acc_id, str) or not acc_id.strip():
            violations.append("Field 'account_id' is missing or empty.")

        # ----------------------------------------------------------------------
        # 4. Monetary Amount Validation
        # ----------------------------------------------------------------------
        raw_amt = cleaned_data.get("amount")
        if raw_amt is None:
            violations.append("Field 'amount' is required.")
        else:
            try:
                amt = Decimal(str(raw_amt))
                if amt < MIN_TRANSACTION_AMOUNT:
                    violations.append(f"Amount {amt} is below minimum allowed value ({MIN_TRANSACTION_AMOUNT}).")
                elif amt > MAX_TRANSACTION_AMOUNT:
                    violations.append(f"Amount {amt} exceeds maximum permitted ceiling ({MAX_TRANSACTION_AMOUNT}).")
            except Exception:
                violations.append(f"Invalid monetary amount format: '{raw_amt}'.")

        # ----------------------------------------------------------------------
        # 5. Currency Validation
        # ----------------------------------------------------------------------
        currency = cleaned_data.get("currency")
        if not currency or not isinstance(currency, str):
            violations.append("Field 'currency' is required.")
        elif currency.upper().strip() not in ALLOWED_CURRENCIES:
            violations.append(
                f"Currency '{currency}' is not supported. Allowed: {sorted(list(ALLOWED_CURRENCIES))}."
            )

        # ----------------------------------------------------------------------
        # 6. Timestamp Validation
        # ----------------------------------------------------------------------
        ts = cleaned_data.get("transaction_timestamp")
        if ts is not None:
            if isinstance(ts, str):
                try:
                    clean_ts = ts.replace("Z", "+00:00")
                    datetime.fromisoformat(clean_ts)
                except Exception:
                    violations.append(f"Invalid timestamp format '{ts}'. Must be valid ISO-8601 string.")
            elif not isinstance(ts, datetime):
                violations.append("Field 'transaction_timestamp' must be ISO-8601 string or datetime.")

        # ----------------------------------------------------------------------
        # 7. Check for Violations
        # ----------------------------------------------------------------------
        if violations:
            self.metrics_store.record_validation_failure()
            return GatewayValidationResult(
                is_valid=False,
                rejection_reason="REQUEST_VALIDATION_FAILED",
                violations=violations,
                untrusted_fields_stripped=stripped_fields,
            )

        # Pass: return sanitized payload
        return GatewayValidationResult(
            is_valid=True,
            sanitized_data=cleaned_data,
            untrusted_fields_stripped=stripped_fields,
        )


# Global default gateway instance
_DEFAULT_GATEWAY = SecurityGateway()


def get_security_gateway() -> SecurityGateway:
    """Access the default SecurityGateway instance."""
    return _DEFAULT_GATEWAY
