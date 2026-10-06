"""Security Event Types and Metrics Tracking for ZT-LedgerGuard.

Zero-Trust Principle:
All security incidents, integrity failures, and validation breaches
produce auditable security events and increment telemetry metrics
without leaking raw PII.
"""

from __future__ import annotations

from enum import Enum
import threading
from typing import Any, Dict


class SecurityEventType(str, Enum):
    """Canonical security-specific event type tokens."""

    SECURITY_VALIDATION_FAILED = "SECURITY_VALIDATION_FAILED"
    PII_ACCESS_DETECTED = "PII_ACCESS_DETECTED"
    AUDIT_INTEGRITY_VERIFICATION_FAILED = "AUDIT_INTEGRITY_VERIFICATION_FAILED"
    SECURITY_POLICY_VIOLATION = "SECURITY_POLICY_VIOLATION"


class SecurityMetricsStore:
    """Thread-safe telemetry store for Zero-Trust security metrics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._total_security_checks: int = 0
        self._validation_failures: int = 0
        self._pii_sanitizations: int = 0
        self._integrity_checks: int = 0
        self._integrity_failures: int = 0
        self._security_policy_violations: int = 0

    def record_security_check(self, count: int = 1) -> None:
        with self._lock:
            self._total_security_checks += count

    def record_validation_failure(self, count: int = 1) -> None:
        with self._lock:
            self._validation_failures += count

    def record_pii_sanitization(self, count: int = 1) -> None:
        with self._lock:
            self._pii_sanitizations += count

    def record_integrity_check(self, count: int = 1) -> None:
        with self._lock:
            self._integrity_checks += count

    def record_integrity_failure(self, count: int = 1) -> None:
        with self._lock:
            self._integrity_failures += count

    def record_policy_violation(self, count: int = 1) -> None:
        with self._lock:
            self._security_policy_violations += count

    def get_metrics(self) -> Dict[str, int]:
        with self._lock:
            return {
                "total_security_checks": self._total_security_checks,
                "validation_failures": self._validation_failures,
                "pii_sanitizations": self._pii_sanitizations,
                "integrity_checks": self._integrity_checks,
                "integrity_failures": self._integrity_failures,
                "security_policy_violations": self._security_policy_violations,
            }

    def reset(self) -> None:
        with self._lock:
            self._total_security_checks = 0
            self._validation_failures = 0
            self._pii_sanitizations = 0
            self._integrity_checks = 0
            self._integrity_failures = 0
            self._security_policy_violations = 0


# Global metrics store instance
_DEFAULT_METRICS_STORE = SecurityMetricsStore()


def get_security_metrics_store() -> SecurityMetricsStore:
    """Access the default SecurityMetricsStore instance."""
    return _DEFAULT_METRICS_STORE
