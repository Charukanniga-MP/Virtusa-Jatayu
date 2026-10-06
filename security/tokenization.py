"""Deterministic Tokenization Engine for ZT-LedgerGuard.

Zero-Trust Principle:
Sensitive identifiers (such as account numbers and customer IDs) are replaced
with surrogate deterministic tokens for internal correlation and processing.

DISTINCTIONS:
- Masking: Irreversible redaction for display/logging (e.g. 'ACC-*****6789').
- Tokenization: Reversible surrogate reference ('tok_...') scoped to an internal vault.
- Hashing: One-way cryptographic digest (SHA-256) for integrity checking.
- Encryption: Cryptographic confidentiality transformation with a secret key.
"""

from __future__ import annotations

import hashlib
import hmac
import threading
from typing import Dict, Optional, Tuple

from security.pii_protection import mask_account_id
from security.security_config import TOKEN_PREFIX, TOKEN_SALT


class TokenVault:
    """Thread-safe in-memory surrogate token store for sensitive identifiers.

    Ensures raw account numbers and customer IDs remain isolated inside
    the trust boundary while allowing internal correlation via tokens.
    """

    def __init__(self, salt: Optional[str] = None) -> None:
        self._salt: str = salt or TOKEN_SALT
        self._token_to_raw: Dict[str, str] = {}
        self._raw_to_token: Dict[str, str] = {}
        self._token_to_masked: Dict[str, str] = {}
        self._lock = threading.Lock()

    def tokenize(self, raw_identifier: str, context: str = "account") -> str:
        """Deterministically tokenize a sensitive identifier.

        Args:
            raw_identifier: The raw sensitive string (e.g., account ID).
            context: Domain namespace (e.g., 'account', 'customer').

        Returns:
            Surrogate token string prefixed with 'tok_'.
        """
        if not raw_identifier:
            return ""

        clean_raw = str(raw_identifier).strip()

        with self._lock:
            # Check existing mapping
            key = f"{context}:{clean_raw}"
            if key in self._raw_to_token:
                return self._raw_to_token[key]

            # Generate deterministic HMAC-SHA256 token
            h = hmac.new(
                key=self._salt.encode("utf-8"),
                msg=f"{context}:{clean_raw}".encode("utf-8"),
                digestmod=hashlib.sha256,
            )
            digest = h.hexdigest()[:16]  # 16-character hex digest
            token = f"{TOKEN_PREFIX}{digest}"

            # Register bidirectional mapping
            self._raw_to_token[key] = token
            self._token_to_raw[token] = clean_raw
            self._token_to_masked[token] = mask_account_id(clean_raw)

            return token

    def lookup(self, token: str) -> Optional[str]:
        """Internal lookup of raw identifier from surrogate token.

        SECURITY: This function must NEVER be exposed directly in public API responses!
        """
        if not token:
            return None
        with self._lock:
            return self._token_to_raw.get(str(token).strip())

    def lookup_masked(self, token: str) -> Optional[str]:
        """Safe lookup returning the masked representation for display/logging."""
        if not token:
            return None
        with self._lock:
            return self._token_to_masked.get(str(token).strip())

    def clear(self) -> None:
        """Clear the in-memory vault (primarily for testing isolation)."""
        with self._lock:
            self._token_to_raw.clear(self._raw_to_token.clear() or {})
            self._token_to_masked.clear()


# Global default vault instance for application runtime
_DEFAULT_VAULT = TokenVault()


def tokenize_identifier(raw_identifier: str, context: str = "account") -> str:
    """Deterministically tokenize an identifier using the default vault."""
    return _DEFAULT_VAULT.tokenize(raw_identifier, context=context)


def tokenize_account_id(account_id: str) -> str:
    """Deterministically tokenize an account ID."""
    return tokenize_identifier(account_id, context="account")


def lookup_by_account_token(token: str) -> Optional[str]:
    """Retrieve raw account ID internally by surrogate token.

    SECURITY INVARIANT:
    Internal operations only. Never expose raw account IDs in public responses.
    """
    return _DEFAULT_VAULT.lookup(token)


def get_token_vault() -> TokenVault:
    """Access the singleton TokenVault instance."""
    return _DEFAULT_VAULT
