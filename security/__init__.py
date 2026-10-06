"""Security module initialization for ZT-LedgerGuard.

Exposes Day 1 foundational utilities and Day 8 Zero-Trust security components.
"""

from security.audit_integrity import AuditIntegrityService, get_audit_integrity_service
from security.hash_chain import (
    ChainIntegrityErrorType,
    ChainVerificationResult,
    HashChain,
    HashChainBlock,
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
    HASH_ALGORITHM,
    MAX_REQUEST_SIZE_BYTES,
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
    get_security_gateway,
)
from security.security_utils import sanitize_log_message
from security.tokenization import (
    TokenVault,
    get_token_vault,
    lookup_by_account_token,
    tokenize_account_id,
    tokenize_identifier,
)

__all__ = [
    # Day 1 backwards compatibility
    "mask_account_id",
    "mask_sensitive_value",
    "sanitize_log_message",
    # Day 8 PII protection
    "PIIClassificationLevel",
    "get_field_classification",
    "is_sensitive_field",
    "mask_card_number",
    "mask_phone_number",
    "mask_email",
    "mask_government_id",
    "mask_pii_value",
    "sanitize_payload",
    "sanitize_text",
    # Day 8 Tokenization
    "tokenize_identifier",
    "tokenize_account_id",
    "lookup_by_account_token",
    "TokenVault",
    "get_token_vault",
    # Day 8 Hashing & Canonicalization
    "canonicalize_json",
    "hash_data",
    "hash_json",
    "hash_transaction",
    "hash_event",
    "hash_decision",
    # Day 8 Hash Chain
    "HashChain",
    "HashChainBlock",
    "ChainVerificationResult",
    "ChainIntegrityErrorType",
    "GENESIS_PREVIOUS_HASH",
    "HASH_ALGORITHM",
    # Day 8 Audit Integrity
    "AuditIntegrityService",
    "get_audit_integrity_service",
    # Day 8 Security Gateway
    "SecurityGateway",
    "get_security_gateway",
    "GatewayValidationResult",
    "MAX_REQUEST_SIZE_BYTES",
    "TOKEN_PREFIX",
    # Day 8 Security Events & Telemetry
    "SecurityEventType",
    "SecurityMetricsStore",
    "get_security_metrics_store",
]
