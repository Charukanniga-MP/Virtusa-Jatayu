"""Base Agent Contract for ZT-LedgerGuard Multi-Agent Architecture.

Defines the lightweight, extensible interface and execution record models
implemented by all specialized domain agents (ReconciliationAgent, FraudDetectionAgent,
PolicyAgent, and DecisionAgent).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class AgentExecutionRecord(BaseModel):
    """Immutable audit record produced by an agent execution."""

    agent_name: str = Field(..., description="Canonical name of the executing agent")
    agent_version: str = Field(..., description="Semantic version of the executing agent")
    transaction_id: str = Field(..., description="Target transaction identifier")
    correlation_id: Optional[str] = Field(default=None, description="End-to-end trace correlation ID")
    started_at: datetime = Field(..., description="Timestamp when agent execution started")
    completed_at: datetime = Field(..., description="Timestamp when agent execution completed")
    execution_time_ms: float = Field(..., ge=0.0, description="Elapsed execution time in milliseconds")
    decision: str = Field(..., description="Primary status or decision returned by the agent")
    evidence: List[Dict[str, Any]] = Field(default_factory=list, description="Structured explainable evidence")
    success: bool = Field(default=True, description="Whether the agent executed successfully without unhandled errors")
    error_message: Optional[str] = Field(default=None, description="Error explanation if execution failed")


class BaseAgent(ABC):
    """Abstract base class establishing the contract for all ZT-LedgerGuard agents."""

    agent_name: str = "BaseAgent"
    agent_version: str = "1.0.0"

    def __init__(self, agent_name: Optional[str] = None, agent_version: Optional[str] = None) -> None:
        if agent_name:
            self.agent_name = agent_name
        if agent_version:
            self.agent_version = agent_version

    @abstractmethod
    def analyze(self, *args: Any, **kwargs: Any) -> Any:
        """Execute agent analysis and return structured result with evidence."""
        raise NotImplementedError("Subclasses must implement analyze()")

    def create_execution_record(
        self,
        transaction_id: str,
        decision: str,
        started_at: datetime,
        completed_at: datetime,
        evidence: Optional[List[Dict[str, Any]]] = None,
        correlation_id: Optional[str] = None,
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> AgentExecutionRecord:
        """Helper to create a validated AgentExecutionRecord."""
        elapsed_ms = max(0.0, (completed_at - started_at).total_seconds() * 1000.0)
        return AgentExecutionRecord(
            agent_name=self.agent_name,
            agent_version=self.agent_version,
            transaction_id=transaction_id,
            correlation_id=correlation_id,
            started_at=started_at,
            completed_at=completed_at,
            execution_time_ms=round(elapsed_ms, 3),
            decision=decision,
            evidence=evidence or [],
            success=success,
            error_message=error_message,
        )
