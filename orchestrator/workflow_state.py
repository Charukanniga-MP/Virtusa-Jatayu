"""Workflow State Tracking for ZT-LedgerGuard Event Workflows.

Maintains in-memory workflow state keyed by correlation_id to provide
end-to-end visibility across multi-stage financial transaction processing.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
import threading

from pydantic import BaseModel, Field


class WorkflowStatus(str, Enum):
    """Overall status of an end-to-end event workflow."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class StageStatus(str, Enum):
    """Execution status for an individual workflow stage."""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    PROCESSING = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class WorkflowState(BaseModel):
    """Snapshot model of a transaction processing workflow."""

    correlation_id: str
    status: str = WorkflowStatus.PENDING.value
    stages: Dict[str, str] = Field(
        default_factory=lambda: {
            "validation": StageStatus.PENDING.value,
            "reconciliation": StageStatus.PENDING.value,
            "fraud_analysis": StageStatus.PENDING.value,
            "risk_analysis": StageStatus.PENDING.value,
            "final_decision": StageStatus.PENDING.value,
        }
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error: Optional[str] = None
    failed_stage: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WorkflowTracker:
    """Thread-safe in-memory tracker for distributed workflow states."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._workflows: Dict[str, WorkflowState] = {}

    def create_workflow(
        self,
        correlation_id: str,
        initial_stages: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> WorkflowState:
        """Initialize and register a new workflow for a given correlation ID."""
        with self._lock:
            state = WorkflowState(
                correlation_id=correlation_id,
                status=WorkflowStatus.PROCESSING.value,
                metadata=metadata or {},
            )
            if initial_stages:
                state.stages.update(initial_stages)
            self._workflows[correlation_id] = state
            return state

    def get_workflow(self, correlation_id: str) -> Optional[WorkflowState]:
        """Retrieve workflow state by its correlation ID."""
        with self._lock:
            return self._workflows.get(correlation_id)

    def update_stage(
        self,
        correlation_id: str,
        stage: str,
        status: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> WorkflowState:
        """Update an individual stage status and mark overall status as PROCESSING."""
        with self._lock:
            if correlation_id not in self._workflows:
                self.create_workflow(correlation_id)

            workflow = self._workflows[correlation_id]
            workflow.stages[stage.lower()] = str(status)
            workflow.updated_at = datetime.now(timezone.utc)
            if workflow.status != WorkflowStatus.FAILED.value:
                workflow.status = WorkflowStatus.PROCESSING.value
            if metadata:
                workflow.metadata.update(metadata)
            return workflow

    def mark_completed(
        self,
        correlation_id: str,
        final_disposition: str = "APPROVED",
    ) -> WorkflowState:
        """Mark the workflow as successfully completed."""
        with self._lock:
            if correlation_id not in self._workflows:
                self.create_workflow(correlation_id)

            workflow = self._workflows[correlation_id]
            workflow.status = WorkflowStatus.COMPLETED.value
            workflow.stages["final_decision"] = StageStatus.COMPLETED.value
            workflow.metadata["final_disposition"] = final_disposition
            workflow.updated_at = datetime.now(timezone.utc)
            return workflow

    def mark_failed(
        self,
        correlation_id: str,
        error: str,
        failed_stage: Optional[str] = None,
    ) -> WorkflowState:
        """Mark the workflow as failed with error details."""
        with self._lock:
            if correlation_id not in self._workflows:
                self.create_workflow(correlation_id)

            workflow = self._workflows[correlation_id]
            workflow.status = WorkflowStatus.FAILED.value
            workflow.error = error
            workflow.failed_stage = failed_stage
            if failed_stage:
                workflow.stages[failed_stage.lower()] = StageStatus.FAILED.value
            workflow.updated_at = datetime.now(timezone.utc)
            return workflow

    def get_all_workflows(self) -> List[WorkflowState]:
        """Return all tracked workflows."""
        with self._lock:
            return list(self._workflows.values())

    def reset(self) -> None:
        """Reset internal workflow tracker storage."""
        with self._lock:
            self._workflows.clear()
