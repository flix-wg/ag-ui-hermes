"""Thread to Hermes session/run mapping."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class ThreadRecord:
    session_id: str | None = None
    run_id: str | None = None
    """Hermes run still owned by the thread (in flight or waiting for approval)."""
    cursor: int = -1
    """Seq of the approval request the AG-UI run was interrupted at."""
    agui_run_ids: list[str] = field(default_factory=list)
    """AG-UI runs attached to ``run_id`` (start, resume, connect)."""
    pending_interrupts: list[str] = field(default_factory=list)
    """Approval request ids the last AG-UI run was interrupted with."""
    started: dict[str, str] = field(default_factory=dict)
    """AG-UI run id -> Hermes run id for the thread's latest started runs, oldest first."""


class ThreadStore(Protocol):
    async def get(self, thread_id: str) -> ThreadRecord | None: ...

    async def put(self, thread_id: str, record: ThreadRecord) -> None: ...

    async def put_if_run(self, thread_id: str, record: ThreadRecord, run_id: str) -> bool:
        """Store ``record`` only if the stored record still owns Hermes run ``run_id``."""
        ...


class InMemoryThreadStore:
    """Process-local store; use a shared implementation when running several workers."""

    def __init__(self) -> None:
        self._records: dict[str, ThreadRecord] = {}

    async def get(self, thread_id: str) -> ThreadRecord | None:
        return copy.deepcopy(self._records.get(thread_id))

    async def put(self, thread_id: str, record: ThreadRecord) -> None:
        self._records[thread_id] = copy.deepcopy(record)

    async def put_if_run(self, thread_id: str, record: ThreadRecord, run_id: str) -> bool:
        current = self._records.get(thread_id)
        if current is None or current.run_id != run_id:
            return False
        self._records[thread_id] = copy.deepcopy(record)
        return True
