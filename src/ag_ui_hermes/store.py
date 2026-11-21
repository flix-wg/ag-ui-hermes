"""Thread to Hermes session/run mapping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass
class ThreadRecord:
    session_id: str | None = None
    run_id: str | None = None
    """Hermes run still owned by the thread (in flight or waiting for approval)."""
    cursor: int = -1
    """Seq of the approval request the AG-UI run was interrupted at."""


class ThreadStore(Protocol):
    async def get(self, thread_id: str) -> ThreadRecord | None: ...

    async def put(self, thread_id: str, record: ThreadRecord) -> None: ...


class InMemoryThreadStore:
    """Process-local store; use a shared implementation when running several workers."""

    def __init__(self) -> None:
        self._records: dict[str, ThreadRecord] = {}

    async def get(self, thread_id: str) -> ThreadRecord | None:
        record = self._records.get(thread_id)
        return ThreadRecord(record.session_id, record.run_id, record.cursor) if record else None

    async def put(self, thread_id: str, record: ThreadRecord) -> None:
        self._records[thread_id] = ThreadRecord(record.session_id, record.run_id, record.cursor)
