"""Thread to Hermes session/run mapping."""

from __future__ import annotations

import copy
from collections import OrderedDict
from dataclasses import asdict, dataclass, field, fields
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass
class ThreadRecord:
    SCHEMA_VERSION: ClassVar[int] = 1

    session_id: str | None = None
    run_id: str | None = None
    """Hermes run still owned by the thread (in flight or waiting for approval)."""
    cursor: int = -1
    """Seq of the approval request the AG-UI run was interrupted at."""
    agui_run_ids: list[str] = field(default_factory=list)
    """AG-UI runs attached to ``run_id`` (start, resume, connect)."""
    pending_interrupts: list[str] = field(default_factory=list)
    """Approval request ids the last AG-UI run was interrupted with."""
    answered_interrupts: list[str] = field(default_factory=list)
    """Approval request ids of ``run_id`` a resume already answered."""
    started: dict[str, str] = field(default_factory=dict)
    """AG-UI run id -> Hermes run id for the thread's latest started runs, oldest first."""
    version: int = 0
    """Bumped by every write, so ``put_if_run`` can refuse a record read before another write."""

    def to_dict(self) -> dict[str, Any]:
        """JSON-ready form with a ``schema_version``, for stores that serialize records."""
        return {"schema_version": self.SCHEMA_VERSION, **asdict(self)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ThreadRecord:
        """Rebuild a record from :meth:`to_dict` output; raises ``ValueError`` on another schema version."""
        if data.get("schema_version") != cls.SCHEMA_VERSION:
            raise ValueError(f"Unsupported ThreadRecord schema_version {data.get('schema_version')!r}")
        names = {f.name for f in fields(cls)}
        return cls(**copy.deepcopy({k: v for k, v in data.items() if k in names}))


class ThreadStore(Protocol):
    async def get(self, thread_id: str) -> ThreadRecord | None: ...

    async def put_if_run(self, thread_id: str, record: ThreadRecord, run_id: str | None) -> bool:
        """Atomically store ``record`` with ``version + 1`` only if the stored record still owns
        Hermes run ``run_id`` and has ``record.version`` (a missing record counts as
        ``ThreadRecord()``)."""
        ...


class InMemoryThreadStore:
    """Process-local store; use a shared implementation when running several workers.

    Keeps at most ``max_threads`` records and evicts the least recently used idle thread
    (one without a Hermes run) first.
    """

    def __init__(self, max_threads: int = 10_000) -> None:
        if max_threads < 1:
            raise ValueError("max_threads must be at least 1")
        self.max_threads = max_threads
        self._records: OrderedDict[str, ThreadRecord] = OrderedDict()

    async def get(self, thread_id: str) -> ThreadRecord | None:
        record = self._records.get(thread_id)
        if record is None:
            return None
        self._records.move_to_end(thread_id)
        return copy.deepcopy(record)

    async def put_if_run(self, thread_id: str, record: ThreadRecord, run_id: str | None) -> bool:
        current = self._records.get(thread_id) or ThreadRecord()
        if current.run_id != run_id or current.version != record.version:
            return False
        stored = copy.deepcopy(record)
        stored.version = current.version + 1
        self._records[thread_id] = stored
        self._records.move_to_end(thread_id)
        while len(self._records) > self.max_threads:
            idle = next((k for k, r in self._records.items() if r.run_id is None and k != thread_id), None)
            del self._records[idle if idle is not None else next(iter(self._records))]
        return True
