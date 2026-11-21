"""AG-UI adapter for the Hermes Agent ``/v1/runs`` API."""

from .agent import AgentForbiddenError, AgentInputError, HermesAgent, HookContext
from .client import (
    HermesAPIError,
    HermesError,
    HermesResponseError,
    HermesRunsClient,
    HermesUnreachableError,
    RunsClient,
    RunStreamGoneError,
    StartedRun,
)
from .store import InMemoryThreadStore, ThreadRecord, ThreadStore
from .translator import HermesTranslator

__version__ = "0.1.0"

__all__ = [
    "AgentForbiddenError",
    "AgentInputError",
    "HermesAPIError",
    "HermesAgent",
    "HermesError",
    "HermesResponseError",
    "HermesRunsClient",
    "HermesTranslator",
    "HermesUnreachableError",
    "HookContext",
    "InMemoryThreadStore",
    "RunStreamGoneError",
    "RunsClient",
    "StartedRun",
    "ThreadRecord",
    "ThreadStore",
    "__version__",
]
