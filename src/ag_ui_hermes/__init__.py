"""AG-UI adapter for the Hermes Agent ``/v1/runs`` API."""

from importlib.metadata import version as _version

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

__version__ = _version("ag-ui-hermes")

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
