"""AG-UI adapter for the Hermes Agent ``/v1/runs`` API."""

from importlib.metadata import version as _version

from ._config import (
    ApprovalPolicy,
    Authorize,
    CustomEventMapper,
    HookContext,
    InputPolicy,
    OnError,
    RequestLike,
    RunBody,
    StateProvider,
    ToolFilter,
)
from .agent import HermesAgent
from .client import HermesRunsClient, RunsClient, StartedRun, SupportsCapabilities, SupportsSessionMessages
from .errors import (
    AgentForbiddenError,
    AgentInputError,
    HermesAPIError,
    HermesError,
    HermesResponseError,
    HermesUnreachableError,
    InvalidInputReason,
    RunStreamGoneError,
    RunTimeoutError,
    StoreConflictError,
)
from .store import InMemoryThreadStore, ThreadRecord, ThreadStore
from .translator import HermesTranslator, UnknownEvents, a2ui_activity_snapshot

__version__ = _version("ag-ui-hermes")

__all__ = [
    "AgentForbiddenError",
    "AgentInputError",
    "ApprovalPolicy",
    "Authorize",
    "CustomEventMapper",
    "HermesAPIError",
    "HermesAgent",
    "HermesError",
    "HermesResponseError",
    "HermesRunsClient",
    "HermesTranslator",
    "HermesUnreachableError",
    "HookContext",
    "InMemoryThreadStore",
    "InputPolicy",
    "InvalidInputReason",
    "OnError",
    "RequestLike",
    "RunBody",
    "RunStreamGoneError",
    "RunTimeoutError",
    "RunsClient",
    "StartedRun",
    "StateProvider",
    "StoreConflictError",
    "SupportsCapabilities",
    "SupportsSessionMessages",
    "ThreadRecord",
    "ThreadStore",
    "ToolFilter",
    "UnknownEvents",
    "__version__",
    "a2ui_activity_snapshot",
]
