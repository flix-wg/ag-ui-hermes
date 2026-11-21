"""Hook types and option groups of :class:`HermesAgent`."""

from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from ag_ui.core import BaseEvent, RunAgentInput

_FORWARDABLE = frozenset({"model", "provider", "model_options"})


class RequestLike(Protocol):
    """What the agent and hooks may read from the incoming HTTP request (a Starlette ``Request`` fits)."""

    @property
    def headers(self) -> Mapping[str, str]: ...

    @property
    def client(self) -> tuple[str, int] | None: ...

    @property
    def url(self) -> Any: ...


@dataclass(frozen=True)
class HookContext:
    """The request a hook runs for; every hook gets it as its first argument."""

    action: Literal["run", "connect", "stop", "capabilities"]
    thread_id: str | None
    """``None`` for ``capabilities``, which concerns no thread."""
    run_id: str | None
    """``None`` for ``capabilities``."""
    input: RunAgentInput | None = None
    """The AG-UI input; ``None`` for ``stop`` and ``capabilities``."""
    request: RequestLike | None = None
    """The incoming HTTP request (a Starlette ``Request`` from the FastAPI endpoint), if any."""


# Every hook may return its result directly or as an awaitable.
Authorize = Callable[[HookContext], bool | Awaitable[bool]]
"""``False`` refuses the request (``AgentForbiddenError``, HTTP 403 from the endpoint)."""
StateProvider = Callable[[HookContext], Any]
"""State (or an awaitable of it) for a ``STATE_SNAPSHOT``; ``None`` emits nothing."""
ToolFilter = Callable[[HookContext, str, str, Mapping[str, Any]], tuple[str, str] | Awaitable[tuple[str, str] | None] | None]
"""``(context, tool, preview, raw_event) -> (tool, preview)`` to rewrite, or ``None`` to drop.

Called on every ``tool.started`` (``None`` hides the whole call) and ``tool.completed``
(``None`` hides only the result), so keep it cheap."""
CustomEventMapper = Callable[[HookContext, Mapping[str, Any]], Sequence[BaseEvent] | Awaitable[Sequence[BaseEvent] | None] | None]
"""Maps a Hermes event the translator does not know; ``None`` falls through to ``unknown_events``."""
RunBody = Callable[[HookContext], Mapping[str, Any] | Awaitable[Mapping[str, Any]]]
"""Extra ``POST /v1/runs`` body fields."""
OnError = Callable[[HookContext, Exception], Awaitable[None] | None]
"""Sees every error the agent reports; its own exceptions are logged and ignored."""


@dataclass(frozen=True)
class InputPolicy:
    """What a client's ``RunAgentInput`` may put into a Hermes run, and what ``connect`` restores."""

    client_system_messages: bool = False
    """Add the client's system and developer messages to Hermes ``instructions``."""
    client_context: bool = True
    """Pass the client's ``context`` to Hermes, fenced as untrusted data."""
    client_state: bool = True
    """Pass the client's ``state`` to Hermes, fenced as untrusted data."""
    allow_image_urls: bool = False
    """Accept ``http(s)`` image URLs, which Hermes fetches itself; by default only inline images."""
    max_image_bytes: int | None = 5 * 2**20
    """Largest decoded inline image; ``None`` for no limit."""
    max_message_image_bytes: int | None = 20 * 2**20
    """Largest total of decoded inline images in one message; ``None`` for no limit."""
    forwarded_props: Collection[str] = frozenset()
    """``forwardedProps`` keys (``model``, ``provider``, ``model_options``) a client may set on the run."""
    restore_tool_results: bool = False
    """Restore stored tool results on ``connect``; unlike live previews they are neither truncated nor redacted."""

    def __post_init__(self) -> None:
        if unknown := set(self.forwarded_props) - _FORWARDABLE:
            raise ValueError(f"forwarded_props allows only {sorted(_FORWARDABLE)}, not {sorted(unknown)}")
        object.__setattr__(self, "forwarded_props", frozenset(self.forwarded_props))


@dataclass(frozen=True)
class ApprovalPolicy:
    """Which approval choices clients may send, and how long Hermes waits on one."""

    allowed_choices: Collection[str] = frozenset({"once", "deny"})
    """Choices a client may send; ``deny`` is always allowed. Add ``session``/``always`` to offer them."""
    timeout: float | None = None
    """Seconds Hermes waits on an approval, announced as the interrupt's ``expiresAt``."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "allowed_choices", frozenset(self.allowed_choices) | {"deny"})
