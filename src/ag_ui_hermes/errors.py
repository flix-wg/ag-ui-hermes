"""Errors of this package; ``code`` and ``public_message`` are what clients see."""

from __future__ import annotations

from typing import ClassVar, Literal

InvalidInputReason = Literal[
    "malformed_body",
    "no_user_message",
    "unsupported_media",
    "invalid_image",
    "image_too_large",
    "bad_forwarded_props",
    "bad_choice",
    "unknown_interrupt",
    "stale_resume",
]
"""Why a request is ``invalid_input``; a fixed value that never echoes client data."""

_INPUT_MESSAGES: dict[InvalidInputReason, str] = {
    "malformed_body": "The request body is not a valid AG-UI request.",
    "no_user_message": "The request has no user message to send.",
    "unsupported_media": "The request has an input part or image source that is not supported.",
    "invalid_image": "An inline image is not valid base64 image data.",
    "image_too_large": "An image is larger than allowed.",
    "bad_forwarded_props": "A forwarded property has the wrong type.",
    "bad_choice": "The approval choice is not allowed.",
    "unknown_interrupt": "The resume answers an interrupt this thread is not waiting on.",
    "stale_resume": "The thread moved on to a newer run.",
}


class HermesError(Exception):
    """Base error of this package.

    ``code`` and ``public_message`` are what a ``RUN_ERROR`` or an error response carries;
    ``str(exc)`` holds the cause and only goes to the log.
    """

    code: ClassVar[str] = "hermes_error"
    public_message: str = "The Hermes request failed."


class HermesUnreachableError(HermesError):
    """Hermes could not be reached (connection, TLS or timeout failure)."""

    code = "hermes_unreachable"
    public_message = "The Hermes server is unreachable."


class HermesAPIError(HermesError):
    """Hermes answered with an HTTP error status."""

    code = "hermes_rejected"
    public_message = "The Hermes server rejected the request."

    def __init__(self, status: int, message: str, upstream_code: str | None = None) -> None:
        super().__init__(f"Hermes API {status}: {message}")
        self.status, self.message, self.upstream_code = status, message, upstream_code


class HermesResponseError(HermesError):
    """Hermes answered 2xx with a body that is not the expected JSON object."""

    code = "hermes_bad_response"
    public_message = "The Hermes server sent an invalid response."


class RunStreamGoneError(HermesError):
    """The run's event stream is no longer available (finished and swept, or unknown).

    The agent handles it by polling the run status; it never reaches a client.
    """

    code = "hermes_stream_gone"
    public_message = "The Hermes run's event stream is gone."


class AgentInputError(HermesError):
    """The request cannot be run as sent; nothing was started or stopped.

    ``reason`` says why, and the public message follows it.
    """

    code = "invalid_input"

    def __init__(self, cause: str, reason: InvalidInputReason) -> None:
        super().__init__(cause)
        self.reason: InvalidInputReason = reason
        self.public_message = _INPUT_MESSAGES[reason]


class AgentForbiddenError(HermesError):
    """The ``authorize`` hook refused the request."""

    code = "forbidden"
    public_message = "The request is not allowed."


class StoreConflictError(HermesError):
    """The thread store kept refusing a compare-and-set write."""

    code = "store_conflict"
    public_message = "The thread is busy; try again."


class RunTimeoutError(HermesError):
    """The run did not finish before ``poll_timeout`` while its status was polled."""

    code = "hermes_timeout"
    public_message = "Timed out waiting for the Hermes run."
