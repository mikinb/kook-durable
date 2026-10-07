"""Client boundary for external actions.

This module does two jobs:

1. **Action registry** — declares, per action type, the safety properties the
   rest of the system depends on: is a repeated call with the same key
   deduplicated by the external system, and is its status lookup authoritative?
   Unregistered action types are refused: we will not execute something whose
   retry safety we do not know.

2. **Error translation** — maps transport-level failures into exactly three
   categories the worker reasons about:

   * :class:`OutcomeUnknownError` — no response; side effect may exist.
   * :class:`TransientExternalError` — explicit "try later"; nothing applied.
   * :class:`PermanentExternalError` (and :class:`AuthenticationExternalError`)
     — explicit rejection; nothing applied; never retried.

Credentials live only in the client (``SecretStr``), are resolved at call time
and are never part of the execution payload, the database or the logs.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from pydantic import SecretStr

from durable_execution.errors import UnknownActionTypeError
from durable_execution.external.fake_service import (
    ExternalActionStatus,
    FakeExternalService,
    SimulatedConnectionError,
    SimulatedHTTPError,
    SimulatedTimeoutError,
)


@dataclass(frozen=True, slots=True)
class ActionSpec:
    """Safety properties of one external action type.

    ``idempotent_by_key``: the external system deduplicates calls carrying the
    same idempotency key, so re-sending is safe even when the first outcome is
    unknown (at-least-once delivery becomes effectively-once).

    ``authoritative_status_lookup``: a "not found" from the status endpoint
    proves the action never happened (strongly consistent lookup). If False,
    "not found" may just mean "not visible yet", so it proves nothing.
    """

    name: str
    idempotent_by_key: bool
    authoritative_status_lookup: bool


class ActionRegistry:
    def __init__(self, specs: Mapping[str, ActionSpec]) -> None:
        self._specs = dict(specs)

    def get(self, action_type: str) -> ActionSpec:
        try:
            return self._specs[action_type]
        except KeyError:
            raise UnknownActionTypeError(f"unregistered action type: {action_type!r}") from None

    def __contains__(self, action_type: object) -> bool:
        return action_type in self._specs


DEFAULT_ACTION_REGISTRY = ActionRegistry(
    {
        # External API honours Idempotency-Key and has a consistent status lookup.
        "create_invoice": ActionSpec("create_invoice", True, True),
        # Not repeat-safe, but the provider's lookup is authoritative.
        "send_notification": ActionSpec("send_notification", False, True),
        # Not repeat-safe, and the fulfilment partner's search is eventually
        # consistent: "not found" does not prove "not shipped".
        "send_physical_order": ActionSpec("send_physical_order", False, False),
    }
)


# ------------------------------------------------------------------ errors


class ExternalError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class OutcomeUnknownError(ExternalError):
    """No response was received. The action may or may not have been applied."""


class TransientExternalError(ExternalError):
    """The external system explicitly declined for now; nothing was applied."""


class PermanentExternalError(ExternalError):
    """The external system explicitly and permanently rejected the request."""


class ValidationExternalError(PermanentExternalError):
    """The request is invalid; repeating it cannot succeed."""


class AuthenticationExternalError(PermanentExternalError):
    """Credentials were rejected. Retrying cannot help until they are fixed."""


class StatusLookupUnavailableError(ExternalError):
    """The status endpoint could not be reached; nothing was learned."""


@dataclass(frozen=True, slots=True)
class ExternalActionResult:
    external_reference: str
    status: ExternalActionStatus
    #: Sanitized, non-sensitive metadata safe to persist in result_payload.
    summary: dict[str, Any]


class ExternalActionClient(Protocol):
    async def perform_action(
        self, *, action_type: str, idempotency_key: str, payload: dict[str, Any]
    ) -> ExternalActionResult: ...

    async def get_action_status(
        self, *, action_type: str, idempotency_key: str
    ) -> ExternalActionResult | None: ...


class FakeServiceClient:
    """Concrete client for :class:`FakeExternalService`.

    A real client would wrap an HTTP library; the translation table would be
    the same: connection errors and timeouts → unknown; 429/5xx → transient;
    401/403 → authentication; other 4xx → permanent.
    """

    def __init__(self, service: FakeExternalService, *, api_key: SecretStr) -> None:
        self._service = service
        self._api_key = api_key

    async def perform_action(
        self, *, action_type: str, idempotency_key: str, payload: dict[str, Any]
    ) -> ExternalActionResult:
        try:
            record = await self._service.perform_action(
                action_type=action_type,
                idempotency_key=idempotency_key,
                payload=payload,
                api_key=self._api_key.get_secret_value(),
            )
        except SimulatedConnectionError as exc:
            raise OutcomeUnknownError("CONNECTION_LOST", str(exc)) from exc
        except SimulatedTimeoutError as exc:
            raise OutcomeUnknownError("TIMEOUT", str(exc)) from exc
        except SimulatedHTTPError as exc:
            raise _translate_http_error(exc) from exc
        return _to_result(record.external_reference, record.status, record.action_type)

    async def get_action_status(
        self, *, action_type: str, idempotency_key: str
    ) -> ExternalActionResult | None:
        try:
            record = await self._service.get_action_status(
                action_type=action_type,
                idempotency_key=idempotency_key,
                api_key=self._api_key.get_secret_value(),
            )
        except (SimulatedConnectionError, SimulatedTimeoutError) as exc:
            raise StatusLookupUnavailableError("LOOKUP_UNAVAILABLE", str(exc)) from exc
        except SimulatedHTTPError as exc:
            if exc.status_code >= 500:
                raise StatusLookupUnavailableError(exc.code, exc.message) from exc
            raise _translate_http_error(exc) from exc
        if record is None:
            return None
        return _to_result(record.external_reference, record.status, record.action_type)


def _translate_http_error(exc: SimulatedHTTPError) -> ExternalError:
    if exc.status_code in (401, 403):
        return AuthenticationExternalError(exc.code, exc.message)
    if exc.status_code == 429 or exc.status_code >= 500:
        return TransientExternalError(exc.code, exc.message)
    if exc.status_code in (400, 422):
        return ValidationExternalError(exc.code, exc.message)
    return PermanentExternalError(exc.code, exc.message)


def _to_result(
    reference: str, status: ExternalActionStatus, action_type: str
) -> ExternalActionResult:
    return ExternalActionResult(
        external_reference=reference,
        status=status,
        summary={"external_reference": reference, "status": str(status), "action": action_type},
    )
