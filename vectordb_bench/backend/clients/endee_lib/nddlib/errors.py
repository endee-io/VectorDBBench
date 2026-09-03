"""Exception hierarchy mapped from the C ABI's OperationResult codes.

The contract (see the comment block in src/main.cpp and src/api/ndd_capi.h):
    0        SUCCESS
    1-99     caller-fixable input error   (1 = not found, 2 = validation, 3 = tier)
    100+     storage / internal / corruption
    101      unknown op
    102      null handle or argument
"""


class NddError(Exception):
    """Base class for every error raised by nddlib."""

    def __init__(self, message: str, code: int = -1) -> None:
        super().__init__(message)
        self.message = message
        self.code = code

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"[{self.code}] {self.message}" if self.code >= 0 else self.message


class NotFoundError(NddError):
    """Code 1 - the collection or object does not exist."""


class ValidationError(NddError):
    """Code 2 - a caller-fixable problem with the request."""


class TierError(NddError):
    """Code 3 - forbidden by tier policy. Not raised in library mode, which is
    unlimited, but mapped for completeness."""


class InternalError(NddError):
    """Code 100+ - storage, corruption, or an escaped exception."""


class UnknownOpError(InternalError):
    """Code 101 - the loaded library does not implement the requested op."""


# Codes with a dedicated class; anything else falls back by range.
_BY_CODE = {
    1: NotFoundError,
    2: ValidationError,
    3: TierError,
    101: UnknownOpError,
}


def error_for(code: int, message: str) -> NddError:
    """Build the most specific exception for an OperationResult code."""
    cls = _BY_CODE.get(code)
    if cls is None:
        cls = InternalError if code >= 100 else NddError
    return cls(message or f"ndd operation failed with code {code}", code)
