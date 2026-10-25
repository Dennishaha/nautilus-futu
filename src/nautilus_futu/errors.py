class FutuError(Exception):
    """Base adapter error."""


class ConfigurationError(FutuError, ValueError):
    """Invalid or incomplete configuration."""


class UnsupportedOperation(FutuError, ValueError):
    """Capability is unavailable or its semantics cannot be preserved."""


class ProviderError(FutuError):
    """Provider rejected a request; raw credential-bearing errors are not logged."""

    def __init__(self, operation: str, code: object = -1) -> None:
        self.operation = operation
        self.code = code
        super().__init__(f"OpenD {operation} failed (code={code})")


class RequestUncertain(FutuError, TimeoutError):
    """The worker may still be completing the request. Never retry a submission."""


class DataConflict(FutuError):
    """The provider reused an identity with different content."""
