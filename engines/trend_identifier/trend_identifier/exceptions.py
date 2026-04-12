"""Deterministic exception types for the Trend Identifier."""

class TrendIdentifierError(Exception):
    """Base deterministic engine error."""


class SchemaValidationError(TrendIdentifierError):
    """Raised when the output payload fails schema validation."""


class NumericInvalidError(TrendIdentifierError):
    """Raised when a formula produces NaN or Inf."""


class StatePersistenceError(TrendIdentifierError):
    """Raised when state persistence fails."""


class PreviousStateLockError(TrendIdentifierError):
    """Raised when prior-state lock/version verification fails."""


class LogWriteError(TrendIdentifierError):
    """Raised when decision logging fails."""


class MissingIntermediateError(TrendIdentifierError):
    """Raised when a load-bearing intermediate is missing."""
