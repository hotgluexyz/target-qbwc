"""Target-specific error types."""

from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import RetriableAPIError


class RetriableInvalidCredentialsError(RetriableAPIError, InvalidCredentialsError):
    """A retryable failure that should be surfaced as an invalid credentials error."""
