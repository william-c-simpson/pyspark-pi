class PiDataSourceError(RuntimeError):
    """Base class for all Pi data source errors."""

class PiDataSourceConfigError(PiDataSourceError):
    """Raised when there is a configuration error for the Pi data source."""

class PiDataSourceHttpError(PiDataSourceError):
    """Raised when there is a connection error to the Pi web API."""
    message: str
    response_message: str
    status_code: int | None

    def __init__(self, message: str, response_message: str, status_code: int | None = None) -> None:
        self.status_code = status_code
        self.message = message
        self.response_message = response_message

        http_hint = f" (HTTP {status_code})" if status_code is not None else ""
        super().__init__(f"{message}{http_hint}: {response_message}")

class PiDataSourceUnexpectedResponseError(PiDataSourceError):
    """Raised when the Pi web API returns a malformed response."""

class PiDataSourceSchemaError(PiDataSourceError):
    """Raised when there is a schema error in the Pi data source."""

class PiDataSourceInternalError(PiDataSourceError):
    """
    Raised when an internal error occurs in the Pi data source.
    These should only be thrown if there is a bug in the library.
    """
    def __init__(self, message: str) -> None:
        super().__init__(f"pyspark_pi internal error, this is almost certainly a bug in the library: {message}.")