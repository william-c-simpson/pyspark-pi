class PiDataSourceError(RuntimeError):
    """Base class for all Pi data source errors."""

class PiDataSourceConfigError(PiDataSourceError):
    """Raised when there is a configuration error for the Pi data source."""

class PiDataSourceHttpError(PiDataSourceError):
    """Raised when there is a connection error to the Pi data source."""

class PiDataSourceSchemaError(PiDataSourceError):
    """Raised when there is a schema error in the Pi data source."""
