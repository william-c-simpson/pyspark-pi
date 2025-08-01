from enum import Enum
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pyspark_pi import errors, pi_time

class AuthMethod(Enum):
    """
    Enum for the authentication methods supported by the Pi Web API.
    """
    ANONYMOUS = "anonymous"
    BASIC = "basic"
    KERBEROS = "kerberos"
    BEARER = "bearer"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_

class RequestType(Enum):
    """
    Enum for the type of request to make to the Pi Web API.
    """
    RECORDED = "recorded"
    INTERPOLATED = "interpolated"
    SUMMARY = "summary"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_

class BoundaryType(Enum):
    """
    Defines the behavior of data retrieval at the end points of a specified time range.
    """
    INSIDE = "inside"
    OUTSIDE = "outside"
    INTERPOLATED = "interpolated"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_

class SummaryType(Enum):
    """
    Values to indicate which summary type calculation(s) should be performed.
    """
    TOTAL = "total"
    AVERAGE = "average"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"
    RANGE = "range"
    STDDEV = "stddev"
    POPULATION_STDDEV = "populationstddev"
    COUNT = "count"
    PERCENT_GOOD = "percentgood"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_

class CalculationBasis(Enum):
    """
    Defines the possible calculation options when performing summary calculations over time-series data.
    """
    TIME_WEIGHTED = "timeweighted"
    EVENT_WEIGHTED = "eventweighted"
    TIME_WEIGHTED_CONTINUOUS = "timeweightedcontinuous"
    TIME_WEIGHTED_DISCRETE = "timeweighteddiscrete"
    EVENT_WEIGHTED_EXCLUDE_MOST_RECENT_EVENT = "eventweightedexcludemostrecentevent"
    EVENT_WEIGHTED_EXCLUDE_EARLIEST_EVENT = "eventweightedexcludeearliestevent"
    EVENT_WEIGHTED_INCLUDE_BOTH_ENDS = "eventweightedincludebothends"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_

class TimeType(Enum):
    """
    Defines the timestamp returned for a value when a summary calculation is done.
    """
    AUTO = "auto"
    EARLIEST_TIME = "earliesttime"
    MOST_RECENT_TIME = "mostrecenttime"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_

class SampleType(Enum):
    """
    Defines the evaluation of an expression over a time range.
    """
    EXPRESSION_RECORDED_VALUES = "expressionrecordedvalues"
    INTERVAL = "interval"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_

class PiDataSourceConfig:
    """Configuration class for the Pi data source."""

    def __init__(self, options: dict):
        self.host = _parse_str(options, "host", required=True).rstrip('/')
        self.auth_method = _parse_enum(options, "authMethod", AuthMethod, required=True, default="anonymous")
        self.username = _parse_str(options, "username", required=self.auth_method == AuthMethod.BASIC)
        self.password = _parse_str(options, "password", required=self.auth_method == AuthMethod.BASIC)
        self.token = _parse_str(options, "token", required=self.auth_method == AuthMethod.BEARER)
        self.verify = _parse_bool(options, "verify", required=True, default=True)
        self.server = _parse_str(options, "server", required=True)
        self.point_names = _parse_points(options)
        self.request_type = _parse_enum(options, "requestType", RequestType, required=True)
        self.start_time = _parse_datetime(options, "startTime", required=True)
        self.end_time = _parse_datetime(options, "endTime", required=True)
        self.desired_units = _parse_str(options, "desiredUnits")
        self.filter_expression = _parse_str(options, "filterExpression")
        self.include_filtered_values = _parse_bool(options, "includeFilteredValues", default=False)
        self.timezone = _parse_timezone(options, "timezone")
        self.max_count = _parse_positive_int(options, "maxCount", default=150000)
        self.boundary_type = _parse_enum(options, "boundaryType", BoundaryType, default="inside")
        self.sync_time = _parse_str(options, "syncTime")
        self.sync_time_boundary_type = _parse_enum(options, "syncTimeBoundaryType", BoundaryType, default="inside")
        self.interval = _parse_timedelta(options, "interval", required=self.request_type == RequestType.INTERPOLATED, default="1h")
        self.summary_type = _parse_enum(options, "summaryType", SummaryType, required=self.request_type == RequestType.SUMMARY, default="total")
        self.calculation_basis = _parse_enum(options, "calculationBasis", CalculationBasis, required=self.request_type == RequestType.SUMMARY, default="timeweighted")
        self.time_type = _parse_enum(options, "timeType", TimeType, default="auto")
        self.summary_duration = _parse_timedelta(options, "summaryDuration", required=self.request_type == RequestType.SUMMARY)
        self.sample_type = _parse_enum(options, "sampleType", SampleType, default="expressionrecordedvalues")
        self.sample_interval = _parse_timedelta(options, "sampleInterval", required=self.request_type == RequestType.SUMMARY and self.sample_type == SampleType.INTERVAL)
        self.rate_limit_duration = timedelta(seconds=_parse_positive_int(options, "rateLimitDuration", default=1)) # In the actual Pi server config, this is specified as a number of seconds, not an AFTimeSpan string.
        self.rate_limit_max_requests = _parse_positive_int(options, "rateLimitMaxRequests", default=1000)
        self.max_returned_items_per_call = _parse_positive_int(options, "maxReturnedItemsPerCall", default=150000)
        self.auth = None

def _parse_str(options: dict, key: str, required: bool = False, default: str = None) -> str:
    val = options.get(key, default)
    if val is None and required:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    if val is None:
        return None
    return val.strip()

def _parse_bool(options: dict, key: str, required: bool = False, default: bool = False) -> bool:
    val = options.get(key, default)
    if val is None and required:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    if val is None:
        return None
    if isinstance(val, str):
        val = val.strip().lower()
        if val not in {"true", "false"}:
            raise errors.PiDataSourceConfigError(f"Invalid boolean value for {key}: {val}")
    return val == "true"

def _parse_enum(options: dict, key: str, enum_type: Enum, required: bool = False, default: str = None) -> Enum:
    val = options.get(key, default)
    if val is None and required:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    if val is None:
        return None
    val = val.lower()
    if not enum_type.has_value(val):
        raise errors.PiDataSourceConfigError(f"Invalid {key} '{val}' specified.")
    return enum_type(val)

def _parse_datetime(options: dict, key: str, required: bool = False) -> datetime:
    val = options.get(key)
    if val is None and required:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    if val is None:
        return None
    try:
        return pi_time.deserialize_pi_time(val)
    except ValueError:
        raise errors.PiDataSourceConfigError(f"Invalid datetime format for {key}: {val}")

def _parse_positive_int(options: dict, key: str, required: bool = False, default: str = None) -> int:
    val = options.get(key, default)
    if val is None and required:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    if val is None:
        return None
    try:
        val = int(val)
    except ValueError:
        raise errors.PiDataSourceConfigError(f"Invalid integer for {key}: {val}")
    if val <= 0:
        raise errors.PiDataSourceConfigError(f"{key} must be a positive integer.")
    return val

def _parse_timedelta(options: dict, key: str, required: bool = False, default: str = None) -> timedelta:
    val = options.get(key, default)
    if val is None and required:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    if val is None:
        return None
    try:
        return pi_time.deserialize_interval(val)
    except ValueError as e:
        raise errors.PiDataSourceConfigError(f"Invalid timedelta format for {key}: {val}. This could be an issue with the des function in the library, not your input. Double-check your input and try formatting it in a different way.") from e

def _parse_timezone(options: dict, key: str, required: bool = False, default: str = "UTC") -> ZoneInfo:
    val = options.get(key, default)
    if val is None and required:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    if val is None:
        return None
    try:
        return ZoneInfo(val)
    except ZoneInfoNotFoundError:
        raise errors.PiDataSourceConfigError(f"Invalid timezone specified for {key}: {val}")

def _parse_points(options: dict[str, str]) -> list[str]:
    """
    Parses a string formatted like: ["thing1","thing2","thing3"]
    """
    if "path" in options:
        point = options["path"]
        return [point]
    elif "paths" in options:
        points = options["paths"].strip("[]").replace('"', '').split(",")
        return points
    else:
        raise errors.PiDataSourceConfigError("A list of points must be provided using the load() method on the reader.")
