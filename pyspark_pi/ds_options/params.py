from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .shared import _BaseOptionEnum, _parse_str, _parse_bool, _parse_enum, _parse_datetime, _parse_positive_int, _parse_timedelta, _parse_timezone

class RequestType(_BaseOptionEnum):
    """
    Enum for the type of request to make to the Pi Web API.
    """
    RECORDED = "recorded"
    INTERPOLATED = "interpolated"
    SUMMARY = "summary"

class BoundaryType(_BaseOptionEnum):
    """
    Defines the behavior of data retrieval at the end points of a specified time range.
    """
    INSIDE = "inside"
    OUTSIDE = "outside"
    INTERPOLATED = "interpolated"

class SummaryType(_BaseOptionEnum):
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

class CalculationBasis(_BaseOptionEnum):
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

class TimeType(_BaseOptionEnum):
    """
    Defines the timestamp returned for a value when a summary calculation is done.
    """
    AUTO = "auto"
    EARLIEST_TIME = "earliesttime"
    MOST_RECENT_TIME = "mostrecenttime"

class SampleType(_BaseOptionEnum):
    """
    Defines the evaluation of an expression over a time range.
    """
    EXPRESSION_RECORDED_VALUES = "expressionrecordedvalues"
    INTERVAL = "interval"

class PiDataSourceRequestParams:
    request_type: RequestType
    start_time: datetime
    end_time: datetime
    desired_units: str | None
    filter_expression: str | None
    include_filtered_values: bool
    timezone: ZoneInfo | None
    max_count: int
    boundary_type: BoundaryType
    sync_time: str | None
    sync_time_boundary_type: BoundaryType
    interval: timedelta
    summary_type: SummaryType
    calculation_basis: CalculationBasis
    time_type: TimeType
    summary_duration: timedelta | None
    sample_type: SampleType
    sample_interval: timedelta | None

    def __init__(self, options: dict[str, str]) -> None:
        self.request_type = _parse_enum(options, "requestType", RequestType, required=True)
        self.start_time = _parse_datetime(options, "startTime", required=True)
        self.end_time = _parse_datetime(options, "endTime", required=True)
        self.desired_units = _parse_str(options, "desiredUnits")
        self.filter_expression = _parse_str(options, "filterExpression")
        self.include_filtered_values = _parse_bool(options, "includeFilteredValues", default="false")
        self.timezone = _parse_timezone(options, "timezone")
        self.max_count = _parse_positive_int(options, "maxCount", default="150000")
        self.boundary_type = _parse_enum(options, "boundaryType", BoundaryType, default="inside")
        self.sync_time = _parse_str(options, "syncTime")
        self.sync_time_boundary_type = _parse_enum(options, "syncTimeBoundaryType", BoundaryType, default="inside")
        self.interval = _parse_timedelta(options, "interval", default="1h")
        self.summary_type = _parse_enum(options, "summaryType", SummaryType, default="total")
        self.calculation_basis = _parse_enum(options, "calculationBasis", CalculationBasis, default="timeweighted")
        self.time_type = _parse_enum(options, "timeType", TimeType, default="auto")
        self.summary_duration = _parse_timedelta(options, "summaryDuration", required=self.request_type == RequestType.SUMMARY)
        self.sample_type = _parse_enum(options, "sampleType", SampleType, default="expressionrecordedvalues")
        self.sample_interval = _parse_timedelta(options, "sampleInterval", required=self.request_type == RequestType.SUMMARY and self.sample_type == SampleType.INTERVAL)
