from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
import time
from enum import Enum
from typing import Any

import requests
from pyspark.sql.types import DataType, StringType, IntegerType, FloatType, BinaryType, TimestampType

from pyspark_pi import errors, config, pi_time

_SHORT_TIMEOUT = 60
_LONG_TIMEOUT = 60 * 5

class Point:
    def __init__(self, name: str, web_id: str = None, freq: timedelta = None, type: 'PointType' = None):
        self.name = name
        self.web_id = web_id
        self.freq = freq
        self.type = type

class RequestRange:
    def __init__(self, point: Point, start_time: datetime, end_time: datetime):
        self.point = point
        self.start_time = start_time
        self.end_time = end_time

class PointType(Enum):
    """
    Enum for the type of point.
    """
    INT16 = "Int16"
    INT32 = "Int32"
    FLOAT16 = "Float16"
    FLOAT32 = "Float32"
    FLOAT64 = "Float64"
    STRING = "String"
    DIGITAL = "Digital"
    BLOB = "Blob"
    TIMESTAMP = "Timestamp"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_
    
    def pyspark_type(self) -> DataType:
        if self == PointType.INT16 or self == PointType.INT32:
            return IntegerType()
        elif self == PointType.FLOAT16 or self == PointType.FLOAT32 or self == PointType.FLOAT64:
            return FloatType()
        elif self == PointType.STRING:
            return StringType()
        elif self == PointType.DIGITAL:
            return StringType()
        elif self == PointType.BLOB:
            return BinaryType()
        elif self == PointType.TIMESTAMP:
            return TimestampType()

def request_point_metadata(
    host: str,
    auth: requests.auth.AuthBase | tuple[str, str] | None,
    verify: str,
    server: str,
    points: list[str]
) -> tuple[PointType, list[Point]]:
    """
    Request web IDs and point types for the given tags from the Pi Web API.
    """
    url = f"{host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload = {
        "server": {
            "Method": "GET",
            "Resource": f"{host}/piwebapi/dataservers?name={server}&selectedFields=WebId"
        },
        "tags": {
            "Method": "GET",
            "Resource": f"{host}/piwebapi/points/search?dataServerWebId={{0}}&query={' OR '.join([f'tag:=%22{tag}%22' for tag in points])}&selectedFields=Items.Name;Items.WebId;Items.PointType",
            "ParentIds": ["server"],
            "Parameters": ["$.server.Content.WebId"]
        }
    }
    res = requests.post(
        url,
        json=payload,
        auth=auth,
        verify=verify,
        headers=headers,
        timeout=_SHORT_TIMEOUT
    )
    try:
        res.raise_for_status()
    except requests.HTTPError as e:
        raise errors.PiDataSourceHttpError(f"Failed to retrieve metadata: {e}") from e
    
    res_body = res.json()
    for value in res_body.values():
        if value.get("Status") < 200 or value.get("Status") >= 300:
            raise errors.PiDataSourceHttpError(f"Failed to retrieve metadata: {value.get('Content').get('Errors')[0]}")
        
    point_metadata = res_body.get("tags", {}).get("Content", {}).get("Items", [])

    points = []
    for item in point_metadata:
        point = Point(
            name=item["Name"],
            web_id=item["WebId"],
            type=PointType(item["PointType"])
        )
        points.append(point)
    
    #TODO: int16 and int32 should be treated as the same
    if len(set(p.type.pyspark_type() for p in points)) > 1:
        raise errors.PiDataSourceConfigError("All tags in a given query must be of the same PointType.")

    return points

def estimate_point_frequencies(
    host: str,
    auth: requests.auth.AuthBase | tuple[str, str] | None,
    verify: str,
    points: list[Point],
    request_type: config.RequestType,
    interval: timedelta,
    rate_limit_duration: timedelta,
    rate_limit_max_requests: int
) -> list[Point]:
    """
    Estimate the frequency of a point by requesting a count for the last 24 hours.
    This is only necessary for Recorded requests.

    If the point is slower than that, the frequency will be set as once per 24 hours.
    It's better to overestimate the frequency than underestimate it, and a frequency of 24 hours
    for points which are actually slower won't undersize input partitions too much.
    """
    if request_type != config.RequestType.RECORDED:
        for point in points:
            point.freq = interval
        return points
    
    batches = [points[i:i + rate_limit_max_requests] for i in range(0, len(points), rate_limit_max_requests)]
    for batch in batches:
        url = f"{host}/piwebapi/batch"
        payload = {}
        for point in batch:
            payload[point.name] = {
                "Method": "GET",
                "Resource": (
                    f"{host}/piwebapi/streams/{point.web_id}/summary"
                    "?summaryType=Count"
                    "&calculationBasis=EventWeighted"
                    "&startTime=*-24h"
                    "&endTime=*"
                    "&selectedFields=Items.Value"
                )
            }
        headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
        res = requests.post(
            url,
            json=payload,
            auth=auth,
            verify=verify,
            headers=headers,
            timeout=_SHORT_TIMEOUT
        )
        _raise_batch_http_errors(res)

        res_body = res.json()
        results = _parse_batch_summary_results(IntegerType(), res_body)
        names_counts_map = {name: count for name, _, count, _, _, _, _, _ in results}
        for point in points:
            count = names_counts_map.get(point.name)
            if count is None or count == 0:
                point.freq = timedelta(days=1)
            else:
                point.freq = timedelta(hours=24 / count)

        time.sleep(rate_limit_duration.total_seconds())
    
    return points

def request_recorded_values(
    host: str,
    auth: requests.auth.AuthBase | tuple[str, str] | None,
    verify: str,
    request_ranges: list[RequestRange],
    point_type: PointType,
    desired_units: str,
    filter_expression: str,
    include_filtered_values: bool,
    timezone: ZoneInfo,
    max_count: int,
    boundary_type: config.BoundaryType
) -> list[tuple[str, str, any]]:
    """
    Request recorded data.
    """
    url = f"{host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload = {}

    for request_range in request_ranges:
        resource = (
            f"{host}/piwebapi/streams/{request_range.point.web_id}/recorded"
            f"?startTime={request_range.start_time.isoformat().replace('+00:00', 'Z')}"
            f"&endTime={request_range.end_time.isoformat().replace('+00:00', 'Z')}"
            f"&boundaryType={boundary_type.value}"
        )
        if desired_units:
            resource += f"&desiredUnits={desired_units}"
        if filter_expression:
            resource += f"&filterExpression={filter_expression}"
        if include_filtered_values:
            resource += f"&includeFilteredValues={str(include_filtered_values).lower()}"
        if timezone:
            resource += f"&timezone={str(timezone)}"
        if max_count:
            resource += f"&maxCount={max_count}"

        payload[f"{request_range.point.name};{request_range.start_time.isoformat()}"] = {
            "Method": "GET",
            "Resource": resource
        }

    res = requests.post(url, json=payload, auth=auth, verify=verify, headers=headers, timeout=_LONG_TIMEOUT)
    _raise_batch_http_errors(res)
    return _parse_batch_results(point_type, res.json())

def request_interpolated_values(
    host: str,
    auth: requests.auth.AuthBase | tuple[str, str] | None,
    verify: str,
    request_ranges: list[RequestRange],
    point_type: PointType,
    interval: timedelta,
    sync_time: str,
    sync_time_boundary_type: config.BoundaryType,
    desired_units: str,
    filter_expression: str,
    include_filtered_values: bool,
    timezone: ZoneInfo,
    max_count: int
) -> list[tuple[str, str, any]]:
    """
    Request interpolated data for a point from the PI Web API.
    """
    url = f"{host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload = {}

    for request_range in request_ranges:
        resource = (
            f"{host}/piwebapi/streams/{request_range.point.web_id}/interpolated"
            f"?startTime={request_range.start_time.isoformat().replace('+00:00', 'Z')}"
            f"&endTime={request_range.end_time.isoformat().replace('+00:00', 'Z')}"
            f"&interval={pi_time.serialize_interval(interval)}"
        )
        if sync_time:
            resource += f"&syncTime={sync_time}"
        if sync_time_boundary_type:
            resource += f"&syncTimeBoundaryType={sync_time_boundary_type.value}"
        if desired_units:
            resource += f"&desiredUnits={desired_units}"
        if filter_expression:
            resource += f"&filterExpression={filter_expression}"
        if include_filtered_values:
            resource += f"&includeFilteredValues={str(include_filtered_values).lower()}"
        if timezone:
            resource += f"&timezone={timezone.key}"
        if max_count:
            resource += f"&maxCount={max_count}"

        payload[f"{request_range.point.name};{request_range.start_time.isoformat()}"] = {
            "Method": "GET",
            "Resource": resource
        }

    res = requests.post(url, json=payload, auth=auth, verify=verify, headers=headers, timeout=_LONG_TIMEOUT)
    _raise_batch_http_errors(res)
    return _parse_batch_results(point_type, res.json())

def request_summary_values(
    host: str,
    auth: requests.auth.AuthBase | tuple[str, str] | None,
    verify: str,
    request_ranges: list[RequestRange],
    point_type: PointType,
    summary_duration: str,
    summary_type: config.SummaryType,
    calculation_basis: config.CalculationBasis,
    time_type: config.TimeType,
    sample_type: config.SampleType,
    sample_interval: str,
    timezone: ZoneInfo,
    max_count: int,
    filter_expression: str
) -> list[tuple[str, str, any]]:
    """
    Request summary data for a point from the PI Web API.
    """
    url = f"{host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload = {}

    for request_range in request_ranges:
        resource = (
            f"{host}/piwebapi/streams/{request_range.point.web_id}/summary"
            f"?summaryType={summary_type.value}"
            f"&startTime={request_range.start_time.isoformat().replace('+00:00', 'Z')}"
            f"&endTime={request_range.end_time.isoformat().replace('+00:00', 'Z')}"
            f"&calculationBasis={calculation_basis.value}"
            f"&summaryDuration={pi_time.serialize_interval(summary_duration)}"
        )
        if time_type:
            resource += f"&timeType={time_type.value}"
        if sample_type:
            resource += f"&sampleType={sample_type.value}"
        if sample_interval:
            resource += f"&sampleInterval={sample_interval}"
        if timezone:
            resource += f"&timezone={str(timezone)}"
        if max_count:
            resource += f"&maxCount={max_count}"
        if filter_expression:
            resource += f"&filterExpression={filter_expression}"

        payload[f"{request_range.point.name};{request_range.start_time.isoformat()}"] = {
            "Method": "GET",
            "Resource": resource
        }

    res = requests.post(url, json=payload, auth=auth, verify=verify, headers=headers, timeout=_LONG_TIMEOUT)
    _raise_batch_http_errors(res)
    return _parse_batch_summary_results(point_type, res.json())


def _raise_batch_http_errors(res: requests.Response):
    """
    Raise an HTTP error based on the response body from a batch request.
    """
    res.raise_for_status()
    res_body = res.json()
    for value in res_body.values():
        if value.get("Status") < 200 or value.get("Status") >= 300:
            raise errors.PiDataSourceHttpError(f"Failed to retrieve data: {value.get('Content').get('Errors')[0]}")

def _parse_batch_results(point_type: PointType, res_body: dict) -> list[tuple[str, str, Any]]:
    """
    Parse the results from a batch request to the Pi Web API.
    """
    results = []
    for key, value in res_body.items():
        point_name = key.split(";")[0]
        for item in value.get("Content", {}).get("Items", []):
            if point_type == PointType.TIMESTAMP:
                value = datetime.fromisoformat(item.get("Value")) if item.get("Value") else None
            elif point_type == PointType.DIGITAL:
                value = item.get("Value").get("Name") if item.get("Value") else None
            elif point_type == PointType.BLOB:
                value = bytes(item.get("Value")) if item.get("Value") else None
            else:
                value = item.get("Value")

            results.append((
                point_name,
                datetime.fromisoformat(item["Timestamp"]),
                value,
                item.get("UnitsAbbreviation"),
                bool(item.get("Good")),
                bool(item.get("Questionable")),
                bool(item.get("Substituted")),
                bool(item.get("Annotated")),
            ))
    return results

def _parse_batch_summary_results(point_type: PointType, res_body: dict) -> list[tuple[str, str, any]]:
    """
    Parse the results from a batch summary request to the Pi Web API.
    """
    results = []
    for key, value in res_body.items():
        point_name = key.split(";")[0]
        for item in [inner_value.get("Value") for inner_value in value.get("Content", {}).get("Items", [])]:
            if point_type == PointType.TIMESTAMP:
                value = datetime.fromisoformat(item["Timestamp"]) if item.get("Timestamp") else None
            elif point_type == PointType.DIGITAL:
                value = item.get("Value").get("Name") if item.get("Value") else None
            else:
                value = item.get("Value")

            results.append((
                point_name, 
                datetime.fromisoformat(item["Timestamp"]), 
                value,
                item.get("UnitsAbbreviation"),
                bool(item.get("Good")),
                bool(item.get("Questionable")),
                bool(item.get("Substituted")),
                bool(item.get("Annotated")),
            ))
    return results
