from datetime import timedelta, datetime
import time
from enum import Enum
from typing import Any

import requests
from pyspark.sql.types import DataType, StringType, IntegerType, FloatType, BinaryType, TimestampType

from pyspark_pi import errors, parse_options, pi_time, context

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
    ctx: context.PiDataSourceContext
) -> tuple[PointType, list[Point]]:
    url = f"{ctx.config.host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload = {
        "server": {
            "Method": "GET",
            "Resource": f"{ctx.config.host}/piwebapi/dataservers?name={ctx.config.server}&selectedFields=WebId"
        },
        "tags": {
            "Method": "GET",
            "Resource": f"{ctx.config.host}/piwebapi/points/search?dataServerWebId={{0}}&query={' OR '.join([f'tag:=%22{tag}%22' for tag in ctx.paths])}&selectedFields=Items.Name;Items.WebId;Items.PointType",
            "ParentIds": ["server"],
            "Parameters": ["$.server.Content.WebId"]
        }
    }
    res = requests.post(
        url,
        json=payload,
        auth=ctx.auth,
        verify=ctx.config.verify,
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
    
    if len(set(p.type.pyspark_type() for p in points)) > 1:
        raise errors.PiDataSourceConfigError("All tags in a given query must be of the same PointType.")

    return points, points[0].type

def estimate_point_frequencies(
    ctx: context.PiDataSourceContext
) -> list[Point]:
    """
    Estimate the frequency of a point by requesting a count for the last 24 hours.
    This is only necessary for Recorded requests.

    If the point is slower than that, the frequency will be set as once per 24 hours.
    It's better to overestimate the frequency than underestimate it, and a frequency of 24 hours
    for points which are actually slower won't undersize input partitions too much.
    """
    if ctx.params.request_type != parse_options.RequestType.RECORDED:
        for point in ctx.points:
            point.freq = ctx.params.interval
        return ctx.points

    batches = [ctx.points[i:i + ctx.config.rate_limit_max_requests] for i in range(0, len(ctx.points), ctx.config.rate_limit_max_requests)]
    for batch in batches:
        url = f"{ctx.config.host}/piwebapi/batch"
        payload = {}
        for point in batch:
            payload[point.name] = {
                "Method": "GET",
                "Resource": (
                    f"{ctx.config.host}/piwebapi/streams/{point.web_id}/summary"
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
            auth=ctx.auth,
            verify=ctx.config.verify,
            headers=headers,
            timeout=_SHORT_TIMEOUT
        )
        _raise_batch_http_errors(res)

        res_body = res.json()
        results = _parse_batch_summary_results(IntegerType(), res_body)
        names_counts_map = {name: count for name, _, count, _, _, _, _, _ in results}
        for point in ctx.points:
            count = names_counts_map.get(point.name)
            if count is None or count == 0:
                point.freq = timedelta(days=1)
            else:
                point.freq = timedelta(hours=24 / count)

        time.sleep(ctx.config.rate_limit_duration.total_seconds())

    return ctx.points

def request_recorded_values(
    ctx: context.PiDataSourceContext,
    request_ranges: list[RequestRange]
) -> list[tuple[str, str, any]]:
    url = f"{ctx.config.host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload = {}

    for request_range in request_ranges:
        resource = (
            f"{ctx.config.host}/piwebapi/streams/{request_range.point.web_id}/recorded"
            f"?startTime={request_range.start_time.isoformat().replace('+00:00', 'Z')}"
            f"&endTime={request_range.end_time.isoformat().replace('+00:00', 'Z')}"
            f"&boundaryType={ctx.params.boundary_type.value}"
        )
        if ctx.params.desired_units:
            resource += f"&desiredUnits={ctx.params.desired_units}"
        if ctx.params.filter_expression:
            resource += f"&filterExpression={ctx.params.filter_expression}"
        if ctx.params.include_filtered_values:
            resource += f"&includeFilteredValues={str(ctx.params.include_filtered_values).lower()}"
        if ctx.params.timezone:
            resource += f"&timezone={str(ctx.params.timezone)}"
        if ctx.params.max_count:
            resource += f"&maxCount={ctx.params.max_count}"

        payload[f"{request_range.point.name};{request_range.start_time.isoformat()}"] = {
            "Method": "GET",
            "Resource": resource
        }

    res = requests.post(url, json=payload, auth=ctx.auth, verify=ctx.config.verify, headers=headers, timeout=_LONG_TIMEOUT)
    _raise_batch_http_errors(res)
    return _parse_batch_results(ctx.point_type, res.json())

def request_interpolated_values(
    ctx: context.PiDataSourceContext,
    request_ranges: list[RequestRange]
) -> list[tuple[str, str, any]]:
    url = f"{ctx.config.host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload = {}

    for request_range in request_ranges:
        resource = (
            f"{ctx.config.host}/piwebapi/streams/{request_range.point.web_id}/interpolated"
            f"?startTime={request_range.start_time.isoformat().replace('+00:00', 'Z')}"
            f"&endTime={request_range.end_time.isoformat().replace('+00:00', 'Z')}"
            f"&interval={pi_time.serialize_interval(ctx.params.interval)}"
        )
        if ctx.params.sync_time:
            resource += f"&syncTime={ctx.params.sync_time}"
        if ctx.params.sync_time_boundary_type:
            resource += f"&syncTimeBoundaryType={ctx.params.sync_time_boundary_type.value}"
        if ctx.params.desired_units:
            resource += f"&desiredUnits={ctx.params.desired_units}"
        if ctx.params.filter_expression:
            resource += f"&filterExpression={ctx.params.filter_expression}"
        if ctx.params.include_filtered_values:
            resource += f"&includeFilteredValues={str(ctx.params.include_filtered_values).lower()}"
        if ctx.params.timezone:
            resource += f"&timezone={ctx.params.timezone.key}"
        if ctx.params.max_count:
            resource += f"&maxCount={ctx.params.max_count}"

        payload[f"{request_range.point.name};{request_range.start_time.isoformat()}"] = {
            "Method": "GET",
            "Resource": resource
        }

    res = requests.post(url, json=payload, auth=ctx.auth, verify=ctx.config.verify, headers=headers, timeout=_LONG_TIMEOUT)
    _raise_batch_http_errors(res)
    return _parse_batch_results(ctx.point_type, res.json())

def request_summary_values(
    ctx: context.PiDataSourceContext,
    request_ranges: list[RequestRange]
) -> list[tuple[str, str, any]]:
    url = f"{ctx.config.host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload = {}

    for request_range in request_ranges:
        resource = (
            f"{ctx.config.host}/piwebapi/streams/{request_range.point.web_id}/summary"
            f"?summaryType={ctx.params.summary_type.value}"
            f"&startTime={request_range.start_time.isoformat().replace('+00:00', 'Z')}"
            f"&endTime={request_range.end_time.isoformat().replace('+00:00', 'Z')}"
            f"&calculationBasis={ctx.params.calculation_basis.value}"
            f"&summaryDuration={pi_time.serialize_interval(ctx.params.summary_duration)}"
        )
        if ctx.params.time_type:
            resource += f"&timeType={ctx.params.time_type.value}"
        if ctx.params.sample_type:
            resource += f"&sampleType={ctx.params.sample_type.value}"
        if ctx.params.sample_interval:
            resource += f"&sampleInterval={ctx.params.sample_interval}"
        if ctx.params.timezone:
            resource += f"&timezone={str(ctx.params.timezone)}"
        if ctx.params.max_count:
            resource += f"&maxCount={ctx.params.max_count}"
        if ctx.params.filter_expression:
            resource += f"&filterExpression={ctx.params.filter_expression}"

        payload[f"{request_range.point.name};{request_range.start_time.isoformat()}"] = {
            "Method": "GET",
            "Resource": resource
        }

    res = requests.post(url, json=payload, auth=ctx.auth, verify=ctx.config.verify, headers=headers, timeout=_LONG_TIMEOUT)
    _raise_batch_http_errors(res)
    return _parse_batch_summary_results(ctx.point_type, res.json())


def _raise_batch_http_errors(res: requests.Response) -> None:
    res.raise_for_status()
    res_body = res.json()
    for value in res_body.values():
        if value.get("Status") < 200 or value.get("Status") >= 300:
            raise errors.PiDataSourceHttpError(f"Failed to retrieve data: {value.get('Content').get('Errors')[0]}")

def _parse_batch_results(point_type: PointType, res_body: dict) -> list[tuple[str, str, Any]]:
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
