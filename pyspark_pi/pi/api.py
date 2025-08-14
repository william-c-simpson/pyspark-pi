from datetime import timedelta, datetime
import time
from typing import Any, TypeAlias

import requests

from pyspark_pi import errors, ds_options, context
from .point import Point, PointType
from .time import serialize_pi_interval

RETURNED_TUPLE_TYPE: TypeAlias = tuple[str, datetime, int | float | str | datetime | bytes | None, str | None, bool, bool, bool, bool]

_SHORT_TIMEOUT = 60
_LONG_TIMEOUT = 60 * 5

_POINT_METADATA_SELECTED_FIELDS = "Items.Object.Name;Items.Object.WebId;Items.Object.PointType;Items.Exception.Errors"
_RECORDED_VALUES_SELECTED_FIELDS = "Items"
_INTERPOLATED_VALUES_SELECTED_FIELDS = _RECORDED_VALUES_SELECTED_FIELDS
_SUMMARY_VALUES_SELECTED_FIELDS = "Items.Value"

class RequestRange:
    def __init__(self, point: Point, start_time: datetime, end_time: datetime):
        self.point = point
        self.start_time = start_time
        self.end_time = end_time

# TODO: There's a limit on the length of a URL which could get hit here
def request_point_metadata(
    ctx: context.PiDataSourceContext
) -> tuple[list[Point], PointType]:
    url = f"{ctx.config.host}/piwebapi/points/multiple?path={'&path='.join([str(path) for path in ctx.paths])}&selectedFields={_POINT_METADATA_SELECTED_FIELDS}"
    res = requests.get(
        url,
        auth=ctx.auth,
        verify=ctx.config.verify,
        timeout=_SHORT_TIMEOUT
    )

    if res.status_code < 200 or res.status_code >= 300:
        message = res.json().get("Message", res.text)
        raise errors.PiDataSourceHttpError("Failed to retrieve point metadata", message, res.status_code)

    res_body = res.json()
    returned_items = res_body.get("Items", [])

    if any("Exception" in item for item in returned_items):
        error_messages = "\n".join(
            item["Exception"]["Errors"][0] for item in returned_items if "Exception" in item
        )
        raise errors.PiDataSourceConfigError("Errors were returned when trying to retrieve point metadata:\n" + error_messages)

    points: list[Point] = []
    for item in returned_items:
        obj = item.get("Object")
        if obj is None:
            raise errors.PiDataSourceUnexpectedResponseError("Unexpected response from Pi Web API when retrieving point metadata: 'Object' field is missing")
        if not all(key in obj for key in ("Name", "WebId", "PointType")):
            raise errors.PiDataSourceUnexpectedResponseError("Unexpected response from Pi Web API when retrieving point metadata: 'Name', 'WebId', or 'PointType' is missing in 'Object'")

        point = Point(
            name=obj.get("Name"),
            web_id=obj.get("WebId"),
            type=PointType(obj.get("PointType"))
        )
        points.append(point)

    if len(set(p.type.pyspark_type() for p in points)) > 1:
        raise errors.PiDataSourceConfigError("All tags in a given query must be of the same PointType")

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
    if ctx.points is None or ctx.point_type is None:
        raise errors.PiDataSourceInternalError("Point metadata must be requested before estimating point frequencies")

    if ctx.params.request_type != ds_options.RequestType.RECORDED:
        for point in ctx.points:
            point.freq = ctx.params.interval
        return ctx.points

    batches = [ctx.points[i:i + ctx.config.rate_limit_max_requests] for i in range(0, len(ctx.points), ctx.config.rate_limit_max_requests)]
    for batch in batches:
        url = f"{ctx.config.host}/piwebapi/batch"
        payload: dict[str, dict[str, str]] = {}
        for point in batch:
            payload[point.name] = {
                "Method": "GET",
                "Resource": (
                    f"{ctx.config.host}/piwebapi/streams/{point.web_id}/summary"
                    "?summaryType=Count"
                    "&calculationBasis=EventWeighted"
                    "&startTime=*-24h"
                    "&endTime=*"
                    f"&selectedFields={_SUMMARY_VALUES_SELECTED_FIELDS}"
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
        results = _parse_batch_summary_results(PointType.INT32, res_body)
        names_counts_map = {name: count for name, _, count, _, _, _, _, _ in results}
        for point in ctx.points:
            count = names_counts_map.get(point.name)
            if count is None or count == 0:
                point.freq = timedelta(days=1)
            else:
                point.freq = timedelta(hours=(24 / count))

        time.sleep(ctx.config.rate_limit_duration.total_seconds())

    return ctx.points

def request_recorded_values(
    ctx: context.PiDataSourceContext,
    request_ranges: list[RequestRange]
) -> list[RETURNED_TUPLE_TYPE]:
    if ctx.point_type is None:
        raise errors.PiDataSourceInternalError("PointType must be known before requesting recorded values")
    
    url = f"{ctx.config.host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload: dict[str, Any] = {}

    for request_range in request_ranges:
        resource = (
            f"{ctx.config.host}/piwebapi/streams/{request_range.point.web_id}/recorded"
            f"?startTime={request_range.start_time.isoformat().replace('+00:00', 'Z')}"
            f"&endTime={request_range.end_time.isoformat().replace('+00:00', 'Z')}"
            f"&boundaryType={ctx.params.boundary_type.value}"
            f"&selectedFields={_RECORDED_VALUES_SELECTED_FIELDS}"
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
) -> list[RETURNED_TUPLE_TYPE]:
    if ctx.point_type is None:
        raise errors.PiDataSourceInternalError("PointType must be known before requesting interpolated values")

    url = f"{ctx.config.host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload: dict[str, Any] = {}

    for request_range in request_ranges:
        resource = (
            f"{ctx.config.host}/piwebapi/streams/{request_range.point.web_id}/interpolated"
            f"?startTime={request_range.start_time.isoformat().replace('+00:00', 'Z')}"
            f"&endTime={request_range.end_time.isoformat().replace('+00:00', 'Z')}"
            f"&interval={serialize_pi_interval(ctx.params.interval)}"
            f"&selectedFields={_INTERPOLATED_VALUES_SELECTED_FIELDS}"
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
) -> list[RETURNED_TUPLE_TYPE]:
    if ctx.point_type is None:
        raise errors.PiDataSourceInternalError("PointType must be known before requesting summary values")
    if ctx.params.summary_duration is None:
        raise errors.PiDataSourceInternalError("request_summary_values was called but summary_duration is not set in the request parameters")
    
    url = f"{ctx.config.host}/piwebapi/batch"
    headers = {"Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest"}
    payload: dict[str, Any] = {}

    for request_range in request_ranges:
        resource = (
            f"{ctx.config.host}/piwebapi/streams/{request_range.point.web_id}/summary"
            f"?summaryType={ctx.params.summary_type.value}"
            f"&startTime={request_range.start_time.isoformat().replace('+00:00', 'Z')}"
            f"&endTime={request_range.end_time.isoformat().replace('+00:00', 'Z')}"
            f"&calculationBasis={ctx.params.calculation_basis.value}"
            f"&summaryDuration={serialize_pi_interval(ctx.params.summary_duration)}"
            f"&selectedFields={_SUMMARY_VALUES_SELECTED_FIELDS}"
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
    if res.status_code < 200 or res.status_code >= 300:
        message = res.json().get("Message", res.text)
        raise errors.PiDataSourceHttpError("Failed to retrieve data from Pi Web API batch endpoint", message, res.status_code)
    res_body = res.json()
    for value in res_body.values():
        if value.get("Status") < 200 or value.get("Status") >= 300:
            content = value.get("Content")
            if not content:
                raise errors.PiDataSourceUnexpectedResponseError("Unexpected response from Pi Web API batch endpoint: 'Content' field is missing in one of the responses")
            message = content.get("Message", str(content))
            raise errors.PiDataSourceHttpError("Failed to retrieve data for one or more points from Pi Web API batch endpoint", message, res.status_code)

def _parse_batch_results(
        point_type: PointType, 
        res_body: dict[str, Any]
) -> list[RETURNED_TUPLE_TYPE]:
    results: list[RETURNED_TUPLE_TYPE] = []
    for key, value_obj in res_body.items():
        point_name = key.split(";")[0]
        content = value_obj.get("Content")
        if not content:
            raise errors.PiDataSourceUnexpectedResponseError("Unexpected response from Pi Web API batch endpoint: 'Content' field is missing in one of the responses")
        items = content.get("Items")
        if not items:
            raise errors.PiDataSourceUnexpectedResponseError("Unexpected response from Pi Web API batch endpoint: 'Items' field is missing in the response content")
        for item in items:
            timestamp = datetime.fromisoformat(item.get("Timestamp"))
            raw_value = item.get("Value")
            if point_type == PointType.DIGITAL:
                value = raw_value.get("Name") if raw_value else None
            elif point_type == PointType.BLOB:
                value = bytes(raw_value) if raw_value else None
            elif point_type == PointType.TIMESTAMP:
                value = datetime.fromisoformat(raw_value) if raw_value else None
            else:
                value = raw_value

            results.append((
                point_name,
                timestamp,
                value,
                item.get("UnitsAbbreviation"),
                bool(item.get("Good")),
                bool(item.get("Questionable")),
                bool(item.get("Substituted")),
                bool(item.get("Annotated")),
            ))
    return results

def _parse_batch_summary_results(
        point_type: PointType,
        res_body: dict[str, Any]
) -> list[RETURNED_TUPLE_TYPE]:
    results: list[RETURNED_TUPLE_TYPE] = []
    for key, value_obj in res_body.items():
        point_name = key.split(";")[0]
        content = value_obj.get("Content")
        if not content:
            raise errors.PiDataSourceUnexpectedResponseError("Unexpected response from Pi Web API batch endpoint: 'Content' field is missing in one of the responses")
        items = content.get("Items")
        if not items:
            raise errors.PiDataSourceUnexpectedResponseError("Unexpected response from Pi Web API batch endpoint: 'Items' field is missing in the response content")
        for item in items:
            inner_value_obj = item.get("Value")
            if not inner_value_obj:
                raise errors.PiDataSourceUnexpectedResponseError("Unexpected response from Pi Web API batch endpoint: 'Value' field is missing in one of the summary items")
            if "Errors" in inner_value_obj:
                error_messages = [err.get("Message", err)[0] for err in inner_value_obj.get("Errors", [])]
                raise errors.PiDataSourceHttpError(f"Error retrieving summary value for point '{point_name}'", "\n".join(error_messages))

            timestamp = datetime.fromisoformat(inner_value_obj.get("Timestamp"))
            raw_value = inner_value_obj.get("Value")
            if point_type == PointType.DIGITAL:
                value = raw_value.get("Name") if raw_value else None
            elif point_type == PointType.BLOB:
                value = bytes(raw_value) if raw_value else None
            elif point_type == PointType.TIMESTAMP:
                value = datetime.fromisoformat(raw_value) if raw_value else None
            else:
                value = raw_value

            results.append((
                point_name,
                timestamp,
                value,
                item.get("UnitsAbbreviation"),
                bool(item.get("Good")),
                bool(item.get("Questionable")),
                bool(item.get("Substituted")),
                bool(item.get("Annotated")),
            ))
    return results

