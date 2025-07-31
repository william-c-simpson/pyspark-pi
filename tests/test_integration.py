from typing import Any
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import os

import pytest
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession

@pytest.mark.integration
def test_get_recorded(
    spark: SparkSession,
    connection_info: dict[str, str],
    test_data: tuple[type, str, list[dict[str, Any]]],
):
    data_type, point_name, data = test_data

    df = spark.read.format("pi").options(
        **connection_info,
        requestType="recorded",
        startTime=data[0]["Timestamp"],
        endTime=data[-1]["Timestamp"],
        timezone="Etc/UTC"
    ).load(point_name)
    df.show()

    rows = df.collect()

    assert df.count() == len(data)
    assert {row["point"] for row in rows} == {point_name}

    expected_timstamps = [datetime.fromisoformat(row["Timestamp"]).replace(tzinfo=timezone.utc) for row in data]
    # Even if you set spark.sql.session.timeZone to UTC, collect() actually returns timestamps in the local timezone anyway with no tzinfo.
    actual_timestamps = [row["timestamp"].replace(tzinfo=ZoneInfo("America/Chicago")).astimezone(timezone.utc) for row in rows]
    assert actual_timestamps == expected_timstamps

    if data_type is int:
        assert {row["value"] for row in rows} == {d["Value"] for d in data}
    elif data_type is float:
        assert all( abs(row["value"] - d["Value"]) < 0.0001 for row, d in zip(rows, data) )
    elif data_type is str:
        assert {row["value"] for row in rows} == {d["Value"] for d in data}
    elif data_type is datetime:
        # Same tz situation as above
        assert all(
            row["value"].replace(tzinfo=ZoneInfo("America/Chicago")).astimezone(timezone.utc) == datetime.fromisoformat(d["Value"]).replace(tzinfo=timezone.utc)
            for row, d in zip(rows, data)
        )

@pytest.mark.integration
def test_get_interpolated(
    spark: SparkSession,
    connection_info: dict[str, str],
    test_data: tuple[type, str, list[dict[str, Any]]],
):
    data_type, point_name, data = test_data

    df = spark.read.format("pi").options(
        **connection_info,
        requestType="interpolated",
        interval="30s",
        startTime=data[0]["Timestamp"],
        endTime=data[-1]["Timestamp"],
        timezone="Etc/UTC"
    ).load(point_name)
    df.show()

    rows = df.collect()

    assert df.count() == (len(data) * 2 - 1)
    assert {row["point"] for row in rows} == {point_name}

    for i in range(df.count()):
        actual_timestamp = rows[i]["timestamp"].replace(tzinfo=ZoneInfo("America/Chicago")).astimezone(timezone.utc)
        if i % 2 == 0: # Actual, non-interpolated value
            expected_timestamp = datetime.fromisoformat(data[i // 2]["Timestamp"]).replace(tzinfo=timezone.utc)
            assert actual_timestamp == expected_timestamp
            if data_type is int:
                assert rows[i]["value"] == data[i // 2]["Value"]
            elif data_type is float:
                assert abs(rows[i]["value"] - data[i // 2]["Value"]) < 0.0001
            elif data_type is str:
                assert rows[i]["value"] == data[i // 2]["Value"]
            elif data_type is datetime:
                assert rows[i]["value"].replace(tzinfo=ZoneInfo("America/Chicago")).astimezone(timezone.utc) == datetime.fromisoformat(data[i // 2]["Value"]).replace(tzinfo=timezone.utc)
        else: # Interpolated value
            expected_timestamp = datetime.fromisoformat(data[i // 2]["Timestamp"]).replace(tzinfo=timezone.utc) + timedelta(seconds=30)
            assert actual_timestamp == expected_timestamp
            if data_type is int:
                # Same as previous value
                assert rows[i]["value"] == data[i // 2]["Value"]
            elif data_type is float:
                interpolated_value = (data[i // 2]["Value"] + data[i // 2 + 1]["Value"]) / 2
                assert abs(rows[i]["value"] - interpolated_value) < 0.0001
            elif data_type is str:
                # Same as previous value
                assert rows[i]["value"] == data[i // 2]["Value"]
            elif data_type is datetime:
                # Same as previous value
                assert rows[i]["value"].replace(tzinfo=ZoneInfo("America/Chicago")).astimezone(timezone.utc) == datetime.fromisoformat(data[i // 2]["Value"]).replace(tzinfo=timezone.utc)
        
@pytest.mark.integration
def test_get_average(
    spark: SparkSession,
    connection_info: dict[str, str],
    test_data: tuple[type, str, list[dict[str, Any]]],
):
    data_type, point_name, data = test_data
    if data_type in [str, datetime]:
        return

    df = spark.read.format("pi").options(
        **connection_info,
        requestType="summary",
        summaryType="average",
        calculationBasis="eventweighted",
        summaryDuration="2m",
        startTime=data[0]["Timestamp"],
        endTime=data[-1]["Timestamp"],
        timezone="Etc/UTC"
    ).load(point_name)
    df.show()
    print(data)

    rows = df.collect()

    assert df.count() == (len(data) // 2 - 1)
    assert {row["point"] for row in rows} == {point_name}

    for i in range(df.count()):
        actual_timestamp = rows[i]["timestamp"].replace(tzinfo=ZoneInfo("America/Chicago")).astimezone(timezone.utc)
        expected_timestamp = datetime.fromisoformat(data[i * 2]["Timestamp"]).replace(tzinfo=timezone.utc)
        assert actual_timestamp == expected_timestamp
        
        if data_type is int:
            expected_value = sum(d["Value"] for d in data[i * 2:i * 2 + 2]) // 2
            assert rows[i]["value"] == expected_value
        elif data_type is float:
            expected_value = sum(d["Value"] for d in data[i * 2:i * 2 + 2]) / 2.0
            assert abs(rows[i]["value"] - expected_value) < 0.0001

@pytest.mark.integration
def test_different_point_types(
    spark: SparkSession,
    connection_info: dict[str, str]
):
    points = [os.getenv("PI_WEB_API_INT_POINT_NAME"), os.getenv("PI_WEB_API_FLOAT_POINT_NAME"),]
    if not all(points):
        pytest.skip("Missing env vars for point names for int and float types")

    with pytest.raises(AnalysisException, match="All tags in a given query must be of the same PointType."):
        df = spark.read.format("pi").options(
            **connection_info,
            requestType="recorded",
            startTime="2000-01-01T00:00:00Z",
            endTime="2000-01-01T01:00:00Z",
            timezone="Etc/UTC"
        ).load(points)