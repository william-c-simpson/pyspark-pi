import pytest
from datetime import timedelta

from pyspark_pi import reader, parse_options, pi, context

@pytest.fixture
def point():
    return pi.Point(name="p1", freq=timedelta(minutes=1))

@pytest.fixture
def points():
    return [
        pi.Point(name="p1", freq=timedelta(minutes=1), type=pi.PointType.INT32),
        pi.Point(name="p2", freq=timedelta(minutes=1), type=pi.PointType.INT32),
    ]

@pytest.fixture
def default_context():
    ctx = context.PiDataSourceContext()
    ctx.config = parse_options.PiDataSourceConfig({
        "host": "https://example.com",
        "username": "user",
        "password": "pass",
        "verify": "true",
        "server": "server1",
        "rateLimitDuration": "1",
        "rateLimitMaxRequests": 2,
        "maxReturnedItemsPerCall": 10,
    })
    ctx.params = parse_options.PiDataSourceRequestParams({
        "requestType": "recorded",
        "startTime": "2000-01-01T00:00:00Z",
        "endTime": "2000-01-01T01:00:00Z"
    })
    ctx.paths = ["point"]
    return ctx

def test_single_point_partitioning(point, default_context):
    default_context.points = [point]
    default_context.auth = None
    rdr = reader.PiDataSourceReader(default_context)
    partitions = rdr.partitions()

    expected_ranges = 6
    expected_partitions = 3

    assert len(partitions) == expected_partitions
    assert sum(len(p.request_ranges) for p in partitions) == expected_ranges
    assert partitions[0].request_ranges[0].start_time == default_context.params.start_time
    assert partitions[-1].request_ranges[-1].end_time == default_context.params.end_time
    for request_range in [r for p in partitions for r in p.request_ranges]:
        assert request_range.start_time < request_range.end_time
        assert request_range.end_time - request_range.start_time <= point.freq * default_context.config.max_returned_items_per_call

def test_multiple_points_partitioning(points, default_context):
    default_context.points = points
    default_context.auth = None
    rdr = reader.PiDataSourceReader(default_context)
    partitions = rdr.partitions()

    expected_ranges = 12
    expected_partitions = 6

    assert len(partitions) == expected_partitions
    assert sum(len(p.request_ranges) for p in partitions) == expected_ranges
    assert partitions[0].request_ranges[0].start_time == default_context.params.start_time
    assert partitions[-1].request_ranges[-1].end_time == default_context.params.end_time
    for request_range in [r for p in partitions for r in p.request_ranges]:
        assert request_range.start_time < request_range.end_time
        assert request_range.end_time - request_range.start_time <= request_range.point.freq * default_context.config.max_returned_items_per_call

def test_partition_execute_times_are_staggered(point, default_context):
    default_context.points = [point]
    default_context.auth = None
    rdr = reader.PiDataSourceReader(default_context)
    partitions = rdr.partitions()

    for i, partition in enumerate(partitions):
        assert partition.execute_cycle_duration == len(partitions) * (default_context.config.rate_limit_duration + reader._ADDITIONAL_PER_PARTITION_DELAY)
        previous_partition = partitions[i - 1] if i > 0 else None
        if previous_partition:
            assert partition.execute_offset > previous_partition.execute_offset
            delta = partition.execute_offset - previous_partition.execute_offset
            assert delta == (default_context.config.rate_limit_duration + reader._ADDITIONAL_PER_PARTITION_DELAY)