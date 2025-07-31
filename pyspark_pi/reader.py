from datetime import datetime, timedelta, timezone
import math
from typing import Sequence
import requests
import time

from pyspark.sql.datasource import DataSourceReader

from pyspark_pi import config, pi, errors, input_partition

_ADDITIONAL_PER_PARTITION_DELAY = timedelta(seconds=1)

class PiDataSourceReader(DataSourceReader):
    """
    A Spark data source reader for the Pi web api. Responsible for reading data from Pi.
    """
    def __init__(
            self,
            config: config.PiDataSourceConfig,
            points: Sequence[pi.Point],
            auth: requests.auth.AuthBase | tuple[str, str] | None
    ):
        super().__init__()
        self.config = config
        self.points = points
        self.auth = auth

    def partitions(self) -> Sequence[input_partition.PiInputPartition]:
        all_request_ranges = []

        for point in self.points:
            freq = point.freq
            total_items = max(1, int((self.config.end_time - self.config.start_time) / freq))
            items_per_call = min(total_items, self.config.max_returned_items_per_call)
            num_ranges = math.ceil(total_items / items_per_call)

            for i in range(num_ranges):
                range_start = self.config.start_time + i * items_per_call * freq
                range_end = min(range_start + items_per_call * freq, self.config.end_time)
                all_request_ranges.append(pi.RequestRange(point, range_start, range_end))

        partitions = []
    
        for i in range(0, len(all_request_ranges), self.config.rate_limit_max_requests):
            partition = input_partition.PiInputPartition()
            partition.request_ranges = all_request_ranges[i:i + self.config.rate_limit_max_requests]
            partitions.append(partition)

        # See _wait_to_execute below
        greater_execute_slot_duration = self.config.rate_limit_duration + _ADDITIONAL_PER_PARTITION_DELAY
        for i, partition in enumerate(partitions):
            partition.execute_offset = i * greater_execute_slot_duration
            partition.execute_cycle_duration = len(partitions) * greater_execute_slot_duration

        return partitions
    
    def read(self, partition):
        """
        Read data from a specific partition.
        """
        self._wait_to_execute(partition)

        if self.config.request_type == config.RequestType.RECORDED:
            results = pi.request_recorded_values(
                host=self.config.host,
                auth=self.auth,
                verify=self.config.verify,
                request_ranges=partition.request_ranges,
                point_type=self.points[0].type,
                desired_units=self.config.desired_units,
                filter_expression=self.config.filter_expression,
                include_filtered_values=self.config.include_filtered_values,
                timezone=self.config.timezone,
                max_count=self.config.max_count,
                boundary_type=self.config.boundary_type
            )
        elif self.config.request_type == config.RequestType.INTERPOLATED:
            results = pi.request_interpolated_values(
                host=self.config.host,
                auth=self.auth,
                verify=self.config.verify,
                request_ranges=partition.request_ranges,
                point_type=self.points[0].type,
                interval=self.config.interval,
                sync_time=self.config.sync_time,
                sync_time_boundary_type=self.config.sync_time_boundary_type,
                desired_units=self.config.desired_units,
                filter_expression=self.config.filter_expression,
                include_filtered_values=self.config.include_filtered_values,
                timezone=self.config.timezone,
                max_count=self.config.max_count
            )
        elif self.config.request_type == config.RequestType.SUMMARY:
            results = pi.request_summary_values(
                host=self.config.host,
                auth=self.auth,
                verify=self.config.verify,
                request_ranges=partition.request_ranges,
                point_type=self.points[0].type,
                summary_duration=self.config.summary_duration,
                summary_type=self.config.summary_type,
                calculation_basis=self.config.calculation_basis,
                time_type=self.config.time_type,
                sample_type=self.config.sample_type,
                sample_interval=self.config.sample_interval,
                max_count=self.config.max_count,
                timezone=self.config.timezone,
                filter_expression=self.config.filter_expression
            )
        else:
            raise errors.PiDataSourceConfigError(f"Unsupported request type: {self.config.request_type}")

        yield from results

    def _wait_to_execute(self, partition):
        """
        Wait until the current time falls within the allowed execution window for a partition.

        Each partition is assigned a recurring time window (slot) within a fixed-length execution cycle, 
        the duration of which is determined by the total number of partitions. This function ensures that 
        requests for a given partition are spaced apart in time to avoid hitting rate limits when 
        multiple tasks have been dispatched, which all need to send their requests.

        The cycle repeats continuously (e.g., every 5 seconds if there are 5 partitions which each have 
        a 1-second slot), and each partition is assigned a specific offset within that cycle (e.g., 
        partition 2 executes at offset 1s in that 5s cycle). Within that cycle, the partition is allowed 
        to execute only during its designated slot:

            [offset, offset + slot_duration]

        There is also a 1s buffer between slots for additional safety. Nothing is allowed to execute during
        this buffer. So, actually, in the example above, the cycle would be 10 seconds long, with each
        partition executing during a 1s slot, with a 1s buffer after each slot.

        If the current time does not fall within the allowed window, this function will sleep until the
        next cycle where it does.
        """
        now = datetime.now(timezone.utc)
        since_epoch = now - datetime(1970, 1, 1, tzinfo=timezone.utc)
        current_time_in_cycle = since_epoch % partition.execute_cycle_duration

        execute_slot_start = partition.execute_offset
        execute_slot_end = execute_slot_start + self.config.rate_limit_duration

        if not (execute_slot_start <= current_time_in_cycle < execute_slot_end):
            wait_time = (execute_slot_start - current_time_in_cycle).total_seconds()
            if wait_time < 0:
                wait_time += partition.execute_cycle_duration.total_seconds()
            time.sleep(wait_time)