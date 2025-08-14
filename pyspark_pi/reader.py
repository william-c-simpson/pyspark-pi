from datetime import datetime, timedelta, timezone
import math
from typing import Sequence, Generator
import time

from pyspark.sql.datasource import DataSourceReader

from pyspark_pi import ds_options, pi, errors, input_partition, context

_ADDITIONAL_PER_PARTITION_DELAY = timedelta(seconds=1)

class PiDataSourceReader(DataSourceReader):
    ctx: context.PiDataSourceContext
    
    def __init__(
            self,
            ctx: context.PiDataSourceContext
    ) -> None:
        self.ctx = ctx

    def partitions(self) -> Sequence[input_partition.PiInputPartition]:
        request_ranges = self._create_request_ranges()
        partitions = self._create_partitions(request_ranges)

        return partitions

    def read(self, partition: input_partition.PiInputPartition) -> Generator[pi.RETURNED_TUPLE_TYPE, None, None]:
        self._wait_to_execute(partition)

        if self.ctx.params.request_type == ds_options.RequestType.RECORDED:
            results = pi.request_recorded_values(self.ctx, partition.request_ranges)
        elif self.ctx.params.request_type == ds_options.RequestType.INTERPOLATED:
            results = pi.request_interpolated_values(self.ctx, partition.request_ranges)
        elif self.ctx.params.request_type == ds_options.RequestType.SUMMARY:
            results = pi.request_summary_values(self.ctx, partition.request_ranges)
        else:
            raise errors.PiDataSourceConfigError(f"Unsupported request type: {self.ctx.params.request_type}")

        yield from results

    def _create_request_ranges(self) -> list[pi.RequestRange]:
        if not self.ctx.points:
            raise errors.PiDataSourceInternalError("No points available to create request ranges.")

        ranges = []
        for point in self.ctx.points:
            freq = point.freq
            total_items = max(1, int((self.ctx.params.end_time - self.ctx.params.start_time) / freq))
            items_per_call = min(total_items, self.ctx.config.max_returned_items_per_call)
            num_ranges = math.ceil(total_items / items_per_call)

            for i in range(num_ranges):
                range_start = self.ctx.params.start_time + i * items_per_call * freq
                range_end = min(range_start + items_per_call * freq, self.ctx.params.end_time)
                ranges.append(pi.RequestRange(point, range_start, range_end))

        return ranges

    def _create_partitions(self, request_ranges: list[pi.RequestRange]) -> list[input_partition.PiInputPartition]:
        partitions = []
        for i in range(0, len(request_ranges), self.ctx.config.rate_limit_max_requests):
            partition_ranges = request_ranges[i:i + self.ctx.config.rate_limit_max_requests]
            partition = input_partition.PiInputPartition(partition_ranges)
            partitions.append(partition)

        # See _wait_to_execute below
        greater_execute_slot_duration = self.ctx.config.rate_limit_duration + _ADDITIONAL_PER_PARTITION_DELAY
        for i, partition in enumerate(partitions):
            partition.execute_offset = i * greater_execute_slot_duration
            partition.execute_cycle_duration = len(partitions) * greater_execute_slot_duration

        return partitions

    def _wait_to_execute(self, partition: input_partition.PiInputPartition) -> None:
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
        if partition.execute_offset is None or partition.execute_cycle_duration is None:
            raise errors.PiDataSourceInternalError("Partition execute_offset and execute_cycle_duration must be set before waiting to execute.")
        
        now = datetime.now(timezone.utc)
        since_epoch = now - datetime(1970, 1, 1, tzinfo=timezone.utc)
        current_time_in_cycle = since_epoch % partition.execute_cycle_duration

        execute_slot_start = partition.execute_offset
        execute_slot_end = execute_slot_start + self.ctx.config.rate_limit_duration

        if not (execute_slot_start <= current_time_in_cycle < execute_slot_end):
            wait_time = (execute_slot_start - current_time_in_cycle).total_seconds()
            if wait_time < 0:
                wait_time += partition.execute_cycle_duration.total_seconds()
            time.sleep(wait_time)