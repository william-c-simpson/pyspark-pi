from typing import Any
from datetime import datetime, timedelta

from pyspark.sql.datasource import InputPartition

from pyspark_pi import pi

class PiInputPartition(InputPartition):
    request_ranges: list[pi.RequestRange]
    execute_offset: timedelta | None
    execute_cycle_duration: timedelta | None

    def __init__(self, request_ranges: list[pi.RequestRange]) -> None:
        self.request_ranges = request_ranges
        self.execute_offset = None
        self.execute_cycle_duration = None
