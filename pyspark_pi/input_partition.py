from typing import Any

from pyspark.sql.datasource import InputPartition

class PiInputPartition(InputPartition):
    def __init__(self, value: Any) -> None:
        super().__init__(value)
        
        self.request_ranges = []
        self.execute_offset = None
        self.execute_cycle_duration = None
