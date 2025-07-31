from pyspark.sql.datasource import InputPartition

class PiInputPartition(InputPartition):
    """
    A Spark input partition for the Pi web API.
    """
    def __init__(self):
        self.request_ranges = []
        self.execute_offset = None
        self.execute_cycle_duration = None
