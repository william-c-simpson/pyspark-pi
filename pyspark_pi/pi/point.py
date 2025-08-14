from datetime import timedelta
from enum import Enum

from pyspark.sql.types import DataType, IntegerType, FloatType, StringType, BinaryType, TimestampType

class Point:
    def __init__(self, name: str, web_id: str = None, freq: timedelta = None, type: 'PointType' = None):
        self.name = name
        self.web_id = web_id
        self.freq = freq
        self.type = type

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