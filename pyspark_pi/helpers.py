"""
User-facing helper functions.
"""

from datetime import datetime

from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DataType, IntegerType, FloatType, BooleanType

def result_schema(dtype: type | DataType) -> StructType:
    """
    Returns a result schema based on the provided data type.

    Args:
        dtype (type | DataType): The data type for the value field. Can be int, float, str, datetime, or a pyspark DataType.
    Returns:
        StructType: The result schema.
    Raises:
        ValueError: If the provided data type is not supported.
    """

    if dtype is int:
        out_type = IntegerType()
    elif dtype is float:
        out_type = FloatType()
    elif dtype is str:
        out_type = StringType()
    elif dtype is datetime:
        out_type = TimestampType()
    elif isinstance(dtype, DataType):
        out_type = dtype
    else:
        raise ValueError(f"Unsupported data type: {dtype}")
    
    return StructType([
        StructField("point", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("value", out_type, nullable=True),
        StructField("units_abbreviation", StringType(), nullable=True),
        StructField("good", BooleanType(), nullable=True),
        StructField("questionable", BooleanType(), nullable=True),
        StructField("substituted", BooleanType(), nullable=True),
        StructField("annotated", BooleanType(), nullable=True),
    ])