from pyspark.sql.datasource import DataSource
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from pyspark.sql import SparkSession

from pyspark_pi import config, pi, errors, auth, reader

class PiDataSource(DataSource):
    """
    A Spark data source for the Pi Web API.
    """
    def __init__(self, options: dict):
        super().__init__(options)
        self.config = None
        self.schema_inferred = False
        self.point_type = None
        self.auth = None

    @classmethod
    def name(cls) -> str:
        return "pi"

    def schema(self) -> StructType:        
        self.config = config.PiDataSourceConfig(self.options)
        self.auth = auth.create_auth(
            self.config.auth_method,
            username=self.config.username,
            password=self.config.password,
            token=self.config.token
        )

        self.points = pi.request_point_metadata(
            host=self.config.host,
            auth=self.auth,
            verify=self.config.verify,
            server=self.config.server,
            points=self.config.point_names
        )
        self.points = pi.estimate_point_frequencies(
            host=self.config.host,
            auth=self.auth,
            verify=self.config.verify,
            points=self.points,
            request_type=self.config.request_type,
            interval=self.config.interval,
            rate_limit_duration=self.config.rate_limit_duration,
            rate_limit_max_requests=self.config.rate_limit_max_requests
        )

        self.schema_inferred = True

        return StructType([
            StructField("point", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("value", self.points[0].type.pyspark_type(), nullable=True),
            StructField("units_abbreviation", StringType(), nullable=True),
            StructField("good", BooleanType(), nullable=True),
            StructField("questionable", BooleanType(), nullable=True),
            StructField("substituted", BooleanType(), nullable=True),
            StructField("annotated", BooleanType(), nullable=True),
        ])

    def reader(self, schema: StructType) -> reader.PiDataSourceReader:
        if self.schema_inferred:
            return reader.PiDataSourceReader(self.config, self.points, self.auth)

        inferred_schema = self.schema()
        if [field.name for field in schema.fields] != [field.name for field in inferred_schema.fields]:
            raise errors.PiDataSourceSchemaError("Schema mismatch: The provided schema does not match that derived from Pi. There should be exactly three columns: 'point', 'timestamp', and 'value'. The type of 'value' should match the PointTypes of the points provided.")
        if schema["value"].dataType != inferred_schema["value"].dataType:
            raise errors.PiDataSourceSchemaError(f"Schema mismatch: The 'value' field should be of type {inferred_schema['value'].dataType.simpleString()}. This was derived from the PointTypes of the points provided. The provided schema has it as {schema['value'].dataType.simpleString()}.")

        return reader.PiDataSourceReader(self.config, self.points, self.auth)