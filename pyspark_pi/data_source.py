from pyspark.sql.datasource import DataSource
from pyspark.sql.types import StructType

from pyspark_pi import parse_options, pi, errors, auth, reader, context, helpers

class PiDataSource(DataSource):
    """
    A Spark data source for the Pi Web API.
    """
    def __init__(self, options: dict) -> None:
        self.context = context.PiDataSourceContext()
        self.context.config = parse_options.PiDataSourceConfig(options)
        self.context.params = parse_options.PiDataSourceRequestParams(options)
        self.context.paths = parse_options.parse_paths(options)
        self.context.auth = auth.create_auth(self.context.config)
        self.schema_inferred = False

    @classmethod
    def name(cls) -> str:
        return "pi"

    def schema(self) -> StructType:
        self.context.points, self.context.point_type = pi.request_point_metadata(self.context)
        self.context.points = pi.estimate_point_frequencies(self.context)

        self.schema_inferred = True
        
        return helpers.result_schema(self.context.points[0].type.pyspark_type())

    def reader(self, schema: StructType) -> reader.PiDataSourceReader:
        """
        Creates a new PiDataSourceReader with the provided schema.

        The schema() method must always be called whether or not a schema is provided by the user because
        the same call which retrieves the PointTypes from which the schema is derived also retrieves
        the WebIDs of the points to be queried, which are needed. For this reason, if the user does provide a schema,
        schema() is still called here. Thusly, providing a schema really only creates an oportunity for
        an error to be raised if the provided schema does not match that derived from Pi.
        """
        if self.schema_inferred:
            return reader.PiDataSourceReader(self.context)

        inferred_schema = self.schema()
        if [field.name for field in schema.fields] != [field.name for field in inferred_schema.fields]:
            raise errors.PiDataSourceSchemaError("Schema mismatch: The provided schema does not match that derived from Pi. Please use the pyspark_pi.result_schema() helper function to generate a compatible schema.")
        if schema["value"].dataType != inferred_schema["value"].dataType:
            raise errors.PiDataSourceSchemaError(f"Schema mismatch: The 'value' field should be of type {inferred_schema['value'].dataType.simpleString()}. This was derived from the PointTypes of the points provided. The provided schema has it as {schema['value'].dataType.simpleString()}.")

        return reader.PiDataSourceReader(self.context)