# pyspark-pi

This is a custom Spark data source for connecting to Aveva's Pi Web API. It was created using the Python data source API which was made available as of Spark 4.0.0 and requires at least Spark 4.0.0.

Currently, only batch read is implemented.

### Example

```shell
pip install pyspark-pi
```

```python
from pispark_pi import PiDataSource

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(PiDataSource)

connection_info = {
    "host": "https://example.com",
    "authMethod": "basic",
    "username": "username",
    "password": "password",
    "server": "name_of_data_archive_server"
}

df = spark.read.format("pi").options(
    **connection_info,
    requestType="recorded",
    startTime="1970-01-01T00:00:00Z",
    endTime="1970-01-02T00:00:00Z"
).load("a.pi.point.name")

# Or, for more than one point:

points = ["a.pi.point.name", "another.pi.point.name"]

.load(points)
```

Either a single point name or a list of multiple point names can be provided to load. 

All points in a single read call must be of the same PointType.

Currently, only retrieves string values for Digital pointTypes.

Additional options can be defined using the option/options functions.

# Options

Most options line up exactly with the query parameters passed to the get recorded, get interpolated, and get summary endpoints of the web API. Additional options are also defined for connection details, authentication, selecting which type of request to send, and working with the web API's rate limit (elaborated on below).

### All Options

| Option | Description | Possible Values | Default Value |
| ------ | ----------- | --------------- | ------------- |
| host | The url of the web API server. | n/a | None |
| authMethod | The authentication method to use. | anonymous, basic, kerberos, bearer | None |
| username | The username to use when basic auth is selected. | n/a | None |
| password | The password to use when basic auth is selected. | n/a | None |
| token | The token to use when bearer auth is selected. | n/a | None |
| verify | Whether or not to verify SSL certificates. | true, false | true |
| server | The name of the data archive server to use. | n/a | None |
| requestType | The type of request to send. | recorded, interpolated, summary | None |
| startTime | The start time of the window for which to retrieve data. | n/a | None |
| endTime | The end time of the window for which to retrieve data. | n/a | None |
| desiredUnits | The name or abbreviation of the desired units of measure for the returned value. | n/a | None |
| filterExpression | A string containing a filter expression. | n/a | None |
| includeFilteredValues | Specify 'true' to indicate that values which fail the filter criteria are present in the returned data. | true, false | false |
| timezone | The time zone with which the time strings will be interpreted. | n/a | None |
| maxCount | The maximum number of values to be returned. This is the only option for which I have changed the default value from what's default with the web API normally. The normal default is 1000. | n/a | 150000 |
| boundaryType | An optional value that determines how the times and values of the returned end points are determined. | inside, outside, interpolated | inside |
| syncTime | An optional start time anchor, in AFTime format. | n/a | None |
| syncTimeBoundaryType | An optional string specifying the boundary type to use when applying a syncTime. | inside, outside | inside |
| interval | The sampling interval, in AFTimeSpan format. | n/a | 1h |
| summaryType | Specifies the kind of summary to produce over the range. | total, average, minimum, maximum, range, stddev, populationstddev, count, percentgood | total |
| calculationBasis | Specifies the method of evaluating the data over the time range. | timeweighted, eventweighted, timeweightedcontinuous, timeweighteddiscrete, eventweightedexcludemostrecentevent, eventweightedexcludeearliestevent, eventweightedincludebothends | timeweighted |
| timeType | Specifies how to calculate the timestamp for each interval. | auto, earliesttime, mostrecenttime, | auto |
| summaryDuration | The duration of each summary interval. | n/a | None |
| sampleType | Defines the evaluation of an expression over a time range. | expressionrecordedvalues, interval | expressionrecordedvalues |
| sampleInterval | When the sampleType is Interval, sampleInterval specifies how often the filter expression is evaluated when computing the summary for an interval. | n/a | None |
| rateLimitDuration | A duration of time, in seconds, in which a client (IP address) is limited to a specific number of requests. | n/a | 1 |
| rateLimitMaxRequests | Maximum number of requests per client (IP address) over a specified duration of time. | n/a | 1000 |
| maxReturnedItemsPerCall | Each web request returns a single item or collection of items; this property limits the maximum number of returned items; it affects the Stream and StreamSet controllers, as well as any action for any controller that contains a maxCount URL parameter. | n/a | 150000 |

# Schema Inference

Unlike many other Spark data sources, when this "infers" the schema, it's actually just asking Pi, "What are the PointTypes for these points?" So the "inferred" schema will always be correct. Additionally, when the schema is not inferred, that exact same request still needs to be sent anyway because the same request that gets the point types also gets the web IDs, which are needed later.

For both of these reasons, I recommend just using schema inference unless you're concerned that the points might not have been configured as you'd expect. You can still pass a schema if you'd like, though; it needs to match this:

```python
schema = StructType([
    StructField("point", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("value", <Pyspark type matching whatever the PointType is>, nullable=True),
    StructField("units_abbreviation", StringType(), nullable=True),
    StructField("good", BooleanType(), nullable=True),
    StructField("questionable", BooleanType(), nullable=True),
    StructField("substituted", BooleanType(), nullable=True),
    StructField("annotated", BooleanType(), nullable=True),
])
```

There's also a helper function which will return the above schema given whatever type you expect as an argument:

```python
from pyspark_pi import result_schema

schema = result_schema(str)

# Or:

from pyspark.sql.types import StringType

schema = result_schema(StringType())
```

# The API's Rate Limit

The Pi Web API has a per-user rate limit which is configurable using the configuration properties `RateLimitDuration`, `RateLimitMaxRequests`, and `MaxReturnedValuesPerCall`. There are also separate limits for search requests, but these shouldn't ever be hit by this connector.

There are three options which correspond to these configuration properties. They have the same defaults as the web API, but if these values have been changed on your API instance, you'll need to adjust them here too. 

It schedules when requests will be sent out before dispatching tasks so that you can request any abitrary amount of data with a single read call and you shouldn't hit the rate limit, as long as the options you set match how your web API instance is configured. Even still, it isn't completely inconceivable that network delays, slow downs of the python processes, discrepancies between computer clocks on the executors, etc. might cause more than one request to land within `RateLimitDuration`, causing you to hit the rate limit. If this happens, I recommend adjusting the options to make the connector think that the rate limit is less permissive than it actually is. As a start, I recommend halving `RateLimitMaxRequests` to `500` so that two requests can be sent simultaneously without triggering the rate limit.

# Time String SerDe

**Year and month intervals are not currently supported.**

Timestamps and intervals are passed using Pi's "Pi Time Syntax." These strings are not included in the web requests sent to the API directly. Instead, the connector deserializes and then reserializes the value. It's done this way because it needs to actually understand the what the time span is in order to split the overall request into appropriately-sized input partitions. This parsing is done using a set of regexs, as well as `dateutil.parser` as a fallback for timestamps:

Those regexs don't cover all edge cases perfectly. If you find yourself passing a time string that you think should be correct and you're getting an error saying it's invalid, or worse - getting an incorrect result, try formatting it in a different way. The format allows for multiple different ways of representing the same time span.

# Relevant Aveva Documentation

https://docs.aveva.com/bundle/pi-web-api/page/1023024.html  
https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/stream/actions/getrecorded.html  
https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/stream/actions/getinterpolated.html  
https://docs.aveva.com/bundle/pi-web-api-reference/page/help/controllers/stream/actions/getsummary.html  
https://docs.aveva.com/bundle/pi-web-api/page/1023116.html  
https://docs.aveva.com/bundle/pi-web-api-reference/page/help/topics/time-strings.html  
https://docs.aveva.com/bundle/af-sdk/page/html/T_OSIsoft_AF_Time_AFTimeSpan.htm

# To-Do:

- Write Batch
- Read Stream
- Write Stream
- All types of data source push down (would require a rewrite in a JVM language if those features aren't added to the Python API)
- A second connector for Pi Asset Framework
- Year and month time spans