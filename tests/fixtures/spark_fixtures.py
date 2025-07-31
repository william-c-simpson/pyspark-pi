import pytest
from pyspark.sql import SparkSession

from pyspark_pi import PiDataSource

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder \
        .appName("PiDataSourceIntegrationTest") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    spark.dataSource.register(PiDataSource)
    return spark