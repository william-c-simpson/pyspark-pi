import urllib3
import packaging.version

import pyspark
from pyspark_pi.data_source import PiDataSource
from pyspark_pi.helpers import result_schema

__all__ = ["PiDataSource", "result_schema"]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if packaging.version.parse(pyspark.__version__) < packaging.version.parse("4.0.0"):
    raise ImportError(
        "pyspark-pi requires PySpark 4.0.0 or later. "
    )