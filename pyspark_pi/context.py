import requests.auth

from pyspark_pi import parse_options, pi

class PiDataSourceContext:
    def __init__(self) -> None:
        self.config: parse_options.PiDataSourceConfig = None
        self.auth: requests.auth.AuthBase = None
        self.params: parse_options.PiDataSourceRequestParams = None
        self.paths: list[str] = None
        self.points: list[pi.Point] = None
        self.point_type: pi.PointType = None
