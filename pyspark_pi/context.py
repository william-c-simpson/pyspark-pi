from __future__ import annotations

import requests.auth

from pyspark_pi import ds_options, pi, auth

class PiDataSourceContext:
    config: 'ds_options.PiDataSourceConfig'
    params: 'ds_options.PiDataSourceRequestParams'
    paths: list['pi.Path']
    auth: requests.auth.AuthBase | None
    points: list['pi.Point'] | None
    point_type: 'pi.PointType | None'

    def __init__(self, config: ds_options.PiDataSourceConfig, params: ds_options.PiDataSourceRequestParams, paths: list[pi.Path]) -> None:
        self.config = config
        self.params = params
        self.paths = paths
        self.auth = auth.create_auth(config)
        self.points = None
        self.point_type = None
