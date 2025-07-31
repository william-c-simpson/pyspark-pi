import os
from datetime import datetime, timedelta, timezone
import random
from typing import Any

import requests
import pytest

from pyspark_pi import pi

@pytest.fixture(scope="session")
def connection_info() -> dict[str, str]:
    host = os.getenv("PI_WEB_API_HOST")
    username = os.getenv("PI_WEB_API_USERNAME")
    password = os.getenv("PI_WEB_API_PASSWORD")
    server = os.getenv("PI_WEB_API_SERVER")

    if not all([host, username, password, server]):
        pytest.skip("Missing PI connection env vars")

    return {
        "host": host,
        "authMethod": "basic",
        "username": username,
        "password": password,
        "verify": "false",
        "server": server
    }

@pytest.fixture(
    scope="session",
    params=[
        (int, pi.PointType.INT32, "PI_WEB_API_INT_POINT_NAME"),
        (float, pi.PointType.FLOAT32, "PI_WEB_API_FLOAT_POINT_NAME"),
        (str, pi.PointType.STRING, "PI_WEB_API_STRING_POINT_NAME"),
        (datetime, pi.PointType.TIMESTAMP, "PI_WEB_API_TIMESTAMP_POINT_NAME"),
        (str, pi.PointType.DIGITAL, "PI_WEB_API_DIGITAL_POINT_NAME")
    ],
    ids=["int", "float", "string", "timestamp", "digital"]
)
def test_data(request, connection_info) -> tuple[type, str, list[dict[str, Any]]]:
    data_type, point_type, env_var = request.param
    point_name = os.getenv(env_var)
    if not point_name:
        pytest.skip(f"Missing env var: {env_var}")

    server_id = _get_server_id(connection_info["host"], connection_info["server"], connection_info["username"], connection_info["password"])
    point_id = _get_point_id(server_id, point_name, connection_info["host"], connection_info["username"], connection_info["password"])

    data = _generate_data(point_type)
    _write_point_data(point_id, data, connection_info["host"], connection_info["username"], connection_info["password"])

    yield data_type, point_name, data

    _delete_point_data(point_id, data, connection_info["host"], connection_info["username"], connection_info["password"])



def _get_server_id(host: str, server: str, username: str, password: str) -> str:
    url = f"{host}/piwebapi/dataservers?name={server}"
    response = requests.get(url, auth=(username, password), verify=False)
    response.raise_for_status()
    content = response.json()
    id = content.get("WebId")
    if not id:
        raise ValueError(f"Server '{server}' not found.")
    return id

def _get_point_id(server_id: str, point_name: str, host: str, username: str, password: str) -> str:
    url = f"{host}/piwebapi/points/search?dataServerWebId={server_id}&query=tag:=%22{point_name}%22"
    response = requests.get(url, auth=(username, password), verify=False)
    response.raise_for_status()
    content = response.json()
    items = content.get("Items", [])
    if not items:
        raise ValueError(f"Point '{point_name}' not found.")
    return items[0].get("WebId")

def _write_point_data(point_id: str, data: list[dict[str, Any]], host: str, username: str, password: str):
    url = f"{host}/piwebapi/streams/{point_id}/recorded?bufferOption=DoNotBuffer"
    headers = {"X-Requested-With": "XMLHttpRequest"}
    print(url)
    print(data)
    response = requests.post(
        url, 
        json=data, 
        headers=headers, 
        auth=(username, password),
        verify=False
    )
    response.raise_for_status()

def _delete_point_data(point_id: str, data: list[dict[str, Any]], host: str, username: str, password: str):
    url = f"{host}/piwebapi/streams/{point_id}/recorded?bufferOption=DoNotBuffer&updateOption=Remove"
    response = requests.post(
        url, 
        json=data,
        headers={"X-Requested-With": "XMLHttpRequest"},
        auth=(username, password),
        verify=False
    )
    response.raise_for_status()

def _generate_data(point_type: pi.PointType) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    timestamps = [(datetime(2025, 1, 1) + timedelta(minutes=i)).isoformat() for i in range(10)]

    if point_type == pi.PointType.INT32:
        return [{"Timestamp": ts, "Value": random.randint(0, 100)} for ts in timestamps]
    elif point_type == pi.PointType.FLOAT32:
        return [{"Timestamp": ts, "Value": random.uniform(0.0, 100.0)} for ts in timestamps]
    elif point_type == pi.PointType.STRING:
        return [{"Timestamp": ts, "Value": f"value-{i}"} for i, ts in enumerate(timestamps)]
    elif point_type == pi.PointType.TIMESTAMP:
        return [{"Timestamp": ts, "Value": ts} for ts in timestamps]
    elif point_type == pi.PointType.DIGITAL:
        return [{"Timestamp": ts, "Value": random.choice(["YES", "NO"])} for ts in timestamps]
    raise ValueError(f"Unsupported point_type: {point_type}")