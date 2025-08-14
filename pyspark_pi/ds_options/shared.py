from enum import Enum
from typing import Any, Type, TypeVar, overload, Literal
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import json

from pyspark_pi import pi, errors

class _BaseOptionEnum(Enum):
    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_
    
# TODO: This could be more robust
def parse_paths(options: dict[str, str]) -> list[pi.Path]:
    """
    Parses a string formatted like: ["thing1","thing2","thing3"]
    """
    server = _parse_str(options, "server")
    if "path" in options:
        path = pi.Path(options["path"])
        paths = [path]
    elif "paths" in options:
        paths = json.loads(options["paths"])
        paths = [pi.Path(p) for p in paths]
    else:
        raise errors.PiDataSourceConfigError("A list of point names must be provided using the load() method on the reader.")

    if server:
        for path in paths:
            path.server = server
        return paths
    
    for path in paths:
        if not path.server:
            raise errors.PiDataSourceConfigError("If no server is specified using the 'server' option, all paths must include a server.")

    return paths

@overload
def _parse_str(options: dict[str, str], key: str, *, required: Literal[True], default: str | None = ...) -> str: ...
@overload
def _parse_str(options: dict[str, str], key: str, *, required: bool = ..., default: str) -> str: ...
@overload
def _parse_str(options: dict[str, str], key: str, *, required: bool = ..., default: None = ...) -> str | None: ...

def _parse_str(options: dict[str, str], key: str, *, required: bool = False, default: str | None = None) -> str | None:
    if required and key not in options:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    val = options.get(key, default)
    if val is None:
        return None
    return val.strip()

@overload
def _parse_bool(options: dict[str, str], key: str, *, required: Literal[True], default: str | None = ...) -> bool: ...
@overload
def _parse_bool(options: dict[str, str], key: str, *, required: bool = ..., default: str) -> bool: ...
@overload
def _parse_bool(options: dict[str, str], key: str, *, required: bool = ..., default: None = ...) -> bool | None: ...

def _parse_bool(options: dict[str, str], key: str, *, required: bool = False, default: str | None = "false") -> bool | None:
    if required and key not in options:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    raw = options.get(key, default)
    if raw is None:
        return None
    val = str(raw).strip().lower()
    if val not in {"true", "false"}:
        raise errors.PiDataSourceConfigError(f"Invalid boolean value for {key}: {val}")
    return val == "true"

T = TypeVar("T", bound=_BaseOptionEnum)
@overload
def _parse_enum(options: dict[str, str], key: str, enum_type: Type[T], *, required: Literal[True], default: str | None = ...) -> T: ...
@overload
def _parse_enum(options: dict[str, str], key: str, enum_type: Type[T], *, required: bool = ..., default: str) -> T: ...
@overload
def _parse_enum(options: dict[str, str], key: str, enum_type: Type[T], *, required: bool = ..., default: None = ...) -> T | None: ...

def _parse_enum(options: dict[str, str], key: str, enum_type: Type[T], *, required: bool = False, default: str | None = None) -> T | None:
    if required and key not in options:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    raw = options.get(key, default)
    if raw is None:
        return None
    val = str(raw).strip().lower()
    if not enum_type.has_value(val):
        raise errors.PiDataSourceConfigError(f"Invalid {key} '{val}' specified.")
    return enum_type(val)

@overload
def _parse_datetime(options: dict[str, str], key: str, *, required: Literal[True], default: str | None = ...) -> datetime: ...
@overload
def _parse_datetime(options: dict[str, str], key: str, *, required: bool = ..., default: str) -> datetime: ...
@overload
def _parse_datetime(options: dict[str, str], key: str, *, required: bool = ..., default: None = ...) -> datetime | None: ...

def _parse_datetime(options: dict[str, str], key: str, *, required: bool = False, default: str | None = None) -> datetime | None:
    if required and key not in options:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    raw = options.get(key, default)
    if raw is None:
        return None
    try:
        return pi.deserialize_pi_time(raw)
    except ValueError:
        raise errors.PiDataSourceConfigError(f"Invalid datetime format for {key}: {raw}")

@overload
def _parse_positive_int(options: dict[str, str], key: str, *, required: Literal[True], default: str | None = ...) -> int: ...
@overload
def _parse_positive_int(options: dict[str, str], key: str, *, required: bool = ..., default: str) -> int: ...
@overload
def _parse_positive_int(options: dict[str, str], key: str, *, required: bool = ..., default: None = ...) -> int | None: ...

def _parse_positive_int(options: dict[str, str], key: str, *, required: bool = False, default: str | None = None) -> int | None:
    if required and key not in options:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    val = options.get(key, default)
    if val is None:
        return None
    try:
        val = int(val)
    except ValueError:
        raise errors.PiDataSourceConfigError(f"Invalid integer for {key}: {val}")
    if val <= 0:
        raise errors.PiDataSourceConfigError(f"{key} must be a positive integer.")
    return val

@overload
def _parse_timedelta(options: dict[str, str], key: str, *, required: Literal[True], default: str | None = ...) -> timedelta: ...
@overload
def _parse_timedelta(options: dict[str, str], key: str, *, required: bool = ..., default: str) -> timedelta: ...
@overload
def _parse_timedelta(options: dict[str, str], key: str, *, required: bool = ..., default: None = ...) -> timedelta | None: ...

def _parse_timedelta(options: dict[str, str], key: str, required: bool = False, default: str | None = None) -> timedelta | None:
    if required and key not in options:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    val = options.get(key, default)
    if val is None:
        return None
    try:
        return pi.deserialize_pi_interval(val)
    except ValueError as e:
        raise errors.PiDataSourceConfigError(f"Invalid timedelta format for {key}: {val}. This could be an issue with the des function in the library, not your input. Double-check your input and try formatting it in a different way.") from e

@overload
def _parse_timezone(options: dict[str, str], key: str, *, required: Literal[True], default: str | None = ...) -> ZoneInfo: ...
@overload
def _parse_timezone(options: dict[str, str], key: str, *, required: bool = ..., default: str) -> ZoneInfo: ...
@overload
def _parse_timezone(options: dict[str, str], key: str, *, required: bool = ..., default: None = ...) -> ZoneInfo | None: ...

def _parse_timezone(options: dict[str, str], key: str, required: bool = False, default: str | None = "UTC") -> ZoneInfo | None:
    if required and key not in options:
        raise errors.PiDataSourceConfigError(f"{key} must be specified.")
    val = options.get(key, default)
    if val is None:
        return None
    try:
        return ZoneInfo(val)
    except ZoneInfoNotFoundError:
        raise errors.PiDataSourceConfigError(f"Invalid timezone specified for {key}: {val}")
