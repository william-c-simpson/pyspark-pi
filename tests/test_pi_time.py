import pytest
from datetime import timedelta, datetime, timezone

from pyspark_pi import pi_time

_NOW = datetime.now()
_TODAY = _NOW.replace(hour=0, minute=0, second=0, microsecond=0)
_YESTERDAY = _TODAY - timedelta(days=1)

@pytest.mark.parametrize("input_str, expected", [
    ("*", _NOW),
    ("", _NOW),
    ("T", _TODAY),
    ("Today", _TODAY),
    ("Y", _YESTERDAY),
    ("Yesterday", _YESTERDAY),
    ("tue", _TODAY - timedelta(days=(_TODAY.weekday() - 1) % 7)),
    ("mon", _TODAY - timedelta(days=(_TODAY.weekday() - 0) % 7)),
    ("wed", _TODAY - timedelta(days=(_TODAY.weekday() - 2) % 7)),
    ("thursday", _TODAY - timedelta(days=(_TODAY.weekday() - 3) % 7)),
    ("jan 1", datetime(_TODAY.year, 1, 1)),
    ("dec 25", datetime(_TODAY.year, 12, 25)),
    ("march 15", datetime(_TODAY.year, 3, 15)),
    ("12/25", datetime(_TODAY.year, 12, 25)),
    ("12-25", datetime(_TODAY.year, 12, 25)),
    ("2020", datetime(2020, _TODAY.month, _TODAY.day)),
    ("1", datetime(_TODAY.year, _TODAY.month, 1)),
    ("15", datetime(_TODAY.year, _TODAY.month, 15)),
    ("2023-08-01T13:45:00Z", datetime(2023, 8, 1, 13, 45, 0, tzinfo=timezone.utc)),
    ("01:00", _TODAY + timedelta(hours=1)),
    ("12:34:56", _TODAY + timedelta(hours=12, minutes=34, seconds=56)),
    ("23:59:59.999", _TODAY + timedelta(hours=23, minutes=59, seconds=59, milliseconds=999)),
    ("T+1h", _TODAY + timedelta(hours=1)),
    ("T-30m", _TODAY - timedelta(minutes=30)),
    ("Yesterday+1h", _YESTERDAY + timedelta(hours=1)),
    ("2020-01-01T00:00:00Z+2h", datetime(2020, 1, 1, 2, 0, 0, tzinfo=timezone.utc)),
    ("12/25+30m", datetime(_TODAY.year, 12, 25) + timedelta(minutes=30)),
    ("12/25-15m", datetime(_TODAY.year, 12, 25) - timedelta(minutes=15)),
    ("1+45m", datetime(_TODAY.year, _TODAY.month, 1) + timedelta(minutes=45)),
    ("15-15m", datetime(_TODAY.year, _TODAY.month, 15) - timedelta(minutes=15)),
    ("2023-08-01T13:45:00+00:00+15m", datetime(2023, 8, 1, 14, 0, 0, tzinfo=timezone.utc)),
    ("2023-08-01T13:45:00-05:00+1h", datetime(2023, 8, 1, 14, 45, 0, tzinfo=timezone(timedelta(hours=-5)))),
    ("mon+1h", (_TODAY - timedelta(days=(_TODAY.weekday() - 0) % 7)) + timedelta(hours=1)),
    ("wed-2h", (_TODAY - timedelta(days=(_TODAY.weekday() - 2) % 7)) - timedelta(hours=2)),
    ("T+1h+30m", _TODAY + timedelta(hours=1, minutes=30)),
    ("T-1h-15m", _TODAY - timedelta(hours=1, minutes=15)),
    ("T+1h-30m", _TODAY + timedelta(minutes=30)),
    ("T-1h+30m", _TODAY - timedelta(minutes=30)),
    ("2023-08-01T00:00:00Z+1h+15m+30s", datetime(2023, 8, 1, 1, 15, 30, tzinfo=timezone.utc)),
    ("2023-08-01T00:00:00Z-1h-15m-30s", datetime(2023, 7, 31, 22, 44, 30, tzinfo=timezone.utc)),
    ("01:00+15m", _TODAY + timedelta(hours=1, minutes=15)),
    ("12:34:56-34m", _TODAY + timedelta(hours=12, minutes=0, seconds=56)),
    ("*+1h", _NOW + timedelta(hours=1)),
    ("*-25m", _NOW - timedelta(minutes=25)),
])
def test_deserialize_pi_time(input_str, expected):
    assert abs(pi_time.deserialize_pi_time(input_str) - expected) < timedelta(seconds=1)

@pytest.mark.parametrize("invalid_input", [
    "now",
    "tuesdayish",
    "foo",
    "2023-13-01",
    "2023-12-32",
    "2023-08-01T25:00:00Z",
    "monkey",
    "december 32",
    "15:99",
    "9999-99-99",
    "T++1h",
    "T+-",
    "+1h",
    "2023-08-01T13:45:00Z++1h",
    "T+1hour",
    "1.5+1.5",
    "12/25+",
    "12/25-+30m",
    "T+1h+",
    "T-1h-",
])
def test_deserialize_invalid_pi_time(invalid_input):
    with pytest.raises(ValueError):
        print(f"Testing invalid input: {invalid_input}")
        print(pi_time.deserialize_pi_time(invalid_input))

@pytest.mark.parametrize("input_str, expected", [
    ("", _NOW),
    ("*", _NOW),
    ("T", _TODAY),
    ("Today", _TODAY),
    ("Y", _YESTERDAY),
    ("Yesterday", _YESTERDAY),
    ("tue", _TODAY - timedelta(days=(_TODAY.weekday() - 1) % 7)),
    ("mon", _TODAY - timedelta(days=(_TODAY.weekday() - 0) % 7)),
    ("wed", _TODAY - timedelta(days=(_TODAY.weekday() - 2) % 7)),
    ("thursday", _TODAY - timedelta(days=(_TODAY.weekday() - 3) % 7)),
    ("jan 1", datetime(_TODAY.year, 1, 1)),
    ("dec 25", datetime(_TODAY.year, 12, 25)),
    ("march 15", datetime(_TODAY.year, 3, 15)),
    ("12/25", datetime(_TODAY.year, 12, 25)),
    ("12-25", datetime(_TODAY.year, 12, 25)),
    ("2020", datetime(2020, _TODAY.month, _TODAY.day)),
    ("1", datetime(_TODAY.year, _TODAY.month, 1)),
    ("15", datetime(_TODAY.year, _TODAY.month, 15)),
    ("2023-08-01T13:45:00Z", datetime(2023, 8, 1, 13, 45, 0, tzinfo=timezone.utc)),
    ("01:00", _TODAY + timedelta(hours=1)),
    ("12:34:56", _TODAY + timedelta(hours=12, minutes=34, seconds=56)),
    ("23:59:59.999", _TODAY + timedelta(hours=23, minutes=59, seconds=59, milliseconds=999)),
])
def test_deserialize_timestamp(input_str, expected):
    print(f"Testing input: {input_str}, expected: {expected}")
    print(f"Deserialized: {pi_time.deserialize_timestamp(input_str)}")
    assert abs(pi_time.deserialize_timestamp(input_str) - expected) < timedelta(seconds=1)

@pytest.mark.parametrize("invalid_input", [
    "now",
    "todayish",
    "foo",
    "2023-13-01",
    "2023-12-32",
    "2023-08-01T25:00:00Z",
    "monkey",
    "december 32",
    "15:99",
    "9999-99-99",
])
def test_deserialize_invalid_timestamp(invalid_input):
    with pytest.raises(ValueError):
        print(f"Testing invalid input: {invalid_input}", pi_time.deserialize_timestamp(invalid_input))


@pytest.mark.parametrize("input_str, expected", [
    ("1:02:03.456", timedelta(hours=1, minutes=2, seconds=3.456)),
    ("2:03", timedelta(hours=2, minutes=3)),
    ("5", timedelta(hours=5)),
    ("1.5h", timedelta(hours=1.5)),
    ("2h30m", timedelta(hours=2, minutes=30)),
    ("+1h-30m", timedelta(minutes=30)),
    ("1h+15m+30s", timedelta(hours=1, minutes=15, seconds=30)),
    ("-1h", timedelta(hours=-1)),
    ("1.25h", timedelta(hours=1.25)),
    ("30m", timedelta(minutes=30)),
    ("45s", timedelta(seconds=45)),
    ("100f", timedelta(milliseconds=100)),
    ("+1h+30m", timedelta(hours=1, minutes=30)),
    ("-2h+90m", timedelta(minutes=-30)),
    ("3h0m", timedelta(hours=3)),
    ("0h+45m", timedelta(minutes=45)),
    ("+1.5h+0.5h", timedelta(hours=2)),
    ("1:2:3", timedelta(hours=1, minutes=2, seconds=3)),
    ("00:00:00", timedelta(0)),
    ("1m30s", timedelta(minutes=1, seconds=30)),
    ("2h15s", timedelta(hours=2, seconds=15)),
    ("0f", timedelta(0)),
])
def test_deserialize_interval(input_str, expected):
    assert abs(pi_time.deserialize_interval(input_str) - expected) < timedelta(milliseconds=1)

@pytest.mark.parametrize("td, expected", [
    (timedelta(hours=1, minutes=2, seconds=3.456), "1h+2m+3s+456f"),
    (timedelta(hours=2, minutes=30), "2h+30m"),
    (timedelta(minutes=45), "45m"),
    (timedelta(seconds=10), "10s"),
    (timedelta(milliseconds=250), "250f"),
    (timedelta(seconds=0), "0s"),
    (timedelta(hours=-1, minutes=-15), "-1h-15m"),
    (timedelta(minutes=1, seconds=30), "1m+30s"),
    (timedelta(hours=1.5), "1h+30m"),
    (timedelta(milliseconds=1), "1f"),
    (timedelta(seconds=0.001), "1f"),
    (timedelta(seconds=0.999), "999f"),
    (timedelta(seconds=1.999), "1s+999f"),
])
def test_serialize_interval(td, expected):
    assert pi_time.serialize_interval(td) == expected

@pytest.mark.parametrize("invalid_input", [
    "1y", 
    "2M", 
    "abc", 
    "1x", 
    "1hour", 
    "", 
    ":", 
    "1::", 
    "1:2:3:4",
    "1..5h",
    "1.5.5h",
    "1hh",
    "+-1h",
    "1h30",
    "h",
    "1h+",
    "++1h",
    "--30m",
    "1h 30x",
    "1.5"
])
def test_deserialize_invalid_interval(invalid_input):
    with pytest.raises(ValueError):
        pi_time.deserialize_interval(invalid_input)
