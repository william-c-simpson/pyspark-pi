"""
https://docs.aveva.com/bundle/pi-web-api-reference/page/help/topics/time-strings.html
"""

from datetime import datetime, timedelta
import dateutil
import re

# ( >︹<)
_TIMESTAMP_ABBREVIATION_RE = re.compile(
    r'''
    ^\s*(
        (?P<star>\*)
        |
        (?P<today>[Tt]oday|[Tt])
        |
        (?P<yesterday>[Yy]esterday|[Yy])
        |
        (?P<weekday>(mon|tue|wed|thu|fri|sat|sun)(day|sday|nesday|rsday|urday)?)
        |
        (?P<monthday>
            (?P<month>(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(uary|ch|il|e|ust|ember)?)
            \s+
            (?P<md_day>\d{1,2})
        )
        |
        (?P<md>\d{1,2}[-/]\d{1,2})
        |
        (?P<year>\d{4})
        |
        (?P<day>\d{1,2})
    )\s*$
    ''',
    re.IGNORECASE | re.VERBOSE
)

_INTERVAL_SEGMENT_FORMAT_RE = re.compile(
    r'(?P<sign>[+-])?(?P<value>\d+(\.\d+)?)\s*(?P<unit>[yMwdhmsf])(?=[^a-zA-Z]|\Z|\s*)',
    re.IGNORECASE
)

_INTERVAL_COLON_FORMAT_RE = re.compile(
    r'^(?P<h>\d+)(:(?P<m>\d+))?(:(?P<s>\d+(\.\d+)?))?$'
)

_UNIT_TO_SECONDS = {
    'w': 604800,
    'd': 86400,
    'h': 3600,
    'm': 60,
    's': 1,
    'f': 0.001,
}

_UNSUPPORTED_UNITS = {'y', 'M'}

def deserialize_pi_time(value: str) -> datetime:
    """
    Deserialize a full Pi Time string with both a timstamp and an optional interval.
    """
    value = value.strip()
    for i in range(1, len(value) - 1):
        if value[i] in '+-':
            if value[i-1] in "+-" or value[i+1] in "+-":
                raise ValueError(f"Invalid Pi Time format: {value}")
            
            left = value[:i]
            right = value[i+1:]
            try:
                timestamp = deserialize_timestamp(left)
                if value[i] == '+':
                    interval = deserialize_interval(right)
                else:
                    interval = deserialize_interval(f"-{right}")
                return timestamp + interval
            except Exception:
                continue

    return deserialize_timestamp(value)

def deserialize_timestamp(value: str) -> datetime:
    if value == "":
        return datetime.now()
    match = _TIMESTAMP_ABBREVIATION_RE.match(value.strip())
    if not match:
        try:
            return dateutil.parser.parse(value.strip())
        except ValueError:
            raise ValueError(f"Invalid timestamp format: {value}")
    elif match.group("star"):
        return datetime.now()
    elif match.group("today"):
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    elif match.group("yesterday"):
        return (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif match.group("weekday"):
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        weekday = match.group("weekday").lower()[:3]
        target_weekday = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"].index(weekday)
        days_ago = (today.weekday() - target_weekday) % 7
        return today - timedelta(days=days_ago)
    elif match.group("monthday"):
        month = match.group("month").lower()[:3]
        target_month = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"].index(month) + 1
        day = int(match.group("md_day"))
        now = datetime.now()
        return datetime(now.year, target_month, day, 0, 0, 0, 0)
    elif match.group("md"):
        month, day = map(int, re.split(r'[-/]', match.group("md")))
        now = datetime.now()
        return datetime(now.year, month, day, 0, 0, 0, 0)
    elif match.group("year"):
        now = datetime.now()
        year = int(match.group("year"))
        return datetime(year, now.month, now.day, 0, 0, 0, 0)
    elif match.group("day"):
        day = int(match.group("day"))
        now = datetime.now()
        return datetime(now.year, now.month, day, 0, 0, 0, 0)
    else:
        raise ValueError(f"Invalid timestamp format: {value}")

def deserialize_interval(value: str) -> timedelta:
    value = value.strip()

    match = _INTERVAL_COLON_FORMAT_RE.match(value)
    if match:
        h = int(match.group("h") or 0)
        m = int(match.group("m") or 0)
        s = float(match.group("s") or 0)
        return timedelta(hours=h, minutes=m, seconds=s)

    matches = list(re.finditer(_INTERVAL_SEGMENT_FORMAT_RE, value))
    if not matches:
        raise ValueError(f"Invalid AFTimeSpan: {value}")
    matches_as_str = "".join(match.group(0) for match in matches)
    if matches_as_str != value:
        raise ValueError(f"Invalid AFTimeSpan: {value}")

    seconds = 0.0
    for part in matches:
        sign = -1 if part.group("sign") == "-" else 1
        num = float(part.group("value"))
        unit = part.group("unit")

        if unit in _UNSUPPORTED_UNITS:
            raise ValueError(f"year and month units are currently not supported: {unit}")
        if unit not in _UNIT_TO_SECONDS:
            raise ValueError(f"Unknown unit: {unit}")

        seconds += sign * num * _UNIT_TO_SECONDS[unit]

    return timedelta(seconds=seconds)

def serialize_interval(td: timedelta) -> str:
    total_seconds = td.total_seconds()
    if total_seconds < 0:
        sign = "-"
        positive = False
    else:
        sign = ""
        positive = True
    total_seconds = abs(total_seconds)

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = (seconds % 1) * 1000
    seconds = int(seconds)

    parts = []
    if hours:
        parts.append(f"{sign}{int(hours)}h")
        if positive: 
            sign = "+"
    if minutes:
        parts.append(f"{sign}{int(minutes)}m")
        if positive: 
            sign = "+"
    if seconds:
        parts.append(f"{sign}{seconds}s")
        if positive: 
            sign = "+"
    if milliseconds:
        parts.append(f"{sign}{int(milliseconds)}f")

    if not parts:
        parts.append("0s")

    return "".join(parts)