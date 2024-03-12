from datetime import datetime, timedelta, timezone

class TimeUtils:

    @staticmethod
    def now_in_ms() -> int:
        return int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000)

    @staticmethod
    def str_to_timestamp(dt_str: str, format: str) -> datetime:
        return datetime.strptime(dt_str, format)
    @staticmethod
    def timestamp_to_ms(dt: datetime) -> int:
        return int(dt.timestamp() * 1000)
    @staticmethod
    def dt_str_to_ms(dt_str: str, format: str):
        return int(datetime.strptime(dt_str, format).replace(tzinfo=timezone.utc).timestamp() * 1000)

    @staticmethod
    def ms_to_timestamp(ms: int) -> datetime:
        return datetime.utcfromtimestamp(ms / 1000).replace(tzinfo=timezone.utc)

    @staticmethod
    def str_to_timedelta(interval_str: str) -> timedelta:
        units = {'s': 'seconds', 'm': 'minutes', 'h': 'hours', 'd': 'days'}
        value = int(interval_str[:-1])
        unit = interval_str[-1]
        if unit in units:
            return timedelta(**{units[unit]: value})
        else:
            raise ValueError(f"Unknown time unit: {unit}")
    @staticmethod
    def timedelta_to_ms(delta: timedelta) -> int:
        return int(delta.total_seconds() * 1000)

    @staticmethod
    def interval_str_to_ms(interval_str: str) -> int:
        units = {'s': 'seconds', 'm': 'minutes', 'h': 'hours', 'd': 'days'}
        value = int(interval_str[:-1])
        unit = interval_str[-1]
        if unit in units:
            delta = timedelta(**{units[unit]: value})
        else:
            raise ValueError(f"Unknown time unit: {unit}")

        return int(delta.total_seconds() * 1000)

    @staticmethod
    def ms_to_timedelta(ms: int) -> timedelta:
        return timedelta(milliseconds=ms)
