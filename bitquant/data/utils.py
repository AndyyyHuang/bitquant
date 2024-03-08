import pandas as pd
from datetime import datetime, timedelta
import logging
from loguru import logger as logu
from datetime import datetime, timedelta, timezone

class TimeManager:

    @staticmethod
    def now_in_ms() -> int:
        return int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000)

    @staticmethod
    def str_to_timestamp(dt_str:str, format: str) -> datetime:
        return datetime.strptime(dt_str, format)
    @staticmethod
    def timestamp_to_ms(dt) -> int:
        return int(dt.timestamp() * 1000)
    @staticmethod
    def dt_str_to_ms(dt_str:str, format: str):
        return int(datetime.strptime(dt_str, format).timestamp() * 1000)

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




def preprocess_data(bar, st="2023-06-01", et="2025-01-01"):
    bar = bar.loc[(bar.index.get_level_values(0) > pd.to_datetime(st, utc=True)) & (bar.index.get_level_values(0) <= pd.to_datetime(et, utc=True))]
    bar['return_1'] = (bar['close'].unstack().diff(1).shift(-1) / bar['close'].unstack()).stack()
    bar = bar.unstack().iloc[:-1].stack()
    symbol_lis = bar.return_1.unstack().isna().sum(axis=0)[bar.return_1.unstack().isna().sum(axis=0) == 0].index.tolist()
    missing_symbol = list(set(bar.index.get_level_values(1).unique()).difference(set(symbol_lis)))
    if len(missing_symbol) == 0:
        pass
    else:
        print('There are uncompleted data in these symbols: ', list(set(bar.index.get_level_values(1).unique()).difference(set(symbol_lis))))

    bar = bar.loc[(slice(None), symbol_lis), :]
    return bar


def get_delta_time(s: str) -> datetime:
    # Extract the number and the time unit from the string
    num = int(s[:-1])
    unit = s[-1]

    # Convert the string into a timedelta object
    if unit == 'h':  # hours
        delta = timedelta(hours=num)
    elif unit == 'm':  # minutes
        delta = timedelta(minutes=num)
    elif unit == 's':  # seconds
        delta = timedelta(seconds=num)
    elif unit == 'd':  # days
        delta = timedelta(days=num)
    else:
        raise ValueError(f"Unknown time unit: {unit}")

    # Add the timedelta to the current timestamp
    return delta


def my_logger(log_type,filename=None):
    if log_type=="console":
        logger = logging.getLogger('influxdb_save')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # 创建一个handler，用于输出控制台，并且设定严重级别
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    else:
        logu.add(filename, retention='30 days')
        logger=logu
    return logger